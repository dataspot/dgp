from hashlib import md5

from dataflows import Flow, PackageWrapper, DataStream
from dataflows import load, concatenate, join, set_type, checkpoint,\
                      dump_to_path, add_computed_field, delete_fields,\
                      sort_rows, join_with_self

from .config import Config
from .context import Context
from ..config.consts import RESOURCE_NAME, CONFIG_PRIMARY_KEY
from ..config.log import logger


class BaseEnricher:

    def __init__(self, config: Config):
        self.config = config
        self.prepare()

    def prepare(self):
        pass

    def test(self):
        return False

    def preflow(self):
        return None

    def postflow(self):
        return None


class ColumnTypeTester(BaseEnricher):

    # REQUIRED_COLUMN_TYPES = []
    # PROHIBITED_COLUMN_TYPES = []

    def test(self):
        return True

    def conditional(self):
        raise NotImplementedError

    def postflow(self):

        def func(package: PackageWrapper):
            res = next(filter(lambda res: res.name == RESOURCE_NAME, package.pkg.resources))
            for x in res.descriptor['schema']['fields']:
                assert x.get('columnType'), 'Missing CT for %s' % x
            all_cts = [
                x['columnType']
                for x in res.descriptor['schema']['fields']
            ]
            process = True
            if not all(x in all_cts for x in self.REQUIRED_COLUMN_TYPES):
                process = False
            if any(x in all_cts for x in self.PROHIBITED_COLUMN_TYPES):
                process = False
            if not process:
                yield package.pkg
                yield from package
            else:
                ds = DataStream(package.pkg, package)
                ds = self.conditional().datastream(ds)
                yield ds.dp
                for res in ds.res_iter:
                    yield res.it
        return Flow(func)


class ColumnReplacer(ColumnTypeTester):

    def work(self):
        def func(rows):
            if rows.res.name == RESOURCE_NAME:
                for row in rows:
                    self.operate_on_row(row)
                    yield row
            else:
                yield from rows
        return func

    def conditional(self):
        new_fields = [x.replace(':', '-') for x in self.PROHIBITED_COLUMN_TYPES]
        old_fields = [x.replace(':', '-') for x in self.REQUIRED_COLUMN_TYPES]
        return Flow(
            add_computed_field([dict(
                    target=f,
                    operation='constant',
                ) for f in new_fields],
                resources=RESOURCE_NAME),
            self.work(),
            *[
                set_type(f, columnType=ct)
                for (f, ct) in zip(new_fields, self.PROHIBITED_COLUMN_TYPES)
            ],
            delete_fields(old_fields, resources=RESOURCE_NAME),
        )


def rename_last_resource(name):

    def func(package: PackageWrapper):
        package.pkg.descriptor['resources'][-1]['name'] = name
        num_resources = len(package.pkg.descriptor['resources'])

        yield package.pkg

        for i, res in enumerate(iter(package)):
            if i == (num_resources - 1):
                yield res.it
            else:
                yield res

    return func


class DatapackageJoiner(ColumnTypeTester):

    # REF_DATAPACKAGE = ''
    # REF_KEY_FIELDS = ['']
    # REF_FETCH_FIELDS = ['']
    # SOURCE_KEY_FIELDS = ['']
    # TARGET_FIELD_COLUMNTYPES = ['']

    def prepare(self):
        self.ref_hash = md5(self.REF_DATAPACKAGE.encode('utf8')).hexdigest()
        self.key = self.__class__.__name__

        check = checkpoint(self.ref_hash)
        if not check.exists():
            Flow(load(self.REF_DATAPACKAGE),
                 rename_last_resource(self.ref_hash),
                 dump_to_path('.cache/{}'.format(self.ref_hash)),
                 check).process()
        logger.debug('DONE PREPARING %s', self.key)

    def preflow(self):
        f = Flow(
            load('.cache/{}/datapackage.json'.format(self.ref_hash)),
            concatenate(
                fields=dict(
                    (f, [])
                    for f in self.REF_KEY_FIELDS + self.REF_FETCH_FIELDS
                ),
                target=dict(
                    name=self.key,
                    path=self.key+'.csv'
                ),
                resources=self.ref_hash
            ),
        )
        return f

    def conditional(self):
        target_field_names = [
            ct.replace(':', '-')
            for ct in self.TARGET_FIELD_COLUMNTYPES
        ]
        steps = [
            join(
                self.key, self.REF_KEY_FIELDS,
                RESOURCE_NAME, self.SOURCE_KEY_FIELDS,
                dict(
                    (
                        target_field_name,
                        dict(name=fetch_field)
                    )
                    for target_field_name, fetch_field
                    in zip(target_field_names, self.REF_FETCH_FIELDS)
                )
            ),
        ]
        steps.extend([
            set_type(target_field_name,
                     resources=RESOURCE_NAME,
                     columnType=target_field_columntype)
            for target_field_name, target_field_columntype
            in zip(target_field_names, self.TARGET_FIELD_COLUMNTYPES)
        ])
        f = Flow(*steps)
        return f


class DuplicateRemover(BaseEnricher):

    # ORDER_BY_KEY = ''

    def test(self):
        return True

    def postflow(self):
        key_field_names = [
            ct.replace(':', '-')
            for ct in self.config.get(CONFIG_PRIMARY_KEY)
        ]

        def save_pks(saved_pk):
            def func(package: PackageWrapper):
                for res in package.pkg.descriptor['resources']:
                    if res['name'] == RESOURCE_NAME:
                        saved_pk['pk'] = res['schema'].get('primaryKey', [])
                yield package.pkg
                yield from package
            return func

        def restore_pks(saved_pk):
            def func(package: PackageWrapper):
                for res in package.pkg.descriptor['resources']:
                    if res['name'] == RESOURCE_NAME:
                        res['schema']['primaryKey'] = saved_pk['pk']
                yield package.pkg
                yield from package
            return func

        saved_pk = dict(pk=[])
        steps = [
            save_pks(saved_pk),
            sort_rows(
                self.ORDER_BY_KEY,
                resources=RESOURCE_NAME
            ),
            join_with_self(
                RESOURCE_NAME,
                key_field_names,
                {
                    **dict((f, {}) for f in key_field_names),
                    '*': dict(aggregate='last')
                }
            ),
            restore_pks(saved_pk)
        ]
        f = Flow(*steps)
        return f


def enrichments_flows(config: Config, context: Context, *classes):
    all_enrichments = [e(config) for e in classes]
    active_enrichments = [e for e in all_enrichments if e.test()]

    presteps = []
    poststeps = []

    for e in all_enrichments:
        f = e.preflow()
        if f:
            presteps.append(f)

    for e in active_enrichments:
        f = e.postflow()
        if f:
            poststeps.append(f)

    return Flow(*presteps), Flow(*poststeps)
