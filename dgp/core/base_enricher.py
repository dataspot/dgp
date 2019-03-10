from hashlib import md5

from dataflows import Flow, PackageWrapper
from dataflows import load, concatenate, join, set_type, checkpoint,\
                      dump_to_path

from .config import Config
from .context import Context
from ..genera.consts import CONFIG_MODEL_MAPPING, RESOURCE_NAME


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
        all_cts = [
            x['columnType']
            for x in self.config.get(CONFIG_MODEL_MAPPING)
            if 'columnType' in x
        ]
        if not all(x in all_cts for x in self.REQUIRED_COLUMN_TYPES):
            return False
        if any(x in all_cts for x in self.PROHIBITED_COLUMN_TYPES):
            return False
        return True


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


class DatapackageJoiner(BaseEnricher):

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
        print('DONE PREPARING', self.key)

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

    def postflow(self):
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
