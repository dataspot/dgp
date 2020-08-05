from dateutil.parser import parse as dateutil_parse
from copy import deepcopy

from dataflows import Flow, concatenate, add_field, update_resource, \
    unpivot, set_primary_key, PackageWrapper, set_type, validate, ResourceWrapper
from dataflows.base.schema_validator import ignore

from ...core import BaseDataGenusProcessor
from ...config.log import logger
from .analyzers import TaxonomiesDGP, MappingDGP
from ...config.consts import CONFIG_MODEL_EXTRA_FIELDS, CONFIG_MODEL_MAPPING,\
            CONFIG_TAXONOMY_CT, CONFIG_CONSTANTS, RESOURCE_NAME,\
            CONFIG_PRIMARY_KEY, CONFIG_TAXONOMY_ID, CONFIG_TAXONOMY_MISSING_VALUES


class TransformDGP(BaseDataGenusProcessor):

    def init(self):
        self.steps = self.init_classes([
            TaxonomiesDGP,
            MappingDGP
        ])

    def join_mapping_taxonomy(self, kind, fieldOptions):
        field_names = [
            f[1]
            for f in self.config.get(CONFIG_MODEL_EXTRA_FIELDS)
            if f[0] == kind
        ]
        field_defs = [
            dict(
                name=name,
                **fieldOptions[name]
            ) 
            for name in field_names
        ]

        return field_defs

    def copy_names_to_titles(self):
        def func(package: PackageWrapper):
            for field in package.pkg.descriptor['resources'][-1]['schema']['fields']:
                field['title'] = field['name']
            yield package.pkg
            yield from package
        return func

    def ct_to_fn(self, ct):
        return ct.replace(':', '-')

    def create_fdp(self):

        def func(package: PackageWrapper):
            descriptor = package.pkg.descriptor
            # Mandatory stuff
            columnTypes = self.config[CONFIG_TAXONOMY_CT]
            descriptor['columnTypes'] = columnTypes

            resource = descriptor['resources'][-1]
            resource['path'] = 'out.csv'
            resource['format'] = 'csv'
            resource['mediatype'] = 'text/csv'
            for k in ('headers', 'encoding', 'sheet'):
                if k in resource:
                    del resource[k]

            schema = resource['schema']

            schema['extraFields'] = []
            normalizationColumnType = None
            if self.config[CONFIG_MODEL_EXTRA_FIELDS]:
                for kind, field, *value in self.config[CONFIG_MODEL_EXTRA_FIELDS]:
                    for entry in self.config[CONFIG_MODEL_MAPPING]:
                        if entry['name'] == field:
                            if kind == 'constant':
                                entry['constant'] = value[0]
                            elif kind == 'normalize':
                                entry['normalizationTarget'] = True
                                normalizationColumnType = entry['columnType']
                            schema['extraFields'].append(entry)
                            break

            if self.config[CONFIG_MODEL_MAPPING]:
                for field in schema['fields']:
                    for entry in self.config[CONFIG_MODEL_MAPPING]:
                        if entry['name'] == field['name']:
                            field.update(entry)
                            break
                    if 'normalize' in field:
                        columnType = normalizationColumnType
                    else:
                        columnType = field.get('columnType')
                    if columnType is not None:
                        for entry in columnTypes:
                            if columnType == entry['name']:
                                if 'dataType' in entry:
                                    field['type'] = entry['dataType']
                                field.update(entry.get('options', {}))
                                break
                    field.update(field.get('options', {}))

            # Missing Values
            schema['missingValues'] = self.config[CONFIG_TAXONOMY_MISSING_VALUES]

            # Our own additions
            descriptor['taxonomyId'] = self.config[CONFIG_TAXONOMY_ID]

            yield package.pkg
            yield from package

        return func

    def datetime_handler(self):
        auto_transforms = dict(
            auto=dict(ignoretz=True, dayfirst=True, yearfirst=False),
            auto_ymd=dict(ignoretz=True, dayfirst=False, yearfirst=True),
            auto_ydm=dict(ignoretz=True, dayfirst=True, yearfirst=True),
            auto_dmy=dict(ignoretz=True, dayfirst=True, yearfirst=False),
            auto_mdy=dict(ignoretz=True, dayfirst=False, yearfirst=False),
        )

        def process_row(row, parsers):
            for name, parser_info, _type in parsers:
                if row.get(name):
                    try:
                        val = dateutil_parse(row[name], **parser_info)
                        if _type == 'date':
                            val = val.date()
                        row[name] = val.isoformat()
                    except ValueError:
                        pass
            return row

        def func(package: PackageWrapper):
            parsers = []
            for resource in package.pkg.descriptor['resources']:
                if resource['name'] != RESOURCE_NAME:
                    continue
                for field in resource['schema']['fields']:
                    if field['type'] in ('datetime', 'date'):
                        if field['format'] in auto_transforms:
                            parsers.append((field['name'], auto_transforms[field['format']], field['type']))
                            field['format'] = 'default'
            yield package.pkg
            for res in package:
                if res.res.name != RESOURCE_NAME or len(parsers) == 0:
                    yield res
                else:
                    yield (process_row(row, parsers) for row in res)
        return func

    def rename(self, dst_src_tuples):

        tuples = dst_src_tuples

        def func(package: PackageWrapper):

            def renamer(row, _tuples):
                return dict(
                    (dst, row.get(src)) for dst, src in _tuples
                )

            for resource in package.pkg.descriptor['resources']:
                if resource['name'] == RESOURCE_NAME:
                    new_fields = []
                    new_tuples = []
                    for f in resource['schema']['fields']:
                        for dst, src in tuples:
                            if src == f['name']:
                                f = deepcopy(f)
                                f['name'] = dst
                                new_tuples.append((dst, src))
                                new_fields.append(f)
                    resource['schema']['fields'] = new_fields

            yield package.pkg
            for res in package:
                if res.res.name != RESOURCE_NAME:
                    yield res
                else:
                    yield (renamer(row, new_tuples) for row in res)

        return func

    def set_consts(self, fieldOptions):

        steps = []
        for kind, name, *value in self.config[CONFIG_MODEL_EXTRA_FIELDS]:
            if kind == 'constant':
                default = value[0]
                options = fieldOptions.get(name)
                if options:
                    type_ = options.pop('type')
                    steps.append(add_field(
                        name, type_, default,
                        resources=RESOURCE_NAME,
                        **options
                    ))
        return Flow(*steps)

    def flow(self):
        if len(self.errors) == 0:
            primaryKey = [self.ct_to_fn(f) for f in self.config.get(CONFIG_PRIMARY_KEY)]

            fieldOptions = {}
            dataTypes = dict(
                (ct['name'], dict(
                    ct.get('options', {}),
                    type=ct['dataType']
                ))
                for ct in self.config.get(CONFIG_TAXONOMY_CT)
                if 'dataType' in ct
            )
            for mf in self.config.get(CONFIG_MODEL_MAPPING):
                ct = mf.get('columnType')
                name = mf['name']
                fieldOptions[name] = {}
                if ct is not None:
                    fieldOptions[name].update(dataTypes.get(ct, {}))
                fieldOptions[name].update(mf.get('options', {}))
                fieldOptions[name]['columnType'] = ct

            extraFieldDefs = self.join_mapping_taxonomy('extra', fieldOptions)
            normalizeFieldDef = self.join_mapping_taxonomy('normalize', fieldOptions)
            unpivotFields = [
                dict(
                    name=f['name'],
                    keys=f['normalize'],
                )
                for f in self.config.get(CONFIG_MODEL_MAPPING)
                if 'normalize' in f
            ]
            if len(normalizeFieldDef) > 0:
                normalizeFieldDef = normalizeFieldDef[0]
            else:
                normalizeFieldDef = None

            logger.error('UNPIVOT %r %r %r', unpivotFields, extraFieldDefs, normalizeFieldDef)

            steps = [
                self.create_fdp(),
                self.datetime_handler(),
                self.set_consts(fieldOptions),
                validate(on_error=ignore),
            ] + ([
                unpivot(
                    unpivotFields, extraFieldDefs, normalizeFieldDef,
                    resources=RESOURCE_NAME
                ),
            ] if normalizeFieldDef else []) + [
                self.copy_names_to_titles(),
                self.rename([
                    (self.ct_to_fn(f['columnType']), f['name'])
                    for f in self.config.get(CONFIG_MODEL_MAPPING)
                    if f.get('columnType') is not None
                ]),
                update_resource(
                    RESOURCE_NAME, path='out.csv'
                ),
                # *[
                #     set_type(
                #         self.ct_to_fn(f['columnType']),
                #         columnType=f['columnType'],
                #         **fieldOptions.get(f['columnType'], {}),
                #         resources=RESOURCE_NAME,
                #         on_error=ignore
                #     )
                #     for f in self.config.get(CONFIG_MODEL_MAPPING)
                #     if f.get('columnType') is not None
                # ],
                set_primary_key(primaryKey, resources=RESOURCE_NAME) if len(primaryKey) else None
                # printer()
            ]
            f = Flow(
                *steps
            )
            return f
