from dataflows import Flow, concatenate, add_computed_field, \
    unpivot, set_primary_key, PackageWrapper, set_type
from dataflows.base.schema_validator import ignore

from ...core import BaseDataGenusProcessor
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

    def join_mapping_taxonomy(self, kind):
        field_names = [
            f[1]
            for f in self.config.get(CONFIG_MODEL_EXTRA_FIELDS)
            if f[0] == kind
        ]
        field_defs = [
            m
            for m in self.config.get(CONFIG_MODEL_MAPPING)
            if m['name'] in field_names
        ]
        for f in field_defs:
            for t in self.config.get(CONFIG_TAXONOMY_CT):
                if f['columnType'] == t['name']:
                    f['type'] = t['dataType']

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

    def flow(self):
        if len(self.errors) == 0:
            primaryKey = [self.ct_to_fn(f) for f in self.config.get(CONFIG_PRIMARY_KEY)]
            extraFieldDefs = self.join_mapping_taxonomy('extra')
            normalizeFieldDef = self.join_mapping_taxonomy('normalize')
            if len(normalizeFieldDef) > 0:
                normalizeFieldDef = normalizeFieldDef[0]
            else:
                normalizeFieldDef = None
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
                if ct is not None:
                    fieldOptions[ct] = dataTypes.get(ct, {})
                    fieldOptions[ct].update(mf.get('options', {}))
            steps = [
                self.create_fdp(),
                add_computed_field([
                    dict(
                        operation='constant',
                        target=k,
                        with_=v
                    )
                    for k, v in self.config.get(CONFIG_CONSTANTS)
                ], resources=RESOURCE_NAME),
            ] + ([
                unpivot(
                    [
                        dict(
                            name=f['name'],
                            keys=f['normalize'],
                        )
                        for f in self.config.get(CONFIG_MODEL_MAPPING)
                        if 'normalize' in f
                    ], extraFieldDefs, normalizeFieldDef,
                    resources=RESOURCE_NAME
                ),
            ] if normalizeFieldDef else []) + [
                self.copy_names_to_titles(),
                concatenate(
                    dict(
                        (self.ct_to_fn(f['columnType']), [f['name']])
                        for f in self.config.get(CONFIG_MODEL_MAPPING)
                        if f.get('columnType') is not None
                    ), dict(
                        name=RESOURCE_NAME,
                        path='out.csv',
                    ), resources=RESOURCE_NAME
                ),
                *[
                    set_type(
                        self.ct_to_fn(f['columnType']),
                        columnType=f['columnType'],
                        **fieldOptions.get(f['columnType'], {}),
                        resources=RESOURCE_NAME,
                        on_error=ignore
                    )
                    for f in self.config.get(CONFIG_MODEL_MAPPING)
                    if f.get('columnType') is not None
                ],
                set_primary_key(primaryKey, resources=RESOURCE_NAME)
                # printer()
            ]
            f = Flow(
                *steps
            )
            return f
