from dataflows import Flow, concatenate, add_computed_field, \
    unpivot, set_primary_key, PackageWrapper

from ...core import BaseDataGenusProcessor
from .analyzers import TaxonomiesDGP, MappingDGP
from ..consts import CONFIG_MODEL_EXTRA_FIELDS, CONFIG_MODEL_MAPPING,\
            CONFIG_TAXONOMY_CT, CONFIG_CONSTANTS, RESOURCE_NAME


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

    def unique_column_types(self):
        columnTypes = self.config.get(CONFIG_TAXONOMY_CT)
        return set(
            c['name']
            for c in columnTypes
            if c.get('unique')
        )

    def ct_to_fn(self, ct):
        return ct.replace(':', '-')

    def flow(self):
        if len(self.errors) == 0:
            uniqueColumnTypes = self.unique_column_types()
            primaryKey = [
                self.ct_to_fn(f['columnType'])
                for f in self.config.get(CONFIG_MODEL_MAPPING)
                if f.get('columnType') in uniqueColumnTypes
            ]
            extraFieldDefs = self.join_mapping_taxonomy('extra')
            normalizeFieldDef = self.join_mapping_taxonomy('normalize')
            if len(normalizeFieldDef) > 0:
                normalizeFieldDef = normalizeFieldDef[0]
            else:
                normalizeFieldDef = None
            steps = [
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
                set_primary_key(primaryKey, resources=RESOURCE_NAME)
                # printer()
            ]
            f = Flow(
                *steps
            )
            return f
