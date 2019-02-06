from dataflows import Flow, concatenate, printer, add_computed_field, \
    unpivot, validate, dump_to_path, PackageWrapper

from ...core import BaseDataGenusProcessor, Required, Validator
from .analyzers import TaxonomiesDGP, MappingDGP
from ..consts import *


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
            for field in package.pkg.descriptor['resources'][0]['schema']['fields']:
                field['title'] = field['name']
            yield package.pkg
            yield from package
        return func

    def flow(self):
        if len(self.errors) == 0:
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
                ]),
            ] + ([
                unpivot(
                    [
                        dict(
                            name=f['name'],
                            keys=f['normalize'],
                        )
                        for f in self.config.get(CONFIG_MODEL_MAPPING)
                        if 'normalize' in f
                    ], extraFieldDefs, normalizeFieldDef
                ),
            ] if normalizeFieldDef else []) + [
                self.copy_names_to_titles(),
                concatenate(dict(
                    (f['columnType'].replace(':', '-'), [f['name']])
                    for f in self.config.get(CONFIG_MODEL_MAPPING)
                    if f.get('columnType') is not None
                ), dict(
                    name='out',
                    path='out.csv'
                )),
                # printer()
            ]
            f = Flow(
                *steps
            )
            return f
