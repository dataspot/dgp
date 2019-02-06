from dataflows import Flow, load, printer, checkpoint, \
    dump_to_path, stream, PackageWrapper

from ...core import BaseDataGenusProcessor, Required, Validator
from .analyzers import FileFormatDGP, StructureDGP
from ..consts import *


class LoaderDGP(BaseDataGenusProcessor):

    PRE_CHECKS = Validator(
        Required(CONFIG_URL, 'Source data URL or path')
    )
    
    def init(self):
        self.steps = self.init_classes([
            FileFormatDGP,
            StructureDGP,
        ])

    def create_fdp(self):

        def func(package: PackageWrapper):
            descriptor = package.pkg.descriptor
            # Mandatory stuff
            columnTypes = self.config[CONFIG_TAXONOMY_CT]
            descriptor['columnTypes'] = columnTypes

            resource = descriptor['resources'][0]
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
                                break

            # Our own additions
            descriptor['taxonomyId'] = self.config[CONFIG_TAXONOMY_ID]

            yield package.pkg        
            yield from package

        return func

    def flow(self):
        if len(self.errors) == 0:
            structure_params = self.context._structure_params()
            source = self.config._unflatten()['source']
            return Flow(
                load(source.pop('path'), validate=False, **source, **structure_params),
                # printer(),
                self.create_fdp(),
            )

        