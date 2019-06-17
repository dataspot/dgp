import os
import json
from hashlib import md5

from dataflows import Flow, load, PackageWrapper, dump_to_path, printer
from dataflows.base.schema_validator import ignore

from ...core import BaseDataGenusProcessor, Required, Validator
from .analyzers import FileFormatDGP, StructureDGP
from ...config.consts import CONFIG_URL, CONFIG_MODEL_EXTRA_FIELDS, CONFIG_TAXONOMY_CT,\
    CONFIG_MODEL_MAPPING, CONFIG_TAXONOMY_ID, CONFIG_PUBLISH_ALLOWED, RESOURCE_NAME


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
                    field.update(field.pop('options', {}))
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

            # Our own additions
            descriptor['taxonomyId'] = self.config[CONFIG_TAXONOMY_ID]

            yield package.pkg
            yield from package

        return func

    def hash_key(self, *args):
        data = json.dumps(args, sort_keys=True, ensure_ascii=False)
        return md5(data.encode('utf8')).hexdigest()

    def flow(self):
        if len(self.errors) == 0:

            config = self.config._unflatten()
            source = config['source']
            ref_hash = self.hash_key(source, config['structure'], config.get('publish'))
            cache_path = os.path.join('.cache', ref_hash)
            datapackage_path = os.path.join(cache_path, 'datapackage.json')
            structure_params = self.context._structure_params()
            loader = load(source.pop('path'), validate=False,
                          name=RESOURCE_NAME,
                          **source, **structure_params,
                          infer_strategy=load.INFER_PYTHON_TYPES,
                          cast_strategy=load.CAST_DO_NOTHING,
                          limit_rows=(
                              None
                              if self.config.get(CONFIG_PUBLISH_ALLOWED)
                              else 5000
                          ))

            if self.config.get(CONFIG_PUBLISH_ALLOWED):
                return Flow(
                    loader,
                    self.create_fdp(),
                )
            else:
                if not os.path.exists(datapackage_path):
                    print('Caching source data into {}'.format(cache_path))
                    Flow(
                        loader,
                        dump_to_path(cache_path, validator_options=dict(on_error=ignore)),
                        self.create_fdp(),
                        # printer(),
                    ).process()
                print('Using cached source data from {}'.format(cache_path))
                return Flow(
                    load(datapackage_path, resources=RESOURCE_NAME),
                    self.create_fdp(),
                )
