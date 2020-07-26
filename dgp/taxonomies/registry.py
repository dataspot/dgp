import os
from importlib.util import spec_from_file_location, module_from_spec

import json
import yaml


def load_module(py_file):
    loaded_module = None
    try:
        filename = py_file.name + ''
        spec = spec_from_file_location('{}'.format(id(filename)), filename)
        if spec is not None:
            loaded_module = module_from_spec(spec)
            spec.loader.exec_module(loaded_module)
    except FileNotFoundError:
        pass
    return loaded_module


class ExtensionModule():

    def __init__(self, module):
        self.module = module

    def has_module(self):
        return not isinstance(self.module, dict)

    def analyzers(self, config, context):
        if self.has_module():
            if hasattr(self.module, 'analyzers'):
                return self.module.analyzers(config, context)

    def flows(self, config, context):
        if self.has_module():
            if hasattr(self.module, 'flows'):
                return self.module.flows(config, context)


class Taxonomy():

    def __init__(self, id, title, column_types, header_mapping,
                 processing_module, publishing_module, loading_module,
                 config):
        self.id = id
        self.title = title
        self.header_mapping = header_mapping
        self.processing = ExtensionModule(processing_module)
        self.publishing = ExtensionModule(publishing_module)
        self.loading = ExtensionModule(loading_module)
        self.config = config
        self.missingValues = self.config.get('missingValues', []) if self.config else []
        if '' not in self.missingValues:
            self.missingValues.append('')

        ct_names = []
        specs = {}
        for spec in column_types:
            ct = spec['name']
            specs.setdefault(ct, {}).update(spec)
            if ct not in ct_names:
                ct_names.append(ct)
        self.column_types = [specs[ct] for ct in ct_names]


class TaxonomyRegistry():

    def __init__(self, index_file):
        self.index = self._build_index(index_file)

    def get(self, id) -> Taxonomy:
        return self.index[id]

    def all_ids(self):
        return sorted(self.index.keys())

    def __iter__(self):
        return iter(self.index.values())

    def _build_index(self, index_file):
        base_path = os.path.dirname(index_file)
        with open(index_file) as index:
            index = yaml.load(index)
        ret = {}
        for k, v in index.items():
            id = k

            title = v.get('title', k)
            args = [title]
            for (key, loader) in [
                    ('column_types', json.load),
                    ('header_mapping', yaml.load),
                    ('processing_module', load_module),
                    ('publishing_module', load_module),
                    ('loading_module', load_module),
                    ('config', None),
                    ]:
                value = v.get(key)
                arg = None
                if value is not None:
                    if not isinstance(value, list):
                        value = [value]
                    for item in value:
                        if isinstance(item, str):
                            with open(os.path.join(base_path, item)) as in_file:
                                item = loader(in_file)
                        if arg is None:
                            arg = item
                        else:
                            arg += item
                else:
                    arg = {}
                args.append(arg)

            ret[id] = Taxonomy(id, *args)
        ret.setdefault('_common_', Taxonomy('_common_', '_common_', {}, {}, {}, {}, {}, {}))
        return ret
