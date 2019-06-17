import os
from importlib.util import spec_from_file_location, module_from_spec

import json
import yaml


def load_module(py_file):
    processing_module = None
    try:
        spec = spec_from_file_location('{}_processing'.format(id), py_file.name)
        if spec is not None:
            processing_module = module_from_spec(spec)
            spec.loader.exec_module(processing_module)
    except FileNotFoundError:
        pass
    return processing_module


class Taxonomy():

    def __init__(self, id, title, column_types, header_mapping,
                 processing_module, config):
        self.id = id
        self.title = title
        self.header_mapping = header_mapping
        self.processing_module = processing_module
        self.config = config

        ct_names = []
        specs = {}
        for spec in column_types:
            ct = spec['name']
            specs.setdefault(ct, {}).update(spec)
            if ct not in ct_names:
                ct_names.append(ct)
        self.column_types = [specs[ct] for ct in ct_names]

    def flows(self, config, context):
        if self.processing_module:
            if hasattr(self.processing_module, 'flows'):
                return self.processing_module.flows(config, context)


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

            args = [v['title']]
            for (key, loader) in [
                    ('column_types', json.load),
                    ('header_mapping', yaml.load),
                    ('processing_module', load_module),
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
        return ret
