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
        self.column_types = column_types
        self.header_mapping = header_mapping
        self.processing_module = processing_module
        self.config = config

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
                if value is not None:
                    if isinstance(value, str):
                        with open(os.path.join(base_path, value)) as in_file:
                            value = loader(in_file)
                else:
                    value = {}
                args.append(value)

            ret[id] = Taxonomy(id, *args)
        return ret
