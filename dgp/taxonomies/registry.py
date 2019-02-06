import os

import json
import yaml

class Taxonomy():
    
    def __init__(self, id, title, column_types, header_mapping):
        self.id = id
        self.title = title
        self.column_types = column_types
        self.header_mapping = header_mapping


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
                    ]:
                value = v.get(key)
                if value is not None:
                    if isinstance(value, str):
                        with open(os.path.join(base_path, value)) as infile:
                            value = loader(infile)
                else:
                    value = {}
                args.append(value)

            ret[id] = Taxonomy(id, *args)
        return ret
            
