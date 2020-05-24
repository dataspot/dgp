from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_TAXONOMY_CT, CONFIG_MODEL_MAPPING, CONFIG_PRIMARY_KEY


class PrimaryKeyAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_TAXONOMY_CT),
        Required(CONFIG_MODEL_MAPPING),
    )

    def run(self):
        column_types = self.config.get(CONFIG_TAXONOMY_CT)
        unique_column_types = set(
            c['name']
            for c in column_types
            if c.get('unique')
        )
        primary_key = [
            f['columnType']
            for f in self.config.get(CONFIG_MODEL_MAPPING)
            if f.get('columnType') in unique_column_types
        ]
        self.config[CONFIG_PRIMARY_KEY] = primary_key
