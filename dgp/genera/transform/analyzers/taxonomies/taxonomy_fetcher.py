from .....core import BaseAnalyzer, Validator, Required
from .....taxonomies import Taxonomy
from .....config.consts import CONFIG_TAXONOMY_ID, CONFIG_TAXONOMY_CT, CONFIG_TAXONOMY_SETTINGS,\
    CONFIG_TAXONOMY_MISSING_VALUES


class TaxonomyFetcherAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_TAXONOMY_ID)
    )

    def run(self):
        tid = self.config.get(CONFIG_TAXONOMY_ID)
        t: Taxonomy = self.context.taxonomies.get(tid)
        self.config[CONFIG_TAXONOMY_CT] = t.column_types
        self.config[CONFIG_TAXONOMY_SETTINGS] = t.config
        self.config[CONFIG_TAXONOMY_MISSING_VALUES] = t.missingValues

    def analyze(self):
        if self.test():
            self.run()
            return True
        return False
