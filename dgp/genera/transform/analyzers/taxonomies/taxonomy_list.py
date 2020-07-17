from .....core import BaseAnalyzer
from .....config.consts import CONFIG_TAXONOMY_LIST


class TaxonomyListAnalyzer(BaseAnalyzer):

    def run(self):
        self.config[CONFIG_TAXONOMY_LIST] = [
            dict(
                id=t.id,
                title=t.title
            )
            for t in self.context.taxonomies
            if t.id != '_common_'
        ]
