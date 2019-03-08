from .....core import BaseAnalyzer
from ....consts import CONFIG_TAXONOMY_LIST


class TaxonomyListAnalyzer(BaseAnalyzer):

    def run(self):
        self.config[CONFIG_TAXONOMY_LIST] = [
            dict(
                id=t.id,
                title=t.title
            )
            for t in self.context.taxonomies
        ]
