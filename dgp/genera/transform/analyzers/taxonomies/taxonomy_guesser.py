from collections import Counter

from .....core import BaseAnalyzer, Validator, Required
from .....taxonomies import Taxonomy
from .....config.consts import CONFIG_HEADER_FIELDS, CONFIG_TAXONOMY_ID


class TaxonomyGuesserAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_HEADER_FIELDS)
    )

    def run(self):
        if self.config[CONFIG_TAXONOMY_ID] is None:
            fields = self.config[CONFIG_HEADER_FIELDS]
            c = Counter()
            taxonomies = self.context.taxonomies
            taxonomy_ids = [x for x in taxonomies.all_ids() if x != '_common_']
            if len(taxonomy_ids) == 1:
                self.config[CONFIG_TAXONOMY_ID] = taxonomy_ids[0]
            else:
                for field in fields:
                    taxonomy: Taxonomy
                    for taxonomy in taxonomies:
                        for tf in taxonomy.header_mapping.keys():
                            if tf == field:
                                c[taxonomy.id] += 1
                selected = c.most_common(1)
                if len(selected) > 0:
                    selected = selected[0][0]
                    self.config[CONFIG_TAXONOMY_ID] = selected
