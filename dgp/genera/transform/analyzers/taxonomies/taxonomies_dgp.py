from .....core import BaseDataGenusProcessor
from .taxonomy_list import TaxonomyListAnalyzer
from .taxonomy_guesser import TaxonomyGuesserAnalyzer
from .taxonomy_fetcher import TaxonomyFetcherAnalyzer


class TaxonomiesDGP(BaseDataGenusProcessor):

    def init(self):
        self.steps = self.init_classes([
            TaxonomyListAnalyzer,
            TaxonomyGuesserAnalyzer,
            TaxonomyFetcherAnalyzer,
        ])
