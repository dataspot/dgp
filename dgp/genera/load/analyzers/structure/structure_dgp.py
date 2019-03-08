from .....core import BaseDataGenusProcessor
from .skip_rows_cols import SkipRowsColsAnalyzer
from .header_fields import HeaderFieldsAnalyzer


class StructureDGP(BaseDataGenusProcessor):

    def init(self):
        self.steps = self.init_classes([
            SkipRowsColsAnalyzer,
            HeaderFieldsAnalyzer
        ])
