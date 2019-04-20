from .....core import BaseDataGenusProcessor
from .file_format import FileFormatAnalyzer
from .csv_format import CSVFormatAnalyzer
from .xls_format import XLSFormatAnalyzer
from .gsheet_format import GSheetFormatAnalyzer
from .json_format import JsonFormatAnalyzer


class FileFormatDGP(BaseDataGenusProcessor):

    def init(self):
        self.steps = self.init_classes([
            FileFormatAnalyzer,
            CSVFormatAnalyzer,
            XLSFormatAnalyzer,
            GSheetFormatAnalyzer,
            JsonFormatAnalyzer,
        ])
