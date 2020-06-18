from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_FORMAT, CONFIG_SHEET, CONFIG_FORCE_STRINGS,\
    CONFIG_SHEET_NAMES, CONFIG_WORKBOOK_CACHE


class XLSFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config[CONFIG_FORMAT] == 'xls':
            self.config[CONFIG_FORCE_STRINGS] = True
            self.config[CONFIG_WORKBOOK_CACHE] = {}
            self.config[CONFIG_SHEET_NAMES] = [
                (i+1, name)
                for i, name in
                enumerate(self.context.stream._Stream__parser._XLSParser__book.sheet_names())
            ]
        elif self.config[CONFIG_FORMAT] == 'xlsx':
            self.config[CONFIG_FORCE_STRINGS] = True
            self.config[CONFIG_WORKBOOK_CACHE] = {}
            self.config[CONFIG_SHEET_NAMES] = [
                (i, name)
                for i, name in
                enumerate(self.context.stream._Stream__parser._XLSXParser__book.sheetnames)
            ]
        if self.config[CONFIG_FORMAT].startswith('xls'):
            self.config.setdefault(CONFIG_SHEET, self.config[CONFIG_SHEET_NAMES][0][1])
