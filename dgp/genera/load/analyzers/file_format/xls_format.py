from .....core import BaseAnalyzer, Validator, Required
from ....consts import CONFIG_FORMAT, CONFIG_SHEET, CONFIG_FORCE_STRINGS


class XLSFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config[CONFIG_FORMAT].startswith('xls'):
            self.config.setdefault(CONFIG_SHEET, 0)
            self.config[CONFIG_FORCE_STRINGS] = True
