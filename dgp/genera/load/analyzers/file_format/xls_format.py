from .....core import BaseAnalyzer, Validator, Required
from ....consts import *


class XLSFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config[CONFIG_FORMAT].startswith('xls'):
            self.config[CONFIG_SHEET] = 0
            self.config[CONFIG_FORCE_STRINGS] = True
