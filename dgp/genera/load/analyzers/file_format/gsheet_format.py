from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_FORMAT, CONFIG_SHEET, CONFIG_FORCE_STRINGS, CONFIG_SHEET_NAMES


class GSheetFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config[CONFIG_FORMAT] == 'gsheet':
            self.config[CONFIG_FORCE_STRINGS] = True
