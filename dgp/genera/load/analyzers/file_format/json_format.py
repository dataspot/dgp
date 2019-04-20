from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_FORMAT, CONFIG_JSON_PROPERTY


class JsonFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    def run(self):
        if self.config[CONFIG_FORMAT] == 'json':
            self.config[CONFIG_JSON_PROPERTY] = ''
