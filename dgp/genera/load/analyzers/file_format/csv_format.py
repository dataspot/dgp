from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_FORMAT, CONFIG_FORMAT_, CONFIG_ENCODING


class CSVFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(*(CONFIG_FORMAT_))
    )

    def run(self):
        if self.config[CONFIG_FORMAT] == 'csv':
            stream = self.context.stream
            self.config[CONFIG_ENCODING] = stream.encoding
