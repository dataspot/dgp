from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_FORMAT, CONFIG_URL, CONFIG_DEDUPLICATE_HEADERS


class FileFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_URL)
    )

    def run(self):
        stream = self.context.stream
        if stream is not None:
            self.config[CONFIG_FORMAT] = stream.format
            self.config[CONFIG_DEDUPLICATE_HEADERS] = True
