from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_FORMAT, CONFIG_URL, CONFIG_DEDUPLICATE_HEADERS

from .....config.log import logger

class FileFormatAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_URL)
    )

    def run(self):
        try:
            stream = self.context.stream
        except Exception as e:
            logger.error('FAILED to open stream: {}'.format(e))
            stream = None
        if stream is not None:
            self.config[CONFIG_FORMAT] = stream.format
            self.config[CONFIG_DEDUPLICATE_HEADERS] = True
