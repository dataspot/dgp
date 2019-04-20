from .....core import BaseAnalyzer
from .....config.consts import CONFIG_FORMAT


class FileFormatAnalyzer(BaseAnalyzer):

    def run(self):
        stream = self.context.stream
        self.config[CONFIG_FORMAT] = stream.format
