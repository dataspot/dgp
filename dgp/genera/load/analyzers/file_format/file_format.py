from .....core import BaseAnalyzer
from ....consts import *


class FileFormatAnalyzer(BaseAnalyzer):

    def run(self):
        stream = self.context.stream
        self.config[CONFIG_FORMAT] = stream.format
