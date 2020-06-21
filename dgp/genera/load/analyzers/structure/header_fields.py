from dataflows import load

from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_SKIP_ROWS, CONFIG_SKIP_COLS, CONFIG_HEADER_FIELDS


class HeaderFieldsAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_SKIP_ROWS),
        Required(CONFIG_SKIP_COLS),
    )

    def run(self):
        self.context.reset_stream()
        stream = self.context.stream
        if stream is not None:
            if stream.headers is not None:
                self.config[CONFIG_HEADER_FIELDS] = load.rename_duplicate_headers(stream.headers)
            else:
                self.config[CONFIG_HEADER_FIELDS] = []
