from .....core import BaseAnalyzer
from .....config.consts import CONFIG_SKIP_ROWS, CONFIG_SKIP_COLS, CONFIG_HEADER_COUNT


class SkipRowsColsAnalyzer(BaseAnalyzer):

    def run(self):
        if CONFIG_SKIP_COLS in self.config or CONFIG_SKIP_ROWS in self.config:
            return True

        stream = self.context.stream
        if stream is None:
            return True

        self.config[CONFIG_SKIP_ROWS] = 0
        self.config[CONFIG_SKIP_COLS] = 0
        sample = stream.sample
        max_score = None

        def has_value(row, col):
            try:
                val = sample[row][col]
                return val == 0 or bool(val)
            except IndexError:
                return False

        def top(_idx):
            return has_value(skip_row, skip_col + _idx)

        def bottom(_idx):
            return has_value(skip_row + 1, skip_col + _idx)

        for skip_row in range(10):
            for skip_col in range(5):
                idx = 0
                score = 0
                headers = 0
                while top(idx) or bottom(idx):
                    score += 1 if top(idx) and bottom(idx) else 0
                    headers += 1 if top(idx) else 0
                    idx += 1
                score = (score, -skip_row, -skip_col, headers)
                if max_score is None or max_score < score:
                    max_score = score

        self.config[CONFIG_HEADER_COUNT] = max_score[3]
        self.config[CONFIG_SKIP_ROWS] = -max_score[1]
        self.config[CONFIG_SKIP_COLS] = -max_score[2]
