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
        max_location = None

        def has_value(val):
            return 1 if val == 0 or bool(val) else -1

        sample = [
            [has_value(v) for v in r]
            for r in sample
        ]

        for skip_row in range(10):
            for skip_col in range(5):
                score = sum(
                    sum(r[skip_col:])
                    for r in sample[skip_row:]
                )
                if max_score is None or max_score < score:
                    max_score = score
                    max_location = (skip_row, skip_col)

        header_count = 0
        for v in sample[max_location[0]][max_location[1]:]:
            if v == 1:
                header_count += 1
            else:
                break

        self.config[CONFIG_HEADER_COUNT] = header_count
        self.config[CONFIG_SKIP_ROWS] = max_location[0]
        self.config[CONFIG_SKIP_COLS] = max_location[1]
