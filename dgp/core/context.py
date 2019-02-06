import copy
import tabulator
import logging

from .config import Config
from ..genera.consts import CONFIG_SKIP_ROWS
from ..taxonomies import TaxonomyRegistry


def trimmer(extended_rows):
    for row_number, headers, row in extended_rows:
        if headers is not None:
            row = row[:len(headers)]
            if len(row) < len(headers):
                continue
        yield (row_number, headers, row)


class Context():

    def __init__(self, config: Config, taxonomies: TaxonomyRegistry):
        self.config = config
        self.taxonomies: TaxonomyRegistry = taxonomies
        self._stream = None

    def _structure_params(self):
        skip_rows = self.config.get(CONFIG_SKIP_ROWS) if CONFIG_SKIP_ROWS in self.config else None
        return dict(
            headers=skip_rows + 1 if skip_rows is not None else None,
            ignore_blank_headers=(skip_rows or 0) > 0,  # Temporary hack as tabulator is kind of limited here
            post_parse=[trimmer]
        )

    def reset_stream(self):
        self._stream = None

    @property
    def stream(self):
        if self._stream is None:
            try:
                source = copy.deepcopy(self.config._unflatten().get('source', {}))
                structure = self._structure_params()
                self._stream = tabulator.Stream(source.pop('path'), **source, **structure).open()
            except Exception:
                logging.exception('Failed to open URL')
                raise
        return self._stream
