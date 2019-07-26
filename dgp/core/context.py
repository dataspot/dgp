import copy
import tabulator

from .config import Config
from ..config.log import logger
from ..config.consts import CONFIG_SKIP_ROWS, CONFIG_TAXONOMY_ID, CONFIG_FORMAT
from ..taxonomies import TaxonomyRegistry, Taxonomy


def trimmer(extended_rows):
    for row_number, headers, row in extended_rows:
        if headers:
            row = row[:len(headers)]
            if len(row) < len(headers):
                continue
        yield (row_number, headers, row)


class Context():

    def __init__(self, config: Config, taxonomies: TaxonomyRegistry):
        self.config = config
        self.taxonomies: TaxonomyRegistry = taxonomies
        self._stream = None
        self.enricher_dir = None

    def _structure_params(self):
        skip_rows = self.config.get(CONFIG_SKIP_ROWS) if CONFIG_SKIP_ROWS in self.config else None
        fmt = self.config.get(CONFIG_FORMAT)
        return dict(
            headers=skip_rows + 1 if skip_rows is not None else None,
            ignore_blank_headers=fmt in ('csv', 'xlsx', 'xls'),  # (skip_rows or 0) > 0,  # Temporary hack as tabulator is kind of limited here
            post_parse=[trimmer]
        )

    def reset_stream(self):
        self._stream = None

    @property
    def stream(self):
        if self._stream is None:
            source = copy.deepcopy(self.config._unflatten().get('source', {}))
            structure = self._structure_params()
            try:
                logger.info('Opening stream %s', source.get('path'))
                self._stream = tabulator.Stream(source.pop('path'), **source, **structure).open()
                for k in source.keys():
                    self.config.get('source.' + k)
                for k in structure.keys():
                    self.config.get('structure.' + k)
            except Exception:
                logger.exception('Failed to open URL, source=%r, structure=%r', source, structure)
                raise
        return self._stream

    @property
    def taxonomy(self) -> Taxonomy:
        if CONFIG_TAXONOMY_ID in self.config:
            return self.taxonomies.get(self.config[CONFIG_TAXONOMY_ID])
