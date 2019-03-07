from dataflows import Flow, dump_to_path

from ...core import BaseDataGenusProcessor
from ..consts import CONFIG_URL


class EnricherDGP(BaseDataGenusProcessor):

    def preflow(self):

    def flow(self):
        config_hash = self.config._calc_hash(CONFIG_URL)
        enricher_dir = '.enrichments/{}'.format(config_hash)
        self.context.enricher_dir = '{}/datapackage.json'.format(enricher_dir)
        return Flow(
            dump_to_path(enricher_dir),
        )
