import os
import json
import requests
from hashlib import md5

from dataflows import Flow, load, dump_to_path, stream, unstream
from dataflows.base.schema_validator import ignore

from ...core import BaseDataGenusProcessor, Required, Validator, ConfigurableDGP
from .analyzers import FileFormatDGP, StructureDGP
from ...config.consts import CONFIG_URL, CONFIG_PUBLISH_ALLOWED, RESOURCE_NAME
from ...config.log import logger


class LoaderDGP(BaseDataGenusProcessor):

    PRE_CHECKS = Validator(
        Required(CONFIG_URL, 'Source data URL or path')
    )

    def init(self):
        self.steps = self.init_classes([
            FileFormatDGP,
            StructureDGP,
        ])

    def hash_key(self, *args):
        data = json.dumps(args, sort_keys=True, ensure_ascii=False)
        return md5(data.encode('utf8')).hexdigest()

    def flow(self):
        if len(self.errors) == 0:

            config = self.config._unflatten()
            source = config['source']
            ref_hash = self.hash_key(source, config['structure'], config.get('publish'))
            cache_path = os.path.join('.cache', ref_hash)
            datapackage_path = os.path.join(cache_path, 'datapackage.json')
            structure_params = self.context._structure_params()
            http_session = self.context.http_session()
            loader = load(source.pop('path'), validate=False,
                          name=RESOURCE_NAME,
                          **source, **structure_params,
                          http_session=http_session,
                          http_timeout=120,
                          infer_strategy=load.INFER_PYTHON_TYPES,
                          cast_strategy=load.CAST_DO_NOTHING,
                          limit_rows=(
                              None
                              if self.config.get(CONFIG_PUBLISH_ALLOWED)
                              else 5000
                          ))

            if self.config.get(CONFIG_PUBLISH_ALLOWED):
                return Flow(
                    loader,
                )
            else:
                if not os.path.exists(cache_path):
                    logger.info('Caching source data into %s', cache_path)
                    Flow(
                        loader,
                        stream(cache_path),
                        # printer(),
                    ).process()
                logger.info('Using cached source data from %s', cache_path)
                return Flow(
                    unstream(cache_path),
                    # load(datapackage_path, resources=RESOURCE_NAME),
                )


class PostLoaderDGP(ConfigurableDGP):

    def init(self):
        super().init('loading', per_taxonomy=False)
        self._flows = None
