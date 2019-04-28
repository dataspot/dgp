from .....core import BaseDataGenusProcessor
from .map_guesser import MappingGuesserAnalyzer
from .mandatory_fields import MandatoryFieldsAnalyzer
from .primary_key import PrimaryKeyAnalyzer

from .....config.consts import CONFIG_MODEL_MAPPING, CONFIG_CONSTANTS, CONFIG_PRIMARY_KEY


class MappingDGP(BaseDataGenusProcessor):

    def init(self):
        self.steps = self.init_classes([
            MappingGuesserAnalyzer,
            MandatoryFieldsAnalyzer,
            PrimaryKeyAnalyzer,
        ])

    def analyze(self):
        self.config.setdefault(CONFIG_MODEL_MAPPING, [])
        self.config.setdefault(CONFIG_PRIMARY_KEY, [])
        self.config.setdefault(CONFIG_CONSTANTS, [])
        return super().analyze()
