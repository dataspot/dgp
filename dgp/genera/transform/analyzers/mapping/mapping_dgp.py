from .....core import BaseDataGenusProcessor
from .map_guesser import MappingGuesserAnalyzer
from .mandatory_fields import MandatoryFieldsAnalyzer

from ....consts import CONFIG_MODEL_MAPPING, CONFIG_CONSTANTS


class MappingDGP(BaseDataGenusProcessor):

    def init(self):
        self.steps = self.init_classes([
            MappingGuesserAnalyzer,
            MandatoryFieldsAnalyzer
        ])

    def analyze(self):
        self.config.setdefault(CONFIG_MODEL_MAPPING, [])
        self.config.setdefault(CONFIG_CONSTANTS, [])
        return super().analyze()
