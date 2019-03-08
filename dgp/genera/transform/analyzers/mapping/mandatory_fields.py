from .....core import BaseAnalyzer, Validator, Required, ConfigValidationError
from ....consts import CONFIG_TAXONOMY_CT, CONFIG_MODEL_MAPPING


class MandatoryFieldsAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_TAXONOMY_CT),
        Required(CONFIG_MODEL_MAPPING),
    )

    def test(self):
        super().test()
        if len(self.errors) == 0:
            mapping = self.config[CONFIG_MODEL_MAPPING]
            types = self.config[CONFIG_TAXONOMY_CT]

            mandatory = dict(
                (t['name'], t)
                for t in types
                if t.get('mandatory')
            )

            for m in mapping:
                ct = m.get('columnType')
                if ct in mandatory:
                    del mandatory[ct]

            mandatory = list(mandatory.values())

            self.errors.extend(
                ConfigValidationError(
                    Validator.INVALID,
                    columnType
                )
                for columnType in mandatory
            )
        return len(self.errors) == 0

    def run(self):
        pass
