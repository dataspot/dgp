from .....core import BaseAnalyzer, Validator, Required, ConfigValidationError
from .....config.consts import CONFIG_TAXONOMY_CT, CONFIG_MODEL_MAPPING


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

            to_del = []
            for ct, m in mandatory.items():
                alternatives = m.get('alternatives', [])
                for alternative in alternatives:
                    alternative = set(alternative)
                    for m in mapping:
                        mct = m.get('columnType')
                        if mct in alternative:
                            alternative.remove(mct)
                        if len(alternative) == 0:
                            break
                    if len(alternative) == 0:
                        to_del.append(ct)
                        break
            for ct in to_del:
                del mandatory[ct]

            mandatory = list(mandatory.values())

            self.errors.extend(
                ConfigValidationError(
                    Validator.MISSING,
                    columnType['name'],
                    description=columnType['title']
                )
                for columnType in mandatory
            )
        return len(self.errors) == 0

    def run(self):
        pass
