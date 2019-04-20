class BaseValidator():

    MISSING = 0
    INVALID = 1

    def check(self, config):
        raise NotImplementedError()


class Validator(BaseValidator):

    def __init__(self, *validators):
        self._validators = validators

    def check(self, config):
        errors = []
        for validator in self._validators:
            errors.extend(validator.check(config))
        return errors


class ConfigValidationError():

    def __init__(self, code, key, **kw):
        self.code = code
        self.key = key
        self.options = kw

    def __str__(self):
        if self.code == BaseValidator.MISSING:
            return 'Missing configuration: {} ({})'.format(self.options.get('description') or self.key, self.key)
        elif self.code == BaseValidator.INVALID:
            return 'Invalid configuration: {}'.format(self.options.get('description') or self.key)

    def __iter__(self):
        return iter((self.code, self.key, self.options))


class Required(BaseValidator):

    def __init__(self, key, description=None):
        self.key = key
        self.description = description

    def check(self, config):
        if self.key in config:
            return []
        else:
            return [ConfigValidationError(self.MISSING, self.key, description=self.description)]


class Empty(BaseValidator):
    def __init__(self, key, description=None):
        self.key = key
        self.description = description

    def check(self, config):
        if self.key in config:
            if not config[self.key]:
                return []
            else:
                return [ConfigValidationError(self.INVALID, self.key,
                        description=self.description)]
        else:
            return [ConfigValidationError(self.MISSING, self.key,
                    description=self.description)]
