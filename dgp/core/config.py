import os
import yaml
import json
import hashlib


class Config():

    def __init__(self, filename=None):
        self.filename = filename
        self._config = {}
        if filename and os.path.exists(filename):
            with open(filename) as input:
                self._config = self._flatten(yaml.load(input))
        self._validators = []
        self._dirty = True
        self._state_keys = set()
        self._save()

    def _unflatten(self):
        ret = {}
        for path, v in self._config.items():
            ptr = ret
            parts = path.split('.')
            while len(parts) > 1:
                part = parts.pop(0)
                ptr = ptr.setdefault(part, {})
            ptr[parts[0]] = v
        return ret

    def _flatten(self, obj, prefix=''):
        ret = {}
        for k, v in obj.items():
            if isinstance(v, dict):
                ret.update(self._flatten(v, prefix=f'{prefix}{k}.'))
            else:
                ret[f'{prefix}{k}'] = v
        return ret

    def _save(self):
        config = self._unflatten()
        if self.filename:
            with open(self.filename, 'w') as out:
                yaml.dump(config, out, default_flow_style=False, indent=2, allow_unicode=True, encoding='utf8')

    def get(self, key):
        self._state_keys.add(key)
        return self._config.get(key)

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        return key in self._config

    def set(self, key, value):
        self._dirty = key not in self or self.get(key) != value
        # self._state_keys.add(key)
        self._config[key] = value
        self._save()

    def setdefault(self, key, value):
        if key not in self:
            self.set(key, value)

    def __setitem__(self, key, value):
        return self.set(key, value)

    @property
    def dirty(self):
        ret = self._dirty
        self._dirty = False
        return ret

    def dirty_state(self, state_key):
        k = self._state_config_key(state_key)
        if k in self:
            state = self.get(k)
            keys = state[0]
            return self._calc_hash(keys) != state[1]
        return True

    def reset_state(self, state_key):
        self._state_keys = set()

    def write_state(self, state_key):
        state_hash = self._calc_hash(self._state_keys)
        self.set(self._state_config_key(state_key),
                 [list(self._state_keys), state_hash])

    def _state_config_key(self, state_key):
        return '__state.{}'.format(state_key)

    def _calc_hash(self, keys):
        keys = sorted(keys)
        md5 = hashlib.md5()
        for key in keys:
            md5.update(json.dumps(self.get(key), sort_keys=True).encode('utf8'))
        return md5.hexdigest()
