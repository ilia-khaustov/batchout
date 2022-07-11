from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Input


class ConstInputConfigInvalid(Exception):
    pass


@with_config_key('data', doc='Payloads of data as list of string values', raise_exc=ConstInputConfigInvalid)
@Registry.bind(Input, 'const')
class ConstInput(Input):

    def __init__(self, config):
        self.set_data(config)
        self._batch_size = 0

    def fetch(self, **_):
        if self._batch_size >= len(self._data):
            return
        payload = self._data[self._batch_size]
        self._batch_size += 1
        return payload.encode('utf8')

    def commit(self):
        pass

    def reset(self):
        self._batch_size = 0
