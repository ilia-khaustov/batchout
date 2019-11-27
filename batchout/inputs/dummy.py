from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.inputs import Input


class DummyInputConfigInvalid(Exception):
    pass


@with_config_key('data', raise_exc=DummyInputConfigInvalid)
@Registry.bind(Input, 'dummy')
class DummyInput(Input):

    def __init__(self, config):
        self.set_data(config)
        self._batch_size = 0

    def fetch(self):
        if self._batch_size >= len(self._data):
            return
        payload = self._data[self._batch_size]
        self._batch_size += 1
        return payload.encode('utf8')

    def commit(self):
        pass

    def reset(self):
        self._batch_size = 0
