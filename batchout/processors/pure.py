from typing import Dict, Any

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.processors import Processor


class ReplaceProcessorConfigInvalid(Exception):
    pass


@with_config_key('old', raise_exc=ReplaceProcessorConfigInvalid)
@with_config_key('new', raise_exc=ReplaceProcessorConfigInvalid)
@with_config_key('count', default=-1)
@Registry.bind(Processor, 'replace')
class ReplaceProcessor(Processor):

    def __init__(self, config: Dict[str, Any]):
        self.set_old(config)
        self.set_new(config)
        self.set_count(config)
        self._old = str(self._old)
        self._new = str(self._new)
        self._count = int(self._count)

    def process(self, value):
        return str(value).replace(self._old, self._new, self._count)
