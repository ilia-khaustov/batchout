from typing import Dict, Any

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Processor


class ReplaceProcessorConfigInvalid(Exception):
    pass


@with_config_key('old', doc='Text to replace', raise_exc=ReplaceProcessorConfigInvalid)
@with_config_key('new', doc='Text to insert instead', raise_exc=ReplaceProcessorConfigInvalid)
@with_config_key('count', doc='Maximum number of replacements (replace all if -1)', default=-1)
@Registry.bind(Processor, 'replace')
class ReplaceProcessor(Processor):

    def __init__(self, config: Dict[str, Any]):
        self.set_old(config)
        self.set_new(config)
        self.set_count(config)
        self._old = str(self._old)
        self._new = str(self._new)
        self._count = int(self._count)

    def process(self, value: Any) -> str:
        return str(value).replace(self._old, self._new, self._count)
