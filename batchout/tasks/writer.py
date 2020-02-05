import logging

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.tasks import Task

log = logging.getLogger(__name__)


class WriterTaskConfigInvalid(Exception):
    pass


@with_config_key('selector', raise_exc=WriterTaskConfigInvalid)
@with_config_key('outputs', raise_exc=WriterTaskConfigInvalid)
@Registry.bind(Task, str(Task.TYPE_WRITER))
class WriterTask(Task):

    def __init__(self, config):
        self.set_selector(config)
        self.set_outputs(config)

    def type(self):
        return Task.TYPE_WRITER

    def components(self):
        return {
            'selector': self._selector,
            'outputs': self._outputs or [],
        }
