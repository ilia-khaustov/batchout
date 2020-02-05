import logging

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.tasks import Task

log = logging.getLogger(__name__)


class ReaderTaskConfigInvalid(Exception):
    pass


@with_config_key('selector')
@with_config_key('inputs', raise_exc=ReaderTaskConfigInvalid)
@Registry.bind(Task, str(Task.TYPE_READER))
class ReaderTask(Task):

    def __init__(self, config):
        self.set_selector(config)
        self.set_inputs(config)

    def type(self):
        return Task.TYPE_READER

    def components(self):
        return {
            'selector': self._selector,
            'inputs': self._inputs or [],
        }
