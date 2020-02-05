import logging

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.tasks import Task

log = logging.getLogger(__name__)


class WalkerTaskConfigInvalid(Exception):
    pass


@with_config_key('indexes')
@with_config_key('columns', raise_exc=WalkerTaskConfigInvalid)
@with_config_key('inputs', raise_exc=WalkerTaskConfigInvalid)
@Registry.bind(Task, str(Task.TYPE_WALKER))
class WalkerTask(Task):

    def __init__(self, config):
        self.set_inputs(config)
        self.set_columns(config)
        self.set_indexes(config)

    def type(self):
        return Task.TYPE_WALKER

    def components(self):
        return {
            'inputs': self._inputs,
            'columns': self._columns,
            'indexes': self._indexes or [],
        }
