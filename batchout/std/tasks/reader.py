import logging

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Task

log = logging.getLogger(__name__)


class ReaderTaskConfigInvalid(Exception):
    pass


@with_config_key('selector')
@with_config_key('threads', default=1, raise_exc=ReaderTaskConfigInvalid)
@with_config_key('inputs', raise_exc=ReaderTaskConfigInvalid)
@Registry.bind(Task, str(Task.TYPE_READER))
class ReaderTask(Task):

    def __init__(self, config):
        self.set_selector(config)
        self.set_inputs(config)
        self.set_threads(config)
        if not isinstance(self._threads, int) or self._threads <= 0:
            raise ReaderTaskConfigInvalid('positive integer greater than 0 expected for threads')

    def type(self):
        return Task.TYPE_READER

    def components(self):
        return {
            'selector': self._selector,
            'inputs': self._inputs or [],
            'threads': self._threads,
        }
