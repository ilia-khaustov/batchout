import logging
from functools import partial

from batchout.core.registry import Registry
from batchout.outputs import Output

log = logging.getLogger(__name__)


@Registry.bind(Output, 'dummy')
class DummyOutput(Output):

    TRUNCATE_MARKER = '...'

    def __init__(self, _):
        pass

    @staticmethod
    def format_cell(size, value):
        value = str(value)
        text_size = size - len(DummyOutput.TRUNCATE_MARKER)
        if len(value) > text_size:
            value = value[:text_size] + DummyOutput.TRUNCATE_MARKER
        return value.ljust(size)

    def ingest(self, data):
        cell_size = max(tuple(map(len, map(str, data.columns)))) + len(self.TRUNCATE_MARKER)
        fmt = partial(DummyOutput.format_cell, cell_size)
        log.debug(' | '.join(list(map(fmt, data.columns))))
        for row in data.rows:
            log.debug(' | '.join(list(map(fmt, row))))

    def commit(self):
        pass
