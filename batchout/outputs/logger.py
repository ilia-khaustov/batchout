import logging
from functools import partial

from batchout.core.registry import Registry
from batchout.outputs import Output

log = logging.getLogger(__name__)


@Registry.bind(Output, 'logger')
class LoggerOutput(Output):

    TRUNCATE_MARKER = '...'

    def __init__(self, _):
        pass

    @staticmethod
    def format_cell(size, value):
        value = str(value)
        text_size = size - len(LoggerOutput.TRUNCATE_MARKER)
        if len(value) > text_size:
            value = value[:text_size] + LoggerOutput.TRUNCATE_MARKER
        return value.ljust(size)

    def ingest(self, columns, rows):
        cell_size = max(max(tuple(map(len, map(str, columns)))), 20) + len(self.TRUNCATE_MARKER)
        fmt = partial(LoggerOutput.format_cell, cell_size)
        log.debug(' | '.join(list(map(fmt, columns))))
        rows_cnt = 0
        for row in rows:
            log.debug(' | '.join(list(map(fmt, row))))
            rows_cnt += 1
        return rows_cnt

    def commit(self):
        pass
