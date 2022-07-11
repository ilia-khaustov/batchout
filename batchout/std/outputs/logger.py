import logging
from functools import partial
from typing import Any, Iterable, Collection

from ...core.registry import Registry
from ...core.config import with_config_key
from .base import Output

log = logging.getLogger(__name__)


@with_config_key('width', default=80)
@Registry.bind(Output, 'logger')
class LoggerOutput(Output):

    TRUNCATE_MARKER = '...'

    def __init__(self, config: dict):
        self.set_width(config)

    @staticmethod
    def format_cell(size: int, value: Any) -> str:
        value = str(value)
        text_size = size - len(LoggerOutput.TRUNCATE_MARKER)
        if len(value) > text_size:
            value = value[:text_size] + LoggerOutput.TRUNCATE_MARKER
        return value.ljust(size)

    def ingest(self, columns: Collection[str], rows: Iterable[Collection[Any]]) -> int:
        column_max_width = (self._width // len(columns))
        column_min_width = min(max(tuple(map(len, map(str, columns)))), column_max_width)
        cell_size = max(column_min_width, column_max_width) - len(self.TRUNCATE_MARKER) + 1
        fmt = partial(LoggerOutput.format_cell, cell_size)
        log.debug('_|_'.join(list(map(fmt, columns))))
        rows_cnt = 0
        for row in rows:
            log.debug(' | '.join(list(map(fmt, row))))
            rows_cnt += 1
        return rows_cnt

    def commit(self):
        pass
