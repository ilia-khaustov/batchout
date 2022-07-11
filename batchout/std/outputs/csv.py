import logging
from csv import writer
from typing import Any, Iterable, Collection
import os.path
from os import W_OK

from ...core.registry import Registry
from ...core.config import with_config_key
from .base import Output

log = logging.getLogger(__name__)


class CsvOutputConfigInvalid(Exception):
    pass


@with_config_key('delimiter', default=',')
@with_config_key('mode', default='append', choices=['append', 'overwrite'])
@with_config_key('path', raise_exc=CsvOutputConfigInvalid)
@Registry.bind(Output, 'csv')
class CsvOutput(Output):

    def __init__(self, config: dict):
        self.set_path(config)
        self.set_mode(config)
        self.set_delimiter(config)
        path_dir = os.path.dirname(self._path)
        path_exists = os.path.exists(self._path)
        if not path_exists and os.access(path_dir, W_OK) or path_exists and not os.access(self._path, W_OK):
            raise CsvOutputConfigInvalid('File or parent directory are not allowed for writing: %s', self._path)

    def ingest(self, columns: Collection[str], rows: Iterable[Collection[Any]]) -> int:
        filemode = {'append': 'a', 'overwrite': 'w'}[self._mode]
        rows_num = 0
        with open(self._path, mode=filemode) as f:
            w = writer(f, delimiter=self._delimiter)
            w.writerow(columns)
            for row in rows:
                rows_num += 1
                w.writerow(row)
        return rows_num

    def commit(self):
        pass
