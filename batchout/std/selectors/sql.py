import logging
import sqlite3
from typing import Collection

from ...core.config import with_config_key
from ...core.registry import Registry
from ...core.data import Data
from .base import Selector


log = logging.getLogger(__name__)


class SqlSelectorConfigInvalid(Exception):
    pass


@with_config_key('query', raise_exc=SqlSelectorConfigInvalid)
@with_config_key('columns', raise_exc=SqlSelectorConfigInvalid)
@Registry.bind(Selector, 'sql')
class SqlSelector(Selector):

    def __init__(self, config):
        self.set_columns(config)
        if not isinstance(self._columns, Collection):
            raise SqlSelectorConfigInvalid('collection expected for columns')
        self.set_query(config)
        if not sqlite3.complete_statement(self._query + ';'):
            raise SqlSelectorConfigInvalid('valid SQL statement expected for query')

    def columns(self):
        return list(self._columns)

    def apply(self, data: Data):
        yield from (row[:len(self._columns)] for row in data.cursor.execute(self._query))
