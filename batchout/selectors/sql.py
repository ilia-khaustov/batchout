import logging
import sqlite3

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.core.data import Data
from batchout.selectors import Selector

log = logging.getLogger(__name__)


class SqlSelectorConfigInvalid(Exception):
    pass


@with_config_key('query', raise_exc=SqlSelectorConfigInvalid)
@with_config_key('columns', raise_exc=SqlSelectorConfigInvalid)
@Registry.bind(Selector, 'sql')
class SqlSelector(Selector):

    def __init__(self, config):
        self.set_columns(config)
        if not isinstance(self._columns, list):
            raise SqlSelectorConfigInvalid('list expected for columns')
        self.set_query(config)
        if not sqlite3.complete_statement(self._query + ';'):
            raise SqlSelectorConfigInvalid('valid SQL statement expected for query')

    def columns(self):
        return list(self._columns)

    def apply(self, data: Data):
        yield from (row[:len(self._columns)] for row in data.cursor.execute(self._query))
