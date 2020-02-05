import json
import logging
from itertools import islice

import psycopg2
from psycopg2.extras import RealDictCursor

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.inputs import Input


log = logging.getLogger(__name__)


class PostgresInputConfigInvalid(Exception):
    pass


@with_config_key('host', raise_exc=PostgresInputConfigInvalid)
@with_config_key('port', raise_exc=PostgresInputConfigInvalid)
@with_config_key('dbname', raise_exc=PostgresInputConfigInvalid)
@with_config_key('user', raise_exc=PostgresInputConfigInvalid)
@with_config_key('password', raise_exc=PostgresInputConfigInvalid)
@with_config_key('sql', raise_exc=PostgresInputConfigInvalid)
@with_config_key('limit', default=1000)
@with_config_key('params')
@Registry.bind(Input, 'postgres')
class PostgresInput(Input):

    def __init__(self, config):
        self.set_host(config)
        self.set_port(config)
        self.set_dbname(config)
        self.set_user(config)
        self.set_password(config)
        self.set_params(config)
        if self._params and not isinstance(self._params, list):
            raise PostgresInputConfigInvalid('list expected for params')
        self.set_limit(config)
        if not isinstance(self._limit, int) or self._limit < 0:
            raise PostgresInputConfigInvalid('positive integer expected for limit')
        self.set_sql(config)
        self._connection, self._cursor, self._data = None, None, None

    @property
    def connection(self):
        if not self._connection:
            self._connection = psycopg2.connect(host=self._host, port=self._port, dbname=self._dbname,
                                                user=self._user, password=self._password,
                                                cursor_factory=RealDictCursor)
        return self._connection

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor

    def _close_db(self, commit=True):
        if self._cursor and not self._cursor.closed:
            if commit:
                self._cursor.execute('commit')
            else:
                self._cursor.execute('rollback')
            self._cursor.close()
        self._cursor = None
        if self._connection and not self._connection.closed:
            self._connection.close()
        self._connection = None

    def fetch(self, **params):
        if self._data:
            return next(self._data, None)
        if self._params:
            params = {
                p: params.get(p, d)
                for p, d in self._params.items()
                if not (p in params and params[p] is None)
            }
            if len(params) < len(self._params):
                return
        else:
            params = {}
        try:
            self.cursor.execute(self._sql.format(**params))
            self._data = (json.dumps(row).encode() for row in islice(self.cursor, self._limit))
            return next(self._data, None)
        except psycopg2.Error:
            self._close_db(commit=False)
            raise

    def commit(self):
        self._close_db()

    def reset(self):
        self._data = None
