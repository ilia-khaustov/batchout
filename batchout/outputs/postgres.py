import uuid
import logging
from itertools import repeat, chain

import psycopg2

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.outputs import Output


log = logging.getLogger(__name__)


class PostgresOutputConfigInvalid(Exception):
    pass


with_mode = with_config_key(
    'mode',
    raise_exc=PostgresOutputConfigInvalid,
    modes=('insert', 'upsert')
)


@with_mode
@with_config_key('host', raise_exc=PostgresOutputConfigInvalid)
@with_config_key('port', raise_exc=PostgresOutputConfigInvalid)
@with_config_key('dbname', raise_exc=PostgresOutputConfigInvalid)
@with_config_key('table', raise_exc=PostgresOutputConfigInvalid)
@with_config_key('user', raise_exc=PostgresOutputConfigInvalid)
@with_config_key('password', raise_exc=PostgresOutputConfigInvalid)
@with_config_key('keys')
@Registry.bind(Output, 'postgres')
class PostgresOutput(Output):

    def __init__(self, config):
        self.set_host(config)
        self.set_port(config)
        self.set_dbname(config)
        self.set_table(config)
        self.set_user(config)
        self.set_password(config)
        self.set_keys(config)
        self.set_mode(config)
        if self._mode == 'upsert' and self._keys is None:
            raise PostgresOutputConfigInvalid('missing keys with mode=upsert')
        self._connection, self._cursor = None, None

    @property
    def connection(self):
        if not self._connection:
            self._connection = psycopg2.connect(host=self._host, port=self._port,
                                                dbname=self._dbname, user=self._user, password=self._password)
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

    def ingest(self, cols, rows):
        fix = uuid.uuid1().hex
        batch_tbl = f'batch_{fix}'
        cdiff_tbl = f'cdiff_{fix}'
        udiff_tbl = f'udiff_{fix}'

        key_cols = list(filter(lambda c: c in self._keys, cols))
        val_cols = list(filter(lambda c: c not in self._keys, cols))

        rows = list(row for row in rows if all(val is not None for col, val in zip(cols, row) if col in key_cols))
        if len(rows) < 1:
            return 0

        batch_values = list(chain.from_iterable(rows))

        _values_tpl = ",".join(f"({r})" for r in repeat((",%s" * len(cols))[1:], len(rows)))
        _just_cols = ','.join(list(map(lambda c: f'"{c}"', cols)))
        _batch_cols = ','.join(list(map(lambda c: f'{batch_tbl}."{c}"', cols)))

        def _set_from(t):
            return ','.join([f'{c}={t}.{c}' for c in val_cols])

        def _same_as(t):
            return ' AND '.join([f'{self._table}.{c}={t}.{c}' for c in key_cols])

        __create_batch = (
            f'CREATE TEMP TABLE {batch_tbl} ON COMMIT DROP AS '
            f'SELECT {_just_cols} FROM {self._table} LIMIT 0'
        )

        __insert_into_batch = (
            f'INSERT INTO {batch_tbl} ({_just_cols}) (VALUES{_values_tpl})'
        )

        __create_cdiff = (
            f'CREATE TEMP TABLE {cdiff_tbl} ON COMMIT DROP AS '
            f'SELECT {_batch_cols} FROM {batch_tbl} '
            f'WHERE NOT EXISTS (SELECT 1 FROM {self._table} WHERE {_same_as(batch_tbl)}) '
        )
        __create_udiff = (
            f'CREATE TEMP TABLE {udiff_tbl} ON COMMIT DROP AS '
            f'SELECT * FROM {batch_tbl} '
            'EXCEPT '
            f'SELECT * FROM {cdiff_tbl} '
        )
        __insert_from_cdiff = (
            f'INSERT INTO {self._table} ({_just_cols}) SELECT {_just_cols} FROM {cdiff_tbl}'
        )
        __insert_from_batch = (
            f'INSERT INTO {self._table} ({_just_cols}) SELECT {_just_cols} FROM {batch_tbl}'
        )
        __update_from_udiff = (
            f'UPDATE {self._table} SET {_set_from(udiff_tbl)} FROM {udiff_tbl} WHERE {_same_as(udiff_tbl)}'
        )

        try:
            log.debug(f'Starting transaction with {len(rows)} rows')
            self.cursor.execute(__create_batch)
            self.cursor.execute(__insert_into_batch, batch_values)
            if self._mode == self.mode_upsert:
                self.cursor.execute(__create_cdiff)
                self.cursor.execute(__create_udiff)
                self.cursor.execute(__insert_from_cdiff)
                self.cursor.execute(__update_from_udiff)
            elif self._mode == self.mode_insert:
                self.cursor.execute(__insert_from_batch)
        except psycopg2.Error:
            self._close_db(commit=False)
            raise

        return len(rows)

    def commit(self):
        self._close_db()
        log.debug(f'Committed transaction')
