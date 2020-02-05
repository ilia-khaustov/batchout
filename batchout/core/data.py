import sqlite3

import arrow


class Data(object):

    def __init__(self, *columns, **types):
        self._columns = columns
        self._types = {c: types.get(c, 'string') for c in columns}
        self._sources = list()
        self._len = 0
        self._db = None
        self._cursor = None

    @property
    def columns(self):
        return list(self._columns)

    @property
    def sources(self):
        return list(self._sources)

    @property
    def cursor(self):
        if not self._db:
            self._db = sqlite3.connect(':memory:')
        if not self._cursor:
            self._cursor = self._db.cursor()
        return self._cursor

    def reset(self):
        self._cursor.close()
        self._cursor = None
        self._db.close()
        self._db = None
        self._sources = list()
        self._len = 0
        return self

    def with_sources(self, *sources):
        for source in sources:
            if source not in self._sources:
                self.cursor.execute(f"CREATE TABLE {source}({','.join(self._columns)})")
                self._sources.append(source)
        return self

    def with_row(self, source, row):
        self.with_sources(source)

        __values = ','.join(['?'] * len(self._columns))
        __columns = ','.join(self._columns)
        row = tuple(row)[:len(self._columns)]

        self.cursor.execute(f"INSERT INTO {source}({__columns}) VALUES ({__values})", row)
        self._len += 1

        return self

    def rows(self, source):
        if source not in self._sources:
            return None
        return [
            self._with_types_cast(row)
            for row
            in self.cursor.execute(f"SELECT {','.join(self.columns)} FROM {source}")
        ]

    def _with_types_cast(self, row):
        newrow = list(row)
        for col, val, i in zip(self._columns, row, range(len(self._columns))):
            if self._types.get(col) == 'timestamp':
                newrow[i] = arrow.get(val).datetime
        return newrow

    def __len__(self):
        return self._len

    def clone(self):
        cloned = Data(*self.columns)
        for source in self.sources:
            for row in self.rows(source):
                cloned.with_row(source, row)
        return cloned
