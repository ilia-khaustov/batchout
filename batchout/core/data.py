from __future__ import annotations

from collections import defaultdict
from datetime import datetime, date
from typing import Collection, Any
import sqlite3


sqlite3.register_adapter(datetime, datetime.isoformat)
sqlite3.register_adapter(date, date.isoformat)


class Data:

    def __init__(self, *columns: str, **types: str):
        self._columns = columns
        self._types = {c: types.get(c, 'string') for c in columns}
        self._sources = list()
        self._len = 0
        self._len_per_source = defaultdict(int)
        self._db = None
        self._cursor = None

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def sources(self) -> list[str]:
        return list(self._sources)

    @property
    def cursor(self) -> sqlite3.Cursor:
        if not self._db:
            self._db = sqlite3.connect(':memory:')
        if not self._cursor:
            self._cursor = self._db.cursor()
        return self._cursor

    def reset(self) -> Data:
        self._cursor.close()
        self._cursor = None
        self._db.close()
        self._db = None
        self._sources = list()
        self._len = 0
        self._len_per_source = defaultdict(int)
        return self

    def with_sources(self, *sources: str) -> Data:
        for source in sources:
            if source not in self._sources:
                self.cursor.execute(f"CREATE TABLE {source}({','.join(self._columns)})")
                self._sources.append(source)
        return self

    def with_row(self, source: str, *rows: Collection[Any]) -> Data:
        self.with_sources(source)
        if not rows:
            return self

        __values = ','.join(['?'] * len(self._columns))
        __columns = ','.join(self._columns)

        self.cursor.executemany(
            f"INSERT INTO {source}({__columns}) VALUES ({__values})",
            (tuple(row)[:len(self._columns)] for row in rows)
        )
        self._len += self.cursor.rowcount
        self._len_per_source[source] += self.cursor.rowcount
        return self

    def rows(self, source: str) -> list[list[Any]]:
        if source not in self._sources:
            return []
        return list(map(
            self._with_typecasts,
            self.cursor.execute(f"SELECT {','.join(self.columns)} FROM {source}")
        ))

    def _with_typecasts(self, row: Collection[Any]) -> list[Any]:
        typed_row = list(row)
        for col, val, i in zip(self._columns, row, range(len(self._columns))):
            if self._types.get(col) == 'datetime':
                typed_row[i] = datetime.fromisoformat(val) if val else None
            elif self._types.get(col) == 'date':
                typed_row[i] = datetime.fromisoformat(val) if val else None
            elif self._types.get(col) == 'boolean':
                typed_row[i] = bool(val)
        return typed_row

    def __len__(self) -> int:
        return self._len

    def count(self, *sources: str) -> int:
        return sum([self._len_per_source[src] for src in sources])

    def clone(self) -> Data:
        cloned = Data(*self.columns)
        for source in self.sources:
            cloned.with_row(source, *self.rows(source))
        return cloned
