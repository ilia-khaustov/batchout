from functools import partial
from itertools import accumulate
from typing import Dict, Any

import arrow

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.columns import Column
from batchout.extractors import Extractor
from batchout.processors import Processor


class ColumnConfigInvalid(Exception):
    pass


class UnknownCast(Exception):
    pass


with_path = with_config_key('path', raise_exc=ColumnConfigInvalid)
with_cast = with_config_key('cast', raise_exc=ColumnConfigInvalid,
                            casts=('string', 'integer', 'float', 'timestamp', 'date'))
with_extractor = with_config_key('extractor', raise_exc=ColumnConfigInvalid,
                                 extractors=Registry.BOUND.get(Extractor, set()))
with_processors = with_config_key('processors')


@with_path
@with_cast
@with_extractor
@with_processors
@with_config_key('timezone', default='UTC')
@Registry.bind(Column, 'extracted')
class ExtractedColumn(Column):

    def __init__(self, config: Dict[str, Any]):
        self.set_path(config)
        self.set_cast(config)
        self.set_extractor(config)
        self._extractor = Registry.create(Extractor, {**config, 'type': self._extractor})
        self._processors = None
        self.set_processors(config)
        self._init_processors()
        self.set_timezone(config)
        self._validate_timezone()

    def _init_processors(self):
        if self._processors is not None:
            self._processors = list(map(partial(Registry.create, Processor), self._processors))
        else:
            self._processors = []

    def _validate_timezone(self):
        if self._timezone is None:
            return
        try:
            arrow.now(self._timezone)
        except arrow.ParserError:
            raise ColumnConfigInvalid(f'{self._timezone} is not a valid timezone')

    def _extract(self, path, payload):
        return self._extractor.extract(path, payload)

    def _process(self, value):
        return tuple(accumulate([value] + self._processors, lambda v, p: p.process(v)))[-1]

    def value(self, payload, **indexes):
        _, v = self._extract(self._path.format(**indexes), payload)
        if v is None:
            return None
        try:
            return self.cast(self._process(v))
        except (ValueError, TypeError):
            return None

    def cast(self, value):
        if self._cast == 'string':
            return str(value)
        elif self._cast == 'integer':
            return int(value)
        elif self._cast == 'float':
            return float(value)
        elif self._cast == 'timestamp':
            return arrow.get(value).to(self._timezone).datetime
        elif self._cast == 'date':
            return arrow.get(value).date
        else:
            raise UnknownCast(self._cast)
