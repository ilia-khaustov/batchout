import logging
from typing import Dict, Any

import arrow

from batchout.core.config import with_config_key
from batchout.core.mixin import WithTimezone, WithProcessors
from batchout.core.registry import Registry
from batchout.columns import Column


log = logging.getLogger(__name__)


class ColumnConfigInvalid(Exception):
    pass


@with_config_key('path', raise_exc=ColumnConfigInvalid)
@with_config_key('extractor', raise_exc=ColumnConfigInvalid)
class ScalarColumn(Column, WithProcessors):

    def __init__(self, config: Dict[str, Any]):
        self.set_path(config)
        self._processors = None
        self.set_processors(config)
        self._init_processors()
        self.set_extractor(config)

    def value(self, extractor, payload, **indexes):
        try:
            _, v = extractor.extract(self._path.format(**indexes), payload)
        except KeyError:
            v = None
        if v is None:
            return
        try:
            return self.cast(self._process(v))
        except (ValueError, TypeError) as e:
            log.error('Failed to cast "%s" into <%s>: %s', v, getattr(self, 'bound_name', 'unknown-scalar'), e)
            return

    def cast(self, value):
        raise NotImplementedError


@with_config_key('timezone', default='UTC')
@Registry.bind(Column, 'timestamp')
class TimestampColumn(ScalarColumn, WithTimezone):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(TimestampColumn, self).__init__(config)
        self.set_timezone(config)
        self._validate_timezone()

    def cast(self, value):
        return arrow.get(value).to(self._timezone).datetime


@with_config_key('timezone', default='UTC')
@Registry.bind(Column, 'date')
class DateColumn(ScalarColumn, WithTimezone):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(DateColumn, self).__init__(config)
        self.set_timezone(config)
        self._validate_timezone()

    def cast(self, value):
        return arrow.get(value).to(self._timezone).date


@Registry.bind(Column, 'float')
class FloatColumn(ScalarColumn):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(FloatColumn, self).__init__(config)

    def cast(self, value):
        return float(value)


@Registry.bind(Column, 'integer')
class IntegerColumn(ScalarColumn):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(IntegerColumn, self).__init__(config)

    def cast(self, value):
        return int(value)


@Registry.bind(Column, 'string')
class StringColumn(ScalarColumn):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(StringColumn, self).__init__(config)

    def cast(self, value):
        return str(value)
