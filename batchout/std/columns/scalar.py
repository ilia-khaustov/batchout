import logging
from datetime import datetime, date
from typing import Dict, Any

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Column
from .mixin import WithTimezone, WithProcessors

log = logging.getLogger(__name__)


class ColumnConfigInvalid(Exception):
    pass


@with_config_key('path', raise_exc=ColumnConfigInvalid, doc='Expression used to search for a value by Extractor')
@with_config_key('extractor', raise_exc=ColumnConfigInvalid, doc='Connect to one of [Extractors](02_extractors.md)')
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


@with_config_key('format',
                 doc='Datetime format string compatible with `datetime.strptime()`')
@with_config_key('parser', default='iso', choices=('iso', 'unix', 'custom'),
                 doc='`iso` to parse ISO format, `unix` to convert from Unix epoch, '
                     '`custom` to use `format` from config and parse with `datetime.strptime()`')
@with_config_key('timezone', default='UTC',
                 doc='Convert timezone-aware datetime or set for timezone-naive datetime')
@Registry.bind(Column, 'datetime')
class DatetimeColumn(ScalarColumn, WithTimezone):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(DatetimeColumn, self).__init__(config)
        self.set_timezone(config)
        self._validate_timezone()
        self.set_parser(config)
        self.set_format(config)
        if self._parser == self.parser_custom:
            if self._format is None:
                raise ColumnConfigInvalid('format is required for parser=custom')
            try:
                datetime.now(tz=self._timezone).strftime(self._format)
            except (ValueError, TypeError) as exc:
                raise ColumnConfigInvalid('format "%s" is invalid: %s', self._format, str(exc))

    def cast(self, value: Any) -> datetime:
        if self._parser == self.parser_iso:
            v = str(value)
            if v.endswith('Z'):  # workaround for datetime failing to recognize Z as UTC offset
                v = v.removesuffix('Z') + '+00:00'
            dt = datetime.fromisoformat(v)
        elif self._parser == self.parser_unix:
            dt = datetime.fromtimestamp(float(value))
        else:
            dt = datetime.strptime(str(value), self._format)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=self._timezone)
        elif dt.tzinfo != self._timezone:
            return dt.astimezone(self._timezone)
        else:
            return dt


@with_config_key('format',
                 doc='Datetime format string compatible with `datetime.strptime()`')
@with_config_key('parser', default='iso', choices=('iso', 'unix', 'custom'),
                 doc='`iso` to parse ISO format, `unix` to convert from Unix epoch, '
                     '`custom` to use `format` from config and parse with `datetime.strptime()`')
@with_config_key('timezone', default='UTC',
                 doc='Convert timezone-aware datetime parsed from custom format before extraction of date')
@Registry.bind(Column, 'date')
class DateColumn(ScalarColumn, WithTimezone):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(DateColumn, self).__init__(config)
        self.set_timezone(config)
        self._validate_timezone()
        self.set_parser(config)
        self.set_format(config)
        if self._parser == self.parser_custom:
            if self._format is None:
                raise ColumnConfigInvalid('format is required for parser=custom')
            try:
                datetime.now(tz=self._timezone).strftime(self._format)
            except (ValueError, TypeError) as exc:
                raise ColumnConfigInvalid('format "%s" is invalid: %s', self._format, str(exc))

    def cast(self, value: Any) -> date:
        if self._parser == self.parser_iso:
            v = str(value)
            d = date.fromisoformat(v)
        elif self._parser == self.parser_unix:
            d = date.fromtimestamp(float(value))
        else:
            dt = datetime.strptime(str(value), self._format)
            if dt.tzinfo and dt.tzinfo != self._timezone:
                dt = dt.astimezone(self._timezone)
            d = dt.date()
        return d


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


@Registry.bind(Column, 'boolean')
class BooleanColumn(ScalarColumn):

    value = ScalarColumn.value

    def __init__(self, config: Dict[str, Any]):
        super(BooleanColumn, self).__init__(config)

    def cast(self, value):
        return bool(value)
