from functools import partial
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ...core.config import with_config_key
from ...core.registry import Registry
from ..processors import Processor


class InvalidTimezone(Exception):
    pass


with_timezone = with_config_key(
    'timezone',
    default='UTC',
)


@with_timezone
class WithTimezone(object):

    def _validate_timezone(self):
        if self._timezone is None:
            return
        try:
            self._timezone = ZoneInfo(self._timezone)
        except ZoneInfoNotFoundError:
            raise InvalidTimezone(f'{self._timezone} is not a valid timezone')


with_processors = with_config_key(
    'processors',
    doc='Apply sequence of Processors to each value from column',
)


@with_processors
class WithProcessors(object):

    def _init_processors(self):
        if self._processors is not None:
            self._processors = list(map(partial(Registry.create, Processor), self._processors))
        else:
            self._processors = []

    def _process(self, value):
        result = value
        for p in self._processors:
            result = p.process(result)
        return result
