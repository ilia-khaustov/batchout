from functools import partial
from itertools import accumulate

import arrow

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.processors import Processor


class UnknownStrategy(Exception):
    pass


class StrategyNotSet(Exception):
    pass


class InvalidTimezone(Exception):
    pass


with_strategy = with_config_key(
    'strategy',
    default='take_first',
    jsonpath_strategies=('take_first', 'take_first_not_null', 'take_last', 'take_last_not_null'),
    xpath_strategies=('take_first', 'take_last', 'take_all'),
)


@with_strategy
class WithStrategy(object):

    def _apply_strategy(self, results):
        if not hasattr(self, '_strategy'):
            raise StrategyNotSet

        p, v = None, None
        if self._strategy == self.strategy_take_first:
            for path, value in results:
                return path, value
        elif self._strategy == self.strategy_take_first_not_null:
            for path, value in results:
                if value is not None:
                    return path, value
        elif self._strategy == self.strategy_take_last:
            for path, value in results:
                p, v = path, value
        elif self._strategy == self.strategy_take_last_not_null:
            for path, value in results:
                if value is not None:
                    p, v = path, value
        elif self._strategy == self.strategy_take_all:
            return None, list(results)
        else:
            raise UnknownStrategy(self._strategy)
        return p, v


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
            arrow.now(self._timezone)
        except arrow.ParserError:
            raise InvalidTimezone(f'{self._timezone} is not a valid timezone')


with_processors = with_config_key(
    'processors',
)


@with_processors
class WithProcessors(object):

    def _init_processors(self):
        if self._processors is not None:
            self._processors = list(map(partial(Registry.create, Processor), self._processors))
        else:
            self._processors = []

    def _process(self, value):
        return tuple(accumulate([value] + self._processors, lambda v, p: p.process(v)))[-1]