from typing import Any, Iterable

from ...core.config import with_config_key


class UnknownStrategy(Exception):
    pass


class StrategyNotSet(Exception):
    pass


with_strategy = with_config_key(
    'strategy',
    default='take_first',
    choices=('take_first', 'take_first_not_null', 'take_last', 'take_last_not_null', 'take_all'),
)


@with_strategy
class WithStrategy(object):

    def _apply_strategy(self, results: Iterable[tuple[str, Any]]) -> tuple[str, Any]:
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
            p, v = tuple(zip(*results)) or (None, None)
        else:
            raise UnknownStrategy(self._strategy)
        return p, v
