import abc
from typing import Any, Iterable, Collection

from ...core.data import Data


class Selector:
    PLURAL_ALIAS = 'selectors'

    @abc.abstractmethod
    def columns(self) -> Collection[Any]:
        raise NotImplementedError

    @abc.abstractmethod
    def apply(self, data: Data) -> Iterable[Collection[Any]]:
        raise NotImplementedError
