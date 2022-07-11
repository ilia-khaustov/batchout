import abc
from typing import Any, Collection, Iterable


class Output:
    PLURAL_ALIAS = 'outputs'

    @abc.abstractmethod
    def ingest(self, columns: Collection[str], rows: Iterable[Collection[Any]]) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError
