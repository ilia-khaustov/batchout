import abc
from typing import Any


class Input:
    PLURAL_ALIAS = 'inputs'

    @abc.abstractmethod
    def fetch(self, **params: Any) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def reset(self) -> None:
        raise NotImplementedError
