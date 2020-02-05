import abc
from typing import Any, Generator, List

from batchout.core.data import Data


class Selector(object):

    @abc.abstractmethod
    def columns(self) -> List[Any]:
        raise NotImplementedError

    @abc.abstractmethod
    def apply(self, data: Data) -> Generator[List[Any], None, None]:
        raise NotImplementedError
