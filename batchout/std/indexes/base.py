import abc
from typing import Union

from ..extractors import Extractor


class Index:
    PLURAL_ALIAS = 'indexes'

    @abc.abstractmethod
    def values(self, extractor: Extractor, payload: bytes, **parent_indexes: Union[str, int]) -> list[Union[str, int]]:
        raise NotImplementedError
