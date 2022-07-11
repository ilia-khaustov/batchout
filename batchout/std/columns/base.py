import abc

from ..extractors import Extractor


class Column:
    PLURAL_ALIAS = 'columns'

    @abc.abstractmethod
    def value(self, extractor: Extractor, payload: bytes, **indexes: str):
        raise NotImplementedError
