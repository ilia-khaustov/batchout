import abc

from batchout.extractors import Extractor


class Column(object):

    @abc.abstractmethod
    def value(self, extractor: Extractor, payload: bytes, **indexes: str):
        raise NotImplementedError
