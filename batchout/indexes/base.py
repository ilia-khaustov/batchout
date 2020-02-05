import abc

from batchout.extractors import Extractor


class Index(object):

    @abc.abstractmethod
    def values(self, extractor: Extractor, payload):
        raise NotImplementedError
