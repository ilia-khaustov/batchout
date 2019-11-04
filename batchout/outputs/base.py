import abc

from batchout.core.data import Data


class Output(object):

    @abc.abstractmethod
    def ingest(self, data: Data):
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError
