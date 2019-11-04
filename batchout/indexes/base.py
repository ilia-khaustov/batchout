import abc


class Index(object):

    @abc.abstractmethod
    def values(self, payload):
        raise NotImplementedError
