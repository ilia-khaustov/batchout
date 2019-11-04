import abc


class Column(object):

    @abc.abstractmethod
    def value(self, payload, **indexes):
        raise NotImplementedError
