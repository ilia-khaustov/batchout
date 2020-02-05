import abc


class Input(object):

    @abc.abstractmethod
    def fetch(self, **params):
        raise NotImplementedError

    def __iter__(self):
        payload = self.fetch()
        while payload:
            yield payload
            payload = self.fetch()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def reset(self):
        raise NotImplementedError
