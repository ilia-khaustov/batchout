import abc


class Extractor(object):

    @abc.abstractmethod
    def extract(self, path, payload):
        raise NotImplementedError
