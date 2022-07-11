import abc


class Processor:
    PLURAL_ALIAS = 'processors'

    @abc.abstractmethod
    def process(self, value):
        raise NotImplementedError
