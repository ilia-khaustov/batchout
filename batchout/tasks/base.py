import abc
from typing import NewType, Dict


TaskType = NewType('TaskType', str)


class Task(object):

    TYPE_READER = TaskType('reader')
    TYPE_WALKER = TaskType('walker')
    TYPE_WRITER = TaskType('writer')
    TYPES = {TYPE_READER, TYPE_WALKER, TYPE_WRITER}

    @abc.abstractmethod
    def type(self) -> TaskType:
        raise NotImplementedError

    @abc.abstractmethod
    def components(self) -> Dict[str, str]:
        raise NotImplementedError
