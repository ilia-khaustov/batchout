import abc
from typing import NewType, Dict


TaskType = NewType('TaskType', str)


class Task:
    PLURAL_ALIAS = 'tasks'

    TYPE_READER = TaskType('reader')
    TYPE_WRITER = TaskType('writer')
    TYPES = {TYPE_READER, TYPE_WRITER}

    @abc.abstractmethod
    def type(self) -> TaskType:
        raise NotImplementedError

    @abc.abstractmethod
    def components(self) -> Dict[str, str]:
        raise NotImplementedError
