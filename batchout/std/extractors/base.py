import abc
from typing import Optional, Any


class Extractor:
    PLURAL_ALIAS = 'extractors'

    first_index = 0

    @abc.abstractmethod
    def extract(self, path: str, payload: bytes) -> tuple[Optional[str], Optional[Any]]:
        raise NotImplementedError
