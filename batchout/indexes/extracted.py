from typing import Dict, Any

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.indexes import Index
from batchout.extractors import Extractor, DEFAULT_EXTRACTOR


class IndexConfigInvalid(Exception):
    pass


with_path = with_config_key('path', raise_exc=IndexConfigInvalid)
with_extractor = with_config_key('extractor', raise_exc=IndexConfigInvalid,
                                 default=DEFAULT_EXTRACTOR.bound_name,
                                 extractors=Registry.BOUND.get(Extractor, set()))


@with_path
@with_extractor
class ExtractedIndex(Index):

    def __init__(self, config: Dict[str, Any]):
        self.set_path(config)
        self.set_extractor(config)
        self._extractor = Registry.create(Extractor, {**config, 'type': self._extractor})

    def _extract(self, payload):
        return self._extractor.extract(self._path, payload)

    def values(self, payload):
        raise NotImplementedError


@Registry.bind(Index, 'for_list')
class IndexForList(ExtractedIndex):

    def values(self, payload):
        _, li = self._extract(payload)
        if isinstance(li, list):
            return list(range(len(li)))
        return []


@Registry.bind(Index, 'for_object')
class IndexForObject(ExtractedIndex):

    def values(self, payload):
        _, ob = self._extract(payload)
        if isinstance(ob, dict):
            return list(ob.keys())
        return []


@Registry.bind(Index, 'from_list')
class IndexFromList(ExtractedIndex):

    def values(self, payload):
        _, li = self._extract(payload)
        if isinstance(li, list):
            return list(li)
        return []
