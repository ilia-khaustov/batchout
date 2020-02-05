from typing import Dict, Any

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.indexes import Index
from batchout.extractors import XPathExtractor


class ScalarIndexConfigInvalid(Exception):
    pass


@with_config_key('path', raise_exc=ScalarIndexConfigInvalid)
@with_config_key('extractor', raise_exc=ScalarIndexConfigInvalid)
class ScalarIndex(Index):

    def __init__(self, config: Dict[str, Any]):
        self.set_path(config)
        self.set_extractor(config)

    def values(self, extractor, payload):
        raise NotImplementedError


@Registry.bind(Index, 'for_list')
class IndexForList(ScalarIndex):

    def values(self, extractor, payload):
        _, li = extractor.extract(self._path, payload)
        if isinstance(li, list):
            if extractor.__class__ is XPathExtractor:
                return list(range(1, len(li)+1))
            return list(range(len(li)))
        return []


@Registry.bind(Index, 'for_object')
class IndexForObject(ScalarIndex):

    def values(self, extractor, payload):
        _, ob = extractor.extract(self._path, payload)
        if isinstance(ob, dict):
            return list(ob.keys())
        return []


@Registry.bind(Index, 'from_list')
class IndexFromList(ScalarIndex):

    def values(self, extractor, payload):
        _, li = extractor.extract(self._path, payload)
        if isinstance(li, list):
            return list(li)
        return []
