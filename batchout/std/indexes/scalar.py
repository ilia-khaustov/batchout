from collections.abc import Sized, Iterable, Mapping
from typing import Any

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Index


class ScalarIndexConfigInvalid(Exception):
    pass


@with_config_key('path', doc='Path expression to use for extracting index values', raise_exc=ScalarIndexConfigInvalid)
@with_config_key('extractor', doc='Type of [Extractor](02_extractors.md) to use', raise_exc=ScalarIndexConfigInvalid)
class ScalarIndex(Index):

    def __init__(self, config: dict[str, Any]):
        self.set_path(config)
        self.set_extractor(config)

    def values(self, extractor, payload, **parent_indexes):
        raise NotImplementedError


@Registry.bind(Index, 'for_list')
class IndexForList(ScalarIndex):

    def values(self, extractor, payload, **parent_indexes):
        _, li = extractor.extract(self._path.format(**parent_indexes), payload)
        if not isinstance(li, (str, bytes)) and isinstance(li, Sized):
            return list(range(extractor.first_index, len(li) + extractor.first_index))
        return []


@Registry.bind(Index, 'for_object')
class IndexForObject(ScalarIndex):

    def values(self, extractor, payload, **parent_indexes):
        _, ob = extractor.extract(self._path.format(**parent_indexes), payload)
        if isinstance(ob, Mapping):
            return list(ob.keys())
        return []


@Registry.bind(Index, 'from_list')
class IndexFromList(ScalarIndex):

    def values(self, extractor, payload, **parent_indexes):
        _, li = extractor.extract(self._path.format(**parent_indexes), payload)
        if not isinstance(li, (str, bytes)) and isinstance(li, Iterable):
            return list(li)
        return []
