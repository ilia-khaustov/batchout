import json
from typing import Dict, Any

from jsonpath_rw import parse

from batchout.core.mixin import WithStrategy
from batchout.core.registry import Registry
from batchout.extractors import Extractor


class JsonpathExtractorConfigInvalid(Exception):
    pass


@Registry.bind(Extractor, 'jsonpath')
class JsonpathExtractor(Extractor, WithStrategy):

    def __init__(self, config: Dict[str, Any]):
        self._parser = None
        self._path = None
        self.set_strategy(config)

    def _prepare(self, path: str):
        if self._path != path:
            self._parser = parse(path)
            self._path = path

    def extract(self, path: str, payload: bytes):
        self._prepare(path)
        payload = json.loads(payload)
        datums = parse(path).find(payload)
        return self._apply_strategy((str(datum.full_path), datum.value) for datum in datums)

