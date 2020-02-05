import json
import logging
from typing import Dict, Any

from jsonpath_rw import parse

from batchout.core.mixin import WithStrategy
from batchout.core.registry import Registry
from batchout.extractors import Extractor


log = logging.getLogger(__name__)


class JsonpathExtractorConfigInvalid(Exception):
    pass


@Registry.bind(Extractor, 'jsonpath')
class JsonpathExtractor(Extractor, WithStrategy):

    def __init__(self, config: Dict[str, Any]):
        self._parsers = {}
        self.set_strategy(config)
        if self._strategy not in self.jsonpath_strategies:
            raise JsonpathExtractorConfigInvalid('strategy must be one of %s', self.jsonpath_strategies)

    def _prepare(self, path: str):
        if path not in self._parsers:
            self._parsers[path] = parse(path)

    def extract(self, path: str, payload: bytes):
        self._prepare(path)
        try:
            payload = json.loads(payload)
            datums = self._parsers[path].find(payload)
            return self._apply_strategy((str(datum.full_path), datum.value) for datum in datums)
        except Exception as e:
            log.error('Failed to extract "%s" from JSON: %s', path, e)
            return None, None
