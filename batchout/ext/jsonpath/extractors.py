import json
import logging
from typing import Dict, Any, Optional

from jsonpath_rw import parse

from ...core.config import with_config_key
from ...core.registry import Registry
from ...std import Extractor
from ...std.extractors.mixin import WithStrategy


log = logging.getLogger(__name__)

with_jsonpath_strategy = with_config_key(
    'strategy',
    default='take_first',
    choices=('take_first', 'take_first_not_null', 'take_last', 'take_last_not_null'),
)


class JsonpathExtractorConfigInvalid(Exception):
    pass


@with_jsonpath_strategy
@Registry.bind(Extractor, 'jsonpath')
class JsonpathExtractor(Extractor, WithStrategy):

    def __init__(self, config: Dict[str, Any]):
        self._parsers = {}
        self.set_strategy(config)
        if self._strategy not in self.strategy_choices:
            raise JsonpathExtractorConfigInvalid('strategy must be one of %s', self.strategy_choices)

    def _prepare(self, path: str):
        if path not in self._parsers:
            self._parsers[path] = parse(path)

    def extract(self, path: str, payload: bytes) -> tuple[Optional[str], Optional[Any]]:
        self._prepare(path)
        try:
            payload = json.loads(payload)
            datums = self._parsers[path].find(payload)
            return self._apply_strategy((str(datum.full_path), datum.value) for datum in datums)
        except Exception as exc:
            log.error('Failed to extract "%s" from JSON: %s', path, exc)
            return None, None
