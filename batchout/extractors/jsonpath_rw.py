import json
from typing import Dict, Any

from jsonpath_rw import jsonpath, parse

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.extractors import Extractor


class JsonpathrwExtractorConfigInvalid(Exception):
    pass


class UnknownStrategy(Exception):
    pass


with_strategy = with_config_key(
    'strategy',
    default='take_first',
    strategies=('take_first', 'take_first_not_null', 'take_last', 'take_last_not_null')
)


@with_strategy
@Registry.bind(Extractor, 'jsonpath_rw')
class JsonpathrwExtractor(Extractor):

    def __init__(self, config: Dict[str, Any]):
        self.set_strategy(config)
        self._parsers: Dict[str, jsonpath.JSONPath] = {}

    def extract(self, path: str, payload: str):
        payload = json.loads(payload)
        if path not in self._parsers:
            self._parsers[path] = parse(path)
        datums = self._parsers[path].find(payload)
        full_path, value = None, None
        if self._strategy == self.strategy_take_first:
            for datum in datums:
                return str(datum.full_path), datum.value
        elif self._strategy == self.strategy_take_first_not_null:
            for datum in datums:
                if datum.value is not None:
                    return str(datum.full_path), datum.value
        elif self._strategy == self._strategy_take_last:
            for datum in datums:
                full_path, value = str(datum.full_path), datum.value
        elif self._strategy == self.strategy_take_last_not_null:
            for datum in datums:
                if datum.value is not None:
                    full_path, value = str(datum.full_path), datum.value
        else:
            raise UnknownStrategy(self._strategy)
        return full_path, value
