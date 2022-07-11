import logging
from typing import Dict, Any
from functools import lru_cache

from lxml import etree

from ...core.config import with_config_key
from ...core.registry import Registry
from ...std import Extractor
from ...std.extractors.mixin import WithStrategy


log = logging.getLogger(__name__)

with_xpath_strategy = with_config_key(
    'strategy',
    default='take_first',
    choices=('take_first', 'take_last', 'take_all'),
)


class XPathExtractorConfigInvalid(Exception):
    pass


@with_config_key('html', default=False, choices=(True, False))
@with_xpath_strategy
@Registry.bind(Extractor, 'xpath')
class XPathExtractor(Extractor, WithStrategy):

    first_index = 1

    def __init__(self, config: Dict[str, Any]):
        self._parsers = {}
        self._root = None
        self.set_strategy(config)
        if self._strategy not in self.strategy_choices:
            raise XPathExtractorConfigInvalid('strategy must be one of %s', self.xpath_strategies)
        self.set_html(config)

    @lru_cache(maxsize=32)
    def _get_root(self, payload):
        if self._html:
            return etree.HTML(payload.decode('utf8'))
        else:
            return etree.fromstring(payload.decode('utf8'))

    def _prepare(self, path: str, payload: bytes):
        if path not in self._parsers:
            self._parsers[path] = etree.XPath(path)
        self._root = self._get_root(payload)

    def extract(self, path: str, payload: bytes):
        try:
            self._prepare(path, payload)
            results = self._parsers[path](self._root)
        except Exception as e:
            log.error('Failed to extract "%s" from XML: %s', path, e)
            return None, None

        if not isinstance(results, list):
            results = [results]

        return self._apply_strategy(
            (self._root.getpath(res), res)
            if (etree.iselement(res) and hasattr(self._root, 'getpath'))
            else (path, res)
            for res in results
        )
