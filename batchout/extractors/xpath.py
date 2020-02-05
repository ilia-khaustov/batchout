import logging
from typing import Dict, Any
from functools import lru_cache

from lxml import etree

from batchout.core.config import with_config_key
from batchout.core.mixin import WithStrategy
from batchout.core.registry import Registry
from batchout.extractors import Extractor


log = logging.getLogger(__name__)


class XPathExtractorConfigInvalid(Exception):
    pass


@with_config_key('html', default=False, yn=(True, False))
@Registry.bind(Extractor, 'xpath')
class XPathExtractor(Extractor, WithStrategy):

    def __init__(self, config: Dict[str, Any]):
        self._parsers = {}
        self._root = None
        self.set_strategy(config)
        if self._strategy not in self.xpath_strategies:
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
