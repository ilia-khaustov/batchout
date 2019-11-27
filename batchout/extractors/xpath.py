from typing import Dict, Any

from lxml import etree

from batchout.core.config import with_config_key
from batchout.core.mixin import WithStrategy
from batchout.core.registry import Registry
from batchout.extractors import Extractor


class XPathExtractorConfigInvalid(Exception):
    pass


@with_config_key('html', default=False, yn=(True, False))
@Registry.bind(Extractor, 'xpath')
class XPathExtractor(Extractor, WithStrategy):

    def __init__(self, config: Dict[str, Any]):
        self._path = None
        self._parser = None
        self._payload = None
        self._root = None
        self.set_strategy(config)
        self.set_html(config)

    def _prepare(self, path: str, payload: bytes):
        if self._path != path:
            self._parser = etree.XPath(path)
            self._path = path
        if self._payload != payload:
            if self._html:
                self._root = etree.HTML(payload.decode('utf8'))
            else:
                self._root = etree.fromstring(payload.decode('utf8'))
            self._payload = payload

    def extract(self, path: str, payload: bytes):
        self._prepare(path, payload)
        results = self._parser(self._root)
        if not isinstance(results, list):
            results = [results]

        return self._apply_strategy(
            (self._root.getpath(res), res)
            if (etree.iselement(res) and hasattr(self._root, 'getpath'))
            else (path, res)
            for res in results
        )
