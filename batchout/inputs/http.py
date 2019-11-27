import logging

import requests

from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.inputs import Input


log = logging.getLogger(__name__)


class HttpInputConfigInvalid(Exception):
    pass


@with_config_key('uri', raise_exc=HttpInputConfigInvalid)
@with_config_key('method', default='get', raise_exc=HttpInputConfigInvalid,
                 http_verbs=['get', 'post', 'put', 'delete', 'head'])
@with_config_key('headers')
@with_config_key('timeout_sec', default=60)
@Registry.bind(Input, 'http')
class HttpInput(Input):

    def __init__(self, config):
        self.set_uri(config)
        self.set_method(config)
        self.set_timeout_sec(config)
        if not isinstance(self._timeout_sec, int):
            raise HttpInputConfigInvalid('integer expected for timeout_sec')
        self.set_headers(config)
        if self._headers and not isinstance(self._headers, dict):
            raise HttpInputConfigInvalid('mapping expected for headers')
        self._response = None

    def fetch(self):
        if self._response:
            return
        self._response = requests.request(
            self._method,
            self._uri,
            headers=dict(self._headers) if self._headers else None,
            timeout=float(self._timeout_sec)
        )
        self._response.raise_for_status()
        return self._response.content

    def commit(self):
        pass

    def reset(self):
        self._response = None
