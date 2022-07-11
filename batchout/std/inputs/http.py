import logging
import time
from collections import OrderedDict
from contextlib import closing
from http.client import HTTPConnection, HTTPSConnection
from typing import Optional, Collection, Mapping
from urllib.parse import urlsplit, quote

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Input


log = logging.getLogger(__name__)


class HttpInputConfigInvalid(Exception):
    pass


class HttpInputBadResponse(Exception):
    pass


@with_config_key('url', doc='Universal Resource Locator', raise_exc=HttpInputConfigInvalid)
@with_config_key('method', doc='HTTP verb', default='get', choices=['get', 'post', 'put', 'delete', 'head'])
@with_config_key('headers', doc='A mapping of header names to header values')
@with_config_key('timeout_sec', doc='Define after how many seconds consider request had no answer', default=60)
@with_config_key('params', doc='Default values for arbitrary params')
@with_config_key('ignore_status_codes', doc='Return None in case of response status code being one of theses')
@with_config_key('retries', doc='Retry request exact number of times in case of empty response', default=3)
@with_config_key('max_backoff_sec', doc='Maximum wait between retries', default=60)
@Registry.bind(Input, 'http')
class HttpInput(Input):

    def __init__(self, config):
        self.set_url(config)
        self.set_method(config)
        self.set_timeout_sec(config)
        if not isinstance(self._timeout_sec, int):
            raise HttpInputConfigInvalid('integer expected for timeout_sec')
        self.set_headers(config)
        if self._headers and not isinstance(self._headers, Mapping):
            raise HttpInputConfigInvalid('mapping expected for headers')
        self.set_params(config)
        if self._params and not isinstance(self._params, Mapping):
            raise HttpInputConfigInvalid('mapping expected for params')
        self.set_ignore_status_codes(config)
        if self._ignore_status_codes and not isinstance(self._ignore_status_codes, Collection):
            raise HttpInputConfigInvalid('collection expected for ignore_status_codes')
        if self._ignore_status_codes:
            self._ignore_status_codes = tuple(map(int, self._ignore_status_codes))
        self.set_retries(config)
        if not isinstance(self._retries, int) or self._retries < 0:
            raise HttpInputConfigInvalid('positive integer expected for retries')
        self.set_max_backoff_sec(config)
        if not isinstance(self._max_backoff_sec, int) or self._max_backoff_sec < 0:
            raise HttpInputConfigInvalid('positive integer expected for max_backoff_sec')
        self._response = None
        self._fixed_headers = OrderedDict({
            'User-Agent': 'batchout.HttpInput',
            'Accept': '*/*',
            'Connection': 'keep-alive',
        })
        for k, v in (self._headers or {}).items():
            k = '-'.join(p.capitalize() for p in k.strip().split('-'))
            self._fixed_headers[k] = v.strip()

    def fetch(self, **params) -> Optional[bytes]:
        if self._response:
            return
        if self._params:
            params = {
                p: quote(params.get(p, d))
                for p, d in self._params.items()
                if not (p in params and params[p] is None)
            }
            if len(params) < len(self._params):
                return
        else:
            params = {}
        retries = 0
        redirects = 0
        location = self._url
        while self._response is None:
            url = urlsplit(location, allow_fragments=False)
            if url.scheme.lower() == 'https':
                conn_cls = HTTPSConnection
            else:
                conn_cls = HTTPConnection
            conn = conn_cls(host=url.hostname, port=url.port, timeout=float(self._timeout_sec))
            path = url.path
            if url.query:
                path += '?' + url.query
            conn.putrequest(self._method.upper(), path.format(**params))
            for k, v in self._fixed_headers.items():
                conn.putheader(k, v)
            conn.endheaders()
            self._response = conn.getresponse()
            if 300 <= self._response.status < 400 and redirects < 10:
                location = self._response.getheader('Location')
                redirects += 1
                continue
            if self._ignore_status_codes and self._response.status in self._ignore_status_codes:
                return
            if 400 <= self._response.status < 600 and retries < self._retries:
                self._response = None
                retries += 1
                time.sleep(min(self._max_backoff_sec, retries ** 2))
        payload = None
        with closing(self._response) as response:
            if 400 <= response.status < 600:
                raise HttpInputBadResponse(response.read().decode())
            else:
                payload = response.read()
        return payload

    def commit(self):
        pass

    def reset(self):
        self._response = None
