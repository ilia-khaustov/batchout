import re
import logging
from operator import or_
from functools import reduce
from typing import Any, Optional

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Extractor
from .mixin import WithStrategy


log = logging.getLogger(__name__)


class RegexExtractorConfigInvalid(Exception):
    pass


def _fmt_flag(flag: re.RegexFlag) -> str:
    return f'{str(flag)[3]}({str(flag)[4:]})'


# noinspection PyTypeChecker
_exc_flags: set[re.RegexFlag] = {re.RegexFlag.ASCII, re.RegexFlag.LOCALE, re.RegexFlag.UNICODE}
_all_flag_names: tuple[str] = tuple(map(_fmt_flag, re.RegexFlag))
_exc_flag_names: tuple[str] = tuple(map(_fmt_flag, tuple(_exc_flags)))

with_regex_strategy = with_config_key(
    'strategy',
    default='take_first',
    choices=('take_first', 'take_last', 'take_all'),
)


@with_config_key('encoding', default='utf8')
@with_config_key('decode_bytes', doc='Decode bytes to string before using regex', default=True, choices=(True, False))
@with_config_key('flags', doc='Regex flags supported by Python', default_factory=list)
@with_config_key('group', doc='Capture group to extract from, starting from 0 (whole match)', default=0)
@with_regex_strategy
@Registry.bind(Extractor, 'regex')
class RegexExtractor(Extractor, WithStrategy):

    def __init__(self, config: dict[str, Any]):
        self._parsers: dict[str, re.Pattern] = {}
        self.set_strategy(config)
        if self._strategy not in self.strategy_choices:
            raise RegexExtractorConfigInvalid('strategy must be one of %s', self.strategy_choices)
        self.set_flags(config)
        self._re_flags = set()
        for flag in self._flags:
            if not hasattr(re.RegexFlag, flag):
                raise RegexExtractorConfigInvalid('each flag must be one of %s', _all_flag_names)
            self._re_flags.add(getattr(re.RegexFlag, flag))
        if len(_exc_flags.intersection(self._re_flags)) > 1:
            raise RegexExtractorConfigInvalid('these flags are mutually exclusive, choose one: %s', _exc_flag_names)
        self.set_decode_bytes(config)
        self.set_encoding(config)
        self.set_group(config)
        self._group = int(self._group)

    def _prepare(self, path: str):
        if path not in self._parsers:
            self._parsers[path] = re.compile(path, reduce(or_, self._re_flags))

    def extract(self, path: str, payload: bytes) -> tuple[Optional[str], Optional[Any]]:
        try:
            if self._decode_bytes:
                payload = payload.decode(self._encoding)
        except Exception as exc:
            log.error('Failed to decode payload from %s: %s', self._encoding, exc)
            return None, None
        self._prepare(path)
        try:
            return self._apply_strategy((path, m.group(self._group)) for m in self._parsers[path].finditer(payload))
        except Exception as exc:
            log.error('Failed searching "%s" in payload: %s', path, exc)
            return None, None
