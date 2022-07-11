from typing import IO, Optional, Iterable, Mapping
from glob import glob

from ...core.config import with_config_key
from ...core.registry import Registry
from .base import Input


class FileInputConfigInvalid(Exception):
    pass


@with_config_key('chunk_endswith', doc='Sequence of bytes delimiting chunks of data, used to split file in chunks')
@with_config_key('chunk_bytes', doc='Maximum number of bytes per chunk, used to split file in chunks')
@with_config_key('recursive', doc='Recursively scan all files matching path', default=False, choices=[True, False])
@with_config_key('path', doc='Path to a file to read from; can be a glob mask', raise_exc=FileInputConfigInvalid)
@Registry.bind(Input, 'file')
class FileInput(Input):

    def __init__(self, config: Mapping):
        self.set_path(config)
        self.set_recursive(config)
        self.set_chunk_bytes(config)
        try:
            self._chunk_bytes = int(self._chunk_bytes) if self._chunk_bytes is not None else None
        except ValueError:
            raise FileInputConfigInvalid('chunk_bytes should be an integer, given: %s', self._chunk_bytes)
        self.set_chunk_endswith(config)
        self._glob_path: Optional[str] = None
        self._glob: Optional[Iterable[str]] = None
        self._active_path: Optional[str] = None
        self._filestream: Optional[IO] = None
        self._buffer: bytes = b''

    def _close(self) -> None:
        if self._filestream is not None and not self._filestream.closed:
            self._filestream.close()
        self._filestream = None
        self._buffer = b''

    def fetch(self, **params) -> Optional[bytes]:
        glob_path = self._path.format(**params)
        if self._glob_path != glob_path:
            self._glob_path = glob_path
            self._glob = iter(glob(glob_path, recursive=self._recursive))
            self._active_path = None
            self._close()
        path, payload = '', None
        while path is not None and payload is None:
            path = self._active_path or next(self._glob, None)
            if path is not None:
                payload = self._fetch_from_file(path)
            if payload is None:
                self._active_path = None
        return payload

    def _fetch_from_file(self, path: str) -> Optional[bytes]:
        if self._active_path == path and self._filestream is None:
            return
        elif self._active_path and self._active_path != path:
            self._close()
        self._active_path = path
        self._filestream = self._filestream or open(self._active_path, mode='rb')
        if self._chunk_bytes is None and self._chunk_endswith is None:
            payload = self._filestream.read()
            self._close()
            return payload or None
        elif self._chunk_bytes is not None:
            chunk_size = self._chunk_bytes
            diff_size = chunk_size - len(self._buffer)
            if diff_size > 0:
                payload, self._buffer = self._buffer + self._filestream.read(diff_size), b''
            else:
                payload, self._buffer = self._buffer[:chunk_size], self._buffer[chunk_size:]
            if self._chunk_endswith:
                mark = self._chunk_endswith.encode()
                payload, *rest = payload.split(mark, 1)
                if rest:
                    payload += mark
                    self._buffer = rest[0] + self._buffer
            if not payload and not self._buffer:
                self._close()
            return payload or None
        elif self._chunk_endswith is not None:
            mark = self._chunk_endswith.encode()
            mark_size = len(mark)
            payload, self._buffer = self._buffer, b''
            while not payload or mark not in payload:
                diff = self._filestream.read(mark_size)
                if not diff:
                    break
                payload += diff
            payload, *rest = payload.split(mark, 1)
            if rest:
                payload += mark
                self._buffer = rest[0] + self._buffer
            if not payload and not self._buffer:
                self._close()
            return payload or None

    def commit(self):
        pass

    def reset(self):
        self._glob_path = None
        self._glob = None
        self._active_path = None
        self._close()
