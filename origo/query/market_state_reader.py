"""Query the market state cube and read its results through the supported reader (PRD-0022).

``query`` asks the local service for a selection and returns the paths of its two Arrow IPC
files. ``open_file`` and ``read_table`` read such a file through the cube reader: before every
read the reader tells the service, which renews the file's 24-hour clock (amendment A01), and
the bytes are returned only after the service confirms the file still exists. A read of a
file already reclaimed raises ``FileNotFoundError``. Plain ``pyarrow`` or memory-mapped reads
of the same paths work but do not renew expiry.

This module needs only the standard library and pyarrow, so a consumer can use it from the
Origo package or from a copy of this file pinned to a release. Result paths are under
``/opt/origo/market-state``; a consuming container mounts the ``tdw-control-plane_market-state``
volume there (read-only is enough) and reaches the service at ``http://127.0.0.1:8486`` from
the host network or ``http://market-state:8486`` from the Compose network::

    from origo.query.market_state_reader import query, read_table

    result = query(t1='2026-09-01T00:00:00Z', t2='2026-09-02T00:00:00Z', tR=900, pR=1000)
    cells = read_table(result.cells)
    summary = read_table(result.summary)

Renewal is keyed by the last two path components, ``<result id>/<file name>``, so a consumer
that mounts the volume elsewhere replaces the ``/opt/origo/market-state`` prefix and reads on.
"""

from __future__ import annotations

import io
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from importlib import import_module
from typing import TYPE_CHECKING, Final, Protocol, cast

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

DEFAULT_URL: Final = 'http://127.0.0.1:8486'
# A refused renewal is retried this long: long enough for a deployment to replace the service.
RETRY_SECONDS: Final = 120.0
QUERY_TIMEOUT_SECONDS: Final = 300.0
ACCESS_TIMEOUT_SECONDS: Final = 10.0


class ArrowTable(Protocol):
    @property
    def num_rows(self) -> int: ...
    def to_pylist(self) -> list[dict[str, object]]: ...


class ArrowFileReader(Protocol):
    @property
    def num_record_batches(self) -> int: ...
    @property
    def schema(self) -> object: ...
    def read_all(self) -> ArrowTable: ...


class _PyArrow(Protocol):
    def PythonFile(self, handle: object, mode: str) -> object: ...


class _IPC(Protocol):
    def open_file(self, source: object) -> ArrowFileReader: ...


class MarketStateError(RuntimeError):
    """The service refused or failed a request; ``body`` is its JSON answer."""

    def __init__(self, status: int, body: Mapping[str, object]) -> None:
        super().__init__(f'Market state service answered {status}: {json.dumps(dict(body))}')
        self.status, self.body = status, body


@dataclass(frozen=True)
class MarketStateResult:
    result_id: str
    cells: str
    summary: str
    expires_at: datetime
    response: Mapping[str, object]


def query(
    *,
    t1: str | None = None,
    t2: str | None = None,
    p1: int | float | Decimal | str | None = None,
    p2: int | float | Decimal | str | None = None,
    tR: int | float | Decimal | None = None,
    pR: int | float | Decimal | None = None,
    url: str = DEFAULT_URL,
) -> MarketStateResult:
    """Ask the service for one selection. Never retried: a retried POST could publish twice."""
    body: dict[str, object] = {}
    for key, value in (('t1', t1), ('t2', t2)):
        if value is not None:
            body[key] = value
    for key, value in (('p1', p1), ('p2', p2)):
        if value is not None:
            body[key] = str(value) if isinstance(value, Decimal) else value
    for key, value in (('tR', tR), ('pR', pR)):
        if value is not None:
            body[key] = _number(value)
    response = _post(url, '/v1/market-state/query', body, QUERY_TIMEOUT_SECONDS)
    return MarketStateResult(
        str(response['result_id']),
        str(response['cells']),
        str(response['summary']),
        datetime.fromisoformat(str(response['expires_at'])),
        response,
    )


def open_file(path: str, *, url: str = DEFAULT_URL) -> ArrowFileReader:
    """An Arrow IPC file reader whose every read renews the file's expiry."""
    pa = cast(_PyArrow, import_module('pyarrow'))
    ipc = cast(_IPC, import_module('pyarrow.ipc'))
    return ipc.open_file(pa.PythonFile(_RenewingFile(path, url), 'r'))


def read_table(path: str, *, url: str = DEFAULT_URL) -> ArrowTable:
    """The whole file as a table, read through the renewing reader."""
    pa = cast(_PyArrow, import_module('pyarrow'))
    ipc = cast(_IPC, import_module('pyarrow.ipc'))
    with _RenewingFile(path, url) as handle:
        return ipc.open_file(pa.PythonFile(handle, 'r')).read_all()


class _RenewingFile(io.RawIOBase):
    """A read-only file whose reads first renew the file's clock at the service."""

    def __init__(self, path: str, url: str) -> None:
        super().__init__()
        self._path, self._url = path, url
        self._descriptor = os.open(path, os.O_RDONLY)
        self._position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            position = offset
        elif whence == os.SEEK_CUR:
            position = self._position + offset
        elif whence == os.SEEK_END:
            position = os.fstat(self._descriptor).st_size + offset
        else:
            raise ValueError(f'Unsupported whence: {whence}.')
        if position < 0:
            raise ValueError(f'Negative seek position: {position}.')
        self._position = position
        return position

    def readinto(self, buffer: WriteableBuffer, /) -> int:
        if self.closed:
            raise ValueError('I/O operation on a closed file.')
        view = memoryview(buffer).cast('B')
        if not len(view):
            return 0
        _renew(self._url, self._path)
        data = os.pread(self._descriptor, len(view), self._position)
        view[: len(data)] = data
        self._position += len(data)
        return len(data)

    def close(self) -> None:
        if not self.closed:
            os.close(self._descriptor)
        super().close()


def _renew(url: str, path: str) -> None:
    deadline = time.monotonic() + RETRY_SECONDS
    while True:
        try:
            _post(url, '/v1/market-state/access', {'path': path}, ACCESS_TIMEOUT_SECONDS)
            return
        except urllib.error.HTTPError as error:
            if error.code == 410:
                raise FileNotFoundError(f'Market state result was reclaimed: {path}') from error
            raise MarketStateError(error.code, _answer(error.read())) from error
        except (urllib.error.URLError, ConnectionError) as error:
            if time.monotonic() >= deadline:
                raise ConnectionError(f'Market state service unreachable at {url}.') from error
            time.sleep(1.0)


def _post(url: str, route: str, body: Mapping[str, object], timeout: float) -> Mapping[str, object]:
    request = urllib.request.Request(
        url.rstrip('/') + route,
        data=json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return _answer(response.read())
    except urllib.error.HTTPError as error:
        if route.endswith('/access'):
            raise
        raise MarketStateError(error.code, _answer(error.read())) from error


def _number(value: int | float | Decimal) -> int | float:
    """A resolution as a JSON number the service decodes to the same exact decimal."""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        if Decimal(float(value)) != value:
            raise ValueError(f'{value} has no exact JSON number form here; pass int or float.')
        return float(value)
    return value


def _answer(raw: bytes) -> Mapping[str, object]:
    value = json.loads(raw or b'{}')
    if not isinstance(value, dict):
        raise ValueError('The market state service answered with a non-object.')
    return cast(Mapping[str, object], value)
