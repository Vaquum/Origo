from __future__ import annotations

import fcntl
import os
import re
import time
from collections.abc import Iterator, Mapping
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from ipaddress import IPv4Address
from math import ceil, isfinite
from pathlib import Path
from queue import LifoQueue
from typing import ClassVar, Literal, TextIO
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from ..arrow_types import ArrowTable
from ..contracts import Partition, Row, SourceError, beat_worker
from .binance_archive import BinanceArchiveDaily, parse_archive_boolean
from .binance_archive import parse_decimal as shared_parse_decimal
from .binance_archive import timestamp_datetime as timestamp_datetime
from .binance_columnar import spot_table


@dataclass(frozen=True)
class Response:
    body: bytes
    headers: Mapping[str, str]
    status: int
    egress_ip: str | None = None
    resource_url: str | None = field(default=None, repr=False, compare=False)


# Static pacing per Binance host family at 60% of the documented IP allowance,
# with the header-driven backstop at 80%: spot allows 6000 REQUEST_WEIGHT/min
# and fapi 2400/min. Unknown hosts keep the previous conservative posture.
REST_HOST_BUDGETS = {
    'api.binance.com': (60, 4800),
    'fapi.binance.com': (24, 1920),
}
REST_DEFAULT_BUDGET = (20, 1200)

_provider_connections: ContextVar[LifoQueue[requests.Session] | None] = ContextVar(
    'book_provider_connections', default=None
)


@contextmanager
def provider_connections() -> Iterator[None]:
    if _provider_connections.get() is not None:
        yield
        return
    with ExitStack() as stack:
        sessions: LifoQueue[requests.Session] = LifoQueue(2)
        for _ in range(2):
            sessions.put(stack.enter_context(requests.Session()))
        token = _provider_connections.set(sessions)
        try:
            yield
        finally:
            _provider_connections.reset(token)


def _request(
    url: str,
    params: Mapping[str, str | int] | None,
    headers: Mapping[str, str] | None,
    egress_ip: str | None = None,
    method: Literal['GET', 'POST'] = 'GET',
    *, resource_url: str | None = None,
) -> requests.Response:
    target = resource_url or url
    if resource_url is not None:
        params = None
        headers = {k: v for k, v in (headers or {}).items()
                   if k.lower() not in ('authorization', 'x-api-key')}
    try:
        if egress_ip is not None:
            with requests.Session() as session:
                session.trust_env = False
                adapter = HTTPAdapter()
                adapter.poolmanager.connection_pool_kw['source_address'] = (egress_ip, 0)
                session.mount('https://', adapter)
                session.mount('http://', adapter)
                if method == 'POST':
                    return session.post(target, params=params, headers=headers, timeout=(5, 30),
                                        allow_redirects=False)
                return session.get(target, params=params, headers=headers, timeout=(5, 30))
        if method == 'POST':
            return requests.post(target, params=params, headers=headers, timeout=(5, 30),
                                 allow_redirects=False)
        sessions = _provider_connections.get()
        if sessions is not None and _budget_host(url) == 'api.cryptohftdata.com':
            session = sessions.get()
            try:
                return session.get(target, params=params, headers=headers, timeout=(5, 30))
            finally:
                sessions.put(session)
        return requests.get(target, params=params, headers=headers, timeout=(5, 30))
    except requests.RequestException as error:
        if _budget_host(url) == 'api.cryptohftdata.com':
            raise SourceError(
                'PROVIDER_TRANSPORT_FAILED', f'Provider request failed: {type(error).__name__}.'
            ) from None
        raise SourceError(
            'PROVIDER_TRANSPORT_FAILED', 'Provider request did not complete.'
        ) from error


def _budget_host(url: str) -> str:
    """The budget family of a request URL: spot aliases share api's allowance."""
    normalized = (urlsplit(url).hostname or '').lower()
    if re.fullmatch(r'api\d*\.binance\.com', normalized):
        return 'api.binance.com'
    return normalized


def _budget_state_file(root: Path, url: str, egress_ip: str | None = None) -> Path:
    host = _budget_host(url)
    safe = ''.join(char if char.isalnum() else '_' for char in host)
    if not safe:
        raise ValueError('Binance request budget requires a URL with a host.')
    suffix = f'.{egress_ip}' if egress_ip is not None else ''
    return root / f'binance_rest_budget.{safe}{suffix}.state'


def _weighted_request(
    url: str,
    params: Mapping[str, str | int] | None,
    headers: Mapping[str, str] | None,
    weight: int,
    egress_ip: str | None = None,
    method: Literal['GET', 'POST'] = 'GET',
    *, resource_url: str | None = None,
) -> requests.Response:
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute():
        raise ValueError('Binance request budget requires the shared absolute lock mount.')
    root.mkdir(parents=True, exist_ok=True)
    if _budget_host(url) == 'api.cryptohftdata.com':
        if resource_url is not None:
            return _vendor_request(root, url, params, headers, weight, egress_ip, method,
                                   resource_url=resource_url)
        return _vendor_request(root, url, params, headers, weight, egress_ip, method)
    # Processes share each host/address allowance; unbound callers keep the legacy file.
    rate, backstop = REST_HOST_BUDGETS.get(_budget_host(url), REST_DEFAULT_BUDGET)
    with _budget_state_file(root, url, egress_ip).open('a+') as state:
        fcntl.flock(state.fileno(), fcntl.LOCK_EX)
        state.seek(0)
        saved = state.read().strip()
        next_request, circuit_until = map(float, saved.split()) if saved else (0.0, 0.0)
        if not all(isfinite(value) and value >= 0 for value in (next_request, circuit_until)):
            raise ValueError('The persisted Binance request budget is invalid.')

        def persist() -> None:
            state.seek(0)
            state.truncate()
            state.write(f'{next_request:.6f} {circuit_until:.6f}')
            state.flush()
            os.fsync(state.fileno())

        now = time.time()
        if now < circuit_until:
            raise SourceError(
                'PROVIDER_RATE_CIRCUIT', f'Binance request circuit is open for {_budget_host(url)}.'
            )
        time.sleep(max(0.0, next_request - now))
        next_request = time.time() + weight / rate
        persist()
        response = (
            _request(url, params, headers, egress_ip)
            if egress_ip is not None
            else _request(url, params, headers)
        )
        beat_worker()
        used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
        if used >= backstop:
            next_request = max(next_request, time.time() + 60)
        if response.status_code in (418, 429):
            retry = response.headers.get('Retry-After')
            if retry is None or not retry.isdigit():
                raise SourceError('PROVIDER_RATE_HEADER_INVALID', 'Binance Retry-After is invalid.')
            deadline = time.time() + int(retry)
            next_request = max(next_request, deadline)
            if response.status_code == 418:
                circuit_until = deadline
        persist()
        return response


def _read_budget(state: TextIO) -> tuple[float, float, float]:
    state.seek(0)
    saved = state.read().strip()
    values = tuple(map(float, saved.split())) if saved else (0.0, 0.0)
    if len(values) not in (2, 3) or not all(isfinite(value) and value >= 0 for value in values):
        raise ValueError('The persisted provider request budget is invalid.')
    return values[0], values[1], values[2] if len(values) == 3 else 0.0


def _save_budget(state: TextIO, next_request: float, pause: float, provider_spacing: float) -> None:
    state.seek(0)
    state.truncate()
    state.write(f'{next_request:.6f} {pause:.6f} {provider_spacing:.6f}')
    state.flush()
    os.fsync(state.fileno())


def _vendor_pause(response: requests.Response, spacing: float) -> tuple[float, float]:
    pause = 0.0
    provider_spacing = 0.0
    for hop in (*response.history, response):
        retry = hop.headers.get('Retry-After')
        if hop.status_code in (418, 429) or (500 <= hop.status_code < 600 and retry is not None):
            if retry is None or not retry.isdigit():
                raise SourceError('PROVIDER_RATE_HEADER_INVALID', 'Provider Retry-After is invalid.')
            pause = max(pause, time.time() + int(retry))
        if hop.headers.get('RateLimit-Remaining') == '0':
            reset = hop.headers.get('RateLimit-Reset')
            if reset is None or not reset.isdigit():
                raise SourceError('PROVIDER_RATE_HEADER_INVALID', 'Provider RateLimit-Reset is invalid.')
            pause = max(pause, time.time() + int(reset))
        policy = hop.headers.get('RateLimit-Policy', '')
        quota = re.search(r'(?:^|[;,\s])q=(\d+)', policy)
        window = re.search(r'(?:^|[;,\s])w=(\d+)', policy)
        limit = hop.headers.get('RateLimit-Limit')
        reset = hop.headers.get('RateLimit-Reset')
        if quota and window and int(quota[1]) > 0:
            provider_spacing = max(provider_spacing, int(window[1]) / int(quota[1]))
        elif limit and reset and limit.isdigit() and reset.isdigit() and int(limit) > 0:
            provider_spacing = max(provider_spacing, int(reset) / int(limit))
    return pause, ceil(provider_spacing * 1_000_000) / 1_000_000 if provider_spacing > spacing else 0.0


def _vendor_request(
    root: Path, url: str, params: Mapping[str, str | int] | None,
    headers: Mapping[str, str] | None, weight: int, egress_ip: str | None,
    method: Literal['GET', 'POST'],
    *, resource_url: str | None = None,
) -> requests.Response:
    spacing = weight / REST_DEFAULT_BUDGET[0]
    with _budget_state_file(root, url, egress_ip).open('a+') as state:
        fcntl.flock(state.fileno(), fcntl.LOCK_EX)
        next_request, pause, provider_spacing = _read_budget(state)
        reserved_spacing = max(spacing, provider_spacing)
        due = max(time.time(), next_request, pause)
        _save_budget(state, due + reserved_spacing, pause, provider_spacing)
        fcntl.flock(state.fileno(), fcntl.LOCK_UN)
        while True:
            time.sleep(max(0.0, due - time.time()))
            fcntl.flock(state.fileno(), fcntl.LOCK_EX)
            next_request, pause, provider_spacing = _read_budget(state)
            current_spacing = max(spacing, provider_spacing)
            if time.time() >= pause and current_spacing <= reserved_spacing:
                fcntl.flock(state.fileno(), fcntl.LOCK_UN)
                break
            due = max(next_request, pause, time.time())
            reserved_spacing = current_spacing
            _save_budget(state, due + reserved_spacing, pause, provider_spacing)
            fcntl.flock(state.fileno(), fcntl.LOCK_UN)
        response = (
            _request(url, params, headers, egress_ip, method, resource_url=resource_url)
            if resource_url is not None else
            _request(url, params, headers, egress_ip, method)
            if method != 'GET' else _request(url, params, headers, egress_ip)
            if egress_ip is not None else _request(url, params, headers)
        )
        try:
            beat_worker()
            deadline, observed_spacing = _vendor_pause(response, spacing)
            fcntl.flock(state.fileno(), fcntl.LOCK_EX)
            next_request, pause, provider_spacing = _read_budget(state)
            provider_spacing = max(provider_spacing, observed_spacing)
            _save_budget(state, max(next_request, deadline, time.time() + observed_spacing),
                         max(pause, deadline), provider_spacing)
        except BaseException:
            response.close()
            raise
        return response


def get_response(
    url: str,
    *,
    params: Mapping[str, str | int] | None = None,
    headers: Mapping[str, str] | None = None,
    weight: int = 0,
    egress_ip: str | None = None,
    method: Literal['GET', 'POST'] = 'GET',
    resource_url: str | None = None,
) -> Response:
    if weight < 0:
        raise ValueError('Binance request weight cannot be negative.')
    if resource_url is not None and (
        method != 'GET' or weight <= 0 or _budget_host(url) != 'api.cryptohftdata.com'
    ):
        raise ValueError('An issued provider object requires its weighted logical GET.')
    if egress_ip is not None:
        egress_ip = str(IPv4Address(egress_ip))
    try:
        response = (
            _weighted_request(url, params, headers, weight, egress_ip, method,
                              resource_url=resource_url)
            if resource_url is not None else
            _weighted_request(url, params, headers, weight, egress_ip, method)
            if method != 'GET' and weight
            else _request(url, params, headers, egress_ip, method)
            if method != 'GET'
            else _weighted_request(url, params, headers, weight, egress_ip)
            if weight
            else _request(url, params, headers, egress_ip)
            if egress_ip is not None
            else _request(url, params, headers)
        )
        try:
            if not 200 <= response.status_code < 300:
                raise SourceError(
                    f'PROVIDER_HTTP_{response.status_code}',
                    f'Provider returned HTTP {response.status_code}.',
                )
            issued = response.url if (
                _budget_host(url) == 'api.cryptohftdata.com' and method == 'GET'
                and params is not None and 'file' in params
            ) else None
            return Response(response.content, dict(response.headers), response.status_code,
                            egress_ip, issued)
        finally:
            if _budget_host(url) == 'api.cryptohftdata.com':
                response.close()
    except SourceError as error:
        if egress_ip is None:
            raise
        raise SourceError(error.code, f'{error} (egress {egress_ip}).') from error


def parse_decimal(text: str) -> Decimal:
    return shared_parse_decimal(text, noun='Spot price, quantity, and quote quantity')


@dataclass(frozen=True)
class BinanceSpotDaily(BinanceArchiveDaily):
    first_day: date = date(2017, 8, 17)
    SOURCE_KEY: ClassVar[str] = 'binance_spot_trades'
    ARCHIVE_BASE_URL_ENV: ClassVar[str] = 'BINANCE_SPOT_TRADES_ARCHIVE_BASE_URL'
    ARCHIVE_BASE_URL_DEFAULT: ClassVar[str] = (
        'https://data.binance.vision/data/spot/daily/trades/BTCUSDT'
    )
    FIELD_COUNT: ClassVar[int] = 7
    HEADER: ClassVar[tuple[str, ...] | None] = None

    def _get_response(self, url: str) -> Response:
        return get_response(url)

    def build_table(self, csv_body: bytes, partition: Partition) -> ArrowTable:
        return spot_table(csv_body, partition)

    def build_row(
        self, trade_id: int, timestamp: int, instant: datetime, fields: list[str]
    ) -> Row:
        return (
            trade_id,
            parse_decimal(fields[1]),
            parse_decimal(fields[2]),
            parse_decimal(fields[3]),
            timestamp,
            parse_archive_boolean(fields[5]),
            parse_archive_boolean(fields[6]),
            instant,
        )


def spot_csv_rows(body: bytes, partition: Partition) -> Iterator[Row]:
    return BinanceSpotDaily().parse_rows(body, partition)
