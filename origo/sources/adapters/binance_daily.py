from __future__ import annotations

import fcntl
import os
import re
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from pathlib import Path
from typing import IO, ClassVar, Literal
from urllib.parse import urlsplit

import requests

from ..arrow_types import ArrowTable
from ..contracts import Partition, Row, SourceError, beat_worker
from .binance_archive import BinanceArchiveDaily, parse_archive_boolean
from .binance_archive import parse_decimal as shared_parse_decimal
from .binance_archive import timestamp_datetime as timestamp_datetime
from .binance_columnar import spot_table

Lane = Literal['shared', 'live']


@dataclass(frozen=True)
class RequestCost:
    """What one provider request cost: its weight, where it waited, and the provider's
    own used-weight reading. Milliseconds are wall-clock; ``lock_wait_ms`` is time
    spent waiting for the shared budget lock, ``pace_wait_ms`` the budget sleep."""

    weight: int = 0
    lane: Lane = 'shared'
    lock_wait_ms: int = 0
    pace_wait_ms: int = 0
    latency_ms: int = 0
    used_weight_1m: int = 0


@dataclass(frozen=True)
class Response:
    body: bytes
    headers: Mapping[str, str]
    status: int
    cost: RequestCost = RequestCost()


# Static pacing per Binance host family at 60% of the documented IP allowance,
# with the header-driven backstop at 80%: spot allows 6000 REQUEST_WEIGHT/min
# and fapi 2400/min. Unknown hosts keep the previous conservative posture.
REST_HOST_BUDGETS = {
    'api.binance.com': (60, 4800),
    'fapi.binance.com': (24, 1920),
}
REST_DEFAULT_BUDGET = (20, 1200)
# The shared lane reserves the host line first-come first-served, so a queue of
# 200-weight repair pages would hold a 5-weight live poll for the length of the
# queue. The live lane instead paces itself at this share of the host rate and
# charges the host line without queueing behind it; the total spend still
# advances the one host line, so the conservative host rate is unchanged.
LIVE_LANE_SHARE = 0.4
# Outstanding requests per host family across every process; the last slot is
# kept for the live lane so slow repair pages cannot hold every slot.
REST_MAX_IN_FLIGHT = 4
_SLOT_POLL_SECONDS = 0.05


def _request(
    url: str, params: Mapping[str, str | int] | None, headers: Mapping[str, str] | None
) -> requests.Response:
    try:
        return requests.get(url, params=params, headers=headers, timeout=(5, 30))
    except requests.RequestException as error:
        raise SourceError(
            'PROVIDER_TRANSPORT_FAILED', 'Provider request did not complete.'
        ) from error


def _budget_host(url: str) -> str:
    """The budget family of a request URL: spot aliases share api's allowance."""
    normalized = (urlsplit(url).hostname or '').lower()
    if re.fullmatch(r'api\d*\.binance\.com', normalized):
        return 'api.binance.com'
    return normalized


def _safe_host(url: str) -> str:
    safe = ''.join(char if char.isalnum() else '_' for char in _budget_host(url))
    if not safe:
        raise ValueError('Binance request budget requires a URL with a host.')
    return safe


def _budget_state_file(root: Path, url: str) -> Path:
    return root / f'binance_rest_budget.{_safe_host(url)}.state'


def _lane_state_file(root: Path, url: str) -> Path:
    return root / f'binance_rest_lane.{_safe_host(url)}.live.state'


def _read_pair(handle: IO[str]) -> tuple[float, float]:
    handle.seek(0)
    saved = handle.read().strip()
    first, second = map(float, saved.split()) if saved else (0.0, 0.0)
    if not all(isfinite(value) and value >= 0 for value in (first, second)):
        raise ValueError('The persisted Binance request budget is invalid.')
    return first, second


def _write_pair(handle: IO[str], first: float, second: float) -> None:
    handle.seek(0)
    handle.truncate()
    handle.write(f'{first:.6f} {second:.6f}')
    handle.flush()
    os.fsync(handle.fileno())


class _Budget:
    """The host line and the live lane under one short exclusive lock.

    The shared lane state keeps ``next_request circuit_until``; the live lane state keeps
    ``next_request hold_until``. Nothing sleeps or performs network I/O while the
    lock is held: a reservation is a few reads and one fsync. Futures lanes partition
    the existing budget 60/40, rather than borrowing quota from future reservations.
    """

    def __init__(self, root: Path, url: str) -> None:
        self.state_path = _budget_state_file(root, url)
        self.lane_path = _lane_state_file(root, url)
        self.rate, self.backstop = REST_HOST_BUDGETS.get(_budget_host(url), REST_DEFAULT_BUDGET)
        self.live_share = LIVE_LANE_SHARE if _budget_host(url) == 'fapi.binance.com' else 0.0
        self.lock_wait = 0.0
        self._state: IO[str] | None = None
        self._lane: IO[str] | None = None

    def __enter__(self) -> _Budget:
        began = time.monotonic()
        self._state = self.state_path.open('a+')
        fcntl.flock(self._state.fileno(), fcntl.LOCK_EX)
        self.lock_wait += time.monotonic() - began
        self._lane = self.lane_path.open('a+')
        return self

    def __exit__(self, *exc_info: object) -> None:
        for handle in (self._lane, self._state):
            if handle is not None:
                handle.close()
        self._state = self._lane = None

    def _handles(self) -> tuple[IO[str], IO[str]]:
        if self._state is None or self._lane is None:
            raise RuntimeError('The Binance budget must be read under its lock.')
        return self._state, self._lane

    def check_circuit(self) -> None:
        state, _ = self._handles()
        _, circuit_until = _read_pair(state)
        if time.time() < circuit_until:
            raise SourceError('PROVIDER_RATE_CIRCUIT', 'Binance request circuit is open.')

    def reserve(self, weight: int, lane: Lane) -> float:
        """Reserve ``weight`` on the host line and return the instant to fire at."""
        state, lane_state = self._handles()
        next_request, circuit_until = _read_pair(state)
        now = time.time()
        if now < circuit_until:
            raise SourceError('PROVIDER_RATE_CIRCUIT', 'Binance request circuit is open.')
        if lane == 'shared':
            fire_at = max(now, next_request)
            _write_pair(state, fire_at + weight / (self.rate * (1.0 - self.live_share)), circuit_until)
            return fire_at
        if self.live_share == 0:
            raise ValueError('A live request lane is declared only for the perpetual capture host.')
        lane_next, hold_until = _read_pair(lane_state)
        fire_at = max(now, lane_next, hold_until)
        _write_pair(lane_state, fire_at + weight / (self.rate * self.live_share), hold_until)
        return fire_at

    def settle(self, response: requests.Response) -> int:
        """Apply the provider's verdict after the response; returns the used weight."""
        state, lane_state = self._handles()
        next_request, circuit_until = _read_pair(state)
        lane_next, hold_until = _read_pair(lane_state)
        used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
        hold = 0.0
        if used >= self.backstop:
            hold = time.time() + 60
        if response.status_code in (418, 429):
            retry = response.headers.get('Retry-After')
            if retry is None or not retry.isdigit():
                raise SourceError('PROVIDER_RATE_HEADER_INVALID', 'Binance Retry-After is invalid.')
            deadline = time.time() + int(retry)
            hold = max(hold, deadline)
            if response.status_code == 418:
                circuit_until = deadline
        if hold:
            _write_pair(state, max(next_request, hold), circuit_until)
            _write_pair(lane_state, lane_next, max(hold_until, hold))
        return used


class _InFlightSlot:
    """One of the host's bounded in-flight slots, held as a file lock so a dying
    process releases it; the live lane may take the last slot, the shared lane may not."""

    def __init__(self, root: Path, url: str, lane: Lane) -> None:
        count = REST_MAX_IN_FLIGHT if lane == 'live' else REST_MAX_IN_FLIGHT - 1
        self.paths = tuple(
            root / f'binance_rest_inflight.{_safe_host(url)}.{index}.slot' for index in range(count)
        )
        self._handle: IO[str] | None = None

    def __enter__(self) -> _InFlightSlot:
        while self._handle is None:
            for path in self.paths:
                handle = path.open('a')
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    handle.close()
                    continue
                self._handle = handle
                break
            else:
                time.sleep(_SLOT_POLL_SECONDS)
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def _weighted_request(
    url: str,
    params: Mapping[str, str | int] | None,
    headers: Mapping[str, str] | None,
    weight: int,
    lane: Lane,
) -> tuple[requests.Response, RequestCost]:
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute():
        raise ValueError('Binance request budget requires the shared absolute lock mount.')
    root.mkdir(parents=True, exist_ok=True)
    # All worker processes share one budget per Binance host: api and fapi
    # enforce separate IP allowances, so a hot host must not pace a cold one.
    budget = _Budget(root, url)
    with budget:
        fire_at = budget.reserve(weight, lane)
    pace_wait = max(0.0, fire_at - time.time())
    time.sleep(pace_wait)
    if pace_wait:
        # A ban raised by another process while this one slept must not be
        # followed by one more request from a reservation made before it.
        with budget:
            budget.check_circuit()
    with _InFlightSlot(root, url, lane):
        started = time.monotonic()
        response = _request(url, params, headers)
        latency = time.monotonic() - started
    beat_worker()
    with budget:
        used = budget.settle(response)
    cost = RequestCost(
        weight=weight,
        lane=lane,
        lock_wait_ms=int(budget.lock_wait * 1000),
        pace_wait_ms=int(pace_wait * 1000),
        latency_ms=int(latency * 1000),
        used_weight_1m=used,
    )
    return response, cost


def get_response(
    url: str,
    *,
    params: Mapping[str, str | int] | None = None,
    headers: Mapping[str, str] | None = None,
    weight: int = 0,
    lane: Lane = 'shared',
) -> Response:
    if weight < 0:
        raise ValueError('Binance request weight cannot be negative.')
    if weight:
        response, cost = _weighted_request(url, params, headers, weight, lane)
    else:
        started = time.monotonic()
        response = _request(url, params, headers)
        cost = RequestCost(latency_ms=int((time.monotonic() - started) * 1000))
    if not 200 <= response.status_code < 300:
        raise SourceError(
            f'PROVIDER_HTTP_{response.status_code}',
            f'Provider returned HTTP {response.status_code}.',
        )
    return Response(response.content, dict(response.headers), response.status_code, cost)


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
