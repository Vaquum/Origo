from __future__ import annotations

import fcntl
import os
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from math import isfinite
from pathlib import Path
from typing import ClassVar

import requests

from ..arrow_types import ArrowTable
from ..contracts import Partition, Row, SourceError
from .binance_archive import BinanceArchiveDaily, parse_archive_boolean
from .binance_archive import timestamp_datetime as timestamp_datetime
from .binance_columnar import spot_table


@dataclass(frozen=True)
class Response:
    body: bytes
    headers: Mapping[str, str]
    status: int


def _request(
    url: str, params: Mapping[str, str | int] | None, headers: Mapping[str, str] | None
) -> requests.Response:
    try:
        return requests.get(url, params=params, headers=headers, timeout=(5, 30))
    except requests.RequestException as error:
        raise SourceError(
            'PROVIDER_TRANSPORT_FAILED', 'Provider request did not complete.'
        ) from error


def _weighted_request(
    url: str,
    params: Mapping[str, str | int] | None,
    headers: Mapping[str, str] | None,
    weight: int,
) -> requests.Response:
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute():
        raise ValueError('Binance request budget requires the shared absolute lock mount.')
    root.mkdir(parents=True, exist_ok=True)
    # All worker processes and Binance host aliases share one IP request allowance.
    with (root / 'binance_rest_budget.state').open('a+') as state:
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
            raise SourceError('PROVIDER_RATE_CIRCUIT', 'Binance request circuit is open.')
        time.sleep(max(0.0, next_request - now))
        next_request = time.time() + weight / 20
        persist()
        response = _request(url, params, headers)
        used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
        if used >= 1200:
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


def get_response(
    url: str,
    *,
    params: Mapping[str, str | int] | None = None,
    headers: Mapping[str, str] | None = None,
    weight: int = 0,
) -> Response:
    if weight < 0:
        raise ValueError('Binance request weight cannot be negative.')
    response = (
        _weighted_request(url, params, headers, weight)
        if weight
        else _request(url, params, headers)
    )
    if not 200 <= response.status_code < 300:
        raise SourceError(
            f'PROVIDER_HTTP_{response.status_code}',
            f'Provider returned HTTP {response.status_code}.',
        )
    return Response(response.content, dict(response.headers), response.status_code)


def parse_decimal(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation as error:
        raise ValueError('Invalid Binance decimal field.') from error
    if not value.is_finite() or value <= 0:
        raise ValueError('Spot price, quantity, and quote quantity must be positive.')
    return value


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
