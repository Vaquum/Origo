from __future__ import annotations

import csv
import fcntl
import hashlib
import io
import json
import os
import re
import time
import zipfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from math import isfinite
from pathlib import Path

import requests
from dagster import get_dagster_logger

from ..archive import verified_archive
from ..columnar import insert_arrow
from ..contracts import Partition, Revision, Row, SourceError
from .binance_columnar import spot_table, table_digest


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


def timestamp_datetime(value: int) -> datetime:
    digits = len(str(value))
    if digits not in (13, 16):
        raise ValueError('Spot timestamp must contain milliseconds or microseconds.')
    micros = value * 1000 if digits == 13 else value
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def _integer(text: str) -> int:
    if not re.fullmatch(r'\d+', text):
        raise ValueError('Spot trade ID and timestamp must be unsigned integers.')
    return int(text)


def _boolean(text: str) -> int:
    if text not in ('True', 'False', 'true', 'false'):
        raise ValueError('Invalid spot boolean field.')
    return int(text.lower() == 'true')


def spot_csv_rows(body: bytes, partition: Partition) -> Iterator[Row]:
    previous_id = -1
    previous_time = partition.start
    count = 0
    with io.TextIOWrapper(io.BytesIO(body), encoding='utf-8', newline='') as stream:
        for fields in csv.reader(stream):
            if len(fields) != 7:
                raise ValueError('Individual spot archives require exactly seven fields.')
            trade_id = _integer(fields[0])
            timestamp = _integer(fields[4])
            instant = timestamp_datetime(timestamp)
            if trade_id <= previous_id or instant < previous_time:
                raise ValueError('Spot rows must have unique ordered IDs and ordered timestamps.')
            if not partition.start <= instant < partition.end:
                raise ValueError('Spot row is outside its partition.')
            yield (
                trade_id,
                parse_decimal(fields[1]),
                parse_decimal(fields[2]),
                parse_decimal(fields[3]),
                timestamp,
                _boolean(fields[5]),
                _boolean(fields[6]),
                instant,
            )
            previous_id, previous_time = trade_id, instant
            count += 1
    if count == 0:
        raise ValueError('An empty archive is not a canonical spot partition.')


def _checksum(body: bytes, member: str) -> str:
    match = re.fullmatch(rb'([0-9a-f]{64})\s+\*?' + re.escape(member.encode()) + rb'\s*', body)
    if match is None:
        raise ValueError('Invalid Binance checksum sidecar.')
    return match.group(1).decode('ascii')


@dataclass(frozen=True)
class BinanceSpotDaily:
    first_day: date = date(2017, 8, 17)

    def candidate(self, now: datetime) -> Partition:
        return self.partition((now.astimezone(UTC).date() - timedelta(days=1)).isoformat())

    def partition(self, key: str) -> Partition:
        day = date.fromisoformat(key)
        if key != day.isoformat() or day < self.first_day:
            raise ValueError('Spot archive partition precedes the declared first day.')
        start = datetime.combine(day, datetime.min.time(), UTC)
        return Partition(key, start, start + timedelta(days=1))

    def _url(self, partition: Partition) -> str:
        base = os.environ.get(
            'BINANCE_SPOT_TRADES_ARCHIVE_BASE_URL',
            'https://data.binance.vision/data/spot/daily/trades/BTCUSDT',
        )
        return f'{base.rstrip("/")}/BTCUSDT-trades-{partition.key}.zip'

    def fetch(self, partition: Partition) -> Revision:
        get_dagster_logger('origo.sources').info(
            'source=binance_spot_trades partition=%s phase=archive_download', partition.key
        )
        url = self._url(partition)
        name = f'BTCUSDT-trades-{partition.key}'
        expected = _checksum(get_response(url + '.CHECKSUM').body, name + '.zip')
        body = verified_archive(
            url,
            expected,
            lambda address: get_response(address).body,
            code='ARCHIVE_CHECKSUM_MISMATCH',
        )
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            if archive.namelist() != [name + '.csv']:
                raise SourceError(
                    'ARCHIVE_MEMBER_INVALID',
                    'Binance archive must contain exactly the expected CSV member.',
                )
            csv_body = archive.read(name + '.csv')
        try:
            table = spot_table(csv_body, partition)
            count = table.num_rows
            normalized = table_digest(table)
        except ValueError as error:
            raise SourceError('ARCHIVE_ROWS_INVALID', str(error)) from error
        evidence = json.dumps(
            {
                'object_url': url,
                'zip_sha256': expected,
                'csv_sha256': hashlib.sha256(csv_body).hexdigest(),
                'member': name + '.csv',
            },
            sort_keys=True,
        )
        get_dagster_logger('origo.sources').info(
            'source=binance_spot_trades partition=%s phase=archive_validated rows=%s revision=%s',
            partition.key,
            count,
            expected,
        )
        return Revision(
            expected,
            normalized,
            evidence,
            count,
            lambda: spot_csv_rows(csv_body, partition),
            insert_bulk=lambda destination: insert_arrow(destination, table),
        )

    def discover(self, partition: Partition) -> str:
        return _checksum(
            get_response(self._url(partition) + '.CHECKSUM').body,
            f'BTCUSDT-trades-{partition.key}.zip',
        )

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError(
                'OFFICIAL_REVISION_CHANGED', 'Official Binance revision changed during the build.'
            )
