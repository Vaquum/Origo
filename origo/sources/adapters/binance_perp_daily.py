from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import zipfile
from collections.abc import Iterator
from itertools import chain
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from dagster import get_dagster_logger

from ..archive import verified_archive
from ..columnar import insert_arrow
from ..contracts import Partition, Revision, Row, SourceError
from .binance_columnar import table_digest
from .binance_daily import get_response
from .binance_perp_columnar import COLUMNS as PERP_COLUMNS
from .binance_perp_columnar import HEADER as PERP_HEADER
from .binance_perp_columnar import perp_table


def parse_decimal(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation as error:
        raise ValueError('Invalid Binance decimal field.') from error
    if not value.is_finite() or value <= 0:
        raise ValueError('Perp price, quantity, and quote quantity must be positive.')
    # The API pads decimals ('76043.50') while the archive trims them ('76043.5');
    # normalize so the same trade parses to the same Decimal from either source.
    return value.normalize()


def timestamp_datetime(value: int) -> datetime:
    digits = len(str(value))
    if digits not in (13, 16):
        raise ValueError('Perp timestamp must contain milliseconds or microseconds.')
    micros = value * 1000 if digits == 13 else value
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def _integer(text: str) -> int:
    if not re.fullmatch(r'\d+', text):
        raise ValueError('Perp trade ID and timestamp must be unsigned integers.')
    return int(text)


def _boolean(text: str) -> int:
    if text not in ('True', 'False', 'true', 'false'):
        raise ValueError('Invalid perp boolean field.')
    return int(text.lower() == 'true')


def perp_csv_rows(body: bytes, partition: Partition) -> Iterator[Row]:
    previous_id = -1
    previous_time = partition.start
    count = 0
    with io.TextIOWrapper(io.BytesIO(body), encoding='utf-8', newline='') as stream:
        rows = csv.reader(stream)
        first = next(rows, None)
        if first is None:
            raise ValueError('An empty archive is not a canonical perp partition.')
        rest = iter((first,)) if first != PERP_HEADER.split(',') else iter(())
        for fields in chain(rest, rows):
            if len(fields) != len(PERP_COLUMNS):
                raise ValueError('Individual perp archives require exactly six fields.')
            trade_id = _integer(fields[0])
            timestamp = _integer(fields[4])
            instant = timestamp_datetime(timestamp)
            if trade_id <= previous_id or instant < previous_time:
                raise ValueError('Perp rows must have unique ordered IDs and ordered timestamps.')
            if not partition.start <= instant < partition.end:
                raise ValueError('Perp row is outside its partition.')
            yield (
                trade_id,
                parse_decimal(fields[1]),
                parse_decimal(fields[2]),
                parse_decimal(fields[3]),
                timestamp,
                _boolean(fields[5]),
                instant,
            )
            previous_id, previous_time = trade_id, instant
            count += 1
    if count == 0:
        raise ValueError('An empty archive is not a canonical perp partition.')


def _checksum(body: bytes, member: str) -> str:
    match = re.fullmatch(rb'([0-9a-f]{64})\s+\*?' + re.escape(member.encode()) + rb'\s*', body)
    if match is None:
        raise ValueError('Invalid Binance checksum sidecar.')
    return match.group(1).decode('ascii')


@dataclass(frozen=True)
class BinancePerpDaily:
    first_day: date = date(2019, 9, 8)

    def candidate(self, now: datetime) -> Partition:
        return self.partition((now.astimezone(UTC).date() - timedelta(days=1)).isoformat())

    def partition(self, key: str) -> Partition:
        day = date.fromisoformat(key)
        if key != day.isoformat() or day < self.first_day:
            raise ValueError('Perp archive partition precedes the declared first day.')
        start = datetime.combine(day, datetime.min.time(), UTC)
        return Partition(key, start, start + timedelta(days=1))

    def _url(self, partition: Partition) -> str:
        base = os.environ.get(
            'BINANCE_PERP_TRADES_ARCHIVE_BASE_URL',
            'https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT',
        )
        return f'{base.rstrip("/")}/BTCUSDT-trades-{partition.key}.zip'

    def fetch(self, partition: Partition) -> Revision:
        get_dagster_logger('origo.sources').info(
            'source=binance_perp_trades partition=%s phase=archive_download', partition.key
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
            table = perp_table(csv_body, partition)
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
            'source=binance_perp_trades partition=%s phase=archive_validated rows=%s revision=%s',
            partition.key,
            count,
            expected,
        )
        return Revision(
            expected,
            normalized,
            evidence,
            count,
            lambda: perp_csv_rows(csv_body, partition),
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
