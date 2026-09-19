"""Shared Binance vision-archive daily adapter base (PRD-0013 rows 1-2).

Spot and perp declare their archive parameters on subclasses; the base owns
download, checksum, partition, and row-validation mechanics. Per-source hooks
stay on the subclasses: the decimal parser (perp normalizes padded API text),
the columnar table builder, and the row builder.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import chain
from typing import TYPE_CHECKING, ClassVar

from dagster import get_dagster_logger

from ..archive import verified_archive
from ..arrow_types import ArrowTable
from ..columnar import insert_arrow
from ..contracts import Partition, Revision, Row, SourceError
from .binance_columnar import table_digest

if TYPE_CHECKING:
    from .binance_daily import Response

_COUNT_WORDS = ('zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight')


def timestamp_datetime(value: int) -> datetime:
    digits = len(str(value))
    if digits not in (13, 16):
        raise ValueError('Binance timestamp must contain milliseconds or microseconds.')
    micros = value * 1000 if digits == 13 else value
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def parse_archive_boolean(text: str) -> int:
    if text not in ('True', 'False', 'true', 'false'):
        raise ValueError('Invalid Binance boolean field.')
    return int(text.lower() == 'true')


def _integer(text: str) -> int:
    if not re.fullmatch(r'\d+', text):
        raise ValueError('Binance trade ID and timestamp must be unsigned integers.')
    return int(text)


def parse_archive_rows(
    body: bytes,
    partition: Partition,
    *,
    field_count: int,
    header: tuple[str, ...] | None,
    timestamp_index: int,
    build_row: Callable[[int, int, datetime, list[str]], Row],
) -> Iterator[Row]:
    previous_id = -1
    previous_time = partition.start
    count = 0
    with io.TextIOWrapper(io.BytesIO(body), encoding='utf-8', newline='') as stream:
        rows = csv.reader(stream)
        first = next(rows, None)
        if first is None:
            raise ValueError('An empty archive is not a canonical Binance partition.')
        rest = iter((first,)) if header is None or first != list(header) else iter(())
        for fields in chain(rest, rows):
            if len(fields) != field_count:
                raise ValueError(
                    'Individual Binance archives require exactly '
                    f'{_COUNT_WORDS[field_count]} fields.'
                )
            trade_id = _integer(fields[0])
            timestamp = _integer(fields[timestamp_index])
            instant = timestamp_datetime(timestamp)
            if trade_id <= previous_id or instant < previous_time:
                raise ValueError('Binance rows must have unique ordered IDs and ordered timestamps.')
            if not partition.start <= instant < partition.end:
                raise ValueError('Binance row is outside its partition.')
            yield build_row(trade_id, timestamp, instant, fields)
            previous_id, previous_time = trade_id, instant
            count += 1
    if count == 0:
        raise ValueError('An empty archive is not a canonical Binance partition.')


def _checksum(body: bytes, member: str) -> str:
    match = re.fullmatch(rb'([0-9a-f]{64})\s+\*?' + re.escape(member.encode()) + rb'\s*', body)
    if match is None:
        raise ValueError('Invalid Binance checksum sidecar.')
    return match.group(1).decode('ascii')


@dataclass(frozen=True)
class BinanceArchiveDaily:
    first_day: date
    SOURCE_KEY: ClassVar[str]
    ARCHIVE_BASE_URL_ENV: ClassVar[str]
    ARCHIVE_BASE_URL_DEFAULT: ClassVar[str]
    MEMBER_PREFIX: ClassVar[str] = 'BTCUSDT-trades'
    FIELD_COUNT: ClassVar[int]
    HEADER: ClassVar[tuple[str, ...] | None]
    TIMESTAMP_INDEX: ClassVar[int] = 4

    def candidate(self, now: datetime) -> Partition:
        return self.partition((now.astimezone(UTC).date() - timedelta(days=1)).isoformat())

    def partition(self, key: str) -> Partition:
        day = date.fromisoformat(key)
        if key != day.isoformat() or day < self.first_day:
            raise ValueError('Binance archive partition precedes the declared first day.')
        start = datetime.combine(day, datetime.min.time(), UTC)
        return Partition(key, start, start + timedelta(days=1))

    def _url(self, partition: Partition) -> str:
        base = os.environ.get(self.ARCHIVE_BASE_URL_ENV, self.ARCHIVE_BASE_URL_DEFAULT)
        return f'{base.rstrip("/")}/{self.MEMBER_PREFIX}-{partition.key}.zip'

    def _get_response(self, url: str) -> Response:
        raise NotImplementedError('Archive subclasses resolve HTTP through their own module.')

    def build_table(self, csv_body: bytes, partition: Partition) -> ArrowTable:
        raise NotImplementedError('Archive subclasses build their own columnar table.')

    def build_row(
        self, trade_id: int, timestamp: int, instant: datetime, fields: list[str]
    ) -> Row:
        raise NotImplementedError('Archive subclasses build their own row shape.')

    def clean_rows(
        self, csv_body: bytes, partition: Partition
    ) -> tuple[bytes, dict[str, int]]:
        """Drop provider-side quirk rows before validation; report drop counts.

        The default is the identity: only adapters with observed, evidenced
        quirks override this. Counts merge into the revision evidence, while
        csv_sha256 there always pins the archive as served.
        """
        return csv_body, {}

    def parse_rows(self, body: bytes, partition: Partition) -> Iterator[Row]:
        return parse_archive_rows(
            body,
            partition,
            field_count=self.FIELD_COUNT,
            header=self.HEADER,
            timestamp_index=self.TIMESTAMP_INDEX,
            build_row=self.build_row,
        )

    def fetch(self, partition: Partition) -> Revision:
        get_dagster_logger('origo.sources').info(
            'source=%s partition=%s phase=archive_download', self.SOURCE_KEY, partition.key
        )
        url = self._url(partition)
        name = f'{self.MEMBER_PREFIX}-{partition.key}'
        expected = _checksum(self._get_response(url + '.CHECKSUM').body, name + '.zip')
        body = verified_archive(
            url,
            expected,
            lambda address: self._get_response(address).body,
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
            rows_body, dropped = self.clean_rows(csv_body, partition)
            table = self.build_table(rows_body, partition)
            count = table.num_rows
            normalized = table_digest(table)
        except ValueError as error:
            raise SourceError('ARCHIVE_ROWS_INVALID', str(error)) from error
        checksums: dict[str, object] = {
            'object_url': url,
            'zip_sha256': expected,
            'csv_sha256': hashlib.sha256(csv_body).hexdigest(),
            'member': name + '.csv',
        }
        if dropped:
            checksums['dropped_rows'] = dropped
        evidence = json.dumps(checksums, sort_keys=True)
        get_dagster_logger('origo.sources').info(
            'source=%s partition=%s phase=archive_validated rows=%s revision=%s',
            self.SOURCE_KEY,
            partition.key,
            count,
            expected,
        )
        return Revision(
            expected,
            normalized,
            evidence,
            count,
            lambda: self.parse_rows(rows_body, partition),
            insert_bulk=lambda destination: insert_arrow(destination, table),
        )

    def discover(self, partition: Partition) -> str:
        return _checksum(
            self._get_response(self._url(partition) + '.CHECKSUM').body,
            f'{self.MEMBER_PREFIX}-{partition.key}.zip',
        )

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        if self.discover(partition) != revision.key:
            raise SourceError(
                'OFFICIAL_REVISION_CHANGED', 'Official Binance revision changed during the build.'
            )
