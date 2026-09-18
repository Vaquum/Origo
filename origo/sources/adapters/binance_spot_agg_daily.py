from __future__ import annotations

import io
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import ClassVar

from ..arrow_types import ArrowTable
from ..contracts import Partition, Row
from .binance_archive import BinanceArchiveDaily, parse_archive_boolean
from .binance_archive import timestamp_datetime as timestamp_datetime
from .binance_daily import Response, get_response
from .binance_spot_agg_columnar import COLUMNS as AGG_COLUMNS
from .binance_spot_agg_columnar import agg_table


def parse_decimal(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation as error:
        raise ValueError('Invalid Binance decimal field.') from error
    if not value.is_finite() or value <= 0:
        raise ValueError('Spot aggregate price and quantity must be positive.')
    # The API pads decimals while the archive may trim them; normalize so the
    # same aggregate parses to the same Decimal from either source.
    return value.normalize()


def _signed(text: str) -> int:
    if text.startswith('-'):
        magnitude = text[1:]
        if not magnitude.isdigit():
            raise ValueError('Binance aggregate trade IDs must be integers.')
        return -int(magnitude)
    if not text.isdigit():
        raise ValueError('Binance aggregate trade IDs must be integers.')
    return int(text)


# Verification retains one line per out-of-order id; past this many distinct
# backward ids the archive is systematically disordered, not quirked.
_MAX_ANOMALY_IDS = 100_000


def _sentinel(fields: list[bytes]) -> bool:
    """A Binance placeholder aggregate: -1 trade ids with zero price and quantity."""
    if len(fields) < 6 or fields[3] != b'-1' or fields[4] != b'-1':
        return False
    try:
        return Decimal(fields[1].decode('ascii')) == 0 and Decimal(fields[2].decode('ascii')) == 0
    except (InvalidOperation, UnicodeDecodeError):
        return False


@dataclass(frozen=True)
class BinanceSpotAggDaily(BinanceArchiveDaily):
    first_day: date = date(2017, 8, 17)
    SOURCE_KEY: ClassVar[str] = 'binance_spot_aggtrades'
    ARCHIVE_BASE_URL_ENV: ClassVar[str] = 'BINANCE_SPOT_AGGTRADES_ARCHIVE_BASE_URL'
    ARCHIVE_BASE_URL_DEFAULT: ClassVar[str] = (
        'https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT'
    )
    MEMBER_PREFIX: ClassVar[str] = 'BTCUSDT-aggTrades'
    FIELD_COUNT: ClassVar[int] = len(AGG_COLUMNS)
    HEADER: ClassVar[tuple[str, ...] | None] = None
    TIMESTAMP_INDEX: ClassVar[int] = 5

    def _get_response(self, url: str) -> Response:
        return get_response(url)

    def build_table(self, csv_body: bytes, partition: Partition) -> ArrowTable:
        return agg_table(csv_body, partition)

    def clean_rows(
        self, csv_body: bytes, partition: Partition
    ) -> tuple[bytes, dict[str, int]]:
        """Drop the two observed Binance-side quirk rows: -1/zero sentinel
        aggregates (2017/2018 days) and byte-identical duplicate lines from
        repackaged chunks (2026-02-11). A reused id with different content, a
        uniquely backward id, or anything else malformed still fails loud in
        validation. Clean archives return the identical bytes with no counts.
        """
        anomalies: set[int] = set()
        sentinels = 0
        max_id = -1
        for line in io.BytesIO(csv_body):
            fields = line.split(b',', 5)
            if len(fields) < 6:
                continue
            if _sentinel(fields):
                sentinels += 1
                continue
            try:
                trade_id = int(fields[0])
            except ValueError:
                continue
            if trade_id <= max_id:
                anomalies.add(trade_id)
                if len(anomalies) > _MAX_ANOMALY_IDS:
                    raise ValueError(
                        'Binance aggregate archive is too disordered to clean.'
                    )
            else:
                max_id = trade_id
        if not sentinels and not anomalies:
            return csv_body, {}
        out = bytearray()
        firsts: dict[int, bytes] = {}
        counts: dict[int, int] = {}
        duplicates = 0
        for line in io.BytesIO(csv_body):
            fields = line.split(b',', 5)
            if len(fields) < 6:
                out += line
                continue
            if _sentinel(fields):
                continue
            try:
                trade_id = int(fields[0])
            except ValueError:
                out += line
                continue
            if trade_id not in anomalies:
                out += line
                continue
            counts[trade_id] = counts.get(trade_id, 0) + 1
            flat = line.rstrip(b'\r\n')
            if trade_id not in firsts:
                firsts[trade_id] = flat
                out += line
                continue
            if firsts[trade_id] != flat:
                raise ValueError(
                    f'Binance aggregate id {trade_id} repeats with different content.'
                )
            duplicates += 1
        backward = sorted(anomaly for anomaly in anomalies if counts.get(anomaly, 0) < 2)
        if backward:
            raise ValueError(f'Binance aggregate id {backward[0]} is out of order.')
        dropped = {'sentinel_rows': sentinels, 'duplicate_rows': duplicates}
        return bytes(out), {kind: count for kind, count in dropped.items() if count}

    def build_row(
        self, trade_id: int, timestamp: int, instant: datetime, fields: list[str]
    ) -> Row:
        return (
            trade_id,
            parse_decimal(fields[1]),
            parse_decimal(fields[2]),
            _signed(fields[3]),
            _signed(fields[4]),
            timestamp,
            parse_archive_boolean(fields[6]),
            parse_archive_boolean(fields[7]),
            instant,
        )


def agg_csv_rows(body: bytes, partition: Partition) -> Iterator[Row]:
    return BinanceSpotAggDaily().parse_rows(body, partition)
