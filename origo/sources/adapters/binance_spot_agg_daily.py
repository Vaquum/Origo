from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import ClassVar

from ..arrow_types import ArrowTable
from ..contracts import Partition, Row
from .binance_archive import BinanceArchiveDaily, parse_archive_boolean
from .binance_archive import parse_decimal as shared_parse_decimal
from .binance_archive import timestamp_datetime as timestamp_datetime
from .binance_daily import Response, get_response
from .binance_spot_agg_columnar import COLUMNS as AGG_COLUMNS
from .binance_spot_agg_columnar import agg_table


def parse_decimal(text: str) -> Decimal:
    return shared_parse_decimal(text, noun='Spot aggregate price and quantity')


def _signed(text: str) -> int:
    if text.startswith('-'):
        magnitude = text[1:]
        if not magnitude.isdigit():
            raise ValueError('Binance aggregate trade IDs must be integers.')
        return -int(magnitude)
    if not text.isdigit():
        raise ValueError('Binance aggregate trade IDs must be integers.')
    return int(text)


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
        """Share the aggregate quirk cleaner; the futures agg source reuses it."""
        return self.clean_agg_rows(csv_body)

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
