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
from .binance_perp_columnar import COLUMNS as PERP_COLUMNS
from .binance_perp_columnar import HEADER as PERP_HEADER
from .binance_perp_columnar import perp_table


def parse_decimal(text: str) -> Decimal:
    return shared_parse_decimal(text, noun='Perp price, quantity, and quote quantity')


@dataclass(frozen=True)
class BinancePerpDaily(BinanceArchiveDaily):
    first_day: date = date(2019, 9, 8)
    SOURCE_KEY: ClassVar[str] = 'binance_perp_trades'
    ARCHIVE_BASE_URL_ENV: ClassVar[str] = 'BINANCE_PERP_TRADES_ARCHIVE_BASE_URL'
    ARCHIVE_BASE_URL_DEFAULT: ClassVar[str] = (
        'https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT'
    )
    FIELD_COUNT: ClassVar[int] = len(PERP_COLUMNS)
    HEADER: ClassVar[tuple[str, ...] | None] = tuple(PERP_HEADER.split(','))

    def _get_response(self, url: str) -> Response:
        return get_response(url)

    def build_table(self, csv_body: bytes, partition: Partition) -> ArrowTable:
        return perp_table(csv_body, partition)

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
            instant,
        )


def perp_csv_rows(body: bytes, partition: Partition) -> Iterator[Row]:
    return BinancePerpDaily().parse_rows(body, partition)
