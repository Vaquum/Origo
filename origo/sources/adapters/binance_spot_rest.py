from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from ..contracts import Row
from .binance_daily import Response, get_response, parse_decimal, timestamp_datetime
from .binance_provisional import BinanceProvisionalBase, _bool, _int, _text


def historical_row(row: Mapping[str, object]) -> Row:
    timestamp = _int(row, 'time')
    if len(str(timestamp)) != 13:
        raise ValueError('The frozen provisional spot timestamp contract is milliseconds.')
    return (
        _int(row, 'id'),
        parse_decimal(_text(row, 'price')),
        parse_decimal(_text(row, 'qty')),
        parse_decimal(_text(row, 'quoteQty')),
        timestamp,
        _bool(row, 'isBuyerMaker'),
        _bool(row, 'isBestMatch'),
        timestamp_datetime(timestamp),
    )


def now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class BinanceSpotProvisional(BinanceProvisionalBase):
    SOURCE_NOUN: ClassVar[str] = 'spot'
    REST_BASE_URL_ENV: ClassVar[str] = 'BINANCE_SPOT_REST_BASE_URL'
    REST_BASE_URL_DEFAULT: ClassVar[str] = 'https://api.binance.com'
    LATEST_SYMBOL_ENV: ClassVar[str] = 'BINANCE_SPOT_LATEST_SYMBOL'
    AGG_TRADES_PATH: ClassVar[str] = '/api/v3/aggTrades'
    HISTORICAL_TRADES_PATH: ClassVar[str] = '/api/v3/historicalTrades'
    WEIGHT_LOCATOR: ClassVar[int] = 4
    WEIGHT_BOUNDARY: ClassVar[int] = 4
    WEIGHT_HISTORICAL: ClassVar[int] = 25
    PAGE_LIMIT: ClassVar[int] = 1000
    CREDENTIAL_REQUIRED: ClassVar[bool] = False
    PAGING_BACKTRACK_IDS: ClassVar[int] = 0

    def map_row(self, row: Mapping[str, object]) -> Row:
        return historical_row(row)

    def _get_response(
        self,
        url: str,
        params: dict[str, str | int],
        headers: dict[str, str],
        weight: int,
    ) -> Response:
        return get_response(url, params=params, headers=headers, weight=weight)

    def _now_utc(self) -> datetime:
        return now_utc()
