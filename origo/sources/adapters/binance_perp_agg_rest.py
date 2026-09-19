from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from ..contracts import Row
from .binance_daily import Response, get_response
from .binance_provisional import BinanceProvisionalBase, _bool, _int, _text
from .binance_perp_agg_daily import parse_decimal, timestamp_datetime


def agg_row(row: Mapping[str, object]) -> Row:
    timestamp = _int(row, 'T')
    if len(str(timestamp)) != 13:
        raise ValueError('The frozen provisional perp aggregate timestamp contract is milliseconds.')
    # Fapi answers `nq` where spot answers nothing and omits spot's `M`
    # best-match flag; `nq` carries no new information, so it is ignored.
    _text(row, 'nq')
    return (
        _int(row, 'a'),
        parse_decimal(_text(row, 'p')),
        parse_decimal(_text(row, 'q')),
        _int(row, 'f'),
        _int(row, 'l'),
        timestamp,
        _bool(row, 'm'),
        timestamp_datetime(timestamp),
    )


def now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class BinancePerpAggProvisional(BinanceProvisionalBase):
    SOURCE_NOUN: ClassVar[str] = 'perp aggregate'
    REST_BASE_URL_ENV: ClassVar[str] = 'BINANCE_PERP_AGGTRADES_REST_BASE_URL'
    REST_BASE_URL_DEFAULT: ClassVar[str] = 'https://fapi.binance.com'
    LATEST_SYMBOL_ENV: ClassVar[str] = 'BINANCE_PERP_AGGTRADES_LATEST_SYMBOL'
    AGG_TRADES_PATH: ClassVar[str] = '/fapi/v1/aggTrades'
    HISTORICAL_TRADES_PATH: ClassVar[str | None] = None
    WEIGHT_LOCATOR: ClassVar[int] = 20
    WEIGHT_BOUNDARY: ClassVar[int] = 20
    WEIGHT_HISTORICAL: ClassVar[int] = 20
    PAGE_LIMIT: ClassVar[int] = 1000
    CREDENTIAL_REQUIRED: ClassVar[bool] = False
    PAGING_BACKTRACK_IDS: ClassVar[int] = 0

    def map_row(self, row: Mapping[str, object]) -> Row:
        return agg_row(row)

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
