from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import ClassVar

from ..contracts import Partition, Revision, Row
from .binance_daily import Response, get_response
from .binance_perp_daily import parse_decimal, timestamp_datetime
from .binance_provisional import BinanceProvisionalBase, _bool, _int, _text


def historical_row(row: Mapping[str, object]) -> Row:
    timestamp = _int(row, 'time')
    if len(str(timestamp)) != 13:
        raise ValueError('The frozen provisional perp timestamp contract is milliseconds.')
    price = parse_decimal(_text(row, 'price'))
    quantity = parse_decimal(_text(row, 'qty'))
    # fapi rounds quoteQty to cents (observed '76.04' for the authoritative
    # '76.0435'); validate the field as a schema tripwire, then recompute the
    # exact quote so the provisional row matches the canonical archive row.
    parse_decimal(_text(row, 'quoteQty'))
    return (
        _int(row, 'id'),
        price,
        quantity,
        (price * quantity).normalize(),
        timestamp,
        _bool(row, 'isBuyerMaker'),
        timestamp_datetime(timestamp),
    )


def now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class BinancePerpProvisional(BinanceProvisionalBase):
    egress_ip: str | None = None
    SOURCE_NOUN: ClassVar[str] = 'perp'
    REST_BASE_URL_ENV: ClassVar[str] = 'BINANCE_PERP_REST_BASE_URL'
    REST_BASE_URL_DEFAULT: ClassVar[str] = 'https://fapi.binance.com'
    LATEST_SYMBOL_ENV: ClassVar[str] = 'BINANCE_PERP_LATEST_SYMBOL'
    AGG_TRADES_PATH: ClassVar[str] = '/fapi/v1/aggTrades'
    HISTORICAL_TRADES_PATH: ClassVar[str] = '/fapi/v1/historicalTrades'
    WEIGHT_LOCATOR: ClassVar[int] = 20
    WEIGHT_BOUNDARY: ClassVar[int] = 20
    WEIGHT_HISTORICAL: ClassVar[int] = 200
    # Fapi caps fromId-paged historicalTrades at 500 rows: limit=1000 answers
    # HTTP 400 code -1130 (verified live 2026-09-19). Do not raise this again.
    PAGE_LIMIT: ClassVar[int] = 500
    CREDENTIAL_REQUIRED: ClassVar[bool] = True
    PAGING_BACKTRACK_IDS: ClassVar[int] = 1000

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        configured = os.environ.get('ORIGO_BINANCE_PERP_EGRESS_IPS')
        selected = self
        if configured is not None:
            ips = configured.split(',')
            if (
                not 1 <= len(ips) <= 2
                or len(set(ips)) != len(ips)
                or any(ip not in ('37.27.112.140', '37.27.112.144') for ip in ips)
            ):
                raise ValueError(
                    'Perp egress requires one or two distinct dedicated IPv4 addresses.'
                )
            selected = replace(
                self, egress_ip=ips[int(partition.start.timestamp()) // 60 % len(ips)]
            )
        return BinanceProvisionalBase.fetch(selected, partition, previous_evidence)

    def map_row(self, row: Mapping[str, object]) -> Row:
        return historical_row(row)

    def _get_response(
        self,
        url: str,
        params: dict[str, str | int],
        headers: dict[str, str],
        weight: int,
    ) -> Response:
        if self.egress_ip is not None:
            return get_response(
                url, params=params, headers=headers, weight=weight, egress_ip=self.egress_ip
            )
        return get_response(url, params=params, headers=headers, weight=weight)

    def _now_utc(self) -> datetime:
        return now_utc()
