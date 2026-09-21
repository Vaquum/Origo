from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar, cast

from origo.steady_state.trade_spool import (
    AckOutcome,
    CaptureMiss,
    SealedMinute,
    TradeSpool,
    spool_directory,
    spool_path,
)

from ..contracts import Partition, Revision, Row
from .binance_daily import Response, get_response
from .binance_perp_daily import parse_decimal, timestamp_datetime
from .binance_provisional import BinanceProvisionalBase, _bool, _int, _text

SOURCE_KEY = 'binance_perp_trades'
log = logging.getLogger(__name__)


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


def _spool_file() -> tuple[str, Path]:
    symbol = os.environ.get(BinancePerpProvisional.LATEST_SYMBOL_ENV, 'BTCUSDT')
    return symbol, spool_path(spool_directory(os.environ), SOURCE_KEY, symbol)


def captured_minute(partition: Partition) -> SealedMinute | CaptureMiss:
    """The capture spool's sealed minute for the partition, or why there is none.

    The spool is read in place and never created here; an unreadable spool is a
    logged miss so the authenticated historical path keeps serving the minute.
    """
    _, path = _spool_file()
    try:
        spool = TradeSpool.attach(path, historical_row)
    except FileNotFoundError:
        return CaptureMiss('spool_missing')
    except sqlite3.Error as error:
        log.error('Trade spool %s is unreadable: %s', path, type(error).__name__)
        return CaptureMiss(f'spool_unreadable:{type(error).__name__}')
    try:
        return spool.sealed_minute(partition.start)
    finally:
        spool.close()


def acknowledge_captured(
    partition: Partition, *, content_hash: str, generation: str
) -> AckOutcome | None:
    """Release the spool's rows for a minute whose accepted generation is durable.

    Main calls this after a verified provisional activation; ``None`` means no spool
    exists on this host, which is not an error for a worker without the mount.
    """
    _, path = _spool_file()
    try:
        spool = TradeSpool.attach(path, historical_row)
    except FileNotFoundError:
        log.warning(
            'Capture acknowledgement unavailable for %s: durable spool is missing', partition.key
        )
        return None
    try:
        outcome = spool.acknowledge(
            partition.start, content_hash=content_hash, generation=generation, now=now_utc()
        )
    finally:
        spool.close()
    if outcome.sealed and not outcome.hash_matched:
        log.error(
            'Captured minute %s sealed a different hash than accepted generation %s',
            partition.key,
            generation,
        )
    return outcome


@dataclass(frozen=True)
class BinancePerpProvisional(BinanceProvisionalBase):
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
    # The retained peak corpus includes a 53,104-row minute: 100 pages is insufficient.
    MAX_REQUEST_PAGES: ClassVar[int] = 256
    CREDENTIAL_REQUIRED: ClassVar[bool] = True
    PAGING_BACKTRACK_IDS: ClassVar[int] = 1000

    def acknowledge(self, partition: Partition, *, content_hash: str, generation: str) -> None:
        from ..contracts import SourceError

        outcome = acknowledge_captured(partition, content_hash=content_hash, generation=generation)
        if outcome is not None and outcome.sealed and not outcome.hash_matched:
            raise SourceError(
                'CAPTURE_CONFLICT', 'Accepted and captured input differ; captured rows retained.'
            )

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

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        """A checksum-verified sealed capture minute first; otherwise the exact
        authenticated historical path, with the capture miss named in its evidence."""
        if not partition.provisional or partition.end > self._now_utc():
            raise ValueError(f'Only closed {self.SOURCE_NOUN} minutes may be fetched.')
        captured = captured_minute(partition)
        if isinstance(captured, SealedMinute):
            rows = captured.rows
            return Revision(
                captured.content_hash,
                captured.content_hash,
                json.dumps({'capture': captured.evidence, 'requests': []}, sort_keys=True),
                len(rows),
                lambda: iter(rows),
            )
        _, path = _spool_file()
        if path.is_file() and os.environ.get('BINANCE_API_KEY'):
            from origo.steady_state.capture_repair import repair_minute

            spool = TradeSpool.attach(path, historical_row)
            try:
                base = os.environ.get(self.REST_BASE_URL_ENV, self.REST_BASE_URL_DEFAULT).rstrip(
                    '/'
                )

                def fetch_gap(start_id: int) -> Response:
                    return self._get_response(
                        base + '/fapi/v1/historicalTrades',
                        {'symbol': 'BTCUSDT', 'fromId': start_id, 'limit': self.PAGE_LIMIT},
                        {'X-MBX-APIKEY': os.environ['BINANCE_API_KEY']},
                        self.WEIGHT_HISTORICAL,
                    )

                repaired = repair_minute(spool, partition, fetch_gap)
                if repaired is not None:
                    return repaired
            finally:
                spool.close()
        revision = super().fetch(partition, previous_evidence)
        decoded: object = json.loads(revision.evidence_json)
        if not isinstance(decoded, dict):
            raise ValueError('Provisional evidence must be an object.')
        evidence = cast(dict[str, object], decoded)
        evidence['capture_miss'] = captured.reason
        return replace(revision, evidence_json=json.dumps(evidence, sort_keys=True))
