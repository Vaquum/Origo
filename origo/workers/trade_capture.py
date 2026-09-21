"""Durable independent capture for the existing perpetual individual-trade source.

Only verified overlap and boundary evidence seal minutes. Gaps remain explicit
and use the authenticated historical repair path; no aggregate substitution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Protocol

from origo.sources.adapters.binance_daily import Response, get_response
from origo.sources.adapters.binance_perp_rest import historical_row
from origo.sources.contracts import failure_code
from origo.sources.locking import source_lock
from origo.steady_state.trade_spool import CaptureOutcome, PageCost, TradeSpool
from origo.steady_state.trade_spool import (
    parse_recent_trades,
    spool_directory,
    spool_max_bytes,
    spool_path,
)
from .runtime import heartbeat_directory, heartbeat_path, touch_heartbeat

SOURCE_KEY = 'binance_perp_trades'
FEED = 'trade-capture'
SYMBOL = 'BTCUSDT'
RECENT_PATH = '/fapi/v1/trades'
PAGE_ROWS = 1000
REQUEST_WEIGHT = 5
POLL_SECONDS = 0.6
HEALTH_SECONDS = 180
log = logging.getLogger('origo.workers.trade_capture')


class Transport(Protocol):
    def __call__(
        self,
        url: str,
        *,
        params: Mapping[str, str | int],
        headers: Mapping[str, str],
        weight: int,
        lane: str,
    ) -> Response: ...


class TradeCapture:
    def __init__(
        self,
        spool: TradeSpool,
        *,
        base_url: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        transport: Transport | None = None,
    ) -> None:
        self.spool = spool
        self.base_url = base_url.rstrip('/')
        self.clock = clock
        self.transport = transport

    def step(self) -> CaptureOutcome:
        """A successful return means the provider page was durably committed."""
        if self.spool.backpressure():
            raise RuntimeError('Unaccepted trade spool reached its byte cap; capture is blocked.')
        started = self.clock()
        params: dict[str, str | int] = {'symbol': SYMBOL, 'limit': PAGE_ROWS}
        response = (
            get_response(
                self.base_url + RECENT_PATH,
                params=params,
                headers={},
                weight=REQUEST_WEIGHT,
                lane='live',
            )
            if self.transport is None
            else self.transport(
                self.base_url + RECENT_PATH,
                params=params,
                headers={},
                weight=REQUEST_WEIGHT,
                lane='live',
            )
        )
        if response.status != 200:
            raise RuntimeError(f'Recent-trade provider returned status {response.status}.')
        trades = parse_recent_trades(response.body)
        if not trades or len(trades) > PAGE_ROWS:
            raise ValueError('Recent-trade page must contain one to 1000 actual trades.')
        for trade in trades:
            historical_row(trade.provider_row())
        cost = response.cost
        outcome = self.spool.record(
            trades,
            captured_at=started,
            completed_at=self.clock(),
            status=response.status,
            body_sha256=hashlib.sha256(response.body).hexdigest(),
            cost=PageCost(
                REQUEST_WEIGHT,
                cost.lock_wait_ms,
                cost.pace_wait_ms,
                cost.latency_ms,
                cost.used_weight_1m,
            ),
        )
        if outcome.closed_reason:
            log.error(
                'Capture discontinuity segment=%s reason=%s; historical repair required',
                outcome.segment,
                outcome.closed_reason,
            )
        return outcome


def check_spool(path: Path, *, now: datetime, max_age_seconds: float = HEALTH_SECONDS) -> int:
    """Health is successful durable input, not a self-refreshed timer."""
    if not path.is_file():
        return 1
    try:
        with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2.0) as connection:
            row = connection.execute(
                'SELECT completed_at,last_time FROM responses WHERE status=200 '
                'ORDER BY seq DESC LIMIT 1'
            ).fetchone()
            if row is None or row[0] is None or row[1] is None:
                return 1
            ages = (now.timestamp() - int(value) / 1000 for value in row)
            return 0 if all(0 <= age <= max_age_seconds for age in ages) else 1
    except (sqlite3.Error, OSError, ValueError) as error:
        log.error('Trade-capture health evidence unreadable: %s', type(error).__name__)
        return 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    if os.environ.get('BINANCE_PERP_LATEST_SYMBOL', SYMBOL) != SYMBOL:
        raise ValueError('The existing source owns BTCUSDT only.')
    path = spool_path(spool_directory(os.environ), SOURCE_KEY, SYMBOL)
    if args.check:
        return check_spool(path, now=datetime.now(UTC))
    heartbeat = heartbeat_path(heartbeat_directory(), FEED)
    base = os.environ.get('BINANCE_PERP_REST_BASE_URL', 'https://fapi.binance.com')
    stopping = False

    def stop(signum: int, frame: FrameType | None) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    with source_lock(root, SOURCE_KEY, 'trade_capture_assignment'):
        spool = TradeSpool.create(path, historical_row, max_bytes=spool_max_bytes(os.environ))
        try:
            capture = TradeCapture(spool, base_url=base)
            failures = 0
            while not stopping:
                began = time.monotonic()
                try:
                    result = capture.step()
                    failures = 0
                    touch_heartbeat(heartbeat)
                    if result.sealed:
                        log.info(
                            'Durably sealed %s minute(s); segment=%s',
                            len(result.sealed),
                            result.segment,
                        )
                    if args.once:
                        print(json.dumps(asdict(result), default=str, sort_keys=True))
                        return 0
                except Exception as error:
                    failures += 1
                    log.exception('Recent-trade capture failed; no completeness was inferred')
                    try:
                        spool.record_fault(
                            datetime.now(UTC), failure_code(error), type(error).__name__
                        )
                    except Exception:
                        log.exception('Capture failure evidence could not be persisted')
                    if args.once:
                        return 1
                interval = min(60.0, 2.0 ** min(failures, 6)) if failures else POLL_SECONDS
                pause = max(0.0, interval - (time.monotonic() - began))
                deadline = time.monotonic() + pause
                while not stopping and time.monotonic() < deadline:
                    time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        finally:
            spool.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
