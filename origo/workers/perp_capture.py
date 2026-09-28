"""Capture recent raw perpetual trades independently of repair and publication."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Literal, TypedDict, cast

from origo.sources.adapters.binance_daily import get_response
from origo.sources.adapters.binance_perp_spool import (
    CaptureCommit,
    append_capture,
    begin_capture_attempt,
    capture_state,
    finish_capture_attempt,
)
from origo.sources.contracts import WORKER_HEARTBEAT_ENV, SourceError, failure_code
from origo.sources.locking import source_lock

from .runtime import (
    HEARTBEAT_MAX_AGE_SECONDS,
    heartbeat_directory,
    heartbeat_is_fresh,
    heartbeat_path,
    start_watchdog,
    touch_heartbeat,
)

POLL_INTERVAL_SECONDS = 0.5
RECENT_TRADES_LIMIT = 1000
RECENT_TRADES_WEIGHT = 5
CAPTURE_EGRESS_IP = '37.27.112.140'
RECENT_TRADES_URL = 'https://fapi.binance.com/fapi/v1/trades'
STATUS_MAX_BYTES = 16 * 1024
FEED = 'perp_capture'
log = logging.getLogger(__name__)


class CaptureStatus(TypedDict):
    schema_version: Literal[1]
    committed_at: str
    last_response_at: str | None
    last_durable_capture_at: str | None
    complete_through: str | None
    segment_id: str | None
    spool_bytes: int
    last_poll_gap_seconds: float | None
    error_code: str | None


def status_path(directory: Path) -> Path:
    return directory / 'perp_capture.status.json'


def _write_status(path: Path, status: CaptureStatus) -> None:
    payload = json.dumps(status, separators=(',', ':'), allow_nan=False).encode()
    if len(payload) > STATUS_MAX_BYTES:
        raise ValueError('Capture status exceeds its 16 KiB bound.')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError('Capture evidence requires a UTC timestamp.')
    return parsed


def parse_recent(body: bytes) -> list[Mapping[str, object]]:
    value: object = json.loads(body)
    if not isinstance(value, list):
        raise ValueError('Recent trades response must be a list.')
    items = cast(list[object], value)
    if not 1 <= len(items) <= RECENT_TRADES_LIMIT:
        raise ValueError('Recent trades response requires one to 1000 actual trades.')
    if any(not isinstance(item, dict) for item in items):
        raise ValueError('Recent trade must be an object.')
    return cast(list[Mapping[str, object]], items)


class PerpCapture:
    def __init__(
        self, root: Path, status: Path, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self.root = root
        self.status = status
        self.clock = clock
        self.commit: CaptureCommit | None = None
        self.last_response: datetime | None = None
        self.last_poll: datetime | None = None
        self.poll_gap: float | None = None
        self.error: str | None = None
        try:
            self.commit = capture_state(root)
        except (OSError, SourceError, ValueError) as error:
            self.publish_status(failure_code(error))
            raise

    def publish_status(self, error: str | None) -> None:
        now = self.clock()
        commit = self.commit
        status: CaptureStatus = {
            'schema_version': 1,
            'committed_at': now.isoformat(),
            'last_response_at': self.last_response.isoformat() if self.last_response else None,
            'last_durable_capture_at': commit.last_durable_capture_at.isoformat() if commit else None,
            'complete_through': (
                commit.complete_through.isoformat() if commit and commit.complete_through else None
            ),
            'segment_id': commit.segment_id if commit else None,
            'spool_bytes': commit.spool_bytes if commit else 0,
            'last_poll_gap_seconds': self.poll_gap,
            'error_code': error,
        }
        _write_status(self.status, status)
        if error != self.error:
            if error:
                log.error('Capture state changed: %s', error)
            else:
                log.info('Capture recovered durable input')
            self.error = error

    def step(self) -> CaptureCommit:
        now = self.clock()
        self.poll_gap = (now - self.last_poll).total_seconds() if self.last_poll else None
        self.last_poll = now
        evidence: dict[str, object] = {
            'url': RECENT_TRADES_URL, 'params': {'symbol': 'BTCUSDT', 'limit': RECENT_TRADES_LIMIT},
            'started_at': now.isoformat(), 'egress_ip': CAPTURE_EGRESS_IP,
            'request_weight_upper_bound': RECENT_TRADES_WEIGHT, 'outcome': 'pending',
        }
        attempt = begin_capture_attempt(self.root, evidence)
        try:
            response = get_response(
                RECENT_TRADES_URL,
                params={'symbol': 'BTCUSDT', 'limit': RECENT_TRADES_LIMIT},
                weight=RECENT_TRADES_WEIGHT,
                egress_ip=CAPTURE_EGRESS_IP,
            )
            self.last_response = self.clock()
            evidence.update({
                'completed_at': self.last_response.isoformat(), 'status': response.status,
                'headers': {
                    key: value for key, value in response.headers.items()
                    if key.lower() in ('x-mbx-used-weight', 'x-mbx-used-weight-1m', 'retry-after', 'date')
                },
                'body_sha256': hashlib.sha256(response.body).hexdigest(),
                'request_weight': RECENT_TRADES_WEIGHT, 'outcome': 'succeeded', 'error_code': None,
            })
            self.commit = append_capture(
                self.root, parse_recent(response.body), received_at=self.last_response,
                evidence=evidence, attempt_id=attempt,
            )
        except (OSError, SourceError, ValueError) as error:
            evidence.update({'completed_at': self.clock().isoformat(), 'outcome': 'failed',
                             'error_code': failure_code(error)})
            finish_capture_attempt(self.root, attempt, evidence)
            raise
        self.publish_status('CAPTURE_OVERLAP_BREAK' if self.commit.pending_gaps else None)
        return self.commit


def check_capture(directory: Path, *, now: datetime) -> int:
    if not heartbeat_is_fresh(
        heartbeat_path(directory, FEED), max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS,
        now=now.timestamp(),
    ):
        return 1
    try:
        with status_path(directory).open('rb') as stream:
            raw = stream.read(STATUS_MAX_BYTES + 1)
        if len(raw) > STATUS_MAX_BYTES:
            raise ValueError('Capture status exceeds its 16 KiB bound.')
        value = _mapping(json.loads(raw))
        if value.get('schema_version') != 1:
            raise ValueError('Unsupported capture status schema.')
        for name in ('committed_at', 'last_durable_capture_at'):
            timestamp = value.get(name)
            if not isinstance(timestamp, str):
                return 1
            if not 0 <= (now - _utc(timestamp)).total_seconds() <= HEARTBEAT_MAX_AGE_SECONDS:
                return 1
        return 0 if value.get('error_code') is None else 1
    except (OSError, ValueError) as error:
        log.error('Capture health evidence is unreadable: %s', type(error).__name__)
        return 1


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError('Evidence must be an object.')
    return cast(dict[str, object], value)


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError('Missing evidence timestamp.')
    return _utc(value)


def validate_closeout(
    reports: Sequence[Mapping[str, object]], *, start: datetime, end: datetime
) -> None:
    """Validate a preregistered six-hour reader window from original law tape records."""
    if end - start != timedelta(hours=6) or start.second or start.microsecond:
        raise ValueError('Closeout requires an exact six-hour minute-aligned window.')
    if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0):
        raise ValueError('Closeout requires UTC bounds.')
    if len(reports) != 361:
        raise ValueError('Closeout requires 361 consecutive observations; missing slots fail.')
    previous_reader: datetime | None = None
    previous_canonical_days: int | None = None
    for index, report in enumerate(reports):
        slot = start + timedelta(minutes=index)
        if report.get('schema_version') != 1 or _timestamp(report.get('sampling_slot')) != slot:
            raise ValueError('Closeout contains duplicate, missing or incompatible observations.')
        began = _timestamp(report.get('evaluation_start'))
        finished = _timestamp(report.get('evaluation_end'))
        if not slot <= began <= finished < slot + timedelta(minutes=1):
            raise ValueError('Closeout contains stale or invalid evaluation timestamps.')
        if not report.get('deployed_sha') or not report.get('catalog_version'):
            raise ValueError('Closeout lacks deployed code or catalog identity.')
        feeds = report.get('feeds')
        if not isinstance(feeds, list):
            raise ValueError('Closeout lacks feed observations.')
        matching = [_mapping(feed) for feed in cast(list[object], feeds)
                    if _mapping(feed).get('source_key') == 'binance_perp_trades']
        if len(matching) != 1:
            raise ValueError('Closeout lacks an unambiguous raw-perp observation.')
        predicates = _mapping(matching[0].get('predicates'))
        r1, c2 = (_mapping(predicates.get(name)) for name in ('R1', 'C2'))
        if r1.get('status') != 'PASS' or c2.get('status') != 'PASS':
            raise ValueError('Closeout contains a failed or UNKNOWN reader/coverage observation.')
        evidence = _mapping(r1.get('evidence'))
        age = evidence.get('age_seconds')
        if (type(age) not in (int, float) or not 0 <= cast(float, age) <= 300
                or evidence.get('budget_seconds') != 300):
            raise ValueError('Closeout reader age exceeds the unchanged 300-second budget.')
        reader = _timestamp(evidence.get('reader_end'))
        if not 0 <= (began - reader).total_seconds() <= 300:
            raise ValueError('Closeout reader timestamp contradicts the reported age.')
        if previous_reader is not None and reader < previous_reader:
            raise ValueError('Closeout reader coverage regressed.')
        previous_reader = reader
        canonical = _mapping(c2.get('evidence'))
        if canonical.get('missing_days') != 0 or canonical.get('unknown_days') != 0:
            raise ValueError('Closeout contains missing or unknown canonical coverage.')
        days = canonical.get('valid_days')
        if type(days) is not int or days < 0:
            raise ValueError('Closeout lacks canonical coverage accounting.')
        if previous_canonical_days is not None and days < previous_canonical_days:
            raise ValueError('Closeout canonical coverage regressed.')
        previous_canonical_days = days


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    directory = heartbeat_directory()
    if args.check:
        return check_capture(directory, now=datetime.now(UTC))
    if os.environ.get('BINANCE_PERP_LATEST_SYMBOL', 'BTCUSDT') != 'BTCUSDT':
        raise ValueError('The raw perpetual source owns BTCUSDT only.')
    root = Path(os.environ.get('ORIGO_PERP_CAPTURE_ROOT', '/var/lib/origo-perp-capture'))
    if not root.is_absolute():
        raise ValueError('Capture requires an absolute persistent spool mount.')
    heartbeat = heartbeat_path(directory, FEED)
    os.environ[WORKER_HEARTBEAT_ENV] = str(heartbeat)
    stopping = False

    def stop(signum: int, frame: FrameType | None) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    touch_heartbeat(heartbeat)
    start_watchdog(heartbeat, max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS)
    with source_lock(root, 'binance_perp_trades', 'capture'):
        capture = PerpCapture(root, status_path(directory))
        while not stopping:
            began = time.monotonic()
            try:
                capture.step()
                touch_heartbeat(heartbeat)
            except SourceError as error:
                capture.publish_status(failure_code(error))
                if not error.code.startswith('PROVIDER_'):
                    raise
            except (OSError, ValueError) as error:
                capture.publish_status(failure_code(error))
                raise
            time.sleep(max(0.0, POLL_INTERVAL_SECONDS - (time.monotonic() - began)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
