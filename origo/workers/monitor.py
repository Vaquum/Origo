"""The monitor: one detector outside Dagster that watches Dagster, the workers, the
collectors and the container log, writes its verdicts into Dagit and e-mails new findings.

Every minute the tick evaluates seven checks on the ``origo_monitor`` asset:

- ``dagster_reachable``: the webserver answers and every required daemon is healthy;
- ``queue_bounded``: fewer queued runs than the threshold, and no run failure or failed
  asset check since the last tick;
- ``workers_alive``: every worker heartbeat is fresh and no worker receipt failed;
- ``collectors_serving``: each depth collector returns rows for the last completed minute;
- ``no_error_logs``: the container log holds no ``ERROR`` row since the last tick;
- ``publication_current``: every public consumer publishes the current source state;
- ``data_current``: the reader law and its evidence/page are current.

Evaluations precede mail preparation. Dashboard and HTML/text mail share one recorded
operator summary. Lifecycle changes and today's digest share a global hourly dispatch
budget; retries preserve an immutable private intent in the existing cursor. Compact
notification observations extend the public tape without introducing another detector.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import logging
import math
import os
import signal
import sys
import time
import urllib.error
import urllib.request
import zlib
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from types import FrameType
from typing import Literal, cast

from origo.alerts.email import AlertSettings
from origo.alerts.summary import (
    DeliveryReceipt, LostInterval, PendingNotification, add_loss_interval, attempt_delivery,
    plan_notification, prune_unsent, validate_delivery_receipt, validate_lost_intervals, validate_pending,
)
from origo.observatory import (
    Document, Json, Measurement, NotificationObservation, ObservationFrame, ObservedRead, OperatorSummary,
    build_summary, consecutive_window, decode_observation_record, lifecycle_transitions,
    operation_sample as _operation_sample, operations_totals, sample_brief as _sample_brief,
)
from origo.assets.build_depth_snapshot_store_arrow import (
    depth_snapshot_chunk_relative_path,
    minute_start_from_partition_key,
)
from origo.assets.create_origo_database import (
    ClickHouseSettings,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from origo.law import (
    LAW_INVENTORY,
    LAW_QUERY_SETTINGS,
    LAW_SAMPLE_NAME,
    LAW_SCHEMA_VERSION,
    LAW_TAPE_ROOT,
    LawReport,
    evaluate,
)
from origo.law_catalog import (
    GateEvaluation,
    LawCatalog,
    ProjectionObservation,
    PublicationPolicy,
    Scalar,
    build_catalog,
    gate_evaluation,
    import_gate_events,
    observe_publications,
    projection_gate_evaluations,
    unknown_observations,
)
from origo.sources.contracts import Client, RolloutStage, identifier
from origo.sources.registry import SOURCE_REGISTRY

from .dagster_reader import DagsterReader, RunFailure
from .depth import DEPTH_SPECS, DepthFeed
from .receipts import ensure_monitoring_tables, error_log_rows_since, failed_receipts_since
from .report import Reporter
from .runtime import (
    HEARTBEAT_MAX_AGE_SECONDS,
    TickOutcome,
    check_heartbeat,
    heartbeat_directory,
    heartbeat_is_fresh,
    heartbeat_path,
    run_forever,
)

MONITOR_ASSET = 'origo_monitor'
CheckName = Literal[
    'dagster_reachable',
    'data_current',
    'queue_bounded',
    'workers_alive',
    'collectors_serving',
    'no_error_logs',
    'publication_current',
]
CHECK_NAMES: tuple[CheckName, ...] = (
    'collectors_serving',
    'dagster_reachable',
    'data_current',
    'no_error_logs',
    'publication_current',
    'queue_bounded',
    'workers_alive',
)
DEFAULT_WEBSERVER_URL = 'http://dagit:3000'
# The market state query service (origo.workers.market_state_api) is expected from first start.
MARKET_STATE_API_FEED = 'market_state_api'
PERP_CAPTURE_FEED = 'perp_capture'
CAPTURE_STATUS_BYTES = 16 * 1024
CAPTURE_SPOOL_BYTES = 16 * 1024**3
PROBE_TIMEOUT_SECONDS = 10
# ClickHouse rows are read up to this far behind the clock: Vector delivers a log line
# seconds after its stamp and a worker stamps a receipt before inserting it, so a row
# stamped just before a read and inserted after it must still fall inside a later window.
DELIVERY_LAG_SECONDS = 60
# A run still queued this long after creation never got a slot: backfill waves
# drain in ~2h, so 12h means stuck, not busy.
QUEUE_STUCK_AFTER = timedelta(hours=12)
PUBLICATION_ROOT_ENV = 'ORIGO_SOURCE_PUBLICATION_ROOT'
PUBLICATION_ROOT_DEFAULT = '/opt/origo/shadow'
# A pinned consumer (mount) renders on every state change, so hours without a render
# while the state advances is stuck; a canonical-only consumer (huggingface) publishes
# daily, so a full missed day is the bound. Both yield while a backfill is active.
PINNED_PUBLICATION_STALE_AFTER = timedelta(hours=3)
CANONICAL_PUBLICATION_STALE_AFTER = timedelta(hours=24)
log = logging.getLogger('origo.workers.monitor')


@dataclass(frozen=True)
class Finding:
    key: str
    check: CheckName
    title: str
    detail: str


@dataclass(frozen=True)
class CollectorProbe:
    name: str
    base_url_env: str
    auth_token_env: str


@dataclass
class Cursor:
    """The monitor's only state: where each detector resumes and what was already sent."""

    failures_after: float
    receipts_after: str
    logs_after: str
    sent: dict[str, float]
    last_digest_date: str
    ticks: int
    findings: int
    law_streaks: dict[str, list[str | int]] = field(default_factory=lambda: {})
    law_history: dict[str, str] = field(default_factory=lambda: {})

    notification_schema: int = 1
    pending_notification: PendingNotification | None = None
    last_delivery: DeliveryReceipt | None = None
    next_distinct_at: float = 0.0
    notified_through: str | None = None
    expired_through: str | None = None
    lost_intervals: list[LostInterval] = field(default_factory=lambda: [])
    clock_checked_at: float = 0.0
    notification_started_at: str | None = None
    notification_fault: str = ''
    invalid_notification_state: dict[str, object] | None = None

    @classmethod
    def load(cls, path: Path, now: datetime, lookback_minutes: int) -> Cursor:
        start = now - timedelta(minutes=lookback_minutes)
        if not path.exists():
            return cls(start.timestamp(), start.isoformat(), start.isoformat(), {}, '', 0, 0)
        with path.open('rb') as stream:
            payload = stream.read(256 * 1024 + 1)
        if len(payload) > 256 * 1024:
            raise ValueError('Private monitor cursor exceeds 256 KiB; dispatch is blocked')
        raw: object = json.loads(payload)
        if not isinstance(raw, dict):
            raise ValueError('The monitor cursor must be an object.')
        data = cast(dict[str, object], raw)
        sent_raw = data.get('sent')
        sent = (
            {
                str(key): float(cast(float, value))
                for key, value in cast(dict[str, object], sent_raw).items()
            }
            if isinstance(sent_raw, dict)
            else {}
        )
        cursor = cls(
            float(cast(float, data.get('failures_after', start.timestamp()))),
            str(data.get('receipts_after', start.isoformat())),
            str(data.get('logs_after', start.isoformat())),
            sent,
            str(data.get('last_digest_date', '')),
            int(cast(int, data.get('ticks', 0))),
            int(cast(int, data.get('findings', 0))),
            cast(dict[str, list[str | int]], data.get('law_streaks', {})),
            cast(dict[str, str], data.get('law_history', {})),
        )
        try:
            if data.get('notification_schema') == 1:
                pending = data.get('pending_notification')
                receipt = data.get('last_delivery')
                cursor.pending_notification = validate_pending(pending) if pending is not None else None
                cursor.last_delivery = validate_delivery_receipt(receipt) if receipt is not None else None
                cursor.lost_intervals = validate_lost_intervals(data.get('lost_intervals', []))
                for key in ('next_distinct_at', 'clock_checked_at'):
                    value = data.get(key, 0.0)
                    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
                        raise ValueError(f'Invalid notification cursor {key}')
                    setattr(cursor, key, float(value))
                for key in ('notified_through', 'expired_through', 'notification_started_at'):
                    value = data.get(key)
                    if value is not None:
                        if not isinstance(value, str) or datetime.fromisoformat(value).tzinfo is None:
                            raise ValueError(f'Invalid notification cursor {key}')
                        setattr(cursor, key, value)
            else:
                # A legacy digest has only a date. Reserve one hour once, then persist the migration.
                recent = any(stamp > now.timestamp() - 3600 for stamp in sent.values())
                if recent or cursor.last_digest_date >= now.date().isoformat():
                    cursor.next_distinct_at = now.timestamp() + 3610
                cursor.clock_checked_at = now.timestamp()
                if any(stamp > now.timestamp() for stamp in sent.values()):
                    add_loss_interval(cursor, now.isoformat(), now.isoformat(), 'Legacy send timestamp is in the future; one-hour dispatch quarantine applied.')
        except (ValueError, TypeError, KeyError) as error:
            cursor.notification_fault = f'{type(error).__name__}: {error}'[:300]
            fields = ('notification_schema', 'pending_notification', 'last_delivery', 'next_distinct_at',
                      'notified_through', 'expired_through', 'lost_intervals', 'clock_checked_at', 'notification_started_at')
            cursor.invalid_notification_state = {key: data[key] for key in fields if key in data}
            cursor.pending_notification = None
            log.warning('Private notification state is corrupt; dispatch is blocked: %s', error)
        return cursor

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pending = path.with_name(path.name + '.partial')
        data = {key: value for key, value in self.__dict__.items() if key not in ('invalid_notification_state', 'notification_fault')}
        if self.invalid_notification_state is not None:
            data.update(self.invalid_notification_state)
        payload = json.dumps(data, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        if len(payload) > 256 * 1024:
            raise ValueError('Private monitor cursor exceeds 256 KiB; no POST permitted')
        if self.pending_notification is not None:
            validate_pending(self.pending_notification)
        descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        pending.replace(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f'Expected a datetime, got {type(value).__name__}.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def probe_collector(probe: CollectorProbe, minute: datetime) -> bool:
    """Whether the collector's history endpoint returns rows for ``minute``."""
    base_url = os.environ[probe.base_url_env].rstrip('/')
    token = os.environ[probe.auth_token_env]
    unix_seconds = int(minute.timestamp())
    request = urllib.request.Request(
        f'{base_url}/history?from={unix_seconds}&to={unix_seconds}',
        headers={'Accept': 'application/json', 'Authorization': f'Bearer {token}'},
    )
    with urllib.request.urlopen(request, timeout=PROBE_TIMEOUT_SECONDS) as response:
        body = response.read().decode('utf-8', 'replace')
    return any(line.strip() for line in body.splitlines())


class LawClient:
    """One native query at a time, with a wall deadline covering DNS and transport."""

    def __init__(self, settings: ClickHouseSettings) -> None:
        factory = getattr(import_module('clickhouse_driver'), 'Client')
        self.client = cast(
            Client,
            factory(
                host=settings.host,
                port=settings.port,
                user='law_reader',
                password=settings.password,
                connect_timeout=5,
                send_receive_timeout=5,
                sync_request_timeout=5,
                disable_reconnect=True,
            ),
        )

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[tuple[object, ...]]:
        seconds = min(5.0, float(str((settings or {}).get('max_execution_time', 5))))
        if seconds <= 0:
            raise TimeoutError('law query deadline elapsed')

        def expired(_signum: int, _frame: FrameType | None) -> None:
            raise TimeoutError('law query transport deadline')

        previous = signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, seconds)
        try:
            return self.client.execute(query, params, settings=settings)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            # The next admission never inherits a timed-out query/connection.
            self.client.disconnect()

    def disconnect(self) -> None:
        self.client.disconnect()


@dataclass
class _EventDay:
    offset: int = 0
    identities: set[tuple[str, str, str]] = field(default_factory=lambda: set())


class BudgetClient:
    def __init__(self, client: Client, deadline: float) -> None:
        self.client, self.deadline = client, deadline

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[tuple[object, ...]]:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Observation budget exhausted')
        rows = self.client.execute(
            query,
            params,
            settings={**(settings or LAW_QUERY_SETTINGS), 'max_execution_time': min(5, remaining)},
        )
        if time.monotonic() >= self.deadline:
            raise TimeoutError('Observation budget exhausted')
        return rows

    def disconnect(self) -> None:
        self.client.disconnect()


class LawTape:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.last: LawReport | None = None
        self.corrupt_tail = False
        self.event_days: OrderedDict[str, _EventDay] = OrderedDict()

    def latest(self, now: datetime) -> LawReport | None:
        if self.last is not None:
            return self.last
        path = self.root / now.strftime(LAW_SAMPLE_NAME)
        if not path.exists():
            return None
        with path.open('rb') as stream:
            stream.seek(max(0, path.stat().st_size - 2 * 1024 * 1024))
            payload = stream.read()
            lines = payload.splitlines()
        if not lines:
            return None
        self.corrupt_tail = False
        try:
            if not payload.endswith(b'\n'):
                raise ValueError('Incomplete law observation')
            value: object = json.loads(lines[-1])
        except ValueError:
            log.error('Corrupt law tape tail; next observation starts a new line')
            self.corrupt_tail = True
            value = None
        if value is None:
            return None
        if not isinstance(value, dict) or 'sampling_slot' not in value:
            raise ValueError('Invalid law tape tail')
        self.last = cast(LawReport, value)
        return self.last

    @staticmethod
    def _append(path: Path, value: object) -> None:
        # A killed partial line stays corrupt evidence, never joins the next valid record.
        with path.open('ab+') as stream:
            stream.seek(0, os.SEEK_END)
            if stream.tell():
                stream.seek(-1, os.SEEK_END)
                if stream.read(1) != b'\n':
                    stream.write(b' [incomplete]\n')
            stream.write(json.dumps(value, separators=(',', ':'), allow_nan=False).encode() + b'\n')
            stream.flush()
            os.fsync(stream.fileno())

    def write_catalog(self, catalog: LawCatalog) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        catalog_path = self.root / f'catalog-{catalog["version"]}.json'
        if not catalog_path.exists():
            partial = catalog_path.with_suffix('.partial')
            partial.write_text(json.dumps(catalog, separators=(',', ':'), allow_nan=False))
            partial.replace(catalog_path)

    def append(self, report: LawReport, catalog: LawCatalog) -> None:
        self.write_catalog(catalog)
        before = datetime.fromisoformat(report['sampling_slot']).date() - timedelta(days=30)
        for pattern, prefix in (
            ('samples-*.jsonl', 'samples-'),
            ('gate-events-*.jsonl', 'gate-events-'),
        ):
            for path in self.root.glob(pattern):
                if date.fromisoformat(path.stem.removeprefix(prefix)) < before:
                    path.unlink()
        # Catalogs are small and immutable; retain versions even if older than the tape.
        self._append(
            self.root / datetime.fromisoformat(report['sampling_slot']).strftime(LAW_SAMPLE_NAME),
            report,
        )
        self.last = report

    def append_events(
        self, events: Sequence[GateEvaluation], *, deadline: float | None = None
    ) -> None:
        deadline = deadline if deadline is not None else time.monotonic() + 5
        for event in sorted(events, key=lambda item: item['evaluated_at']):
            if not event['evidence_id'] or event['outcome'] == 'NOT_EVALUATED':
                continue
            day = datetime.fromisoformat(event['evaluated_at']).astimezone(UTC).date().isoformat()
            path = self.root / f'gate-events-{day}.jsonl'
            if day not in self.event_days:
                self.event_days[day] = _EventDay()
                if len(self.event_days) > 4:
                    self.event_days.popitem(last=False)
            self.event_days.move_to_end(day)
            index = self.event_days[day]
            if path.exists() and index.offset < path.stat().st_size:
                with path.open('rb') as stream:
                    stream.seek(index.offset)
                    while time.monotonic() < deadline:
                        line = stream.readline(1024 * 1024 + 1)
                        if not line:
                            break
                        if len(line) > 1024 * 1024:
                            raise ValueError('Gate event exceeds observation budget')
                        try:
                            value: object = json.loads(line) if line.endswith(b'\n') else None
                        except ValueError:
                            log.error('Corrupt gate evidence line in %s', path.name)
                        else:
                            if isinstance(value, dict):
                                item = cast(dict[str, object], value)
                                index.identities.add(
                                    (
                                        str(item.get('gate_id')),
                                        str(item.get('definition_version')),
                                        str(item.get('evidence_id')),
                                    )
                                )
                        index.offset = stream.tell()
                        if len(index.identities) > 100_000:
                            raise ValueError('Gate daily index exceeds observation budget')
            if time.monotonic() >= deadline:
                raise TimeoutError('Gate evidence indexing deferred to next tick')
            identity = (event['gate_id'], event['definition_version'], event['evidence_id'])
            if identity not in index.identities:
                self._append(path, event)
                index.identities.add(identity)
                index.offset = path.stat().st_size



class _NotificationHistory:
    """Bounded replay of compact observations, independently of successful law samples."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.frames: dict[str, bytes] = {}
        self.briefs: dict[str, Document] = {}
        self.operations: dict[str, bytes] = {}
        self.starts: dict[Path, int] = {}
        self.ends: dict[Path, int] = {}
        self.loading = False
        self.limited = False

    decode = staticmethod(decode_observation_record)

    def remember(self, raw: bytes, now: datetime) -> None:
        frame, brief = self.decode(raw)
        slot = frame['sampling_slot']
        observed = datetime.fromisoformat(slot)
        if observed.tzinfo is None or observed > now + timedelta(seconds=60):
            raise ValueError('Notification observation has an invalid sampling time')
        if observed >= now - timedelta(hours=24):
            if slot in self.frames and self.frames[slot] != raw:
                self.limited = True
            else:
                self.frames[slot] = raw
            evidence: list[Json] = []
            for metric, check in (('error_lines', 'no_error_logs'), ('failed_receipts', 'workers_alive')):
                window = frame['read_windows'].get(metric)
                if window is not None:
                    evidence.append({'gate_id': f'monitor.{check}', 'evidence': {
                        metric: window['count'], 'window_start': window['window_start'],
                        'window_end': window['window_end'], 'counts_limited': window['counts_limited'],
                    }})
            self.operations[slot] = _operation_sample({'gates': evidence})
        if brief is not None and observed >= now.replace(second=0, microsecond=0) - timedelta(hours=72, minutes=1):
            self.briefs[slot] = brief

    def append(self, frame: ObservationFrame, report: Document | None, now: datetime) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        brief = _sample_brief(report) if report is not None else None
        rows = frame['observations']
        omitted = frame['omitted_groups'] or 0
        while True:
            encoded_frame = json.dumps(frame, separators=(',', ':'), allow_nan=False).encode()
            envelope = {'frame': base64.b64encode(zlib.compress(encoded_frame)).decode(), 'brief': brief}
            raw = json.dumps(envelope, separators=(',', ':'), allow_nan=False).encode()
            if len(encoded_frame) <= 128 * 1024 and len(raw) <= 8192 and len(rows) <= 64:
                break
            if not rows:
                raise ValueError('Notification frame cannot fit the record budget')
            removed = rows.pop()
            if 'notification_transitions' in frame:
                frame['notification_transitions'] = [item for item in frame['notification_transitions'] if item['group_id'] != removed['group_id']]
            omitted += 1
            frame['omitted_groups'] = omitted
            frame['complete'] = False
        slot = frame['sampling_slot']
        if slot in self.frames:
            # A retry of a sampled minute cannot manufacture another observation.
            return
        path = self.root / now.strftime('notification-observations-%Y-%m-%d.jsonl')
        if path.exists() and path.stat().st_size:
            with path.open('rb') as existing:
                existing.seek(-1, os.SEEK_END)
                if existing.read(1) != b'\n':
                    raise ValueError('Notification tape has an uncommitted tail; append blocked')
        with path.open('ab') as stream:
            stream.write(raw + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        self.remember(raw, now)
        self.ends[path] = path.stat().st_size
        self.starts.setdefault(path, max(0, self.ends[path] - len(raw) - 1))

    def refresh(self, now: datetime) -> None:
        cutoff = now.replace(second=0, microsecond=0) - timedelta(hours=24)
        self.frames = {slot: value for slot, value in self.frames.items()
                       if datetime.fromisoformat(slot) >= cutoff}
        self.operations = {slot: value for slot, value in self.operations.items() if slot in self.frames}
        self.briefs = {slot: value for slot, value in self.briefs.items()
                       if datetime.fromisoformat(slot) >= now.replace(second=0, microsecond=0) - timedelta(hours=72, minutes=1)}
        before = now.date() - timedelta(days=30)
        for path in self.root.glob('notification-observations-????-??-??.jsonl'):
            day = date.fromisoformat(path.stem.removeprefix('notification-observations-'))
            if day < before:
                path.unlink()
        budget = 1024 * 1024
        deadline = time.monotonic() + 1
        paths = [self.root / (now - timedelta(days=offset)).strftime('notification-observations-%Y-%m-%d.jsonl')
                 for offset in range(4)]
        for path in paths:
            if budget <= 0 or time.monotonic() >= deadline:
                break
            if not path.exists():
                continue
            size = path.stat().st_size
            end = self.starts.get(path, size)
            if self.ends.get(path, size) != size:
                # There is one writer. An externally replaced segment requires bounded replay.
                end = size
                self.limited = True
            start = max(0, end - budget)
            self.ends[path] = size
            if end <= start:
                self.starts[path] = 0
                continue
            with path.open('rb') as stream:
                stream.seek(start)
                data = stream.read(end - start)
            budget -= len(data)
            if start:
                newline = data.find(b'\n')
                if newline < 0:
                    self.limited = True
                    self.starts[path] = start
                    continue
                start += newline + 1
                data = data[newline + 1:]
            position = start + len(data)
            for raw in reversed(data.splitlines(keepends=True)):
                if time.monotonic() >= deadline:
                    break
                position -= len(raw)
                if not raw.endswith(b'\n') or len(raw) > 8193:
                    self.limited = True
                    continue
                try:
                    self.remember(raw.rstrip(b'\n'), now)
                except (ValueError, TypeError, KeyError, zlib.error) as error:
                    self.limited = True
                    log.warning('Notification history is incomplete: %s', error)
            self.starts[path] = position
        self.loading = any(path.exists() and self.starts.get(path, path.stat().st_size) > 0 for path in paths)
        # Compressed slots and compact core briefs share an explicit memory budget.
        weight = sum(sys.getsizeof(key) + sys.getsizeof(raw) for key, raw in self.frames.items())
        weight += sum(len(json.dumps(brief)) * 4 + 1024 for brief in self.briefs.values())
        while weight > 16 * 1024 * 1024 and self.briefs:
            oldest_brief = min(self.briefs)
            weight -= len(json.dumps(self.briefs.pop(oldest_brief))) * 4 + 1024
            self.limited = True
        while weight > 16 * 1024 * 1024 and self.frames:
            oldest = min(self.frames)
            weight -= sys.getsizeof(oldest) + sys.getsizeof(self.frames.pop(oldest))
            self.operations.pop(oldest, None)
            self.limited = True

    def observations(self) -> list[ObservationFrame]:
        frames = [self.decode(self.frames[slot])[0] for slot in sorted(self.frames)]
        if (self.loading or self.limited) and frames:
            frames[0]['complete'] = False
        return frames


def law_findings(report: LawReport) -> list[Finding]:
    findings: list[Finding] = []
    for feed in report['feeds']:
        for predicate, result in feed['predicates'].items():
            if result['status'] in ('FAIL', 'UNKNOWN'):
                key = f'law:{predicate}:{feed["source_key"]}:{result["status"]}:{result["reason"]}'
                findings.append(
                    Finding(
                        key,
                        'data_current',
                        f'{feed["source_key"]} {predicate} {result["status"]}',
                        result['reason'],
                    )
                )
    if report['status'] != 'PASS' and not findings:
        findings.append(
            Finding(
                'law:inventory:UNKNOWN',
                'data_current',
                'Reader law unavailable',
                'inventory_unknown',
            )
        )
    return findings


def held_law_keys(cursor: Cursor, findings: Sequence[Finding], slot: str) -> set[str]:
    held: set[str] = set()
    next_streaks: dict[str, list[str | int]] = {}
    for finding in findings:
        parts = finding.key.split(':')
        if (
            len(parts) < 5
            or parts[0] != 'law'
            or parts[1] not in ('R1', 'C1', 'D1', 'M1')
            or parts[3] != 'FAIL'
        ):
            continue
        prior = cursor.law_streaks.get(finding.key, ['', 0])
        previous_slot, previous_count = str(prior[0]), int(prior[1])
        count = 1
        if previous_slot == slot:
            count = previous_count
        elif previous_slot and datetime.fromisoformat(slot) - datetime.fromisoformat(
            previous_slot
        ) == timedelta(minutes=1):
            count = previous_count + 1
        next_streaks[finding.key] = [slot, count]
        if count < 5:
            held.add(finding.key)
    cursor.law_streaks = next_streaks
    return held


class Monitor:
    name = 'monitor'
    lookback_minutes = 15

    def __init__(
        self,
        *,
        dagster: DagsterReader,
        client: Client,
        database: str,
        heartbeat_dir: Path,
        probes: Sequence[CollectorProbe],
        settings: AlertSettings | None,
        reporter: Reporter,
        cursor_path: Path,
        publication_root: Path,
        law_client: Client,
        law_root: Path,
        deployed_sha: str = '',
        page_url: str = '',
        arrow_root: Path = Path('/opt/arrow'),
        queue_threshold: int = 200,
    ) -> None:
        self.law_client = law_client
        # A retained catalog may precede an intervening deployment or configuration.
        self.law_known_since = datetime.now(UTC)
        self.law_tape = LawTape(law_root)
        self.catalog: LawCatalog | None = None
        self.pending_report: LawReport | None = None
        self.tick_evidence: dict[str, dict[str, Scalar]] = {}
        self.publication_policy: dict[str, PublicationPolicy] = {}
        self.deployed_sha = deployed_sha
        self.page_url = page_url
        self.arrow_root = arrow_root
        self.dagster = dagster
        self.client = client
        self.database = database
        self.heartbeat_dir = heartbeat_dir
        self.heartbeat_inventory: set[Path] = set()
        self.probes = tuple(probes)
        self.settings = settings
        self.reporter = reporter
        self.cursor_path = cursor_path
        self.publication_root = publication_root
        self.queue_threshold = settings.queue_threshold if settings else queue_threshold
        self.notification_history = _NotificationHistory(law_root)
        self.notification_clock: tuple[float, float] | None = None
        self.event_counts: dict[str, int] = {}
        self.condition_states: dict[str, tuple[str, str, bool]] = {}
        self.capture_evidence: dict[str, int | float | None] = {}

    def tick(self, now: datetime) -> TickOutcome:
        tick_started = time.monotonic()
        now = now.astimezone(UTC)
        minute = now.replace(second=0, microsecond=0)
        cursor = Cursor.load(self.cursor_path, now, self.lookback_minutes)
        if not cursor.notification_fault:
            self._notification_clock(cursor, now)
        delivery_status = self._resume_notification(cursor, now)
        lookup_faults: list[Finding] = []
        try:
            previous = self.law_tape.latest(now)
            if previous is not None and previous['sampling_slot'] == minute.isoformat():
                own = [event for event in previous['gates'] if event['gate_id'] == 'monitor.data_current']
                if (previous['schema_version'] != LAW_SCHEMA_VERSION
                    or not set(LAW_INVENTORY) <= set(previous['inventory'])
                    or set(previous['inventory']) != {feed['source_key'] for feed in previous['feeds']}
                    or not previous['catalog_version'] or len(own) != 1
                    or own[0]['outcome'] not in ('PASS', 'FAIL')
                    or not own[0]['definition_version']
                    or own[0]['evidence_id'] != f'monitor.data_current:{minute.isoformat()}'
                    or own[0]['evaluated_at'] != previous['evaluation_start']):
                    raise ValueError('Invalid committed monitor sample')
                law_findings(previous)
                log.info('Skipping already committed monitor minute %s', minute.isoformat())
                return TickOutcome(self.name, minute, (), ())
        except Exception as error:
            log.exception('Law sample lookup failed; independent detectors will still run')
            lookup_faults.append(Finding('detector_failed:law', 'data_current',
                'Committed law sample unavailable', f'{type(error).__name__}: {error}'[:300]))
        window_end = now - timedelta(seconds=DELIVERY_LAG_SECONDS)
        self.pending_report = None
        self.event_counts = {}
        self.condition_states = {}
        self.publication_policy = {}
        self.tick_evidence = {
            'dagster_reachable': {'reachable': None, 'unhealthy_daemons': None},
            'queue_bounded': {'queued_runs': None, 'queue_threshold': self.queue_threshold},
            'workers_alive': {
                'workers_fresh': None, 'workers_expected': None, 'workers_unknown': None,
                'failed_receipts': None, 'window_start': None, 'window_end': None,
                'counts_limited': False,
            },
            'collectors_serving': {'collectors_serving': None, 'collectors_expected': None},
            'no_error_logs': {
                'error_lines': None, 'window_start': None, 'window_end': None,
                'counts_limited': False,
            },
        }
        # Every detector is isolated: a fault in one becomes its own finding on its check,
        # the other checks still run, the evaluations are still written and the e-mail is still
        # sent. A detector that did not complete its read leaves its cursor where it was.
        dagster, dagster_read = self._guarded(
            'queue_bounded', 'dagster', lambda: self._dagster_findings(cursor, window_end)
        )
        workers, workers_read = self._guarded(
            'workers_alive', 'workers', lambda: (self._worker_findings(cursor, window_end), True)
        )
        collectors, collectors_read = self._guarded(
            'collectors_serving',
            'collectors',
            lambda: (self._collector_findings(minute - timedelta(minutes=1)), True),
        )
        logs, logs_read = self._guarded(
            'no_error_logs', 'logs', lambda: (self._log_findings(cursor, window_end), True)
        )
        publication, publication_read = self._guarded(
            'publication_current',
            'publication',
            lambda: (self._publication_findings(), True),
        )
        findings: list[Finding] = [*dagster, *workers, *collectors, *logs, *publication]
        law = lookup_faults
        if not law:
            law, _ = self._guarded(
                'data_current', 'law', lambda: (self._law_findings(now, findings), True)
            )
        findings.extend(law)
        page, _ = self._guarded('data_current', 'law_page', lambda: (self._page_findings(), True))
        findings.extend(page)
        held = held_law_keys(cursor, law, minute.isoformat())
        if not any(
            'evaluation_timeout' in item.key
            or 'evidence_unavailable' in item.key
            or item.key in ('queue_backlog', 'detector_failed:law')
            for item in findings
        ):
            history_findings, _ = self._guarded(
                'data_current', 'law_history', lambda: (self._history_findings(cursor, now), True)
            )
            findings.extend(history_findings)

        report = self.pending_report
        saved, law_committed = self._guarded(
            'data_current', 'law', lambda: (self._commit_law(now, findings), True)
        )
        findings.extend(saved)
        if not law_committed:
            report = None
        notification_fault = ''
        history: list[ObservationFrame] = []
        frame: ObservationFrame | None = None
        try:
            self.notification_history.refresh(now)
            history = self.notification_history.observations()
            frame = self._observation_frame(report, findings, held, cursor, {
                'queue_bounded': dagster_read, 'dagster_reachable': self.tick_evidence['dagster_reachable'].get('reachable') is True,
                'workers_alive': workers_read, 'collectors_serving': collectors_read,
                'no_error_logs': logs_read, 'publication_current': publication_read,
                'data_current': report is not None,
            }, now)
            frame['notification_transitions'] = [
                transition for transition in lifecycle_transitions([*history, frame], now=now)
                if transition['sampling_slot'] == frame['sampling_slot']
            ]
            self.notification_history.append(frame, cast(Document, report) if report else None, now)
            if not any(item['sampling_slot'] == frame['sampling_slot'] for item in history):
                history.append(frame)
            if frame['omitted_groups']:
                add_loss_interval(cursor, minute.isoformat(), minute.isoformat(), 'Notification capture exceeded its group or byte limit.')
        except (OSError, ValueError, TypeError, KeyError, zlib.error) as error:
            self.notification_history.limited = True
            notification_fault = f'{type(error).__name__}: {error}'[:300]
            log.warning('Notification history unavailable: %s', notification_fault)
            add_loss_interval(cursor, minute.isoformat(), minute.isoformat(), 'Notification observation persistence failed.')
            # Current evidence remains usable with an explicitly incomplete history.
            if frame is not None:
                frame['complete'] = False
                raw = json.dumps({'frame': base64.b64encode(zlib.compress(json.dumps(frame).encode())).decode(), 'brief': None}).encode()
                self.notification_history.remember(raw, now)
                history = [item for item in history if item['sampling_slot'] != frame['sampling_slot']] + [frame]
        by_check: dict[CheckName, list[Finding]] = {name: [] for name in CHECK_NAMES}
        for finding in findings:
            by_check[finding.check].append(finding)
        writes = [
            self.reporter.check(
                MONITOR_ASSET,
                name,
                passed=not items,
                metadata={
                    'findings': len(items),
                    'keys': ', '.join(item.key for item in items)[:2000],
                    'evaluated_at': now.isoformat(),
                    'notification_delivery': cursor.notification_fault or delivery_status,
                    'notification_history': notification_fault or 'recorded',
                    'notification_configuration': self.settings.dashboard_url_fault if self.settings else 'alerts disabled',
                },
            )
            for name, items in by_check.items()
        ]
        written = all(writes)
        cursor.ticks += 1
        cursor.findings += len(findings)
        if dagster_read:
            cursor.failures_after = now.timestamp()
        if workers_read:
            cursor.receipts_after = window_end.isoformat()
        if logs_read:
            cursor.logs_after = window_end.isoformat()
        cursor.notification_started_at = cursor.notification_started_at or minute.isoformat()
        if not cursor.notified_through and not cursor.expired_through and datetime.fromisoformat(cursor.notification_started_at) < now - timedelta(hours=24):
            add_loss_interval(cursor, cursor.notification_started_at, (now - timedelta(hours=24)).isoformat(), 'Unsent evidence exceeded the 24-hour replay horizon.')
        prune_unsent(cursor, now)
        try:
            cursor.save(self.cursor_path)
            summary = self._notification_summary(report, now, delivery_status, history)
            floor = cursor.notified_through or ''
            changed = any(transition['sampling_slot'] > floor for frame in history for transition in frame.get('notification_transitions', []))
            delivery_now = now + timedelta(seconds=max(0.0, time.monotonic() - tick_started))
            if not cursor.notification_fault and self.settings is not None and plan_notification(cursor, summary, self.settings, delivery_now, written, transitions=changed):
                cursor.save(self.cursor_path)
                delivery_status = self._resume_notification(cursor, delivery_now)
            summary['delivery_status'] = delivery_status
            payload = json.dumps(summary, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
            if len(payload) > 16 * 1024:
                raise ValueError('Operator summary exceeds 16 KiB')
            temporary = self.law_tape.root / 'operator-summary.json.partial'
            temporary.write_bytes(payload)
            temporary.replace(self.law_tape.root / 'operator-summary.json')
        except (OSError, ValueError, TypeError, KeyError, zlib.error) as error:
            log.warning('Notification summary/storage unavailable: %s', error)
            self.reporter.check(MONITOR_ASSET, 'data_current', passed=not by_check['data_current'], metadata={
                'notification_fault': f'{type(error).__name__}: {error}'[:300], 'evaluated_at': now.isoformat(),
            })
        return TickOutcome(
            self.name,
            minute,
            tuple(CHECK_NAMES),
            tuple(finding.key for finding in findings),
        )

    def _law_findings(self, now: datetime, existing: Sequence[Finding]) -> list[Finding]:
        slot = now.replace(second=0, microsecond=0).isoformat()
        if self.catalog is None:
            self.catalog = build_catalog(self.deployed_sha)
        report = evaluate(self.law_client, self.database, now)
        report['catalog_version'] = self.catalog['version']
        report['deployed_sha'] = self.catalog['deployed_sha']
        observation_findings: list[Finding] = []
        try:
            report['projections'].extend(self._output_observations(report, now))
        except Exception:
            log.exception('Law output observations failed')
            observation_findings.append(
                Finding(
                    'law:outputs:UNKNOWN',
                    'data_current',
                    'Output evidence unavailable',
                    'output_evidence_unavailable',
                )
            )
        observed = {item['id']: item for item in report['projections']}
        report['projections'] = [
            observed.get(item['id'], item) for item in unknown_observations(self.catalog, now)
        ]
        descriptors = {item['id']: item for item in self.catalog['gates']}
        events: list[GateEvaluation] = []
        for feed in report['feeds']:
            for predicate, result in feed['predicates'].items():
                identity = (
                    'law.inventory'
                    if predicate == 'inventory'
                    else f'law.{predicate}:{feed["source_key"]}'
                )
                descriptor = descriptors[identity]
                events.append(
                    gate_evaluation(
                        descriptor,
                        evidence_id=f'{identity}:{slot}',
                        evaluated_at=report['evaluation_start'],
                        outcome='EXPECTED_WAIT'
                        if result['status'] == 'NOT_DUE'
                        else result['status'],
                        evidence={**result['evidence'], 'core_status': result['status']},
                        reason=result['reason'],
                    )
                )
        events.extend(projection_gate_evaluations(self.catalog, report['projections']))
        if not any(event['gate_id'] == 'law.inventory' for event in events):
            events.append(
                gate_evaluation(
                    descriptors['law.inventory'],
                    evidence_id=f'law.inventory:{slot}',
                    evaluated_at=now.isoformat(),
                    outcome='PASS',
                    evidence={'sources': len(report['inventory'])},
                    reason='inventory_covered',
                )
            )
        for name in CHECK_NAMES:
            identity = f'monitor.{name}'
            items = [
                item
                for item in [*existing, *law_findings(report), *observation_findings]
                if item.check == name
            ]
            events.append(
                gate_evaluation(
                    descriptors[identity],
                    evidence_id=f'{identity}:{slot}',
                    evaluated_at=now.isoformat(),
                    outcome='FAIL' if items else 'PASS',
                    evidence={'finding_count': len(items), **self.tick_evidence.get(name, {})},
                    reason='finding_present' if items else 'check_passed',
                )
            )
        for event in events:
            event['catalog_version'] = self.catalog['version']
        report['gates'] = events
        for observation in report['projections']:
            if observation['id'] in self.publication_policy:
                observation['publication_policy'] = self.publication_policy[observation['id']]
            observation['gate_ids'] = sorted(
                set(observation['gate_ids'])
                | {
                    gate['id']
                    for gate in self.catalog['gates']
                    if observation['id'] in gate['scope']
                }
            )
        if self.law_tape.corrupt_tail:
            observation_findings.append(
                Finding(
                    'law:tape:UNKNOWN',
                    'data_current',
                    'Previous observation was incomplete',
                    'tape_corrupt_tail',
                )
            )
            self.law_tape.corrupt_tail = False
        self.pending_report = report
        return law_findings(report) + observation_findings

    def _commit_law(self, now: datetime, findings: Sequence[Finding]) -> list[Finding]:
        report = self.pending_report
        if report is None or self.catalog is None:
            return []
        self.law_tape.write_catalog(self.catalog)
        for event in report['gates']:
            if (
                event['gate_id'].startswith('source.component_integrity.')
                and datetime.fromisoformat(event['evaluated_at']) < self.law_known_since
            ):
                event.update(
                    outcome='NOT_EVALUATED',
                    evidence_id='',
                    reason='historical_definition_unavailable',
                )
        faults: list[Finding] = []
        cutoff = now - timedelta(days=30)
        deadline = time.monotonic() + 5
        try:
            self.law_tape.append_events(
                [
                    event
                    for event in report['gates']
                    if event['gate_id'] != 'monitor.data_current'
                    and datetime.fromisoformat(event['evaluated_at']) >= cutoff
                ],
                deadline=deadline,
            )
        except Exception:
            log.exception('Gate evidence append failed; current sample will record the fault')
            faults.append(
                Finding(
                    'law:gate_events:UNKNOWN',
                    'data_current',
                    'Gate history unavailable',
                    'gate_event_append_failed',
                )
            )
        own = next(event for event in report['gates'] if event['gate_id'] == 'monitor.data_current')
        count = sum(item.check == 'data_current' for item in [*findings, *faults])
        own.update(
            outcome='FAIL' if count else 'PASS',
            evidence={'finding_count': count},
            reason='finding_present' if count else 'check_passed',
        )
        # The committed sample is the sole store for this verdict; history reads it there.
        self.law_tape.append(report, self.catalog)
        self.pending_report = None
        return faults

    def _output_observations(self, report: LawReport, now: datetime) -> list[ProjectionObservation]:
        deadline = time.monotonic() + 5
        observations: list[ProjectionObservation] = []
        feeds = {item['source_key']: item for item in report['feeds']}
        if self.catalog is None:
            raise ValueError('Output observations require a catalog')
        observations.extend(
            observe_publications(
                self.law_client,
                self.database,
                self.catalog,
                now,
                self.publication_root,
                deadline=deadline,
            )
        )
        for spec in DEPTH_SPECS:
            source = spec.run_key_prefix + '_1m'
            feed = feeds.get(source)
            predicate = feed['predicates'].get('D1') if feed else None
            newest = predicate['evidence'].get('newest_minute') if predicate else None
            if newest is not None:
                minute = datetime.fromisoformat(str(newest))
                fresh = now - (minute + timedelta(minutes=1)) <= timedelta(minutes=2)
                observations.append(
                    {
                        'id': f'{source}:minute',
                        'status': 'CURRENT' if fresh else 'STALE',
                        'observed_at': now.isoformat(),
                        'evidence_at': now.isoformat(),
                        'evidence_id': f'{source}:{minute.isoformat()}',
                        'data_through': (minute + timedelta(minutes=1)).isoformat(),
                        'reason': 'depth_store_rows',
                        'gate_ids': [f'law.D1:{source}'],
                        'dagit_url': None,
                    }
                )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('output observation budget exhausted')
                rows = self.law_client.execute(
                    f'SELECT count() FROM {identifier(self.database)}.{identifier(spec.snapshot_table_name)} '
                    'WHERE datetime >= %(start)s AND datetime < %(end)s',
                    {
                        'start': minute,
                        'end': minute + timedelta(minutes=1),
                    },
                    settings={**LAW_QUERY_SETTINGS, 'max_execution_time': min(5, remaining)},
                )
                count = int(str(rows[0][0]))
                observations.append(
                    {
                        'id': f'{source}:raw',
                        'status': ('CURRENT' if fresh else 'STALE') if count else 'FAILED',
                        'observed_at': now.isoformat(),
                        'evidence_at': now.isoformat(),
                        'evidence_id': f'{spec.snapshot_table_name}:{minute.isoformat()}:{count}',
                        'data_through': (minute + timedelta(minutes=1)).isoformat()
                        if count
                        else None,
                        'reason': 'depth_raw_rows' if count else 'depth_raw_missing',
                        'gate_ids': [],
                        'dagit_url': None,
                    }
                )
            path = self.arrow_root / spec.series / 'latest.json'
            if path.is_file():
                data = self._manifest(path)
                minute = minute_start_from_partition_key(str(data['source_partition_key']))
                chunk = path.parent / depth_snapshot_chunk_relative_path(minute)
                through = minute + timedelta(minutes=1)
                observations.append(
                    {
                        'id': f'{source}:consumer:arrow',
                        'status': ('CURRENT' if now - through <= timedelta(minutes=2) else 'STALE')
                        if chunk.is_file()
                        else 'FAILED',
                        'observed_at': now.isoformat(),
                        'evidence_at': datetime.fromtimestamp(
                            int(str(data['updated_at_unix_ns'])) / 1e9, UTC
                        ).isoformat(),
                        'evidence_id': str(data['version']),
                        'data_through': through.isoformat(),
                        'reason': 'arrow_manifest_and_chunk'
                        if chunk.is_file()
                        else 'arrow_chunk_missing',
                        'gate_ids': [],
                        'dagit_url': None,
                    }
                )
        if time.monotonic() > deadline:
            raise TimeoutError('output observation budget exhausted')
        return observations

    @staticmethod
    def _manifest(path: Path) -> dict[str, object]:
        with path.open('rb') as stream:
            payload = stream.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise ValueError('Publication manifest exceeds observation budget')
        value: object = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError('Publication manifest must be an object')
        return cast(dict[str, object], value)

    def _history_findings(self, cursor: Cursor, now: datetime) -> list[Finding]:
        if self.catalog is None:
            return []
        catalog_path = self.law_tape.root / f'catalog-{self.catalog["version"]}.json'
        if not catalog_path.is_file():
            return []
        deadline = time.monotonic() + 5
        version = self.catalog['version']
        events, after = import_gate_events(
            BudgetClient(self.law_client, deadline),
            self.database,
            self.catalog,
            now - timedelta(seconds=DELIVERY_LAG_SECONDS),
            known_since=self.law_known_since,
            cursor=cursor.law_history.get(version),
        )
        self.law_tape.append_events(events, deadline=deadline)
        if after is not None:
            cursor.law_history = {version: after}
        return []

    def _page_findings(self) -> list[Finding]:
        if not self.page_url:
            return []
        try:
            with urllib.request.urlopen(self.page_url, timeout=2) as response:
                serving = response.status == 200 and response.read(16) == b'ok\n'
        except (OSError, http.client.HTTPException):
            serving = False
        return (
            []
            if serving
            else [
                Finding(
                    'law_page_unreachable',
                    'data_current',
                    'Public reader law is unreachable',
                    'page_probe_failed',
                )
            ]
        )

    @staticmethod
    def _guarded(
        check: CheckName, key: str, detector: Callable[[], tuple[list[Finding], bool]]
    ) -> tuple[list[Finding], bool]:
        try:
            return detector()
        except Exception as error:
            # A failing detector is loud on its own check; its cursor stays put so the rows
            # it could not read are read by the next tick.
            log.exception('%s detector failed', key)
            detail = f'{type(error).__name__}: {error}'[:300]
            return [
                Finding(
                    f'detector_failed:{key}',
                    check,
                    f'The {key} detector failed',
                    detail,
                )
            ], False

    def _observation_frame(
        self, report: LawReport | None, findings: Sequence[Finding], held: set[str],
        cursor: Cursor, reads: dict[str, bool], now: datetime,
    ) -> ObservationFrame:
        slot = now.replace(second=0, microsecond=0).isoformat()
        descriptors = {item['id']: item for item in self.catalog['gates']} if self.catalog else {}
        reads.update(run_failure=reads['queue_bounded'], check_failed=reads['queue_bounded'], failed_receipts=reads['workers_alive'], error_logs=reads['no_error_logs'])
        observations: dict[str, NotificationObservation] = {}
        windows: dict[str, ObservedRead] = {}
        for metric, check in (('error_lines', 'no_error_logs'), ('failed_receipts', 'workers_alive')):
            evidence = self.tick_evidence[check]
            count = evidence.get(metric)
            windows[metric] = ObservedRead(
                window_start=str(evidence['window_start']) if evidence.get('window_start') else None,
                window_end=str(evidence['window_end']) if evidence.get('window_end') else None,
                count=int(count) if isinstance(count, (int, float)) else None,
                counts_limited=evidence.get('counts_limited') is True, complete=reads.get(check, False),
            )
        for metric in ('run_failure', 'check_failed'):
            windows[metric] = ObservedRead(
                window_start=datetime.fromtimestamp(cursor.failures_after, UTC).isoformat(),
                window_end=now.isoformat(), count=sum(count for key, count in self.event_counts.items() if key.startswith(metric + ':')) if reads['queue_bounded'] else None,
                counts_limited=(metric == 'check_failed' or sum(count for key, count in self.event_counts.items() if key.startswith(metric + ':')) >= 200),
                complete=reads['queue_bounded'],
            )

        def observation(group: str, check: str, scope: str,
                        status: Literal['FAIL', 'UNKNOWN', 'PASS', 'EXPECTED_WAIT', 'NO_NEW_EVENTS'],
                        keys: list[str], *, event: bool = False, evidence: Mapping[str, object] | None = None,
                        reference: str = '', complete: bool = True) -> NotificationObservation:
            owner = {'run_failure': 'queue_bounded', 'check_failed': 'queue_bounded', 'failed_receipts': 'workers_alive', 'error_logs': 'no_error_logs'}.get(check, check)
            descriptor_id = group if group.startswith('law.') else f'monitor.{owner}'
            descriptor = descriptors.get(descriptor_id)
            version = descriptor['definition_version'] if descriptor else ''
            measurements: list[Measurement] = []
            values = evidence or {}
            allowed = ('age_seconds', 'missing_slots', 'expected_slots', 'raw_proof_rows', 'day_count',
                       'expected_days', 'valid_days', 'missing_days', 'unknown_days',
                       'committed_age_seconds', 'response_age_seconds', 'durable_capture_age_seconds', 'spool_bytes',
                       'queued_runs', 'queue_threshold', 'unhealthy_daemons', 'workers_fresh',
                       'workers_expected', 'workers_unknown', 'collectors_serving', 'collectors_expected',
                       'lag_seconds', 'grace_seconds', 'event_count', 'heartbeat_age_seconds')
            for name in allowed:
                value = values.get(name)
                if name not in values:
                    continue
                threshold = values.get('budget_seconds') if name == 'age_seconds' else values.get('max_missing') if name == 'missing_slots' else values.get('grace_seconds') if name == 'lag_seconds' else values.get('queue_threshold') if name == 'queued_runs' else values.get('expected_days') if name == 'valid_days' else HEARTBEAT_MAX_AGE_SECONDS if name in ('committed_age_seconds', 'response_age_seconds', 'durable_capture_age_seconds') else CAPTURE_SPOOL_BYTES if name == 'spool_bytes' else None
                measurements.append(Measurement(
                    name=name, value=value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None,
                    unit='seconds' if name.endswith('_seconds') else 'minutes' if name in ('missing_slots', 'expected_slots') else {'run_failure': 'failed runs', 'check_failed': 'failed check evaluations', 'failed_receipts': 'failed receipts', 'error_logs': 'error lines'}.get(check, 'events') if name == 'event_count' else 'workers' if name.startswith('workers_') else 'collectors' if name.startswith('collectors_') else 'runs' if name in ('queued_runs', 'queue_threshold') else 'daemons' if name == 'unhealthy_daemons' else 'rows' if name == 'raw_proof_rows' else 'bytes' if name == 'spool_bytes' else 'days',
                    threshold=threshold if isinstance(threshold, (int, float)) and not isinstance(threshold, bool) else None,
                    observed_at=now.isoformat(), definition_version=version,
                ))
            if check == 'law.D1':
                end = values.get('window_end')
                expected = values.get('expected_slots')
                for measurement in measurements:
                    if measurement['name'] == 'missing_slots' and isinstance(end, str):
                        measurement['window_end'] = end
                        measurement['window_start'] = (datetime.fromisoformat(end) - timedelta(minutes=expected)).isoformat() if isinstance(expected, (int, float)) else None
            return NotificationObservation(
                group_id=group, check=check, scope=scope, kind='event_stream' if event else 'condition',
                status=status, definition_version=version, detector_keys=sorted(set(keys)),
                eligible=any(key not in held for key in keys) if keys else False,
                read_window_key=('error_lines' if group.startswith('error_logs:') else 'failed_receipts' if group.startswith('receipt_failed:') else group.split(':', 1)[0]) if event else None,
                measurements=measurements, evidence_refs=[reference] if reference else [], complete=complete,
            )

        if report is not None:
            for feed in report['feeds']:
                for predicate, result in feed['predicates'].items():
                    group = f'law.{predicate}:{feed["source_key"]}'
                    keys = [item.key for item in findings if item.key.startswith(f'law:{predicate}:{feed["source_key"]}:')]
                    observations[group] = observation(
                        group, f'law.{predicate}', feed['source_key'],
                        'EXPECTED_WAIT' if result['status'] == 'NOT_DUE' else result['status'], keys,
                        evidence=result['evidence'], reference=f'{now.strftime(LAW_SAMPLE_NAME)}:{slot}#{group}',
                    )
        for item in findings:
            if item.key.startswith('law:') and len(item.key.split(':')) >= 5:
                continue
            parts = item.key.split(':')
            event = parts[0] in ('run_failure', 'check_failed', 'receipt_failed', 'error_logs')
            group = 'collector_capture' if parts[0] == 'collector_capture' else 'publication:' + ':'.join(parts[1:]) if parts[0] in ('publication_stale', 'publication_manifest_unreadable') else item.key
            status: Literal['FAIL', 'UNKNOWN', 'PASS', 'EXPECTED_WAIT'] = 'UNKNOWN' if parts[0] in ('detector_failed', 'publication_manifest_unreadable') or item.key == 'collector_capture:unavailable' or item.key.startswith('law:') else 'FAIL'
            prior = observations.get(group)
            keys = [*prior['detector_keys'], item.key] if prior else [item.key]
            values: Mapping[str, object] = {'event_count': self.event_counts.get(item.key)} if event else {'heartbeat_age_seconds': None} if parts[0] == 'heartbeat_stale' else self.tick_evidence.get(item.check, {}) if parts[0] in ('queue_backlog', 'dagster_unreachable') else self.capture_evidence if parts[0] == 'collector_capture' else {'collectors_serving': 0, 'collectors_expected': 1} if parts[0] == 'collector_silent' else {}
            observations[group] = observation(group, ('failed_receipts' if parts[0] == 'receipt_failed' else parts[0]) if event else item.check, 'Raw-perp capture' if parts[0] == 'collector_capture' else ':'.join(parts[1:]), status, keys, event=event, evidence=values, reference=item.key)
        for group, (check, scope, healthy) in self.condition_states.items():
            if group not in observations:
                observations[group] = observation(group, check, scope, 'PASS' if healthy else 'UNKNOWN', [], evidence={'heartbeat_age_seconds': None} if group.startswith('heartbeat_stale:') else None)
        for group, policy in self.publication_policy.items():
            if policy['reason'] == 'not_applicable':
                continue
            source, _, consumer = group.partition(':consumer:')
            identity = f'publication:{source}:{consumer}'
            status = 'EXPECTED_WAIT' if policy['reason'] == 'backfill_active' or policy['reason'] == 'within_budget' and (policy['lag_seconds'] or 0) > 0 else 'UNKNOWN' if policy['reason'] in ('unknown', 'unreadable') else 'FAIL' if policy['reason'] == 'stale' else 'PASS'
            prior = observations.get(identity)
            observations[identity] = observation(identity, 'publication_current', f'{source} · {consumer}', status, prior['detector_keys'] if prior else [], evidence=policy)
        # Explicit clearance only when the responsible original read completed. Removed
        # heartbeat members and unavailable law predicates cannot be inferred healthy.
        for previous in ([self.notification_history.decode(self.notification_history.frames[max(self.notification_history.frames)])[0]] if self.notification_history.frames else []):
            for prior in previous['observations']:
                group, check = prior['group_id'], prior['check']
                if group in observations or prior['kind'] == 'event_stream':
                    continue
                healthy = reads.get(check, False) and not group.startswith(('law.', 'heartbeat_stale:'))
                observations[group] = observation(group, check, prior['scope'], 'PASS' if healthy else 'UNKNOWN', [], complete=healthy)
        self.notification_check_outcomes = {check: 'FAIL' if any(item.check == check for item in findings) else 'PASS' if reads.get(check, False) else 'UNKNOWN' for check in CHECK_NAMES}
        ordered = sorted(observations.values(), key=lambda row: (row['status'] not in ('FAIL', 'UNKNOWN'), row['group_id']))
        return ObservationFrame(
            sampling_slot=slot, catalog_version=self.catalog['version'] if self.catalog else None,
            law_sample_ref=f'samples-{now.date().isoformat()}.jsonl#{slot}' if report is not None else None,
            law_observed=report is not None, checks_complete=reads, read_windows=windows,
            observations=ordered, omitted_groups=0, complete=all(reads.values()),
        )

    def _notification_summary(self, report: LawReport | None, now: datetime, status: str, history: Sequence[ObservationFrame]) -> OperatorSummary:
        briefs = self.notification_history.briefs
        terminal = report['sampling_slot'] if report else None
        clear = consecutive_window(list(briefs.values()), terminal or '')
        clear_seconds = (datetime.fromisoformat(str(clear[0]['start'])) - datetime.fromisoformat(str(clear[-1]['start']))).total_seconds() if clear else 0.0
        current: Document = {
            'last_report': cast(Document, report) if report else None,
            'status': report['status'] if report else 'UNKNOWN',
            'reason': '' if report else 'law_observation_unavailable',
            'checked_at': report['evaluation_end'] if report else now.isoformat(),
            'operations_history': operations_totals(self.notification_history.operations, loading=self.notification_history.loading, limited=self.notification_history.limited),
            'delivery_status': status,
            'operational_gates': [{
                'gate_id': f'monitor.{check}', 'outcome': self.notification_check_outcomes[check],
                'evidence': dict(evidence),
            } for check, evidence in self.tick_evidence.items()] if history else [],
            'consecutive_clear_seconds': clear_seconds,
            'consecutive_clear_slots': len(clear),
            'not_due_slots': sum(item.get('not_due') is True for item in clear),
        }
        summary = build_summary(current, cast(Document, self.catalog) if self.catalog else {}, history, now=now)
        return summary

    def _resume_notification(self, cursor: Cursor, now: datetime) -> str:
        if cursor.notification_fault:
            return 'state_corrupt: ' + cursor.notification_fault
        try:
            return attempt_delivery(cursor, self.settings, now, lambda: cursor.save(self.cursor_path))
        except (OSError, ValueError) as error:
            # Persist failures prohibit dispatch. Ordinary delivery faults cannot produce
            # ERROR logs that the monitor would read back as new operator incidents.
            log.warning('Notification delivery/storage unavailable: %s', error)
            return f'storage_or_validation_fault: {type(error).__name__}: {error}'[:300]

    def _notification_clock(self, cursor: Cursor, now: datetime) -> None:
        timestamp, monotonic = now.timestamp(), time.monotonic()
        if self.notification_clock is None:
            pending = cursor.pending_notification
            unresolved = pending is not None and pending['attempt_started_at'] is not None and pending['last_completion_at'] is None
            reversed_clock = cursor.clock_checked_at > timestamp
            # The process was absent: elapsed wall time does not prove elapsed real time.
            prior_dispatch = cursor.last_delivery is not None or pending is not None and pending['first_dispatch_at'] is not None
            if unresolved or reversed_clock or prior_dispatch:
                cursor.next_distinct_at = max(cursor.next_distinct_at, timestamp + 3610)
                if pending and (unresolved or reversed_clock):
                    pending['next_attempt_at'] = max(pending['next_attempt_at'], timestamp + 60)
                if unresolved or reversed_clock:
                    add_loss_interval(cursor, now.isoformat(), now.isoformat(), 'Restart after an unresolved request or clock reversal; delivery timing is uncertain.')
                cursor.clock_checked_at = timestamp
                try:
                    cursor.save(self.cursor_path)
                except (OSError, ValueError) as error:
                    cursor.notification_fault = f'Clock quarantine persistence failed: {error}'[:300]
                    log.warning('%s', cursor.notification_fault)
        else:
            old_wall, old_mono = self.notification_clock
            if abs((timestamp - old_wall) - (monotonic - old_mono)) > 120:
                cursor.next_distinct_at = max(cursor.next_distinct_at, timestamp + 3610)
                add_loss_interval(cursor, now.isoformat(), now.isoformat(), 'Wall clock changed; notification dispatch is quarantined for one hour.')
        self.notification_clock = (timestamp, monotonic)
        cursor.clock_checked_at = timestamp

    def _dagster_findings(self, cursor: Cursor, window_end: datetime) -> tuple[list[Finding], bool]:
        """Findings from Dagster and whether the failure queries ran, so the failure cursor
        only advances past what was actually read."""
        findings: list[Finding] = []
        health = self.dagster.health()
        self.tick_evidence['dagster_reachable'] = {
            'reachable': health.reachable,
            'unhealthy_daemons': len(health.unhealthy_daemons) if health.reachable else None,
        }
        self.tick_evidence['queue_bounded'] = {
            'queued_runs': health.queued_runs if health.reachable else None,
            'queue_threshold': self.queue_threshold,
        }
        self.condition_states['dagster_unreachable'] = ('dagster_reachable', '', health.reachable)
        self.condition_states['queue_backlog'] = ('queue_bounded', '', health.reachable and health.queued_runs <= self.queue_threshold)
        if not health.reachable:
            return [
                Finding(
                    'dagster_unreachable',
                    'dagster_reachable',
                    'Dagster webserver unreachable',
                    'The GraphQL endpoint did not answer.',
                )
            ], False
        for daemon in health.unhealthy_daemons:
            findings.append(
                Finding(
                    f'daemon_unhealthy:{daemon}',
                    'dagster_reachable',
                    f'Daemon {daemon} unhealthy',
                    'A required daemon reports unhealthy.',
                )
            )
        if health.queued_runs > self.queue_threshold:
            findings.append(
                Finding(
                    'queue_backlog',
                    'queue_bounded',
                    f'{health.queued_runs} queued runs',
                    f'The threshold is {self.queue_threshold}.',
                )
            )
        stuck_before = (window_end - QUEUE_STUCK_AFTER).timestamp()
        for stuck in self.dagster.stuck_queued_runs(stuck_before):
            findings.append(
                Finding(
                    f'queue_stuck:{stuck.job_name}',
                    'queue_bounded',
                    f'Run of {stuck.job_name} queued since '
                    f'{datetime.fromtimestamp(stuck.created_at, UTC).isoformat()}',
                    f'Older than {QUEUE_STUCK_AFTER}; run {stuck.run_id}.',
                )
            )
        by_job: dict[str, list[RunFailure]] = {}
        for failure in self.dagster.failures_since(cursor.failures_after):
            by_job.setdefault(failure.job_name, []).append(failure)
        for job_name, failures in by_job.items():
            self.event_counts[f'run_failure:{job_name}'] = len(failures)
            # One key per job: a job failing on successive partitions is one outage under
            # the cooldown, and the detail names the partitions and runs.
            detail = '; '.join(
                f'partition {failure.partition or "-"}, run {failure.run_id}'
                for failure in failures[:10]
            )
            if len(failures) > 10:
                detail += f'; and {len(failures) - 10} more'
            findings.append(
                Finding(
                    f'run_failure:{job_name}',
                    'queue_bounded',
                    f'{len(failures)} run{"s" if len(failures) > 1 else ""} of {job_name} failed',
                    detail + '.',
                )
            )
        for check in self.dagster.failed_checks_since(
            cursor.failures_after, exclude_asset=MONITOR_ASSET
        ):
            self.event_counts[f'check_failed:{check.asset_key}:{check.check_name}'] = 1
            findings.append(
                Finding(
                    f'check_failed:{check.asset_key}:{check.check_name}',
                    'queue_bounded',
                    f'Check {check.check_name} failed on {check.asset_key}',
                    datetime.fromtimestamp(check.timestamp, UTC).isoformat(),
                )
            )
        return findings, True

    def _heartbeats(self) -> list[Path]:
        ignored = {heartbeat_path(self.heartbeat_dir, name) for name in (self.name, 'provisional')}
        expected = {
            heartbeat_path(self.heartbeat_dir, f'provisional_{spec.key}')
            for spec in SOURCE_REGISTRY
            if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT
        } | {heartbeat_path(self.heartbeat_dir, MARKET_STATE_API_FEED),
             heartbeat_path(self.heartbeat_dir, PERP_CAPTURE_FEED)}
        self.heartbeat_inventory = set(self.heartbeat_dir.glob('*.heartbeat')) - ignored
        return sorted(self.heartbeat_inventory | expected)

    def _worker_findings(self, cursor: Cursor, window_end: datetime) -> list[Finding]:
        findings: list[Finding] = []
        now = window_end + timedelta(seconds=DELIVERY_LAG_SECONDS)
        heartbeats = self._heartbeats()
        fresh = 0
        for heartbeat in heartbeats:
            healthy = heartbeat_is_fresh(
                heartbeat, max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS, now=now.timestamp()
            )
            self.condition_states[f'heartbeat_stale:{heartbeat.stem}'] = ('workers_alive', heartbeat.stem, healthy)
            if not healthy:
                feed = heartbeat.name.removesuffix('.heartbeat')
                findings.append(
                    Finding(
                        f'heartbeat_stale:{feed}',
                        'workers_alive',
                        f'Worker {feed} heartbeat stale',
                        f'Older than {HEARTBEAT_MAX_AGE_SECONDS} seconds.',
                    )
                )
            else:
                fresh += 1
        receipts = failed_receipts_since(
            self.client, self.database, datetime.fromisoformat(cursor.receipts_after), window_end
        )
        members = set(heartbeats)
        if DEPTH_SPECS:
            members.add(heartbeat_path(self.heartbeat_dir, DepthFeed.name))
        unknown = len(members - self.heartbeat_inventory)
        self.tick_evidence['workers_alive'] = {
            'workers_fresh': fresh, 'workers_expected': len(members),
            'workers_unknown': unknown, 'failed_receipts': len(receipts),
            'window_start': datetime.fromisoformat(cursor.receipts_after).astimezone(UTC).replace(microsecond=0).isoformat(),
            'window_end': window_end.astimezone(UTC).replace(microsecond=0).isoformat(),
            'counts_limited': len(receipts) >= 1000,
        }
        for receipt in receipts:
            key = f'receipt_failed:{receipt.feed}:{receipt.series}'
            self.event_counts[key] = self.event_counts.get(key, 0) + 1
            findings.append(
                Finding(
                    f'receipt_failed:{receipt.feed}:{receipt.series}',
                    'workers_alive',
                    f'Worker {receipt.feed} failed {receipt.series}',
                    f'minute {receipt.minute.isoformat()}: {receipt.error_code} {receipt.error[:200]}'.rstrip(),
                )
            )
        return findings

    def _capture_finding(self, now: datetime) -> Finding | None:
        self.capture_evidence = {}
        path = self.heartbeat_dir / 'perp_capture.status.json'
        try:
            with path.open('rb') as stream:
                if os.fstat(stream.fileno()).st_size > CAPTURE_STATUS_BYTES:
                    raise ValueError('Capture status exceeds 16 KiB')
                decoded: object = json.loads(stream.read(CAPTURE_STATUS_BYTES))
            if not isinstance(decoded, dict):
                raise ValueError('Capture status must be an object')
            status = cast(dict[str, object], decoded)
            if status.get('schema_version') != 1:
                raise ValueError('Unsupported capture status version')
            for field in ('committed_at', 'last_response_at', 'last_durable_capture_at'):
                stamp = status.get(field)
                if not isinstance(stamp, str):
                    raise ValueError(f'Capture has no {field}')
                parsed = datetime.fromisoformat(stamp)
                if parsed.tzinfo is None:
                    raise ValueError(f'Capture {field} has no timezone')
                age = (now - parsed).total_seconds()
                measurement = {'committed_at': 'committed_age_seconds', 'last_response_at': 'response_age_seconds', 'last_durable_capture_at': 'durable_capture_age_seconds'}[field]
                self.capture_evidence[measurement] = age
                if age > HEARTBEAT_MAX_AGE_SECONDS or age < -60:
                    raise ValueError(f'Capture {field} is stale or in the future')
            used = status.get('spool_bytes')
            self.capture_evidence['spool_bytes'] = used if type(used) is int else None
            if type(used) is not int or used < 0 or used >= CAPTURE_SPOOL_BYTES:
                raise ValueError('Capture spool capacity reached or invalid')
            error = status.get('error_code')
            if error is not None:
                if not isinstance(error, str) or not error:
                    raise ValueError('Capture error code is invalid')
                return Finding(
                    f'collector_capture:{error}', 'collectors_serving',
                    'Raw-perp capture needs repair', error[:200],
                )
        except (OSError, ValueError, TypeError) as error:
            return Finding(
                'collector_capture:unavailable', 'collectors_serving',
                'Raw-perp durable capture unavailable', f'{type(error).__name__}: {error}'[:300],
            )
        return None

    def _collector_findings(self, minute: datetime) -> list[Finding]:
        findings: list[Finding] = []
        for probe in self.probes:
            try:
                serving = probe_collector(probe, minute)
            except (
                urllib.error.URLError,
                http.client.HTTPException,
                TimeoutError,
                OSError,
                KeyError,
                ValueError,
            ) as error:
                serving = False
                reason = f'{type(error).__name__}: {error}'
            else:
                reason = 'no rows for the last completed minute'
            self.condition_states[f'collector_silent:{probe.name}'] = ('collectors_serving', probe.name, serving)
            if not serving:
                findings.append(
                    Finding(
                        f'collector_silent:{probe.name}',
                        'collectors_serving',
                        f'Collector {probe.name} not serving',
                        f'{reason} ({minute.isoformat()}).',
                    )
                )
        capture = self._capture_finding(minute + timedelta(minutes=1))
        self.condition_states['collector_capture'] = ('collectors_serving', 'Raw-perp capture', capture is None)
        if capture is not None:
            findings.append(capture)
        self.tick_evidence['collectors_serving'] = {
            'collectors_serving': len(self.probes) + 1 - len(findings),
            'collectors_expected': len(self.probes) + 1,
        }
        return findings

    def _log_findings(self, cursor: Cursor, window_end: datetime) -> list[Finding]:
        rows = error_log_rows_since(
            self.client, self.database, datetime.fromisoformat(cursor.logs_after), window_end
        )
        self.tick_evidence['no_error_logs'] = {
            'error_lines': len(rows),
            'window_start': datetime.fromisoformat(cursor.logs_after).astimezone(UTC).replace(microsecond=0).isoformat(),
            'window_end': window_end.astimezone(UTC).replace(microsecond=0).isoformat(),
            'counts_limited': len(rows) >= 1000,
        }
        by_service: dict[str, list[str]] = {}
        for row in rows:
            by_service.setdefault(row.service, []).append(row.message)
        self.event_counts.update({f'error_logs:{service}': len(messages) for service, messages in by_service.items()})
        return [
            Finding(
                f'error_logs:{service}',
                'no_error_logs',
                f'{len(messages)} error line(s) from {service}',
                messages[-1][:300],
            )
            for service, messages in sorted(by_service.items())
        ]

    def _publication_findings(self) -> list[Finding]:
        """One finding per public consumer whose published end lags the source state."""
        self.publication_policy = {
            node: PublicationPolicy(reason='unknown' if applicable else 'not_applicable',
                                    lag_seconds=None, grace_seconds=None,
                                    state_through=None, published_through=None)
            for node, applicable in [
                *((f'{spec.key}:consumer:{consumer.key}',
                   consumer.public and spec.rollout_stage != RolloutStage.DORMANT)
                  for spec in SOURCE_REGISTRY for consumer in spec.consumers),
                *((f'{spec.projection_table_name}:consumer:arrow', False) for spec in DEPTH_SPECS),
            ]
        }
        policies = {key: value.copy() for key, value in self.publication_policy.items()}
        if not self.publication_root.is_dir():
            raise RuntimeError(f'Publication root {self.publication_root} is not mounted.')
        spans: dict[str, tuple[datetime, datetime, datetime, datetime, int]] = {}
        for row in self.client.execute(
            f"""SELECT source_key, min(partition_end), max(partition_end),
            minIf(partition_end, NOT provisional), maxIf(partition_end, NOT provisional),
            countIf(NOT provisional)
            FROM {identifier(self.database)}.source_active_partitions GROUP BY source_key"""
        ):
            spans[str(row[0])] = (
                _utc(row[1]),
                _utc(row[2]),
                _utc(row[3]),
                _utc(row[4]),
                int(str(row[5])),
            )
        findings: list[Finding] = []
        for spec in SOURCE_REGISTRY:
            if spec.rollout_stage == RolloutStage.DORMANT:
                continue
            span = spans.get(spec.key)
            if span is None:
                continue
            oldest, current, canonical_oldest, canonical_current, canonical_count = span
            if self.dagster.backfill_owns_publication(spec.key):
                for consumer in spec.consumers:
                    if consumer.public:
                        policies[f'{spec.key}:consumer:{consumer.key}']['reason'] = 'backfill_active'
                continue
            for consumer in spec.consumers:
                if not consumer.public:
                    continue
                if consumer.canonical_only:
                    if not canonical_count:
                        continue
                    start, end = canonical_oldest, canonical_current
                    grace = CANONICAL_PUBLICATION_STALE_AFTER
                else:
                    start, end = oldest, current
                    grace = PINNED_PUBLICATION_STALE_AFTER
                policy = policies[f'{spec.key}:consumer:{consumer.key}']
                policy.update(grace_seconds=grace.total_seconds(), state_through=end.isoformat())
                manifest = self.publication_root / spec.key / consumer.key / 'latest.json'
                try:
                    published = _utc(
                        datetime.fromisoformat(
                            str(json.loads(manifest.read_text())['active_through'])
                        )
                    )
                    policy['published_through'] = published.isoformat()
                except FileNotFoundError:
                    # Never published: the span of the state itself is the lag, so a
                    # fresh source stays quiet while an old one pages.
                    published = start
                except (OSError, ValueError, KeyError, TypeError) as error:
                    policy['reason'] = 'unreadable'
                    findings.append(
                        Finding(
                            f'publication_manifest_unreadable:{spec.key}:{consumer.key}',
                            'publication_current',
                            f'{spec.key} {consumer.key} manifest unreadable',
                            f'{type(error).__name__}: {error}'[:300],
                        )
                    )
                    continue
                policy.update(lag_seconds=(end - published).total_seconds(),
                              reason='stale' if end - published > grace else 'within_budget')
                if end - published > grace:
                    findings.append(
                        Finding(
                            f'publication_stale:{spec.key}:{consumer.key}',
                            'publication_current',
                            f'{spec.key} {consumer.key} publication stale',
                            f'published through {published.isoformat()}, '
                            f'state through {end.isoformat()}.',
                        )
                    )
        self.publication_policy = policies
        return findings


def build_monitor(environ: dict[str, str]) -> Monitor:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    ensure_monitoring_tables(client, settings.database)
    heartbeat_dir = heartbeat_directory()
    probes = tuple(
        CollectorProbe(name, base_env, token_env)
        for name, base_env, token_env in (
            ('depth20', 'BINANCE_SPOT_DEPTH20_BASE_URL', 'BINANCE_SPOT_DEPTH20_AUTH_TOKEN'),
            ('depth200', 'BINANCE_SPOT_DEPTH200_BASE_URL', 'BINANCE_SPOT_DEPTH200_AUTH_TOKEN'),
        )
        if environ.get(base_env) and environ.get(token_env)
    )
    alert_settings = AlertSettings.from_environment(environ)
    if alert_settings is None:
        log.warning('alerts disabled: ORIGO_ALERT_* is not set; findings are logged only')
    base_url = environ.get('DAGSTER_WEBSERVER_URL', DEFAULT_WEBSERVER_URL)
    return Monitor(
        dagster=DagsterReader(base_url),
        client=client,
        law_client=LawClient(settings),
        law_root=LAW_TAPE_ROOT,
        deployed_sha=environ.get('ORIGO_LAW_APP_IMAGE', '').rpartition(':')[2],
        page_url=environ.get('ORIGO_LAW_PAGE_URL', 'http://law:8484/law.json'),
        database=settings.database,
        heartbeat_dir=heartbeat_dir,
        probes=probes,
        settings=alert_settings,
        reporter=Reporter(base_url),
        cursor_path=heartbeat_dir / 'monitor.cursor.json',
        publication_root=Path(environ.get(PUBLICATION_ROOT_ENV, PUBLICATION_ROOT_DEFAULT)),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m origo.workers.monitor',
        description='Watch Dagster, the workers, the collectors and the container log; '
        'write check evaluations to Dagit and e-mail new findings.',
    )
    parser.add_argument(
        '--check', action='store_true', help='healthcheck: exit 0 when the heartbeat is fresh'
    )
    parser.add_argument('--once', action='store_true', help='run one tick and exit')
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    heartbeat = heartbeat_path(heartbeat_directory(), Monitor.name)
    if arguments.check:
        return check_heartbeat(heartbeat)
    monitor = build_monitor(dict(os.environ))
    if arguments.once:
        outcome = monitor.tick(datetime.now(UTC))
        print(json.dumps({'minute': outcome.minute.isoformat(), 'findings': list(outcome.failed)}))
        return 0
    run_forever(monitor, heartbeat=heartbeat)


if __name__ == '__main__':
    sys.exit(main())
