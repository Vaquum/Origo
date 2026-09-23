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

The evaluations are written to Dagit first, then one e-mail lists every new finding key
and says whether that write succeeded. A key repeats inside the cooldown without a second
e-mail. A daily digest goes out once at the configured hour. The only state is a cursor
file; evaluated law evidence is appended to the public read-only tape.
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from types import FrameType
from typing import Literal, cast

from origo.alerts.email import AlertSettings, send_alert
from origo.assets.build_depth_snapshot_store_arrow import (
    depth_snapshot_chunk_relative_path,
    minute_start_from_partition_key,
)
from origo.assets.create_origo_database import (
    ClickHouseSettings,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from origo.law import LAW_QUERY_SETTINGS, LAW_SAMPLE_NAME, LAW_TAPE_ROOT, LawReport, evaluate
from origo.law_catalog import (
    GateEvaluation,
    LawCatalog,
    ProjectionObservation,
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
from .depth import DEPTH_SPECS
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

    @classmethod
    def load(cls, path: Path, now: datetime, lookback_minutes: int) -> Cursor:
        start = now - timedelta(minutes=lookback_minutes)
        if not path.exists():
            return cls(start.timestamp(), start.isoformat(), start.isoformat(), {}, '', 0, 0)
        raw: object = json.loads(path.read_text())
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
        return cls(
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

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pending = path.with_name(path.name + '.partial')
        pending.write_text(json.dumps(self.__dict__, sort_keys=True, indent=1))
        pending.replace(path)


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
        self._append(
            self.root / datetime.fromisoformat(report['sampling_slot']).strftime(LAW_SAMPLE_NAME),
            report,
        )
        self.last = report
        before = datetime.fromisoformat(report['sampling_slot']).date() - timedelta(days=30)
        for pattern, prefix in (
            ('samples-*.jsonl', 'samples-'),
            ('gate-events-*.jsonl', 'gate-events-'),
        ):
            for path in self.root.glob(pattern):
                if date.fromisoformat(path.stem.removeprefix(prefix)) < before:
                    path.unlink()
        # Catalogs are small and immutable; retain versions even if older than the tape.

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
            or parts[1] not in ('R1', 'C1', 'D1')
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
        self.law_tape = LawTape(law_root)
        self.catalog: LawCatalog | None = None
        self.pending_report: LawReport | None = None
        self.deployed_sha = deployed_sha
        self.page_url = page_url
        self.arrow_root = arrow_root
        self.dagster = dagster
        self.client = client
        self.database = database
        self.heartbeat_dir = heartbeat_dir
        self.probes = tuple(probes)
        self.settings = settings
        self.reporter = reporter
        self.cursor_path = cursor_path
        self.publication_root = publication_root
        self.queue_threshold = settings.queue_threshold if settings else queue_threshold

    def tick(self, now: datetime) -> TickOutcome:
        now = now.astimezone(UTC)
        minute = now.replace(second=0, microsecond=0)
        window_end = now - timedelta(seconds=DELIVERY_LAG_SECONDS)
        cursor = Cursor.load(self.cursor_path, now, self.lookback_minutes)
        self.pending_report = None
        # Every detector is isolated: a fault in one becomes its own finding on its check,
        # the other checks still run, the evaluations are still written and the e-mail is still
        # sent. A detector that did not complete its read leaves its cursor where it was.
        dagster, dagster_read = self._guarded(
            'queue_bounded', 'dagster', lambda: self._dagster_findings(cursor, window_end)
        )
        workers, workers_read = self._guarded(
            'workers_alive', 'workers', lambda: (self._worker_findings(cursor, window_end), True)
        )
        collectors, _ = self._guarded(
            'collectors_serving',
            'collectors',
            lambda: (self._collector_findings(minute - timedelta(minutes=1)), True),
        )
        logs, logs_read = self._guarded(
            'no_error_logs', 'logs', lambda: (self._log_findings(cursor, window_end), True)
        )
        publication, _ = self._guarded(
            'publication_current',
            'publication',
            lambda: (self._publication_findings(), True),
        )
        findings: list[Finding] = [*dagster, *workers, *collectors, *logs, *publication]
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
            history, _ = self._guarded(
                'data_current', 'law_history', lambda: (self._history_findings(cursor, now), True)
            )
            findings.extend(history)

        saved, _ = self._guarded(
            'data_current', 'law', lambda: (self._commit_law(now, findings), True)
        )
        findings.extend(saved)
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
                },
            )
            for name, items in by_check.items()
        ]
        written = all(writes)
        cooldown = self.settings.cooldown_seconds if self.settings else 0
        fresh = [
            finding
            for finding in findings
            if finding.key not in held
            and now.timestamp() - cursor.sent.get(finding.key, 0.0) > cooldown
        ]
        if fresh:
            self._send(
                f'Origo alert: {len(fresh)} new finding{"s" if len(fresh) > 1 else ""}',
                self._alert_body(fresh, written, now),
            )
            for finding in fresh:
                cursor.sent[finding.key] = now.timestamp()
        cursor.ticks += 1
        cursor.findings += len(findings)
        today = now.date().isoformat()
        if (
            self.settings is not None
            and now.hour == self.settings.digest_hour_utc
            and cursor.last_digest_date != today
        ):
            self._send(
                f'Origo daily digest {today}',
                f'ticks: {cursor.ticks}\nfindings: {cursor.findings}\n'
                f'open now: {len(findings)}\nworker heartbeats: {len(self._heartbeats())}\n'
                f'collectors probed: {len(self.probes)}\n',
            )
            cursor.last_digest_date = today
        if dagster_read:
            cursor.failures_after = now.timestamp()
        if workers_read:
            cursor.receipts_after = window_end.isoformat()
        if logs_read:
            cursor.logs_after = window_end.isoformat()
        cursor.sent = {
            key: stamp for key, stamp in cursor.sent.items() if now.timestamp() - stamp <= cooldown
        }
        cursor.save(self.cursor_path)
        return TickOutcome(
            self.name,
            minute,
            tuple(CHECK_NAMES),
            tuple(finding.key for finding in findings),
        )

    def _law_findings(self, now: datetime, existing: Sequence[Finding]) -> list[Finding]:
        previous = self.law_tape.latest(now)
        slot = now.replace(second=0, microsecond=0).isoformat()
        if previous is not None and previous['sampling_slot'] == slot:
            return law_findings(previous)
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
                    evidence={'finding_count': len(items)},
                    reason='finding_present' if items else 'check_passed',
                )
            )
        report['gates'] = events
        for observation in report['projections']:
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
        catalog_path = self.law_tape.root / f'catalog-{self.catalog["version"]}.json'
        known_since = datetime.fromtimestamp(catalog_path.stat().st_mtime, UTC)
        for event in report['gates']:
            if (
                event['gate_id'].startswith('source.component_integrity.')
                and datetime.fromisoformat(event['evaluated_at']) < known_since
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
        if not faults:
            try:
                self.law_tape.append_events([own], deadline=deadline)
            except Exception:
                log.exception('Final monitor evaluation could not be retained')
                faults.append(
                    Finding(
                        'law:gate_events:UNKNOWN',
                        'data_current',
                        'Gate history unavailable',
                        'gate_event_append_failed',
                    )
                )
                own.update(
                    outcome='FAIL',
                    evidence={'finding_count': count + 1},
                    reason='gate_event_append_failed',
                )
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
            known_since=datetime.fromtimestamp(catalog_path.stat().st_mtime, UTC),
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

    def _send(self, subject: str, body: str) -> None:
        if self.settings is None:
            log.warning('alerts disabled (ORIGO_ALERT_* not set): %s\n%s', subject, body)
            return
        send_alert(self.settings, subject, body)

    @staticmethod
    def _alert_body(findings: Sequence[Finding], written: bool, now: datetime) -> str:
        lines = [f'{now.isoformat()}: {len(findings)} new finding(s).', '']
        lines.extend(f'- {f.key}: {f.title}. {f.detail}'.rstrip() for f in findings)
        lines.append('')
        lines.append(
            'Dagit check evaluations: written.'
            if written
            else 'Dagit check evaluations: FAILED to write.'
        )
        lines.append('Investigate in this order: Dagit, ClickHouse, Docker, the collectors.')
        return '\n'.join(lines) + '\n'

    def _dagster_findings(self, cursor: Cursor, window_end: datetime) -> tuple[list[Finding], bool]:
        """Findings from Dagster and whether the failure queries ran, so the failure cursor
        only advances past what was actually read."""
        findings: list[Finding] = []
        health = self.dagster.health()
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
        }
        return sorted((set(self.heartbeat_dir.glob('*.heartbeat')) - ignored) | expected)

    def _worker_findings(self, cursor: Cursor, window_end: datetime) -> list[Finding]:
        findings: list[Finding] = []
        now = window_end + timedelta(seconds=DELIVERY_LAG_SECONDS)
        for heartbeat in self._heartbeats():
            if not heartbeat_is_fresh(
                heartbeat, max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS, now=now.timestamp()
            ):
                feed = heartbeat.name.removesuffix('.heartbeat')
                findings.append(
                    Finding(
                        f'heartbeat_stale:{feed}',
                        'workers_alive',
                        f'Worker {feed} heartbeat stale',
                        f'Older than {HEARTBEAT_MAX_AGE_SECONDS} seconds.',
                    )
                )
        for receipt in failed_receipts_since(
            self.client, self.database, datetime.fromisoformat(cursor.receipts_after), window_end
        ):
            findings.append(
                Finding(
                    f'receipt_failed:{receipt.feed}:{receipt.series}',
                    'workers_alive',
                    f'Worker {receipt.feed} failed {receipt.series}',
                    f'minute {receipt.minute.isoformat()}: {receipt.error_code} {receipt.error[:200]}'.rstrip(),
                )
            )
        return findings

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
            if not serving:
                findings.append(
                    Finding(
                        f'collector_silent:{probe.name}',
                        'collectors_serving',
                        f'Collector {probe.name} not serving',
                        f'{reason} ({minute.isoformat()}).',
                    )
                )
        return findings

    def _log_findings(self, cursor: Cursor, window_end: datetime) -> list[Finding]:
        rows = error_log_rows_since(
            self.client, self.database, datetime.fromisoformat(cursor.logs_after), window_end
        )
        by_service: dict[str, list[str]] = {}
        for row in rows:
            by_service.setdefault(row.service, []).append(row.message)
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
                manifest = self.publication_root / spec.key / consumer.key / 'latest.json'
                try:
                    published = _utc(
                        datetime.fromisoformat(
                            str(json.loads(manifest.read_text())['active_through'])
                        )
                    )
                except FileNotFoundError:
                    # Never published: the span of the state itself is the lag, so a
                    # fresh source stays quiet while an old one pages.
                    published = start
                except (OSError, ValueError, KeyError, TypeError) as error:
                    findings.append(
                        Finding(
                            f'publication_manifest_unreadable:{spec.key}:{consumer.key}',
                            'publication_current',
                            f'{spec.key} {consumer.key} manifest unreadable',
                            f'{type(error).__name__}: {error}'[:300],
                        )
                    )
                    continue
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
