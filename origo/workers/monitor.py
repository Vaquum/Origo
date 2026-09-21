"""The monitor: one detector outside Dagster that watches Dagster, the workers, the
collectors, the container log, the certified source coverage and the published products,
writes its verdicts into Dagit and e-mails new findings.

Every minute the tick evaluates seven checks on the ``origo_monitor`` asset:

- ``dagster_reachable``: the webserver answers and every required daemon is healthy;
- ``queue_bounded``: fewer queued runs than the threshold, and no run failure or failed
  asset check since the last tick;
- ``workers_alive``: every required worker heartbeat exists and is fresh, and no worker
  unit has failed without a later success (liveness and failed work, not data currency);
- ``collectors_serving``: each depth collector returns rows for the last completed minute;
- ``no_error_logs``: the container log holds no ``ERROR`` row since the last tick;
- ``data_current``: every required source's contiguous frontier ``F`` and every depth
  path's committed Arrow are within their wall-clock bounds, and the tick itself fit its
  period;
- ``publication_current``: every required consumer, the CANARY local ones included,
  publishes a readable manifest whose files exist and whose delivered end ``P`` is within
  its cadence's wall-clock bound.

Sources and products are compared with the clock, not only with each other, so a source
and its publisher freezing together stay red. Every required entity of the pinned
inventory is accounted for: a missing heartbeat, source, manifest, series or file is a
failure, not an empty healthy result. Receipt and log reads are paged to completion
before their cursors advance. The evaluations are written to Dagit first, then one
e-mail lists every new finding key and says whether that write succeeded. A key repeats
inside the cooldown without a second e-mail, and an unresolved condition stays a failed
check across quiet ticks. The only state is a cursor file; every fact lives in Dagit,
ClickHouse and the product stores.
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

from origo.alerts.email import AlertSettings, send_alert
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.contracts import Client, RolloutStage
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.steady_state.coverage import Coverage, read_coverage
from origo.steady_state.monitoring import (
    MAX_OPEN_UNITS,
    Bounds,
    DepthState,
    FileEvidence,
    Manifest,
    OpenUnit,
    PagedRead,
    RemoteVerifier,
    SourceAge,
    covered_minute,
    delivered_end,
    depth_state,
    parse_manifest,
    read_pages,
    remote_file_sha256,
    required_workers,
    resolved_units,
    source_age,
    verify_files,
    verify_uploads,
)
from origo.steady_state.policy import Inventory, Policy, load_inventory, load_policy
from origo.utils.arrow_store import arrow_store_root, parquet_source_root

from .dagster_reader import DagsterReader, RunFailure
from .receipts import (
    LogRow,
    Receipt,
    ensure_monitoring_tables,
    error_log_rows_since,
    failed_receipts_since,
)
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
    'queue_bounded',
    'workers_alive',
    'collectors_serving',
    'no_error_logs',
    'data_current',
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
    """The monitor's only state: where each detector resumes, what was already sent, which
    failed units await resolution, and which product identities were already verified."""

    failures_after: float
    receipts_after: str
    logs_after: str
    sent: dict[str, float]
    last_digest_date: str
    ticks: int
    findings: int
    open_units: dict[str, dict[str, str]] = field(default_factory=dict[str, dict[str, str]])
    products: dict[str, dict[str, object]] = field(default_factory=dict[str, dict[str, object]])
    canonical_seen: dict[str, list[object]] = field(default_factory=dict[str, list[object]])

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
            {str(key): float(cast(float, value)) for key, value in cast(dict[str, object], sent_raw).items()}
            if isinstance(sent_raw, dict)
            else {}
        )
        open_raw = data.get('open_units')
        open_units = (
            {
                str(key): {str(name): str(value) for name, value in cast(dict[str, object], entry).items()}
                for key, entry in cast(dict[str, object], open_raw).items()
                if isinstance(entry, dict)
            }
            if isinstance(open_raw, dict)
            else {}
        )
        products_raw = data.get('products')
        products = (
            {
                str(key): dict(cast(dict[str, object], entry))
                for key, entry in cast(dict[str, object], products_raw).items()
                if isinstance(entry, dict)
            }
            if isinstance(products_raw, dict)
            else {}
        )
        seen_raw = data.get('canonical_seen')
        canonical_seen = (
            {
                str(key): list(cast(list[object], entry))
                for key, entry in cast(dict[str, object], seen_raw).items()
                if isinstance(entry, list)
            }
            if isinstance(seen_raw, dict)
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
            open_units,
            products,
            canonical_seen,
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pending = path.with_name(path.name + '.partial')
        pending.write_text(json.dumps(self.__dict__, sort_keys=True, indent=1))
        pending.replace(path)

    def product(self, source: str, consumer: str, slot: str) -> dict[str, object]:
        entry = self.products.setdefault(f'{source}:{consumer}', {})
        state = entry.get(slot)
        if not isinstance(state, dict):
            state = {}
            entry[slot] = state
        return cast(dict[str, object], state)


def _seconds(value: float) -> str:
    return f'{int(value)} s'


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
        queue_threshold: int = 200,
        inventory: Inventory | None = None,
        policy: Policy | None = None,
        remote_verifier: RemoteVerifier = remote_file_sha256,
        arrow_root: Path | None = None,
        parquet_root: Path | None = None,
    ) -> None:
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
        self.inventory = inventory if inventory is not None else load_inventory()
        self.bounds = Bounds.from_policy(policy if policy is not None else load_policy())
        self.remote_verifier = remote_verifier
        self.arrow_root = arrow_root if arrow_root is not None else arrow_store_root()
        self.parquet_root = parquet_root if parquet_root is not None else parquet_source_root()
        self.specs = {spec.key: spec for spec in SOURCE_REGISTRY}
        # What the last tick measured, for the check metadata; reset at every tick.
        self._ages: dict[str, SourceAge] = {}
        self._depths: dict[str, DepthState] = {}
        self._products: dict[str, dict[str, object]] = {}

    def tick(self, now: datetime) -> TickOutcome:
        started = time.monotonic()
        now = now.astimezone(UTC)
        minute = now.replace(second=0, microsecond=0)
        window_end = now - timedelta(seconds=DELIVERY_LAG_SECONDS)
        cursor = Cursor.load(self.cursor_path, now, self.lookback_minutes)
        self._ages = {}
        self._depths = {}
        self._products = {}
        # Every detector is isolated: a fault in one becomes its own finding on its check,
        # the others still run, the evaluations are still written and the e-mail is still
        # sent. A detector that did not complete its read leaves its cursor where it was.
        dagster, dagster_read = self._guarded(
            'queue_bounded', 'dagster', lambda: self._dagster_findings(cursor, window_end)
        )
        heartbeats, _ = self._guarded(
            'workers_alive', 'heartbeats', lambda: (self._heartbeat_findings(now), None)
        )
        workers, receipts_read = self._guarded(
            'workers_alive', 'workers', lambda: self._worker_findings(cursor, window_end)
        )
        collectors, _ = self._guarded(
            'collectors_serving',
            'collectors',
            lambda: (self._collector_findings(minute - timedelta(minutes=1)), None),
        )
        logs, logs_read = self._guarded(
            'no_error_logs', 'logs', lambda: self._log_findings(cursor, window_end)
        )
        coverage_by_source: dict[str, Coverage] = {}
        ages, _ = self._guarded(
            'data_current', 'coverage', lambda: (self._source_findings(coverage_by_source, now), None)
        )
        depth, _ = self._guarded('data_current', 'depth', lambda: (self._depth_findings(now), None))
        publication, _ = self._guarded(
            'publication_current',
            'publication',
            lambda: (self._publication_findings(cursor, coverage_by_source, now), None),
        )
        # Failed units are resolved by later facts, not by time: the check stays failed
        # until an OK receipt, a later OK publication or certified coverage exists.
        resolution, _ = self._guarded(
            'workers_alive',
            'resolution',
            lambda: (self._open_unit_findings(cursor, coverage_by_source), None),
        )
        findings: list[Finding] = [
            *dagster, *heartbeats, *workers, *resolution, *collectors, *logs, *ages, *depth,
            *publication,
        ]
        evaluation_seconds = time.monotonic() - started
        if evaluation_seconds > self.bounds.evaluation_seconds:
            findings.append(
                Finding(
                    'monitor_period_exceeded',
                    'data_current',
                    'Monitor evaluation exceeded its period',
                    f'{evaluation_seconds:.1f} s against {_seconds(self.bounds.evaluation_seconds)}.',
                )
            )

        by_check: dict[CheckName, list[Finding]] = {name: [] for name in CHECK_NAMES}
        for finding in findings:
            by_check[finding.check].append(finding)
        detail: dict[CheckName, dict[str, object]] = {name: {} for name in CHECK_NAMES}
        detail['data_current'] = {
            'sources': {age.source: age.metadata() for age in self._ages.values()},
            'depth': {key: self._depth_metadata(state, minute) for key, state in self._depths.items()},
            'evaluation_seconds': round(evaluation_seconds, 3),
        }
        detail['publication_current'] = {'consumers': dict(self._products)}
        detail['workers_alive'] = {'open_units': len(cursor.open_units)}
        writes = [
            self.reporter.check(
                MONITOR_ASSET,
                name,
                passed=not items,
                metadata={
                    'findings': len(items),
                    'keys': ', '.join(item.key for item in items)[:2000],
                    'evaluated_at': now.isoformat(),
                    **detail[name],
                },
            )
            for name, items in by_check.items()
        ]
        written = all(writes)
        cooldown = self.settings.cooldown_seconds if self.settings else 0
        fresh = [
            finding
            for finding in findings
            if now.timestamp() - cursor.sent.get(finding.key, 0.0) > cooldown
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
        if dagster_read is not None:
            cursor.failures_after = now.timestamp()
        if receipts_read is not None:
            cursor.receipts_after = receipts_read.isoformat()
        if logs_read is not None:
            cursor.logs_after = logs_read.isoformat()
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

    @staticmethod
    def _guarded(
        check: CheckName, key: str, detector: Callable[[], tuple[list[Finding], datetime | None]]
    ) -> tuple[list[Finding], datetime | None]:
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
            ], None

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
            'Dagit check evaluations: written.' if written else 'Dagit check evaluations: FAILED to write.'
        )
        lines.append('Investigate in this order: Dagit, ClickHouse, Docker, the collectors.')
        return '\n'.join(lines) + '\n'

    def _dagster_findings(
        self, cursor: Cursor, window_end: datetime
    ) -> tuple[list[Finding], datetime | None]:
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
            ], None
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
        return findings, window_end

    def _heartbeats(self) -> list[Path]:
        own = heartbeat_path(self.heartbeat_dir, self.name)
        return sorted(path for path in self.heartbeat_dir.glob('*.heartbeat') if path != own)

    def _heartbeat_findings(self, now: datetime) -> list[Finding]:
        """Liveness only: every required worker's heartbeat exists and is fresh."""
        findings: list[Finding] = []
        required = required_workers(self.inventory, own=self.name)
        present = {path.name.removesuffix('.heartbeat'): path for path in self._heartbeats()}
        for feed in required:
            if feed not in present:
                findings.append(
                    Finding(
                        f'heartbeat_missing:{feed}',
                        'workers_alive',
                        f'Worker {feed} heartbeat missing',
                        f'No {feed}.heartbeat under {self.heartbeat_dir}; the worker never ran here.',
                    )
                )
        for feed, heartbeat in present.items():
            if not heartbeat_is_fresh(
                heartbeat, max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS, now=now.timestamp()
            ):
                findings.append(
                    Finding(
                        f'heartbeat_stale:{feed}',
                        'workers_alive',
                        f'Worker {feed} heartbeat stale',
                        f'Older than {HEARTBEAT_MAX_AGE_SECONDS} seconds.',
                    )
                )
        return findings

    def _worker_findings(
        self, cursor: Cursor, window_end: datetime
    ) -> tuple[list[Finding], datetime | None]:
        """Every failed receipt in the window, read to completion and kept as an open unit
        until a later fact resolves it; the cursor advances only as far as was read."""
        findings: list[Finding] = []
        read: PagedRead[Receipt] = read_pages(
            lambda since, until: failed_receipts_since(self.client, self.database, since, until),
            lambda receipt: receipt.recorded_at,
            lambda receipt: (
                receipt.feed, receipt.series, receipt.minute, receipt.recorded_at,
                receipt.error_code, receipt.error, receipt.worker_host,
            ),
            datetime.fromisoformat(cursor.receipts_after),
            window_end,
        )
        for receipt in read.rows:
            unit = OpenUnit(
                receipt.feed, receipt.series, receipt.minute, receipt.recorded_at,
                receipt.error_code, receipt.error,
            )
            cursor.open_units[unit.key] = unit.to_json()
        if len(cursor.open_units) > MAX_OPEN_UNITS:
            dropped = len(cursor.open_units) - MAX_OPEN_UNITS
            for key in sorted(cursor.open_units)[:dropped]:
                del cursor.open_units[key]
            findings.append(
                Finding(
                    'open_units_truncated',
                    'workers_alive',
                    f'{dropped} oldest failed units dropped from tracking',
                    f'More than {MAX_OPEN_UNITS} failed units await resolution.',
                )
            )
        if not read.complete:
            findings.append(
                Finding(
                    'receipts_read_incomplete',
                    'workers_alive',
                    'Failed receipts exceed one tick of paging',
                    f'Read through {read.read_through.isoformat()}; the rest is read next tick.',
                )
            )
        return findings, read.read_through

    def _open_unit_findings(
        self, cursor: Cursor, coverage_by_source: dict[str, Coverage]
    ) -> list[Finding]:
        """One finding per (feed, series) with failed units no later fact has resolved."""
        units = [OpenUnit.from_json(key, data) for key, data in cursor.open_units.items()]
        if not units:
            return []

        def covered(unit: OpenUnit) -> bool:
            coverage = coverage_by_source.get(unit.series)
            return coverage is not None and covered_minute(coverage, unit.minute)

        for key in resolved_units(self.client, self.database, units, covered):
            del cursor.open_units[key]
        open_by_series: dict[tuple[str, str], list[OpenUnit]] = {}
        for unit in units:
            if unit.key in cursor.open_units:
                open_by_series.setdefault((unit.feed, unit.series), []).append(unit)
        findings: list[Finding] = []
        for (feed, series), items in sorted(open_by_series.items()):
            items.sort(key=lambda unit: unit.minute)
            oldest, latest = items[0], items[-1]
            findings.append(
                Finding(
                    f'receipt_failed:{feed}:{series}',
                    'workers_alive',
                    f'Worker {feed} failed {series}',
                    f'{len(items)} unresolved unit(s) since minute {oldest.minute.isoformat()}; '
                    f'latest minute {latest.minute.isoformat()}: {latest.error_code} '
                    f'{latest.error[:200]}'.rstrip(),
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

    def _log_findings(
        self, cursor: Cursor, window_end: datetime
    ) -> tuple[list[Finding], datetime | None]:
        read: PagedRead[LogRow] = read_pages(
            lambda since, until: error_log_rows_since(self.client, self.database, since, until),
            lambda row: row.timestamp,
            lambda row: (row.timestamp, row.service, row.container, row.level, row.message),
            datetime.fromisoformat(cursor.logs_after),
            window_end,
        )
        by_service: dict[str, list[str]] = {}
        for row in sorted(read.rows, key=lambda row: row.timestamp):
            by_service.setdefault(row.service, []).append(row.message)
        findings = [
            Finding(
                f'error_logs:{service}',
                'no_error_logs',
                f'{len(messages)} error line(s) from {service}',
                messages[-1][:300],
            )
            for service, messages in sorted(by_service.items())
        ]
        if not read.complete:
            findings.append(
                Finding(
                    'logs_read_incomplete',
                    'no_error_logs',
                    'Error log rows exceed one tick of paging',
                    f'Read through {read.read_through.isoformat()}; the rest is read next tick.',
                )
            )
        return findings, read.read_through

    def _source_findings(
        self, coverage_by_source: dict[str, Coverage], now: datetime
    ) -> list[Finding]:
        """SS-01 per required source: the contiguous frontier against the clock. Each
        source is read on its own, so one unreadable source is one finding."""
        findings: list[Finding] = []
        for key, required in self.inventory.sources.items():
            spec = self.specs.get(key)
            if spec is None or spec.rollout_stage == RolloutStage.DORMANT:
                findings.append(
                    Finding(
                        f'source_missing:{key}',
                        'data_current',
                        f'Required source {key} is not enabled',
                        'The pinned inventory requires it; the registry does not serve it.',
                    )
                )
                continue
            if spec.rollout_stage.value != required.rollout_stage:
                findings.append(
                    Finding(
                        f'inventory_mismatch:{key}',
                        'data_current',
                        f'{key} rollout stage differs from the inventory',
                        f'registry {spec.rollout_stage.value}, inventory {required.rollout_stage}.',
                    )
                )
            try:
                coverage = read_coverage(SourceStore(self.client, self.database, spec), now)
            except Exception as error:
                log.exception('%s coverage unreadable', key)
                findings.append(
                    Finding(
                        f'source_unreadable:{key}',
                        'data_current',
                        f'{key} coverage could not be read',
                        f'{type(error).__name__}: {error}'[:300],
                    )
                )
                continue
            coverage_by_source[key] = coverage
            age = source_age(key, coverage)
            self._ages[key] = age
            stale = age.lag_seconds > self.bounds.source_lag_seconds
            gap = age.oldest_gap_age_seconds > self.bounds.uncovered_minute_age_seconds
            if stale or gap or age.incomplete_partitions:
                tail = (
                    'the tail is not advancing'
                    if age.newest_lag_seconds > self.bounds.source_lag_seconds
                    else 'the tail advances behind a gap'
                )
                findings.append(
                    Finding(
                        f'source_stale:{key}',
                        'data_current',
                        f'{key} contiguous coverage is {_seconds(age.lag_seconds)} behind the clock',
                        f'F {age.contiguous_end.isoformat()}, N {age.newest_end.isoformat()}, '
                        f'C {age.canonical_end.isoformat()}, U {age.due.isoformat()}; '
                        f'{age.missing_minutes} missing minute(s), oldest gap '
                        f'{_seconds(age.oldest_gap_age_seconds)} old, '
                        f'{age.incomplete_partitions} partition(s) without complete component '
                        f'evidence; {tail}.',
                    )
                )
        return findings

    def _depth_findings(self, now: datetime) -> list[Finding]:
        """SS-03 per depth path: the committed Arrow against the clock, and every closed
        minute inside retention whose deadline passed without a chunk."""
        findings: list[Finding] = []
        due = now.replace(second=0, microsecond=0)
        if not self.arrow_root.is_dir():
            return [
                Finding(
                    'product_root_unmounted:arrow',
                    'data_current',
                    f'Arrow store {self.arrow_root} is not mounted',
                    'Depth commits and Arrow series cannot be verified without it.',
                )
            ]
        for key, required in self.inventory.depth.items():
            state = depth_state(
                self.arrow_root,
                required.series,
                manifest_name=required.arrow_manifest,
                chunk_pattern=required.chunk_pattern,
                retention_minutes=required.chunk_retention_minutes,
                due=due,
                close_to_arrow_seconds=self.bounds.depth_close_to_arrow_seconds,
            )
            self._depths[key] = state
            lag = state.lag_seconds(due)
            if lag is None:
                findings.append(
                    Finding(
                        f'depth_missing:{key}',
                        'data_current',
                        f'{required.series} has no committed Arrow manifest',
                        f'{self.arrow_root / required.series / required.arrow_manifest} is absent.',
                    )
                )
                continue
            if not state.latest_chunk_present:
                findings.append(
                    Finding(
                        f'depth_files_invalid:{key}',
                        'data_current',
                        f'{required.series} manifest names an absent chunk',
                        f'latest minute {state.latest_minute.isoformat() if state.latest_minute else "-"}.',
                    )
                )
            if lag > self.bounds.depth_close_to_arrow_seconds:
                findings.append(
                    Finding(
                        f'depth_stale:{key}',
                        'data_current',
                        f'{required.series} Arrow is {_seconds(lag)} behind the clock',
                        f'latest committed minute {state.latest_minute.isoformat() if state.latest_minute else "-"}, '
                        f'U {due.isoformat()}, bound {_seconds(self.bounds.depth_close_to_arrow_seconds)}.',
                    )
                )
            if state.missing_minutes:
                findings.append(
                    Finding(
                        f'depth_gap:{key}',
                        'data_current',
                        f'{required.series} lacks {len(state.missing_minutes)} closed minute chunk(s)',
                        f'oldest {state.missing_minutes[0].isoformat()}, '
                        f'newest {state.missing_minutes[-1].isoformat()} inside retention.',
                    )
                )
        return findings

    @staticmethod
    def _depth_metadata(state: DepthState, due: datetime) -> dict[str, object]:
        lag = state.lag_seconds(due)
        return {
            'latest_minute': state.latest_minute.isoformat() if state.latest_minute else None,
            'lag_seconds': int(lag) if lag is not None else None,
            'missing_closed_minutes': len(state.missing_minutes),
        }

    def _publication_findings(
        self, cursor: Cursor, coverage_by_source: dict[str, Coverage], now: datetime
    ) -> list[Finding]:
        """SS-02/SS-04 per required consumer: a readable manifest naming every required
        series with existing, verified files, whose delivered end ``P`` is within its
        cadence's bound of the clock."""
        if not self.publication_root.is_dir():
            raise RuntimeError(f'Publication root {self.publication_root} is not mounted.')
        findings: list[Finding] = []
        if not self.parquet_root.is_dir():
            findings.append(
                Finding(
                    'product_root_unmounted:parquet',
                    'publication_current',
                    f'Parquet mirror {self.parquet_root} is not mounted',
                    'Mount month files cannot be verified without it.',
                )
            )
        for key, required in self.inventory.sources.items():
            age = self._ages.get(key)
            held = self._backfill_hold(key)
            self._note_canonical(cursor, key, age, now)
            for consumer in required.consumers:
                label = f'{key}:{consumer.key}'
                scope = 'public' if consumer.public else f'{required.rollout_stage} local'
                spec = self.specs.get(key)
                if spec is not None and consumer.key not in {item.key for item in spec.consumers}:
                    findings.append(
                        Finding(
                            f'inventory_mismatch:{label}',
                            'publication_current',
                            f'{label} is required but not declared by the source',
                            f'The {scope} consumer is pinned in the inventory.',
                        )
                    )
                root = self.publication_root / key / consumer.key
                try:
                    manifest = parse_manifest((root / 'latest.json').read_text())
                except FileNotFoundError:
                    findings.append(
                        Finding(
                            f'publication_missing:{label}',
                            'publication_current',
                            f'{label} has never published',
                            f'No manifest under {root} ({scope}).',
                        )
                    )
                    continue
                except (OSError, ValueError, KeyError, TypeError) as error:
                    findings.append(
                        Finding(
                            f'publication_manifest_unreadable:{label}',
                            'publication_current',
                            f'{label} manifest unreadable',
                            f'{type(error).__name__}: {error}'[:300],
                        )
                    )
                    continue
                series = frozenset(item.name for item in required.series)
                findings.extend(
                    self._product_findings(cursor, label, scope, series, manifest, root, now)
                )
                if age is None:
                    findings.append(
                        Finding(
                            f'publication_unverified:{label}',
                            'publication_current',
                            f'{label} delivered end cannot be bounded',
                            'The source coverage was not read this tick.',
                        )
                    )
                    continue
                delivered = delivered_end(manifest, age, canonical_only=consumer.canonical_only)
                lag = max(0.0, (age.due - delivered).total_seconds())
                self._products[label]['P'] = delivered.isoformat()
                self._products[label]['lag_seconds'] = int(lag)
                self._products[label]['backfill_hold'] = held
                if consumer.canonical_only:
                    findings.extend(
                        self._canonical_consumer_findings(cursor, key, label, scope, age, delivered, now)
                    )
                elif lag > self.bounds.mount_lag_seconds:
                    findings.append(
                        Finding(
                            f'publication_stale:{label}',
                            'publication_current',
                            f'{label} delivered coverage is {_seconds(lag)} behind the clock',
                            f'P {delivered.isoformat()} (manifest through '
                            f'{manifest.active_through.isoformat()}, source F '
                            f'{age.contiguous_end.isoformat()}), U {age.due.isoformat()}; {scope}'
                            f'{"; a backfill holds publication" if held else ""}.',
                        )
                    )
        return findings

    def _backfill_hold(self, source: str) -> bool | None:
        """Whether a native backfill owns the source's publication; reported in the
        detail, never a reason to stop measuring. ``None`` when Dagster cannot say."""
        try:
            return self.dagster.backfill_owns_publication(source)
        except Exception as error:
            log.error('%s backfill hold unknown: %s', source, error)
            return None

    def _note_canonical(
        self, cursor: Cursor, source: str, age: SourceAge | None, now: datetime
    ) -> None:
        """Remember when the canonical frontier C was first observed at its current value:
        the consumer completion bound counts from that observation."""
        if age is None:
            return
        seen = cursor.canonical_seen.get(source)
        if seen is None or str(seen[0]) != age.canonical_end.isoformat():
            cursor.canonical_seen[source] = [age.canonical_end.isoformat(), now.timestamp()]

    def _canonical_consumer_findings(
        self,
        cursor: Cursor,
        source: str,
        label: str,
        scope: str,
        age: SourceAge,
        delivered: datetime,
        now: datetime,
    ) -> list[Finding]:
        findings: list[Finding] = []
        absolute = (now - delivered).total_seconds()
        if absolute > self.bounds.canonical_delivered_age_seconds:
            findings.append(
                Finding(
                    f'publication_stale:{label}',
                    'publication_current',
                    f'{label} delivered end is {absolute / 3600:.1f} h behind the clock',
                    f'P {delivered.isoformat()}, bound '
                    f'{self.bounds.canonical_delivered_age_seconds / 3600:.0f} h; {scope}.',
                )
            )
            return findings
        seen = cursor.canonical_seen.get(source)
        if delivered < age.canonical_end and seen is not None:
            observed = float(cast(float, seen[1]))
            waited = now.timestamp() - observed
            if waited > self.bounds.consumer_completion_seconds:
                findings.append(
                    Finding(
                        f'publication_stale:{label}',
                        'publication_current',
                        f'{label} has not published the canonical frontier',
                        f'P {delivered.isoformat()} while C {age.canonical_end.isoformat()} was '
                        f'observed {_seconds(waited)} ago, bound '
                        f'{_seconds(self.bounds.consumer_completion_seconds)}; {scope}.',
                    )
                )
        return findings

    def _product_findings(
        self,
        cursor: Cursor,
        label: str,
        scope: str,
        series: frozenset[str],
        manifest: Manifest,
        root: Path,
        now: datetime,
    ) -> list[Finding]:
        """The files and remote objects a manifest names, verified within the tick's budget."""
        source, consumer = label.split(':', 1)
        findings: list[Finding] = []
        missing_series = sorted(series - manifest.series)
        if len(series) != self.bounds.required_series or missing_series:
            findings.append(
                Finding(
                    f'publication_series_missing:{label}',
                    'publication_current',
                    f'{label} lacks {len(missing_series)} required series',
                    f'{", ".join(missing_series) or "inventory does not pin twelve"}; {scope}.',
                )
            )
        evidence: FileEvidence = verify_files(manifest, root, cursor.product(source, consumer, 'files'))
        self._products[label] = {
            'version': manifest.version,
            'active_through': manifest.active_through.isoformat(),
            'series': len(manifest.series),
            **evidence.metadata(),
        }
        if evidence.invalid:
            named = [*evidence.missing[:5], *evidence.mismatched[:5]]
            findings.append(
                Finding(
                    f'publication_files_invalid:{label}',
                    'publication_current',
                    f'{label} names {len(evidence.missing)} absent and '
                    f'{len(evidence.mismatched)} mismatched file(s)',
                    f'{"; ".join(named)}; {scope}.'[:300],
                )
            )
        if manifest.kind == 'huggingface':
            problems = verify_uploads(
                manifest,
                series,
                cursor.product(source, consumer, 'remote'),
                verifier=self.remote_verifier,
                now=now,
            )
            self._products[label]['remote_problems'] = len(problems)
            if problems:
                findings.append(
                    Finding(
                        f'publication_remote_unverified:{label}',
                        'publication_current',
                        f'{label} remote publication not verified for {len(problems)} series',
                        '; '.join(problems[:3])[:300],
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
        description='Watch Dagster, the workers, the collectors, the container log, the '
        'certified coverage and the published products; write check evaluations to Dagit '
        'and e-mail new findings.',
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
