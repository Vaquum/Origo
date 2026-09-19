"""The monitor: one detector outside Dagster that watches Dagster, the workers, the
collectors and the container log, writes its verdicts into Dagit and e-mails new findings.

Every minute the tick evaluates six checks on the ``origo_monitor`` asset:

- ``dagster_reachable``: the webserver answers and every required daemon is healthy;
- ``queue_bounded``: fewer queued runs than the threshold, and no run failure or failed
  asset check since the last tick;
- ``workers_alive``: every worker heartbeat is fresh and no worker receipt failed;
- ``collectors_serving``: each depth collector returns rows for the last completed minute;
- ``no_error_logs``: the container log holds no ``ERROR`` row since the last tick;
- ``publication_current``: every public consumer publishes the current source state.

The evaluations are written to Dagit first, then one e-mail lists every new finding key
and says whether that write succeeded. A key repeats inside the cooldown without a second
e-mail. A daily digest goes out once at the configured hour. The only state is a cursor
file; every fact lives in Dagit and ClickHouse.
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

from origo.alerts.email import AlertSettings, send_alert
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.contracts import Client, RolloutStage, identifier
from origo.sources.registry import SOURCE_REGISTRY

from .dagster_reader import DagsterReader, DagsterUnreachable, RunFailure
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
    'queue_bounded',
    'workers_alive',
    'collectors_serving',
    'no_error_logs',
    'publication_current',
]
CHECK_NAMES: tuple[CheckName, ...] = (
    'collectors_serving',
    'dagster_reachable',
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
        return cls(
            float(cast(float, data.get('failures_after', start.timestamp()))),
            str(data.get('receipts_after', start.isoformat())),
            str(data.get('logs_after', start.isoformat())),
            sent,
            str(data.get('last_digest_date', '')),
            int(cast(int, data.get('ticks', 0))),
            int(cast(int, data.get('findings', 0))),
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

    def tick(self, now: datetime) -> TickOutcome:
        now = now.astimezone(UTC)
        minute = now.replace(second=0, microsecond=0)
        window_end = now - timedelta(seconds=DELIVERY_LAG_SECONDS)
        cursor = Cursor.load(self.cursor_path, now, self.lookback_minutes)
        # Every detector is isolated: a fault in one becomes its own finding on its check,
        # the other five still run, the evaluations are still written and the e-mail is still
        # sent. A detector that did not complete its read leaves its cursor where it was.
        dagster, dagster_read = self._guarded(
            'queue_bounded', 'dagster', lambda: self._dagster_findings(cursor)
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
            'Dagit check evaluations: written.' if written else 'Dagit check evaluations: FAILED to write.'
        )
        lines.append('Investigate in this order: Dagit, ClickHouse, Docker, the collectors.')
        return '\n'.join(lines) + '\n'

    def _dagster_findings(self, cursor: Cursor) -> tuple[list[Finding], bool]:
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
        own = heartbeat_path(self.heartbeat_dir, self.name)
        return sorted(path for path in self.heartbeat_dir.glob('*.heartbeat') if path != own)

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
            try:
                owned = self.dagster.backfill_owns_publication(spec.key)
            except DagsterUnreachable as error:
                log.warning('source=%s publication hold state unknown: %s', spec.key, error)
                continue
            if owned:
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
                except (OSError, ValueError, KeyError):
                    # Never published, or published unreadably: the span of the state
                    # itself is the lag, so a fresh source stays quiet either way.
                    published = start
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
