"""The provisional worker: the live minute path of every revisioned source with a
provisional adapter.

One tick builds each eligible closed interval once through the source runtime (the
adapter's candidates: the last completed minute plus the missing minutes inside its
window, oldest first), then publishes every consumer that pins provisional rows when the
pinned state changed, unless a native backfill of the source owns publication. Each
interval and each publication writes one receipt; every tick reports the source's live
feed asset so its freshness check sees the worker. A failing interval or publication is
retried with a doubling delay from one minute up to the source's ``retry_delay``, at most
``retry_count`` times, then left to the operator. Each built interval, each
publication, and each paced REST request touches the worker heartbeat, so a slow
catch-up tick proves liveness instead of tripping the watchdog.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.bundle import SourceRunConfig, execute_source
from origo.sources.contracts import (
    WORKER_HEARTBEAT_ENV,
    Partition,
    RevisionedSourceSpec,
    RolloutStage,
    SourceError,
    failure_code,
    failure_message,
)
from origo.sources.failures import FailureLog
from origo.sources.publication import publication_current
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.steady_state.coverage import Coverage, read_coverage
from origo.steady_state.progress import report_data_progress
from origo.steady_state.ownership import WorkerOwner, fence_retired_owners, worker_execution
from origo.steady_state.prerequisites import open_prerequisites, render_active
from origo.steady_state.receipt_identity import outstanding_owner_epochs

from .dagster_reader import DagsterReader, DagsterUnreachable
from .receipts import (
    ensure_monitoring_tables,
    failed_attempts,
    reconcile_died_receipts,
    record_receipt,
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
    touch_heartbeat,
    utc_now,
)

DEFAULT_WEBSERVER_URL = 'http://dagit:3000'
log = logging.getLogger('origo.workers.provisional')


def live_feed_asset(spec: RevisionedSourceSpec) -> str:
    return f'{spec.key}_provisional_feed'


def _rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage if sys.platform == 'darwin' else usage * 1024


class ProvisionalFeed:
    name = 'provisional'
    lookback_minutes = 15

    def __init__(
        self,
        specs: Sequence[RevisionedSourceSpec],
        *,
        publication_root: Path,
        reporter: Reporter,
        dagster: DagsterReader,
        host: str = '',
        clock: Callable[[], datetime] = utc_now,
        heartbeat: Path | None = None,
        max_workers: int = 4,
    ) -> None:
        self.specs = tuple(
            spec
            for spec in specs
            if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT
        )
        self._reported_frontiers: dict[str, datetime] = {}
        self.publication_root = publication_root
        self.reporter = reporter
        self.dagster = dagster
        self.host = host or os.uname().nodename
        self.clock = clock
        self.heartbeat = heartbeat
        if not 1 <= max_workers <= 4:
            raise ValueError("A provisional assignment admits one to four builds.")
        self.max_workers = max_workers
        self._owner_claim: WorkerOwner | None = None
        self._owner_mutex = threading.Lock()

    def _owner(self) -> WorkerOwner:
        with self._owner_mutex:
            if self._owner_claim is None:
                self._owner_claim = WorkerOwner(
                    Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')), self.name,
                )
            return self._owner_claim

    def _recover_owners(self, store: SourceStore, now: datetime) -> int:
        owner = self._owner()
        try:
            epochs = outstanding_owner_epochs(store.client, store.database, self.name)
            probe = fence_retired_owners(owner.root, self.name, epochs)
            if probe.unknown:
                log.error('Unresolved owner epochs remain UNKNOWN: %s', probe.unknown)
            return reconcile_died_receipts(
                store.client, store.database, feed=self.name, now=now,
                confirmed_dead_owner_epochs=probe.retired,
            )
        except Exception:
            log.exception('Worker ownership reconciliation failed; no death was inferred')
            return 0

    def _beat(self) -> None:
        """Prove the loop is alive after each unit of work inside a slow tick."""
        if self.heartbeat is not None:
            touch_heartbeat(self.heartbeat)

    def _run_id(self, now: datetime) -> str:
        return f'worker:{self.name}:{self.host}:{now.strftime("%Y%m%dT%H%M%SZ")}'

    def _may_attempt(
        self,
        store: SourceStore,
        spec: RevisionedSourceSpec,
        *,
        series: str,
        work: str,
        minute: datetime | None = None,
        token: str | None = None,
        exhaustible: bool = True,
    ) -> bool:
        """Whether the work's failures allow another attempt now: none so far, or the
        doubling delay since the last failure elapsed. Exhaustible work stops after
        ``retry_count`` for an operator run; the current-view frontier gap is admitted
        inexhaustible, so the readers can never freeze behind a parked hole — a hole
        that later becomes the frontier resumes retrying on the capped delay."""
        attempts, last_failed = failed_attempts(
            store.client, store.database, feed=self.name, series=series, minute=minute, token=token
        )
        if attempts == 0 or last_failed is None:
            return True
        if exhaustible and attempts > spec.orchestration.retry_count:
            log.error(
                'source=%s %s attempts exhausted after %d failures; an operator run is required',
                spec.key,
                work,
                attempts,
            )
            return False
        delay = min(spec.orchestration.retry_delay, 60 * 2 ** min(attempts - 1, 60))
        return self.clock() >= last_failed + timedelta(seconds=delay)

    @staticmethod
    def _built_rows(
        store: SourceStore, spec: RevisionedSourceSpec, partition: Partition, build_id: str
    ) -> int:
        """The rows of the interval's largest component for the build just activated."""
        rows = store.execute(
            f"""SELECT max(row_count) FROM {store.table('source_component_log')}
            WHERE source_key = %(source)s AND partition_key = %(partition)s
              AND build_id = %(build)s""",
            {'source': spec.key, 'partition': partition.key, 'build': build_id},
        )
        return int(str(rows[0][0])) if rows and rows[0][0] is not None else 0

    def _build_one(
        self, store: SourceStore, spec: RevisionedSourceSpec, partition: Partition, now: datetime
    ) -> tuple[str | None, str | None]:
        """Build one admitted minute; returns (processed key, failed key), one set."""
        key = f'{spec.key}:{partition.key}'
        started = time.monotonic()
        owner = self._owner()
        attempt = owner.attempt(key)
        record_receipt(
            store.client,
            store.database,
            feed=self.name,
            series=spec.key,
            minute=partition.start,
            rows=0,
            sha256='',
            duration_ms=0,
            status='STARTED', attempt=attempt,
        )
        try:
            with worker_execution(owner):
                result = execute_source(
                    spec,
                    'provisional',
                    SourceRunConfig(partition_key=partition.key),
                    run_id=self._run_id(now),
                )
        except Exception as error:
            log.exception('source=%s partition=%s provisional build failed', spec.key, partition.key)
            record_receipt(
                store.client,
                store.database,
                feed=self.name,
                series=spec.key,
                minute=partition.start,
                rows=0,
                sha256='',
                duration_ms=int((time.monotonic() - started) * 1000),
                status='FAILED', attempt=attempt,
                error_code=failure_code(error),
                error=str(error),
            )
            self._beat()
            return None, key
        record_receipt(
            store.client,
            store.database,
            feed=self.name,
            series=spec.key,
            minute=partition.start,
            rows=self._built_rows(store, spec, partition, str(result.get('build_id', ''))),
            sha256='',
            duration_ms=int((time.monotonic() - started) * 1000),
            status='OK', attempt=attempt,
        )
        self._beat()
        return key, None

    def _frontier_gap_key(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime,
        *, coverage: Coverage | None = None,
    ) -> str | None:
        """First uncovered minute, not max(end) retaining later canonical days."""
        observed = coverage if coverage is not None else read_coverage(store, now)
        gap = observed.contiguous_end
        if gap >= observed.due:
            return None
        return gap.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _build_intervals(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime
    ) -> tuple[list[str], list[str]]:
        adapter = spec.provisional
        if adapter is None:
            raise ValueError('No provisional adapter is declared.')
        coverage = read_coverage(store, now)
        if coverage.incomplete_partitions:
            raise SourceError(
                'COVERAGE_COMPONENT_EVIDENCE_INVALID',
                f'Active component evidence is incomplete for {len(coverage.incomplete_partitions)} partitions.',
            )
        covered = coverage.intervals
        candidates = sorted(
            adapter.candidates(now, coverage.anchor, covered),
            key=lambda partition: partition.start,
        )
        frontier = self._frontier_gap_key(store, spec, now, coverage=coverage)
        ordered = list(candidates)
        if frontier is not None and all(partition.key != frontier for partition in ordered):
            gap_start = datetime.strptime(frontier, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC)
            if not any(interval.start <= gap_start < interval.end for interval in covered):
                ordered.insert(0, adapter.partition(frontier))
        admitted = [
            partition
            for partition in ordered
            if self._may_attempt(
                store,
                spec,
                series=spec.key,
                work=f'partition={partition.key}',
                minute=partition.start,
                exhaustible=partition.key != frontier,
            )
        ]
        if not admitted:
            return [], []

        def build(partition: Partition) -> tuple[str | None, str | None]:
            # Connections are not shared across threads: each unit gets its own.
            client = make_clickhouse_client(get_clickhouse_settings())
            try:
                return self._build_one(SourceStore(client, store.database, spec), spec, partition, now)
            finally:
                client.disconnect()

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            outcomes = list(pool.map(build, admitted))
        processed = [key for key, _ in outcomes if key is not None]
        failed = [key for _, key in outcomes if key is not None]
        return processed, failed

    def _publish(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime
    ) -> tuple[list[str], list[str]]:
        """Publish every consumer that pins provisional rows when its pinned state changed."""
        pinned_consumers = [consumer for consumer in spec.consumers if not consumer.canonical_only]
        if not pinned_consumers:
            return [], []
        try:
            owned = self.dagster.backfill_owns_publication(spec.key)
        except DagsterUnreachable as error:
            # Publication follows the state in ClickHouse; an unreadable Dagster is the
            # monitor's finding, not a reason to stop publishing.
            log.error('backfill state unavailable, publishing anyway: %s', error)
            owned = False
        if owned:
            log.info('source=%s historical backfill active; publish only already accepted generations', spec.key)
        processed: list[str] = []
        failed: list[str] = []
        for consumer in pinned_consumers:
            series = f'{spec.key}:{consumer.key}'
            owner = self._owner()
            if render_active(owner.root, spec.key, consumer.key):
                log.info('source=%s consumer=%s native render active; ingestion continues', spec.key, consumer.key)
                continue
            if open_prerequisites(store, consumer=consumer.key):
                log.info('source=%s consumer=%s full-render prerequisite belongs to the native bulk sensor', spec.key, consumer.key)
                continue
            snapshot = store.snapshot(canonical_only=False)
            if not snapshot.records or publication_current(
                spec, consumer.key, snapshot.token, root=self.publication_root, pinned=True
            ):
                continue
            if not store.canonical_ready():
                log.info('source=%s canonical state not ready for publication', spec.key)
                continue
            if not self._may_attempt(
                store,
                spec,
                series=series,
                work=f'consumer={consumer.key} state={snapshot.token[:12]}',
                token=snapshot.token,
            ):
                continue
            started = time.monotonic()
            owner = self._owner()
            attempt = owner.attempt(
                f'{series}:{snapshot.token}', state_token=snapshot.token, prerequisite_key=series,
            )
            record_receipt(
                store.client,
                store.database,
                feed=self.name,
                series=series,
                minute=now.replace(second=0, microsecond=0),
                rows=0,
                sha256=snapshot.token,
                duration_ms=0,
                status='STARTED', attempt=attempt,
            )
            try:
                with worker_execution(owner):
                    execute_source(
                        spec,
                        f'consumer_{consumer.key}',
                        SourceRunConfig(
                            destination=str(self.publication_root / spec.key / consumer.key),
                            publication_wait=False,
                        ),
                        run_id=self._run_id(now),
                    )
            except Exception as error:
                log.exception('source=%s consumer=%s publication failed', spec.key, consumer.key)
                record_receipt(
                    store.client,
                    store.database,
                    feed=self.name,
                    series=series,
                    minute=now.replace(second=0, microsecond=0),
                    rows=0,
                    sha256=snapshot.token,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    status='FAILED', attempt=attempt,
                    error_code=failure_code(error),
                    error=str(error),
                )
                failed.append(series)
                self._beat()
                continue
            record_receipt(
                store.client,
                store.database,
                feed=self.name,
                series=series,
                minute=now.replace(second=0, microsecond=0),
                rows=len(snapshot.records),
                sha256=snapshot.token,
                duration_ms=int((time.monotonic() - started) * 1000),
                status='OK', attempt=attempt,
            )
            processed.append(series)
            self._beat()
        return processed, failed

    def tick(self, now: datetime) -> TickOutcome:
        now = now.astimezone(UTC)
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        processed: list[str] = []
        failed: list[str] = []
        try:
            died = (self._recover_owners(SourceStore(client, settings.database, self.specs[0]), now)
                    if self.specs else 0)
            if died:
                log.warning('reconciled %d receipts for units the previous process died on', died)
            for spec in self.specs:
                store = SourceStore(client, settings.database, spec)
                failures = FailureLog(
                    store, Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
                    self._run_id(now),
                )
                try:
                    built, broken = self._build_intervals(store, spec, now)
                    processed.extend(built)
                    failed.extend(broken)
                    published, unpublished = self._publish(store, spec, now)
                    processed.extend(published)
                    failed.extend(unpublished)
                    failures.recover(operation='worker_tick')
                    coverage = read_coverage(store, now)
                    reported = report_data_progress(
                        self.reporter, live_feed_asset(spec),
                        source_end=coverage.contiguous_end, observed_at=self.clock(),
                        previous_end=self._reported_frontiers.get(spec.key), max_age_seconds=120,
                        metadata={'minute': now.replace(second=0, microsecond=0).isoformat(),
                                  'intervals': len(built), 'publications': len(published),
                                  'failed': len(broken) + len(unpublished), 'rss_bytes': _rss_bytes()},
                    )
                    if reported is not None:
                        self._reported_frontiers[spec.key] = reported
                except Exception as error:
                    key = f'{spec.key}:tick'
                    failed.append(key)
                    log.exception('source=%s provisional source tick failed', spec.key)
                    # Shared-store outages keep every source failed; source-local failures
                    # must not prevent independent work from reaching its own admission.
                    try:
                        failures.record(
                            operation='worker_tick', error_code=failure_code(error),
                            scope='SOURCE', message=failure_message(error),
                        )
                        record_receipt(
                            store.client, store.database, feed=self.name, series=key,
                            minute=now.replace(second=0, microsecond=0), rows=0, sha256='',
                            duration_ms=0, status='FAILED', error_code=failure_code(error),
                            error=failure_message(error),
                        )
                    except Exception:
                        log.exception('source=%s tick failure evidence could not be persisted', spec.key)
        finally:
            client.disconnect()
        return TickOutcome(
            self.name, now.replace(second=0, microsecond=0), tuple(processed), tuple(failed)
        )


def build_feed(
    environ: dict[str, str], *, heartbeat: Path | None = None, source_key: str | None = None,
) -> ProvisionalFeed:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        ensure_monitoring_tables(client, settings.database)
    finally:
        client.disconnect()
    base_url = environ.get('DAGSTER_WEBSERVER_URL', DEFAULT_WEBSERVER_URL)
    selected = SOURCE_REGISTRY if source_key is None else tuple(
        spec for spec in SOURCE_REGISTRY if spec.key == source_key
    )
    if not selected:
        raise ValueError("Unknown provisional source assignment.")
    return ProvisionalFeed(
        selected,
        publication_root=Path(environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')),
        reporter=Reporter(base_url),
        dagster=DagsterReader(base_url),
        heartbeat=heartbeat,
        max_workers=4 if source_key is None else 1,
    )


def source_heartbeat(directory: Path, source_key: str) -> Path:
    return heartbeat_path(directory, f'provisional-{source_key}')


def _enabled_sources() -> tuple[str, ...]:
    return tuple(spec.key for spec in SOURCE_REGISTRY
                 if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT)


def supervise_sources(heartbeat: Path) -> int:
    """Four independent minute clocks with one admitted build per source, four total.

    Child processes have separate connections, owner epochs and progress watchdogs.
    A slow provider does not postpone another source's next tick. The supervisor
    never submits data work and exits on child loss so Compose owns restart policy.
    """
    sources = _enabled_sources()
    if not sources:
        raise RuntimeError('The provisional service has no enabled source assignments.')
    children: list[subprocess.Popen[bytes]] = []

    def stopping(signum: int, frame: FrameType | None) -> None:
        raise SystemExit(128 + signum)

    old_term = signal.signal(signal.SIGTERM, stopping)
    old_int = signal.signal(signal.SIGINT, stopping)
    try:
        for source in sources:
            child = subprocess.Popen(
                [sys.executable, '-m', 'origo.workers.provisional', '--source', source],
                stdin=subprocess.DEVNULL,
            )
            children.append(child)
        while True:
            for source, child in zip(sources, children, strict=True):
                code = child.poll()
                if code is not None:
                    raise RuntimeError(f'Provisional source {source} exited with status {code}.')
            touch_heartbeat(heartbeat)
            time.sleep(5.0)
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                log.error('Provisional child %s did not stop; terminating its owned process', child.pid)
                child.kill()
                child.wait(timeout=5)
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m origo.workers.provisional',
        description='Build the provisional tails of every source every minute and publish '
        'the consumers that pin them.',
    )
    parser.add_argument(
        '--check', action='store_true', help='healthcheck: exit 0 when the heartbeat is fresh'
    )
    parser.add_argument('--once', action='store_true', help='run one tick and exit')
    parser.add_argument('--source', choices=_enabled_sources(), help='code-owned source assignment')
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    directory = heartbeat_directory()
    heartbeat = (source_heartbeat(directory, arguments.source) if arguments.source
                 else heartbeat_path(directory, ProvisionalFeed.name))
    if arguments.check:
        result = check_heartbeat(heartbeat)
        if arguments.source is None:
            now = time.time()
            if not all(heartbeat_is_fresh(source_heartbeat(directory, source),
                       max_age_seconds=HEARTBEAT_MAX_AGE_SECONDS, now=now)
                       for source in _enabled_sources()):
                return 1
        return result
    if not arguments.once and arguments.source is None:
        return supervise_sources(heartbeat)
    os.environ[WORKER_HEARTBEAT_ENV] = str(heartbeat)
    feed = build_feed(dict(os.environ), heartbeat=heartbeat, source_key=arguments.source)
    if arguments.once:
        outcome = feed.tick(datetime.now(UTC))
        print(json.dumps({'minute': outcome.minute.isoformat(), 'processed': list(outcome.processed), 'failed': list(outcome.failed)}))
        return 0
    from origo.sources.locking import source_lock

    # A duplicated container cannot create a second owner for the same assignment.
    lock_root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    with source_lock(lock_root, arguments.source, 'provisional_assignment'):
        run_forever(feed, heartbeat=heartbeat)


if __name__ == '__main__':
    sys.exit(main())
