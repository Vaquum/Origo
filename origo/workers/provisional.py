"""The provisional worker: the live minute path of every revisioned source with a
provisional adapter.

One tick builds each eligible closed interval once through the source runtime (the
adapter's candidates: the last completed minute plus the missing minutes inside its
window, newest minute first), then publishes every consumer that pins provisional rows
when the pinned state changed, unless a native backfill owns publication. Each
interval and each publication writes one receipt; every tick reports the source's live
feed asset so its freshness check sees the worker. A failing interval or publication is
retried until it succeeds, with a doubling delay from one minute up to the source's
``retry_delay``. Each built interval, each
publication, and each paced REST request touches the worker heartbeat, so a slow
catch-up tick proves liveness instead of tripping the watchdog.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import resource
import sys
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.binance_perp_rest import BinancePerpProvisional
from origo.sources.adapters.binance_perp_spool import (
    acknowledge_spooled_revision,
    classify_spooled_partition,
    cleanup_spool,
    spooled_repair_identity,
)
from origo.sources.adapters.binance_provisional import BinanceProvisionalBase
from origo.sources.adapters.render_book import (
    REPLICAS,
    RenderBookEndpoint,
    RenderBookProvisional,
)
from origo.sources.bundle import SourceRunConfig, execute_source
from origo.sources.contracts import (
    WORKER_HEARTBEAT_ENV,
    Partition,
    RevisionedSourceSpec,
    RolloutStage,
    failure_code,
)
from origo.sources.publication import publication_current
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore

from .dagster_reader import DagsterReader, DagsterUnreachable
from .receipts import (
    ensure_monitoring_tables,
    failed_attempts,
    reconcile_died_receipts,
    record_receipt,
)
from .report import Reporter
from .runtime import (
    TickOutcome,
    check_heartbeat,
    heartbeat_directory,
    heartbeat_path,
    run_forever,
    touch_heartbeat,
    utc_now,
)

PROVISIONAL_MAX_WORKERS = 4
TRADE_SOURCES = ('binance_spot_trades', 'binance_perp_trades')
DEFAULT_WEBSERVER_URL = 'http://dagit:3000'
log = logging.getLogger('origo.workers.provisional')


def live_feed_asset(spec: RevisionedSourceSpec) -> str:
    return f'{spec.key}_provisional_feed'


def _split_trade_ingestion(spec: RevisionedSourceSpec) -> bool:
    return spec.key in TRADE_SOURCES and os.environ.get('ORIGO_TRADE_MAINTENANCE', '0') == '1'


def selected_specs(environ: dict[str, str]) -> tuple[RevisionedSourceSpec, ...]:
    source = environ.get('ORIGO_PROVISIONAL_SOURCE')
    specs = tuple(
        spec for spec in SOURCE_REGISTRY
        if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT
        and (source is None or spec.key == source)
    )
    if source is not None and not specs:
        raise ValueError(f'Unknown or disabled provisional source: {source}')
    return specs


def _rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage if sys.platform == 'darwin' else usage * 1024


class ProvisionalFeed:
    name = 'provisional'
    lookback_minutes = 15
    role: Literal['publication', 'repair'] | None = None

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
    ) -> None:
        self.specs = tuple(
            spec
            for spec in specs
            if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT
        )
        self.publication_root = publication_root
        self.reporter = reporter
        self.dagster = dagster
        self.host = host or os.uname().nodename
        self.clock = clock
        self.heartbeat = heartbeat

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
        minute: datetime | None = None,
        token: str | None = None,
    ) -> bool:
        """Retry required work indefinitely, after a bounded exponential delay."""
        attempts, last_failed = failed_attempts(
            store.client, store.database, feed=self.name, series=series, minute=minute, token=token
        )
        if attempts == 0 or last_failed is None:
            return True
        cap = spec.orchestration.retry_delay
        delay = min(cap, 60 * 2 ** min(attempts - 1, cap.bit_length()))
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
        record_receipt(
            store.client,
            store.database,
            feed=self.name,
            series=spec.key,
            minute=partition.start,
            rows=0,
            sha256='',
            duration_ms=0,
            status='STARTED',
        )
        try:
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
                status='FAILED',
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
            status='OK',
        )
        self._beat()
        return key, None

    def _frontier_gap_key(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime
    ) -> str | None:
        """The current-view frontier gap: the first minute the readers are missing.

        The readers clip at the first gap, so this minute — not the oldest-5
        window — is what unfreezes `*_current`. It is admitted first even past
        the 36h lookback; a past-window gap retries loud on the capped delay
        until it builds or an operator backfills it.
        """
        end = max((record.partition.end for record in store.records()), default=None)
        anchor = store.anchor().replace(second=0, microsecond=0)
        gap = anchor
        if isinstance(end, datetime):
            naive = end.replace(tzinfo=None) if end.tzinfo is not None else end
            # max() over no rows is the epoch, not NULL; the frontier never
            # precedes the anchor.
            gap = max(naive.replace(tzinfo=UTC, second=0, microsecond=0), anchor)
        last = now.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
        if gap > last:
            return None
        return gap.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _build_intervals(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime
    ) -> tuple[list[str], list[str]]:
        adapter = spec.provisional
        if adapter is None:
            raise ValueError('No provisional adapter is declared.')
        covered = store.active_intervals()
        frontier = self._frontier_gap_key(store, spec, now)
        # Acknowledge accepted coverage and reclaim it before repair needs spool space.
        ordered = list(
            BinanceProvisionalBase.candidates(adapter, now, store.anchor(), covered)
            if self.role == 'repair' and isinstance(adapter, BinancePerpProvisional)
            else adapter.candidates(now, store.anchor(), covered)
        )
        if (
            isinstance(adapter, BinancePerpProvisional) and frontier is not None
            and self.role is None
            and not _split_trade_ingestion(spec)
            and not adapter.admits(adapter.partition(frontier))
        ):
            # Spend one bounded bridge allowance on the actual reader gap, even
            # outside the ordinary candidate lookback. Pending work has no receipt.
            adapter.repair_pending(adapter.partition(frontier))
        if frontier is not None and all(partition.key != frontier for partition in ordered):
            gap_start = datetime.strptime(frontier, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC)
            if not any(interval.start <= gap_start < interval.end for interval in covered):
                ordered.insert(1, adapter.partition(frontier))
        enabled = store.enabled_groups()
        if enabled:
            queued = {partition.key for partition in ordered}
            missing = [
                record.partition for record in reversed(store.records())
                if record.partition.provisional and record.partition.key not in queued
                and store.missing_components(record, enabled=enabled)
            ]
            ordered.extend(missing[:PROVISIONAL_MAX_WORKERS])
        admitted = [
            partition
            for partition in ordered
            if (not isinstance(adapter, BinancePerpProvisional) or adapter.admits(partition))
            and (
                self.role != 'repair'
                or classify_spooled_partition(Path(os.environ['ORIGO_PERP_CAPTURE_ROOT']), partition) == 'fallback'
            )
            and self._may_attempt(
                store,
                spec,
                series=spec.key,
                minute=partition.start,
            )
        ]
        if not admitted:
            return [], []

        def build(partition: Partition) -> tuple[str | None, str | None]:
            # Connections are not shared across threads: each unit gets its own.
            client = make_clickhouse_client(get_clickhouse_settings())
            try:
                if isinstance(adapter, BinancePerpProvisional) and _split_trade_ingestion(spec):
                    root = Path(os.environ['ORIGO_PERP_CAPTURE_ROOT'])
                    with (root / 'read.lock').open('a+') as reader:
                        try:
                            fcntl.flock(reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
                        except BlockingIOError:
                            log.info('source=%s minute=%s capture read deferred', spec.key, partition.key)
                            record_receipt(
                                client, store.database, feed=self.name, series=spec.key,
                                minute=partition.start, rows=0, sha256='', duration_ms=0,
                                status='REJECTED', error_code='CAPTURE_BUSY',
                                error='Spool reclamation owns read.lock; ingestion deferred.',
                            )
                            return None, None
                        try:
                            return self._build_one(
                                SourceStore(client, store.database, spec), spec, partition, now
                            )
                        finally:
                            fcntl.flock(reader, fcntl.LOCK_UN)
                return self._build_one(SourceStore(client, store.database, spec), spec, partition, now)
            finally:
                client.disconnect()

        with ThreadPoolExecutor(max_workers=PROVISIONAL_MAX_WORKERS) as pool:
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
            log.info('source=%s a backfill owns publication', spec.key)
            return [], []
        processed: list[str] = []
        failed: list[str] = []
        for consumer in pinned_consumers:
            series = f'{spec.key}:{consumer.key}'
            snapshot = store.snapshot(canonical_only=False)
            if not snapshot.records or publication_current(
                spec, consumer.key, snapshot.token, root=self.publication_root, pinned=True
            ):
                continue
            if not store.canonical_ready():
                log.info('source=%s canonical state not ready for publication', spec.key)
                continue
            try:
                owned = self.dagster.publication_owns_consumer(spec.key, consumer.key)
            except DagsterUnreachable as error:
                log.error('publication state unavailable, trying nonblocking publication: %s', error)
                owned = False
            if owned:
                log.info('source=%s consumer=%s a publication run owns the render', spec.key, consumer.key)
                continue
            if not self._may_attempt(
                store,
                spec,
                series=series,
                token=snapshot.token,
            ):
                continue
            started = time.monotonic()
            record_receipt(
                store.client,
                store.database,
                feed=self.name,
                series=series,
                minute=now.replace(second=0, microsecond=0),
                rows=0,
                sha256=snapshot.token,
                duration_ms=0,
                status='STARTED',
            )
            try:
                execute_source(
                    spec,
                    f'consumer_{consumer.key}',
                    SourceRunConfig(
                        destination=str(self.publication_root / spec.key / consumer.key)
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
                    status='FAILED',
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
                status='OK',
            )
            processed.append(series)
            self._beat()
        return processed, failed

    def _repair(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime,
    ) -> tuple[list[str], list[str]]:
        adapter = spec.provisional
        if not isinstance(adapter, BinancePerpProvisional):
            raise ValueError('Historical trade repair requires the perpetual adapter.')
        root = Path(os.environ['ORIGO_PERP_CAPTURE_ROOT'])
        accepted: Partition | None = None
        for interval in sorted(store.active_intervals(), key=lambda part: part.start):
            if accepted is None:
                accepted = interval
            elif interval.start <= accepted.end:
                accepted = replace(accepted, end=max(accepted.end, interval.end))
            else:
                acknowledge_spooled_revision(root, accepted)
                self._beat()
                accepted = interval
        if accepted is not None:
            acknowledge_spooled_revision(root, accepted)
        cleanup_spool(root)
        self._beat()
        frontier = self._frontier_gap_key(store, spec, now)
        if frontier is None:
            return [], []
        partition = adapter.partition(frontier)
        state = classify_spooled_partition(root, partition)
        token = spooled_repair_identity(root, partition)
        series = f'{spec.key}:repair'
        processed: list[str] = []
        failed: list[str] = []
        if state != 'ready' and self._may_attempt(store, spec, series=series, token=token):
            started = time.monotonic()
            record_receipt(
                store.client, store.database, feed=self.name, series=series,
                minute=now.replace(second=0, microsecond=0), rows=0, sha256=token,
                duration_ms=0, status='STARTED',
            )
            error_code, message = '', ''
            try:
                adapter.repair_pending(partition)
            except Exception as error:
                log.exception('source=%s bridge repair failed', spec.key)
                error_code, message = failure_code(error), str(error)
                failed.append(series)
            record_receipt(
                store.client, store.database, feed=self.name, series=series,
                minute=now.replace(second=0, microsecond=0), rows=0, sha256=token,
                duration_ms=int((time.monotonic() - started) * 1000),
                status='FAILED' if error_code else 'OK', error_code=error_code, error=message,
            )
        built, broken = self._build_intervals(store, spec, now)
        processed.extend(built)
        failed.extend(broken)
        self._beat()
        return processed, failed

    def _import_books(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime, *, tail: bool,
    ) -> tuple[list[str], list[str]]:
        adapter = spec.provisional
        if not isinstance(adapter, RenderBookProvisional):
            raise ValueError('Render import requires a top20 source.')
        cache = adapter.cache

        def read(replica: str) -> tuple[int, int]:
            endpoint = replace(RenderBookEndpoint.configured(replica), deadline=time.monotonic() + 12)
            if tail:
                return cache.sync_tail(replica, endpoint.read)
            return cache.sync(replica, endpoint.read, max_segments=8), 0

        processed: list[str] = []
        failed: list[str] = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {replica: pool.submit(read, replica) for replica in REPLICAS}
            started = time.monotonic()
            for replica, future in futures.items():
                series = f'{spec.key}:import:{replica}'
                count, code, message = 0, '', ''
                try:
                    count, pruned = future.result()
                    previous = cache.cursor(replica)
                    if tail and 0 < previous < pruned:
                        evidence = json.dumps({'operation': 'render_retention_gap', 'replica': replica,
                                               'previous_cursor': previous, 'pruned_through': pruned})
                        store.execute(
                            f'INSERT INTO {store.table("source_observation_log")} VALUES',
                            [(spec.key, f'retention:{replica}:{previous}:{pruned}', evidence, 0, now)],
                        )
                        cache.resume_retained(replica, previous, pruned)
                        code, message = 'RENDER_EXPIRED', 'Expired input was recorded; import resumes at retained history.'
                except Exception as error:
                    code, message = failure_code(error), str(error)
                    if code != 'RENDER_IMPORT_BUDGET':
                        log.exception('source=%s replica=%s Render import failed', spec.key, replica)
                deferred = code == 'RENDER_IMPORT_BUDGET'
                record_receipt(
                    store.client, store.database, feed=self.name, series=series,
                    minute=now.replace(second=0, microsecond=0), rows=count, sha256='',
                    duration_ms=int((time.monotonic() - started) * 1000),
                    status='REJECTED' if deferred else 'FAILED' if code else 'OK',
                    error_code=code, error=message,
                )
                (failed if code and not deferred else processed).append(series)
                self._beat()
        return processed, failed

    def tick(self, now: datetime) -> TickOutcome:
        tick_started = time.monotonic()
        now = now.astimezone(UTC)
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        processed: list[str] = []
        failed: list[str] = []
        try:
            died = reconcile_died_receipts(
                client, settings.database, feed=self.name, now=now,
                source_keys=tuple(spec.key for spec in self.specs),
            )
            if died:
                log.warning('reconciled %d receipts for units the previous process died on', died)
            for spec in self.specs:
                store = SourceStore(client, settings.database, spec)
                started = time.monotonic()
                split = self.role is None and _split_trade_ingestion(spec)
                try:
                    if self.role == 'publication':
                        built, broken = [], []
                        published, unpublished = self._publish(store, spec, now)
                    elif self.role == 'repair':
                        built, broken = self._repair(store, spec, now)
                        published, unpublished = [], []
                    else:
                        if isinstance(spec.provisional, RenderBookProvisional):
                            imported, unavailable = self._import_books(store, spec, now, tail=True)
                            processed.extend(imported)
                            failed.extend(unavailable)
                        built, broken = self._build_intervals(store, spec, now)
                        published, unpublished = ([], []) if split else self._publish(store, spec, now)
                        if isinstance(spec.provisional, RenderBookProvisional):
                            imported, unavailable = self._import_books(store, spec, now, tail=False)
                            processed.extend(imported)
                            failed.extend(unavailable)
                    processed.extend([*built, *published])
                    failed.extend([*broken, *unpublished])
                    if self.role is None:
                        self.reporter.materialized(
                            live_feed_asset(spec),
                            partition=None,
                            metadata={
                                'minute': now.replace(second=0, microsecond=0).isoformat(),
                                'intervals': len(built),
                                'publications': len(published),
                                'failed': len(broken) + len(unpublished),
                                'rss_bytes': _rss_bytes(),
                                'source_timestamp': now.isoformat(),
                            },
                        )
                    if split:
                        record_receipt(
                            client, settings.database, feed=self.name, series=f'{spec.key}:tick',
                            minute=now.replace(second=0, microsecond=0), rows=len(built), sha256='',
                            duration_ms=int((time.monotonic() - tick_started) * 1000),
                            status='FAILED' if broken else 'OK',
                            error_code='INGESTION_UNIT_FAILED' if broken else '',
                            error=', '.join(broken),
                        )
                except Exception as error:
                    series = f'{spec.key}:tick'
                    log.exception('source=%s provisional tick failed', spec.key)
                    record_receipt(
                        client, settings.database, feed=self.name, series=series,
                        minute=now.replace(second=0, microsecond=0), rows=0, sha256='',
                        duration_ms=int((time.monotonic() - (tick_started if split else started)) * 1000),
                        status='FAILED', error_code=failure_code(error), error=str(error),
                    )
                    failed.append(series)
                    self._beat()
        finally:
            client.disconnect()
        return TickOutcome(
            self.name, now.replace(second=0, microsecond=0), tuple(processed), tuple(failed)
        )


def build_feed(environ: dict[str, str], *, heartbeat: Path | None = None) -> ProvisionalFeed:
    specs = selected_specs(environ)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        ensure_monitoring_tables(client, settings.database)
    finally:
        client.disconnect()
    base_url = environ.get('DAGSTER_WEBSERVER_URL', DEFAULT_WEBSERVER_URL)
    return ProvisionalFeed(
        specs,
        publication_root=Path(environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')),
        reporter=Reporter(base_url),
        dagster=DagsterReader(base_url),
        heartbeat=heartbeat,
    )


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
    parser.add_argument('--role', choices=('publication', 'repair'), help='run independent trade maintenance')
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    specs = selected_specs(dict(os.environ))
    role = cast(Literal['publication', 'repair'] | None, arguments.role)
    if role is not None:
        if len(specs) != 1 or specs[0].key not in TRADE_SOURCES:
            parser.error('Maintenance requires one trade source in ORIGO_PROVISIONAL_SOURCE.')
        if role == 'repair' and specs[0].key != 'binance_perp_trades':
            parser.error('Historical repair supports perpetual trades only.')
        if os.environ.get('ORIGO_TRADE_MAINTENANCE', '0') != '0':
            parser.error('Maintenance processes require ORIGO_TRADE_MAINTENANCE=0.')
    role_name = 'trade_publication' if role == 'publication' else 'perp_repair' if role == 'repair' else ProvisionalFeed.name
    name = (
        f'provisional_{specs[0].key}'
        if 'ORIGO_PROVISIONAL_SOURCE' in os.environ else ProvisionalFeed.name
    )
    if role is not None:
        name = f'{role_name}_{specs[0].key}'
    heartbeat = heartbeat_path(heartbeat_directory(), name)
    if arguments.check:
        return check_heartbeat(heartbeat)
    if role == 'repair' and not os.environ.get('ORIGO_PERP_CAPTURE_ROOT'):
        parser.error('Historical repair requires ORIGO_PERP_CAPTURE_ROOT.')
    os.environ[WORKER_HEARTBEAT_ENV] = str(heartbeat)
    feed = build_feed(dict(os.environ), heartbeat=heartbeat)
    feed.role, feed.name = role, role_name
    if arguments.once:
        outcome = feed.tick(datetime.now(UTC))
        print(json.dumps({'minute': outcome.minute.isoformat(), 'processed': list(outcome.processed), 'failed': list(outcome.failed)}))
        return 1 if role is not None and outcome.failed else 0
    run_forever(feed, heartbeat=heartbeat)


if __name__ == '__main__':
    sys.exit(main())
