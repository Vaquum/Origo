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
import json
import logging
import os
import resource
import sys
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
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

DEFAULT_WEBSERVER_URL = 'http://dagit:3000'
log = logging.getLogger('origo.workers.provisional')


def live_feed_asset(spec: RevisionedSourceSpec) -> str:
    return f'{spec.key}_provisional_feed'


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
        ordered = list(adapter.candidates(now, store.anchor(), covered))
        frontier = self._frontier_gap_key(store, spec, now)
        if frontier is not None and all(partition.key != frontier for partition in ordered):
            gap_start = datetime.strptime(frontier, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC)
            if not any(interval.start <= gap_start < interval.end for interval in covered):
                ordered.insert(1, adapter.partition(frontier))
        admitted = [
            partition
            for partition in ordered
            if self._may_attempt(
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
                return self._build_one(SourceStore(client, store.database, spec), spec, partition, now)
            finally:
                client.disconnect()

        with ThreadPoolExecutor(max_workers=4) as pool:
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

    def tick(self, now: datetime) -> TickOutcome:
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
                try:
                    built, broken = self._build_intervals(store, spec, now)
                    published, unpublished = self._publish(store, spec, now)
                    processed.extend([*built, *published])
                    failed.extend([*broken, *unpublished])
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
                except Exception as error:
                    series = f'{spec.key}:tick'
                    log.exception('source=%s provisional tick failed', spec.key)
                    record_receipt(
                        client, settings.database, feed=self.name, series=series,
                        minute=now.replace(second=0, microsecond=0), rows=0, sha256='',
                        duration_ms=int((time.monotonic() - started) * 1000),
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
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    specs = selected_specs(dict(os.environ))
    name = (
        f'provisional_{specs[0].key}'
        if 'ORIGO_PROVISIONAL_SOURCE' in os.environ else ProvisionalFeed.name
    )
    heartbeat = heartbeat_path(heartbeat_directory(), name)
    if arguments.check:
        return check_heartbeat(heartbeat)
    os.environ[WORKER_HEARTBEAT_ENV] = str(heartbeat)
    feed = build_feed(dict(os.environ), heartbeat=heartbeat)
    if arguments.once:
        outcome = feed.tick(datetime.now(UTC))
        print(json.dumps({'minute': outcome.minute.isoformat(), 'processed': list(outcome.processed), 'failed': list(outcome.failed)}))
        return 0
    run_forever(feed, heartbeat=heartbeat)


if __name__ == '__main__':
    sys.exit(main())
