"""The provisional worker: the live minute path of every revisioned source with a
provisional adapter.

One tick builds each eligible closed interval once through the source runtime (the
adapter's candidates: the last completed minute plus the missing minutes inside its
window, oldest first), then publishes every consumer that pins provisional rows when the
pinned state changed, unless a native backfill of the source owns publication. Each
interval and each publication writes one receipt; every tick reports the source's live
feed asset so its freshness check sees the worker. A failing interval or publication is
retried with a doubling delay from one minute up to the source's ``retry_delay``, at most
``retry_count`` times, then left to the operator.
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
from datetime import UTC, datetime, timedelta
from pathlib import Path

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.bundle import SourceRunConfig, execute_source
from origo.sources.contracts import Partition, RevisionedSourceSpec, RolloutStage, failure_code
from origo.sources.publication import publication_current
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore

from .dagster_reader import DagsterReader, DagsterUnreachable
from .receipts import ensure_monitoring_tables, failed_attempts, record_receipt
from .report import Reporter
from .runtime import (
    TickOutcome,
    check_heartbeat,
    heartbeat_directory,
    heartbeat_path,
    run_forever,
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
    ) -> bool:
        """Whether the work's failures allow another attempt now: none so far, or at most
        ``retry_count`` retries with the doubling delay since the last failure elapsed."""
        attempts, last_failed = failed_attempts(
            store.client, store.database, feed=self.name, series=series, minute=minute, token=token
        )
        if attempts == 0 or last_failed is None:
            return True
        if attempts > spec.orchestration.retry_count:
            log.error(
                'source=%s %s attempts exhausted after %d failures; an operator run is required',
                spec.key,
                work,
                attempts,
            )
            return False
        delay = min(spec.orchestration.retry_delay, 60 * 2 ** (attempts - 1))
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

    def _build_intervals(
        self, store: SourceStore, spec: RevisionedSourceSpec, now: datetime
    ) -> tuple[list[str], list[str]]:
        adapter = spec.provisional
        if adapter is None:
            raise ValueError('No provisional adapter is declared.')
        candidates = sorted(
            adapter.candidates(now, store.anchor(), store.active_intervals()),
            key=lambda partition: partition.start,
        )
        processed: list[str] = []
        failed: list[str] = []
        for partition in candidates:
            key = f'{spec.key}:{partition.key}'
            if not self._may_attempt(
                store, spec, series=spec.key, work=f'partition={partition.key}', minute=partition.start
            ):
                continue
            started = time.monotonic()
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
                failed.append(key)
                continue
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
            processed.append(key)
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
            log.info('source=%s a native backfill owns publication', spec.key)
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
            if not self._may_attempt(
                store,
                spec,
                series=series,
                work=f'consumer={consumer.key} state={snapshot.token[:12]}',
                token=snapshot.token,
            ):
                continue
            started = time.monotonic()
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
        return processed, failed

    def tick(self, now: datetime) -> TickOutcome:
        now = now.astimezone(UTC)
        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        processed: list[str] = []
        failed: list[str] = []
        try:
            for spec in self.specs:
                store = SourceStore(client, settings.database, spec)
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
        finally:
            client.disconnect()
        return TickOutcome(
            self.name, now.replace(second=0, microsecond=0), tuple(processed), tuple(failed)
        )


def build_feed(environ: dict[str, str]) -> ProvisionalFeed:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        ensure_monitoring_tables(client, settings.database)
    finally:
        client.disconnect()
    base_url = environ.get('DAGSTER_WEBSERVER_URL', DEFAULT_WEBSERVER_URL)
    return ProvisionalFeed(
        SOURCE_REGISTRY,
        publication_root=Path(environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')),
        reporter=Reporter(base_url),
        dagster=DagsterReader(base_url),
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
    heartbeat = heartbeat_path(heartbeat_directory(), ProvisionalFeed.name)
    if arguments.check:
        return check_heartbeat(heartbeat)
    feed = build_feed(dict(os.environ))
    if arguments.once:
        outcome = feed.tick(datetime.now(UTC))
        print(json.dumps({'minute': outcome.minute.isoformat(), 'processed': list(outcome.processed), 'failed': list(outcome.failed)}))
        return 0
    run_forever(feed, heartbeat=heartbeat)


if __name__ == '__main__':
    sys.exit(main())
