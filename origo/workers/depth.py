"""The depth worker: the live minute path of the Binance spot depth20 and depth200 feeds.

One tick covers the last completed UTC minute plus every missing minute inside the
lookback window, oldest first: raw snapshots from the collector's history endpoint, the
1m projection and the Arrow chunk, each only when absent. Every processed minute writes
one receipt and reports a partition materialization to Dagster; every tick reports the
live feed asset so its freshness check sees the worker. The per-minute Dagster jobs stay
for operator use; nothing schedules them.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import requests
from dagster import TimeWindowPartitionsDefinition

from origo.assets.build_depth_snapshot_store_arrow import (
    LATEST_MANIFEST_NAME,
    build_depth_snapshot_frame,
    depth_snapshot_chunk_relative_path,
    minute_start_from_partition_key,
    publish_depth_snapshot_chunk,
    spec_for_depth_snapshot_series,
)
from origo.assets.create_binance_spot_depth20_1m_table_origo import DEPTH20_1M_TABLE_NAME
from origo.assets.create_binance_spot_depth20_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH20_SNAPSHOTS_TABLE_NAME,
)
from origo.assets.create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseClient as AssetClient,
)
from origo.assets.create_binance_spot_depth20_snapshots_table_origo import (
    clickhouse_scalar_int,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from origo.assets.create_binance_spot_depth200_1m_table_origo import DEPTH200_1M_TABLE_NAME
from origo.assets.create_binance_spot_depth200_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH200_SNAPSHOTS_TABLE_NAME,
)
from origo.assets.refresh_binance_spot_depth20_1m_origo import refresh_minute as refresh_depth20
from origo.assets.refresh_binance_spot_depth200_1m_origo import (
    refresh_minute as refresh_depth200,
)
from origo.assets.sync_binance_spot_depth20_snapshots_to_origo import (
    depth20_minute_partitions,
)
from origo.assets.sync_binance_spot_depth20_snapshots_to_origo import (
    sync_minute as sync_depth20,
)
from origo.assets.sync_binance_spot_depth200_snapshots_to_origo import (
    depth200_minute_partitions,
)
from origo.assets.sync_binance_spot_depth200_snapshots_to_origo import (
    sync_minute as sync_depth200,
)
from origo.sources.contracts import Client
from origo.utils.arrow_store import series_store_dir

from .receipts import ensure_monitoring_tables, reconcile_died_receipts, record_receipt
from .report import Reporter
from .runtime import TickOutcome, check_heartbeat, heartbeat_directory, heartbeat_path, run_forever

DEPTH_SOURCE_LOOKBACK_MINUTES = 15
LIVE_FEED_ASSET = 'binance_spot_depth_live_feed'
DEFAULT_WEBSERVER_URL = 'http://dagit:3000'
log = logging.getLogger('origo.workers.depth')


@dataclass(frozen=True)
class DepthLiveStoreStatus:
    snapshot_rows: int
    projection_rows: int
    arrow_chunk_exists: bool
    latest_manifest_minute: datetime | None


@dataclass(frozen=True)
class DepthLiveReconciliationSpec:
    label: str
    partitions: TimeWindowPartitionsDefinition
    run_key_prefix: str
    snapshot_table_name: str
    projection_table_name: str
    series: str
    base_url_env: str
    auth_token_env: str
    sync_asset: str
    projection_asset: str


DEPTH20_LIVE_RECONCILIATION_SPEC = DepthLiveReconciliationSpec(
    label='Depth20',
    partitions=depth20_minute_partitions,
    run_key_prefix='binance_spot_depth20',
    snapshot_table_name=DEPTH20_SNAPSHOTS_TABLE_NAME,
    projection_table_name=DEPTH20_1M_TABLE_NAME,
    series='depth20_snapshots',
    base_url_env='BINANCE_SPOT_DEPTH20_BASE_URL',
    auth_token_env='BINANCE_SPOT_DEPTH20_AUTH_TOKEN',
    sync_asset='sync_binance_spot_depth20_snapshots_to_origo',
    projection_asset='refresh_binance_spot_depth20_1m_origo',
)
DEPTH200_LIVE_RECONCILIATION_SPEC = DepthLiveReconciliationSpec(
    label='Depth200',
    partitions=depth200_minute_partitions,
    run_key_prefix='binance_spot_depth200',
    snapshot_table_name=DEPTH200_SNAPSHOTS_TABLE_NAME,
    projection_table_name=DEPTH200_1M_TABLE_NAME,
    series='depth200_snapshots',
    base_url_env='BINANCE_SPOT_DEPTH200_BASE_URL',
    auth_token_env='BINANCE_SPOT_DEPTH200_AUTH_TOKEN',
    sync_asset='sync_binance_spot_depth200_snapshots_to_origo',
    projection_asset='refresh_binance_spot_depth200_1m_origo',
)
DEPTH_SPECS = (DEPTH20_LIVE_RECONCILIATION_SPEC, DEPTH200_LIVE_RECONCILIATION_SPEC)


def _required_env_value(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'{name} environment variable must be set.')
    return value


def _clickhouse_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')


def _clickhouse_datetime64(value: datetime) -> str:
    return value.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.000')


def last_completed_minute(now: datetime) -> datetime:
    reference = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    return reference.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)


def candidate_minutes(
    spec: DepthLiveReconciliationSpec, now: datetime, lookback_minutes: int
) -> tuple[tuple[datetime, str], ...]:
    """The minutes one tick covers, oldest first: the lookback window up to the last
    completed minute, restricted to the series' partition calendar."""
    if lookback_minutes < 1:
        raise RuntimeError('The depth lookback must be at least one minute.')
    last_minute = last_completed_minute(now)
    candidates: list[tuple[datetime, str]] = []
    for offset in reversed(range(lookback_minutes)):
        minute_start = last_minute - timedelta(minutes=offset)
        partition_key = spec.partitions.get_partition_key_for_timestamp(minute_start.timestamp())
        if spec.partitions.has_partition_key(partition_key):
            candidates.append((minute_start, partition_key))
    return tuple(candidates)


def source_history_url(base_url: str, minute_start: datetime) -> str:
    unix_seconds = int(minute_start.timestamp())
    return f'{base_url.rstrip("/")}/history?from={unix_seconds}&to={unix_seconds}'


def source_has_rows(spec: DepthLiveReconciliationSpec, minute_start: datetime) -> bool:
    """Whether the collector serves any row for the minute; a request failure is ``False``,
    which the monitor's collector probe reports separately from worker silence."""
    try:
        response = requests.get(
            source_history_url(_required_env_value(spec.base_url_env), minute_start),
            headers={
                'Accept': 'application/x-ndjson',
                'Authorization': f'Bearer {_required_env_value(spec.auth_token_env)}',
            },
            timeout=30,
            stream=True,
        )
        try:
            response.raise_for_status()
            return any(line for line in response.iter_lines())
        finally:
            response.close()
    except requests.RequestException:
        return False


def snapshot_rows(
    client: Client,
    database: str,
    spec: DepthLiveReconciliationSpec,
    minute_start: datetime,
) -> int:
    minute_end = minute_start + timedelta(minutes=1)
    return clickhouse_scalar_int(
        client.execute(
            f"""
        SELECT count()
        FROM {database}.{spec.snapshot_table_name} FINAL
        WHERE datetime >= toDateTime64('{_clickhouse_datetime64(minute_start)}', 3)
          AND datetime < toDateTime64('{_clickhouse_datetime64(minute_end)}', 3)
        """
        )
    )


def projection_rows(
    client: Client,
    database: str,
    spec: DepthLiveReconciliationSpec,
    minute_start: datetime,
) -> int:
    return clickhouse_scalar_int(
        client.execute(
            f"""
        SELECT count()
        FROM {database}.{spec.projection_table_name} FINAL
        WHERE datetime = toDateTime('{_clickhouse_datetime(minute_start)}')
        """
        )
    )


def arrow_chunk_exists(spec: DepthLiveReconciliationSpec, minute_start: datetime) -> bool:
    return (series_store_dir(spec.series) / depth_snapshot_chunk_relative_path(minute_start)).is_file()


def latest_manifest_minute(spec: DepthLiveReconciliationSpec) -> datetime | None:
    manifest_path = series_store_dir(spec.series) / LATEST_MANIFEST_NAME
    if not manifest_path.exists():
        return None
    manifest: object = json.loads(manifest_path.read_text(encoding='utf-8'))
    if not isinstance(manifest, dict):
        raise RuntimeError(f'{manifest_path} is not a manifest object.')
    source_partition_key = cast(dict[str, object], manifest).get('source_partition_key')
    if not isinstance(source_partition_key, str):
        raise RuntimeError(f'{manifest_path} does not contain source_partition_key.')
    return minute_start_from_partition_key(source_partition_key)


def store_status(
    client: Client,
    database: str,
    spec: DepthLiveReconciliationSpec,
    minute_start: datetime,
) -> DepthLiveStoreStatus:
    return DepthLiveStoreStatus(
        snapshot_rows=snapshot_rows(client, database, spec, minute_start),
        projection_rows=projection_rows(client, database, spec, minute_start),
        arrow_chunk_exists=arrow_chunk_exists(spec, minute_start),
        latest_manifest_minute=latest_manifest_minute(spec),
    )


def arrow_is_complete(status: DepthLiveStoreStatus, minute_start: datetime) -> bool:
    return (
        status.arrow_chunk_exists
        and status.latest_manifest_minute is not None
        and status.latest_manifest_minute >= minute_start
    )


def _sync(spec: DepthLiveReconciliationSpec, client: Client, database: str, minute: datetime) -> int:
    sync = sync_depth20 if spec.series == 'depth20_snapshots' else sync_depth200
    return sync(
        _asset_client(client),
        database,
        minute,
        base_url=_required_env_value(spec.base_url_env),
        auth_token=_required_env_value(spec.auth_token_env),
    )


def _refresh(spec: DepthLiveReconciliationSpec, client: Client, database: str, minute: datetime) -> int:
    refresh = refresh_depth20 if spec.series == 'depth20_snapshots' else refresh_depth200
    return refresh(_asset_client(client), database, minute)


def _asset_client(client: Client) -> AssetClient:
    """The worker's client under the depth assets' protocol. Both protocols describe the one
    clickhouse_driver client; only their ``execute`` annotations differ."""
    return cast(AssetClient, client)


def _rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return usage if sys.platform == 'darwin' else usage * 1024


class DepthFeed:
    name = 'depth'
    lookback_minutes = DEPTH_SOURCE_LOOKBACK_MINUTES

    def __init__(
        self,
        client: Client,
        database: str,
        specs: Sequence[DepthLiveReconciliationSpec],
        reporter: Reporter,
    ) -> None:
        self.client = client
        self.database = database
        self.specs = tuple(specs)
        self.reporter = reporter

    def process_minute(
        self, spec: DepthLiveReconciliationSpec, minute_start: datetime, partition_key: str
    ) -> dict[str, object] | None:
        """Bring one minute to completion: raw rows, projection row, Arrow chunk. Returns the
        receipt metadata, or ``None`` when the minute was complete or the collector has no
        rows for it yet."""
        status = store_status(self.client, self.database, spec, minute_start)
        if (
            status.snapshot_rows > 0
            and status.projection_rows > 0
            and arrow_is_complete(status, minute_start)
        ):
            return None
        if status.snapshot_rows == 0 and not source_has_rows(spec, minute_start):
            return None
        record_receipt(
            self.client,
            self.database,
            feed=self.name,
            series=spec.series,
            minute=minute_start,
            rows=0,
            sha256='',
            duration_ms=0,
            status='STARTED',
        )
        synced = 0
        if status.snapshot_rows == 0:
            synced = _sync(spec, self.client, self.database, minute_start)
            self.reporter.materialized(
                spec.sync_asset, partition=partition_key, metadata={'rows_inserted': synced}
            )
        projected = 0
        if status.projection_rows == 0:
            projected = _refresh(spec, self.client, self.database, minute_start)
            self.reporter.materialized(
                spec.projection_asset, partition=partition_key, metadata={'rows_inserted': projected}
            )
        version = ''
        if not arrow_is_complete(status, minute_start):
            build = build_depth_snapshot_frame(
                _asset_client(self.client), self.database, spec_for_depth_snapshot_series(spec.series), minute_start
            )
            outcome = publish_depth_snapshot_chunk(spec.series, partition_key, build)
            version = outcome.version or ''
        return {'rows': synced or status.snapshot_rows, 'projected': projected, 'sha256': version}

    def tick(self, now: datetime) -> TickOutcome:
        now = now.astimezone(UTC)
        processed: list[str] = []
        failed: list[str] = []
        died = reconcile_died_receipts(self.client, self.database, feed=self.name, now=now)
        if died:
            log.warning('reconciled %d receipts for units the previous process died on', died)
        for spec in self.specs:
            for minute_start, partition_key in candidate_minutes(spec, now, self.lookback_minutes):
                started = time.monotonic()
                key = f'{spec.series}:{partition_key}'
                try:
                    result = self.process_minute(spec, minute_start, partition_key)
                except Exception as error:
                    # The minute stays a candidate for the rest of the lookback; the receipt
                    # and the ERROR line make the failure visible now.
                    log.exception('%s %s failed', spec.series, partition_key)
                    record_receipt(
                        self.client,
                        self.database,
                        feed=self.name,
                        series=spec.series,
                        minute=minute_start,
                        rows=0,
                        sha256='',
                        duration_ms=int((time.monotonic() - started) * 1000),
                        status='FAILED',
                        error_code=type(error).__name__,
                        error=str(error),
                    )
                    failed.append(key)
                    continue
                if result is None:
                    continue
                record_receipt(
                    self.client,
                    self.database,
                    feed=self.name,
                    series=spec.series,
                    minute=minute_start,
                    rows=int(str(result['rows'])),
                    sha256=str(result['sha256']),
                    duration_ms=int((time.monotonic() - started) * 1000),
                    status='OK',
                )
                processed.append(key)
        minute = last_completed_minute(now)
        self.reporter.materialized(
            LIVE_FEED_ASSET,
            partition=None,
            metadata={
                'minute': minute.isoformat(),
                'processed': len(processed),
                'failed': len(failed),
                'rss_bytes': _rss_bytes(),
                'source_timestamp': now.isoformat(),
            },
        )
        return TickOutcome(self.name, minute, tuple(processed), tuple(failed))


def build_feed(environ: dict[str, str]) -> DepthFeed:
    settings = get_clickhouse_settings()
    client = cast(Client, make_clickhouse_client(settings))
    ensure_monitoring_tables(client, settings.database)
    return DepthFeed(
        client,
        settings.database,
        DEPTH_SPECS,
        Reporter(environ.get('DAGSTER_WEBSERVER_URL', DEFAULT_WEBSERVER_URL)),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m origo.workers.depth',
        description='Ingest, project and publish the Binance spot depth feeds minute by minute.',
    )
    parser.add_argument(
        '--check', action='store_true', help='healthcheck: exit 0 when the heartbeat is fresh'
    )
    parser.add_argument('--once', action='store_true', help='run one tick and exit')
    arguments = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    heartbeat = heartbeat_path(heartbeat_directory(), DepthFeed.name)
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
