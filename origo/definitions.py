# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

from collections.abc import Iterator
from datetime import date
from typing import Protocol

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    Failure,
    FreshnessPolicy,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    SkipReason,
    asset_sensor,
    define_asset_job,
    in_process_executor,
    multi_asset_check,
)

from .orchestration.recovery import recover_orchestration_job

from .assets.publish_btc_briefing_feed import publish_btc_briefing_feed
from .assets.publish_btc_briefing_history import publish_btc_briefing_history
from .assets.create_origo_database import (
    create_origo_database,
    get_clickhouse_settings as get_origo_clickhouse_settings,
    make_clickhouse_client as make_origo_clickhouse_client,
)
from .assets.create_binance_spot_depth20_1m_table_origo import (
    create_binance_spot_depth20_1m_table_origo,
)
from .assets.create_binance_spot_depth20_snapshots_table_origo import (
    create_binance_spot_depth20_snapshots_table_origo,
)
from .assets.create_binance_spot_depth200_1m_table_origo import (
    create_binance_spot_depth200_1m_table_origo,
)
from .assets.create_binance_spot_depth200_snapshots_table_origo import (
    create_binance_spot_depth200_snapshots_table_origo,
)
from .assets.refresh_binance_spot_depth20_1m_origo import (
    refresh_binance_spot_depth20_1m_origo,
    repair_binance_spot_depth20_1m_history_origo,
)
from .assets.refresh_binance_spot_depth200_1m_origo import (
    refresh_binance_spot_depth200_1m_origo,
)
from .assets.sync_binance_spot_depth20_snapshots_to_origo import (
    sync_binance_spot_depth20_snapshots_to_origo,
)
from .assets.reconcile_binance_spot_depth20_partition_state_origo import (
    reconcile_binance_spot_depth20_partition_state_origo,
)
from .assets.sync_binance_spot_depth200_snapshots_to_origo import (
    sync_binance_spot_depth200_snapshots_to_origo,
)
from .assets.reconcile_binance_spot_depth200_partition_state_origo import (
    reconcile_binance_spot_depth200_partition_state_origo,
)
from .assets.build_depth_snapshot_store_arrow import build_depth_snapshot_store_arrow
from .workers.depth import LIVE_FEED_ASSET as DEPTH_LIVE_FEED_ASSET
from .workers.market_state_api import ASSET as MARKET_STATE_QUERY_ASSET
from .workers.runtime import LIVE_FEED_FRESHNESS_WINDOW


class _DagsterEventLike(Protocol):
    partition: str | None


class _AssetEventLike(Protocol):
    dagster_event: _DagsterEventLike | None
    run_id: str


# Database Maintenance Jobs

create_origo_database_job = define_asset_job(
    name="create_origo_database_job",
    selection=["create_origo_database"]
)


create_binance_spot_depth20_snapshots_table_origo_job = define_asset_job(
    name="create_binance_spot_depth20_snapshots_table_origo_job",
    selection=["create_binance_spot_depth20_snapshots_table_origo"]
)

create_binance_spot_depth20_1m_table_origo_job = define_asset_job(
    name="create_binance_spot_depth20_1m_table_origo_job",
    selection=["create_binance_spot_depth20_1m_table_origo"]
)

create_binance_spot_depth200_snapshots_table_origo_job = define_asset_job(
    name='create_binance_spot_depth200_snapshots_table_origo_job',
    selection=['create_binance_spot_depth200_snapshots_table_origo'],
)

create_binance_spot_depth200_1m_table_origo_job = define_asset_job(
    name='create_binance_spot_depth200_1m_table_origo_job',
    selection=['create_binance_spot_depth200_1m_table_origo'],
)


# Data Insertion Jobs


_BINANCE_SPOT_DEPTH20_DATA_SOURCE_SELECTION = [
    'sync_binance_spot_depth20_snapshots_to_origo',
    'refresh_binance_spot_depth20_1m_origo',
]

_BINANCE_SPOT_DEPTH200_DATA_SOURCE_SELECTION = [
    'sync_binance_spot_depth200_snapshots_to_origo',
    'refresh_binance_spot_depth200_1m_origo',
]

refresh_binance_spot_depth20_data_source_job = define_asset_job(
    name='refresh_binance_spot_depth20_data_source_job',
    selection=_BINANCE_SPOT_DEPTH20_DATA_SOURCE_SELECTION,
)

refresh_binance_spot_depth200_data_source_job = define_asset_job(
    name='refresh_binance_spot_depth200_data_source_job',
    selection=_BINANCE_SPOT_DEPTH200_DATA_SOURCE_SELECTION,
)


backfill_binance_spot_depth20_data_source_job = define_asset_job(
    name='backfill_binance_spot_depth20_data_source_job',
    selection=_BINANCE_SPOT_DEPTH20_DATA_SOURCE_SELECTION,
)

backfill_binance_spot_depth200_data_source_job = define_asset_job(
    name='backfill_binance_spot_depth200_data_source_job',
    selection=_BINANCE_SPOT_DEPTH200_DATA_SOURCE_SELECTION,
)

repair_binance_spot_depth20_projection_job = define_asset_job(
    name='repair_binance_spot_depth20_projection_job',
    selection=['refresh_binance_spot_depth20_1m_origo'],
)

repair_binance_spot_depth200_projection_job = define_asset_job(
    name='repair_binance_spot_depth200_projection_job',
    selection=['refresh_binance_spot_depth200_1m_origo'],
)

repair_binance_spot_depth20_1m_history_job = define_asset_job(
    name='repair_binance_spot_depth20_1m_history_job',
    selection=['repair_binance_spot_depth20_1m_history_origo'],
)

reconcile_binance_spot_depth20_partition_state_origo_job = define_asset_job(
    name='reconcile_binance_spot_depth20_partition_state_origo_job',
    selection=['reconcile_binance_spot_depth20_partition_state_origo'],
)

reconcile_binance_spot_depth200_partition_state_origo_job = define_asset_job(
    name='reconcile_binance_spot_depth200_partition_state_origo_job',
    selection=['reconcile_binance_spot_depth200_partition_state_origo'],
)

publish_btc_briefing_feed_job = define_asset_job(
    name="publish_btc_briefing_feed_job",
    selection=["publish_btc_briefing_feed"])

publish_btc_briefing_history_job = define_asset_job(
    name="publish_btc_briefing_history_job",
    selection=["publish_btc_briefing_history"])

build_depth_snapshot_store_arrow_job = define_asset_job(
    name='build_depth_snapshot_store_arrow_job',
    selection=[build_depth_snapshot_store_arrow],
    executor_def=in_process_executor,
)

# The first day of binance_spot_depth20_1m in production. A canonical spot day before it
# (a historical relaunch) has no order-book rows, so a briefing run for it can only fail.
BRIEFING_FIRST_DAY = date(2026, 5, 14)


def _partitioned_run_request(
    asset_event: _AssetEventLike,
    *,
    run_key_prefix: str,
) -> RunRequest | SkipReason:
    if not asset_event.dagster_event:
        return SkipReason("No Dagster event was attached to the materialization.")

    partition_key = asset_event.dagster_event.partition
    if partition_key is None:
        return SkipReason("The materialization did not include a partition key.")
    if date.fromisoformat(partition_key) < BRIEFING_FIRST_DAY:
        return SkipReason(
            f"{partition_key} precedes the book projection's first day {BRIEFING_FIRST_DAY}."
        )

    return RunRequest(
        partition_key=partition_key,
        run_key=f"{run_key_prefix}::{partition_key}::{asset_event.run_id}",
    )


@asset_sensor(
    asset_key=AssetKey("build_binance_spot_trades_canonical_revision_origo"),
    job=publish_btc_briefing_feed_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def publish_btc_briefing_feed_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _partitioned_run_request(
        asset_event,
        run_key_prefix="publish_btc_briefing_feed",
    )


def _publish_btc_briefing_history_run_request(
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    """Republish the rolling history for the day whose briefing feed just landed.

    The history covers the span before its partition day and the feed file for
    that day continues it, so the two are only consistent when the history is
    rebuilt off the feed's own materialization rather than off a second clock.
    """
    if not asset_event.dagster_event:
        return SkipReason(
            "No Dagster event was attached to the BTC briefing feed materialization."
        )

    partition_key = asset_event.dagster_event.partition
    if partition_key is None:
        return SkipReason("BTC briefing feed materialization did not include a partition key.")

    return RunRequest(
        partition_key=partition_key,
        run_key=f"publish_btc_briefing_history::{partition_key}::{asset_event.run_id}",
    )


@asset_sensor(
    asset_key=AssetKey("publish_btc_briefing_feed"),
    job=publish_btc_briefing_history_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def publish_btc_briefing_history_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_btc_briefing_history_run_request(asset_event)


# The monitor worker (origo.workers.monitor) evaluates these checks every minute from
# outside Dagster and reports them through the webserver; Dagit shows them on this asset.
MONITOR_CHECK_NAMES = (
    'collectors_serving',
    'dagster_reachable',
    'data_current',
    'no_error_logs',
    'publication_current',
    'queue_bounded',
    'workers_alive',
)
origo_monitor = AssetsDefinition(
    specs=[
        AssetSpec(
            'origo_monitor',
            group_name='monitoring',
            description='The monitor worker: seven checks evaluated every minute outside Dagster.',
        )
    ]
)


@multi_asset_check(
    specs=[AssetCheckSpec(name=name, asset='origo_monitor') for name in MONITOR_CHECK_NAMES]
)
def origo_monitor_checks() -> Iterator[AssetCheckResult]:
    raise Failure(
        'origo_monitor checks are evaluated by the monitor worker, not inside Dagster.'
    )
    yield AssetCheckResult(passed=False)  # pragma: no cover - unreachable, typing only


# The live feeds: external assets the workers materialize every tick. Their freshness
# policy is the feed's liveness in Dagit, evaluated by the daemon without a run; the
# monitor's heartbeat check is what alerts on a stalled worker.
binance_spot_depth_live_feed = AssetsDefinition(
    specs=[
        AssetSpec(
            DEPTH_LIVE_FEED_ASSET,
            group_name='live_feeds',
            description='The depth worker materializes this every tick for depth20 and depth200.',
            freshness_policy=FreshnessPolicy.time_window(fail_window=LIVE_FEED_FRESHNESS_WINDOW),
        )
    ]
)
market_state_query_service = AssetsDefinition(
    specs=[
        AssetSpec(
            MARKET_STATE_QUERY_ASSET,
            group_name='live_feeds',
            description='The market state query service materializes this every cleanup tick.',
            freshness_policy=FreshnessPolicy.time_window(fail_window=LIVE_FEED_FRESHNESS_WINDOW),
        )
    ]
)


defs = Definitions(
    assets=[origo_monitor,
            binance_spot_depth_live_feed,
            market_state_query_service,
            create_origo_database,
            create_binance_spot_depth20_snapshots_table_origo,
            create_binance_spot_depth20_1m_table_origo,
            create_binance_spot_depth200_snapshots_table_origo,
            create_binance_spot_depth200_1m_table_origo,
            sync_binance_spot_depth20_snapshots_to_origo,
            refresh_binance_spot_depth20_1m_origo,
            repair_binance_spot_depth20_1m_history_origo,
            reconcile_binance_spot_depth20_partition_state_origo,
            sync_binance_spot_depth200_snapshots_to_origo,
            refresh_binance_spot_depth200_1m_origo,
            reconcile_binance_spot_depth200_partition_state_origo,
            publish_btc_briefing_feed,
            publish_btc_briefing_history,
            build_depth_snapshot_store_arrow],

    schedules=[
    ],

    asset_checks=[origo_monitor_checks],

    sensors=[
        publish_btc_briefing_feed_sensor,
        publish_btc_briefing_history_sensor,
    ],

    jobs=[create_origo_database_job,
          create_binance_spot_depth20_snapshots_table_origo_job,
          create_binance_spot_depth20_1m_table_origo_job,
          create_binance_spot_depth200_snapshots_table_origo_job,
          create_binance_spot_depth200_1m_table_origo_job,
          refresh_binance_spot_depth20_data_source_job,
          refresh_binance_spot_depth200_data_source_job,
          backfill_binance_spot_depth20_data_source_job,
          backfill_binance_spot_depth200_data_source_job,
          repair_binance_spot_depth20_projection_job,
          repair_binance_spot_depth200_projection_job,
          repair_binance_spot_depth20_1m_history_job,
          reconcile_binance_spot_depth20_partition_state_origo_job,
          reconcile_binance_spot_depth200_partition_state_origo_job,
          publish_btc_briefing_feed_job,
          publish_btc_briefing_history_job,
          build_depth_snapshot_store_arrow_job])

# TODO: Put everything in to same order in all segments of the code

from .maintenance.dagster_metadata import (
    maintain_operational_metadata,
    maintain_operational_metadata_job,
    operational_metadata_maintenance_schedule,
)

from .sources.bootstrap import prepare_revisioned_sources_job
from .sources.bundle import build_source_bundle
from .sources.registry import SOURCE_REGISTRY

_registered_source_bundles = tuple(
    build_source_bundle(spec) for spec in SOURCE_REGISTRY
)

defs = Definitions.merge(
    defs,
    Definitions(
        assets=[maintain_operational_metadata],
        jobs=[maintain_operational_metadata_job, prepare_revisioned_sources_job, recover_orchestration_job],
        schedules=[operational_metadata_maintenance_schedule],
    ),
    *(
        Definitions(
            assets=bundle.assets,
            jobs=bundle.jobs,
            schedules=bundle.schedules,
            sensors=bundle.sensors,
        )
        for bundle in _registered_source_bundles
    ),
)
