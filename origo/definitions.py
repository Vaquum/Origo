# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

import os
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Protocol

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    Failure,
    FreshnessPolicy,
    DagsterInstance,
    DagsterRun,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    RunsFilter,
    ScheduleEvaluationContext,
    SkipReason,
    asset_sensor,
    build_schedule_from_partitioned_job,
    define_asset_job,
    in_process_executor,
    multi_asset_check,
    schedule,
)

from .orchestration.recovery import recover_orchestration_job

from .assets.publish_btc_briefing_feed import publish_btc_briefing_feed
from .assets.publish_btc_briefing_history import publish_btc_briefing_history
from .assets.create_origo_database import (
    create_origo_database,
    get_clickhouse_settings as get_origo_clickhouse_settings,
    make_clickhouse_client as make_origo_clickhouse_client,
)
from .assets.create_binance_futures_trades_table_origo import (
    LEDGER_TABLE_NAME as FUTURES_DAILY_LEDGER_TABLE_NAME,
)
from .utils.daily_gap_repair import (
    REPAIR_TERMINATION_GRACE_SECONDS,
    DailyGapRepairSpec,
    gap_repair_run_requests,
)
from .assets.create_binance_futures_trades_table_origo import (
    create_binance_daily_futures_trades_table_origo,
)
from .assets.create_binance_futures_klines_table_origo import (
    create_binance_futures_klines_table_origo,
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
from .assets.refresh_binance_futures_klines_origo import refresh_binance_futures_klines_origo
from .assets.refresh_binance_spot_depth20_1m_origo import (
    refresh_binance_spot_depth20_1m_origo,
)
from .assets.refresh_binance_spot_depth200_1m_origo import (
    refresh_binance_spot_depth200_1m_origo,
)
from .assets.create_aligned_1m_exchange_table_origo import (
    create_aligned_1m_exchange_table_origo,
)
from .assets.daily_futures_trades_to_origo import (
    DEFAULT_BINANCE_FUTURES_DAILY_TRADES_BASE_URL,
    daily_partitions as futures_daily_partitions,
    insert_daily_binance_futures_trades_to_origo,
)
from .assets.refresh_aligned_1m_exchange_from_binance_futures_origo import (
    refresh_aligned_1m_exchange_from_binance_futures_origo,
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
from .workers.runtime import LIVE_FEED_FRESHNESS_WINDOW


class _DagsterEventLike(Protocol):
    partition: str | None


class _AssetEventLike(Protocol):
    dagster_event: _DagsterEventLike | None
    run_id: str


def _futures_daily_trades_base_url() -> str:
    return os.environ.get(
        'BINANCE_FUTURES_DAILY_TRADES_BASE_URL', DEFAULT_BINANCE_FUTURES_DAILY_TRADES_BASE_URL
    )


FUTURES_DAILY_GAP_REPAIR_SPEC = DailyGapRepairSpec(
    market='futures',
    ledger_table=FUTURES_DAILY_LEDGER_TABLE_NAME,
    earliest_partition=futures_daily_partitions.start.date(),
    get_base_url=_futures_daily_trades_base_url,
)


# Database Maintenance Jobs

create_origo_database_job = define_asset_job(
    name="create_origo_database_job",
    selection=["create_origo_database"]
)


create_binance_daily_futures_trades_table_origo_job = define_asset_job(
    name="create_binance_daily_futures_trades_table_origo_job",
    selection=["create_binance_daily_futures_trades_table_origo"]
)


create_binance_futures_klines_table_origo_job = define_asset_job(
    name="create_binance_futures_klines_table_origo_job",
    selection=["create_binance_futures_klines_table_origo"]
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


create_aligned_1m_exchange_table_origo_job = define_asset_job(
    name="create_aligned_1m_exchange_table_origo_job",
    selection=["create_aligned_1m_exchange_table_origo"]
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

refresh_binance_futures_data_source_job = define_asset_job(
    name="refresh_binance_futures_data_source_job",
    selection=[
        "insert_daily_binance_futures_trades_to_origo",
        "refresh_binance_futures_klines_origo",
        "refresh_aligned_1m_exchange_from_binance_futures_origo",
    ])

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

def _scheduled_time(context: ScheduleEvaluationContext) -> datetime:
    return context.scheduled_execution_time or datetime.now(timezone.utc)


daily_binance_futures_pipeline_schedule = build_schedule_from_partitioned_job(
    refresh_binance_futures_data_source_job,
    name='daily_binance_futures_pipeline_schedule',
    hour_of_day=10,
    default_status=DefaultScheduleStatus.RUNNING,
)


_IN_PROGRESS_RUN_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]


_RECENTLY_TERMINAL_RUN_STATUSES = [
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
]


def _partition_days(runs: Sequence[DagsterRun]) -> set[date]:
    days: set[date] = set()
    for run in runs:
        partition_key = run.tags.get('dagster/partition')
        if partition_key is not None:
            days.add(date.fromisoformat(partition_key))
    return days


def _active_partition_days(instance: DagsterInstance, job_name: str) -> set[date]:
    """Partitions of ``job_name`` that repair must not touch right now.

    Two groups: runs currently in progress (a regular daily tick inside its
    op-retry backoff stays STARTED for up to ~23h), and runs that reached a
    terminal state within the last REPAIR_TERMINATION_GRACE_SECONDS — run
    monitoring force-marks a timed-out run FAILED without confirming its
    worker exited, so the partition may still be written by the old worker.
    Racing either with a repair run would interleave the non-atomic
    delete-then-insert.
    """
    in_progress = instance.get_runs(
        filters=RunsFilter(job_name=job_name, statuses=_IN_PROGRESS_RUN_STATUSES)
    )
    recently_terminal = instance.get_runs(
        filters=RunsFilter(
            job_name=job_name,
            statuses=_RECENTLY_TERMINAL_RUN_STATUSES,
            updated_after=datetime.now(timezone.utc)
            - timedelta(seconds=REPAIR_TERMINATION_GRACE_SECONDS),
        )
    )
    return _partition_days(in_progress) | _partition_days(recently_terminal)


def _daily_gap_repair_run_requests(
    context: ScheduleEvaluationContext,
    spec: DailyGapRepairSpec,
    job_name: str,
) -> list[RunRequest] | SkipReason:
    settings = get_origo_clickhouse_settings()
    client = make_origo_clickhouse_client(settings)
    try:
        return gap_repair_run_requests(
            client,
            settings.database,
            spec,
            _scheduled_time(context).astimezone(timezone.utc).date(),
            _active_partition_days(context.instance, job_name),
        )
    finally:
        client.disconnect()


@schedule(
    job=refresh_binance_futures_data_source_job,
    cron_schedule='30 * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_futures_daily_gap_repair_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _daily_gap_repair_run_requests(
        context, FUTURES_DAILY_GAP_REPAIR_SPEC, 'refresh_binance_futures_data_source_job'
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
    'no_error_logs',
    'queue_bounded',
    'workers_alive',
)
origo_monitor = AssetsDefinition(
    specs=[
        AssetSpec(
            'origo_monitor',
            group_name='monitoring',
            description='The monitor worker: five checks evaluated every minute outside Dagster.',
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


defs = Definitions(
    assets=[origo_monitor,
            binance_spot_depth_live_feed,
            create_origo_database,
            create_binance_daily_futures_trades_table_origo,
            create_binance_futures_klines_table_origo,
            create_binance_spot_depth20_snapshots_table_origo,
            create_binance_spot_depth20_1m_table_origo,
            create_binance_spot_depth200_snapshots_table_origo,
            create_binance_spot_depth200_1m_table_origo,
            create_aligned_1m_exchange_table_origo,
            insert_daily_binance_futures_trades_to_origo,
            refresh_binance_futures_klines_origo,
            sync_binance_spot_depth20_snapshots_to_origo,
            refresh_binance_spot_depth20_1m_origo,
            reconcile_binance_spot_depth20_partition_state_origo,
            sync_binance_spot_depth200_snapshots_to_origo,
            refresh_binance_spot_depth200_1m_origo,
            reconcile_binance_spot_depth200_partition_state_origo,
            refresh_aligned_1m_exchange_from_binance_futures_origo,
            publish_btc_briefing_feed,
            publish_btc_briefing_history,
            build_depth_snapshot_store_arrow],

    schedules=[
        daily_binance_futures_pipeline_schedule,
        binance_futures_daily_gap_repair_schedule,
    ],

    asset_checks=[origo_monitor_checks],

    sensors=[
        publish_btc_briefing_feed_sensor,
        publish_btc_briefing_history_sensor,
    ],

    jobs=[create_origo_database_job,
          create_binance_daily_futures_trades_table_origo_job,
          create_binance_futures_klines_table_origo_job,
          create_binance_spot_depth20_snapshots_table_origo_job,
          create_binance_spot_depth20_1m_table_origo_job,
          create_binance_spot_depth200_snapshots_table_origo_job,
          create_binance_spot_depth200_1m_table_origo_job,
          create_aligned_1m_exchange_table_origo_job,
          refresh_binance_futures_data_source_job,
          refresh_binance_spot_depth20_data_source_job,
          refresh_binance_spot_depth200_data_source_job,
          backfill_binance_spot_depth20_data_source_job,
          backfill_binance_spot_depth200_data_source_job,
          repair_binance_spot_depth20_projection_job,
          repair_binance_spot_depth200_projection_job,
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
