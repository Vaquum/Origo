"""One queue policy for scheduled, sensor, manual and backfill runs."""

import fcntl
import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import cast

from dagster import DagsterInstance, DagsterRun, DagsterRunStatus, RunsFilter
from dagster._core.storage.runs.sql_run_storage import SqlRunStorage

SHORT_JOB_MAX_RUNTIME_SECONDS = 1800
DEFAULT_JOB_MAX_RUNTIME_SECONDS = 93600
WORKLOAD_TAG = 'origo/workload'
IDENTITY_TAG = 'origo/request_identity'
ROUTINE_JOB_TAG = 'origo/routine_job'
SHARE_TAG = 'origo/backfill_share'
FRONTIER_KEY = 'origo/backfill_frontier'
WORKER_TAG = 'origo/worker_container'
CLAIM_TAG = 'origo/launch_claimed'
REDUNDANT_TAG = 'origo/redundant_run'
# The one job with its own queue lane: it must run while backfill and routine lanes are full.
MAINTENANCE_JOB = 'maintain_operational_metadata_job'
ACTIVE = [DagsterRunStatus.STARTING, DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]
OUTSTANDING = [DagsterRunStatus.QUEUED, *ACTIVE]
# These tags affect execution. Scheduler timestamps/run keys describe requests,
# not work; they must not turn a repeated request into a new unit of work.
INPUT_TAGS = (
    'dagster/partition',
    'dagster/asset_partition_range_start',
    'dagster/asset_partition_range_end',
    'origo_source_partition',
    'origo_source_state_token',
    'origo_source_event',
)


def request_identity(run: DagsterRun) -> str:
    payload = {
        'job': run.job_name,
        'definition': run.job_snapshot_id,
        'config': run.run_config,
        'inputs': {key: run.tags[key] for key in INPUT_TAGS if key in run.tags},
        'assets': sorted(key.to_user_string() for key in run.asset_selection or ()),
        'checks': sorted(str(key) for key in run.asset_check_selection or ()),
        'steps': sorted(run.step_keys_to_execute or ()),
        # Each native backfill must retain its own receipts and completion barrier.
        'backfill': run.tags.get('dagster/backfill'),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def bulk_share(run: DagsterRun) -> str | None:
    """A bulk run's share of the historical lane: its native backfill, else its bulk job."""
    backfill = run.tags.get('dagster/backfill')
    return backfill or (run.job_name if run.job_name.startswith('backfill_') else None)


def bulk_order(run: DagsterRun) -> int:
    """A bulk run's place in the historical lane; lower places dequeue first."""
    return -int(run.tags['dagster/priority'])


def frontier(instance: DagsterInstance) -> int:
    """The highest historical-lane place ever launched; it only moves forward with service."""
    value = cast(SqlRunStorage, instance.run_storage).get_cursor_values({FRONTIER_KEY})
    return int(value.get(FRONTIER_KEY, '0'))


def advance_frontier(instance: DagsterInstance, run: DagsterRun) -> None:
    """Record a launched bulk run's place; callers hold the admission lock."""
    if bulk_share(run) is not None and SHARE_TAG in run.tags and bulk_order(run) > frontier(instance):
        cast(SqlRunStorage, instance.run_storage).set_cursor_values(
            {FRONTIER_KEY: str(bulk_order(run))}
        )


def execution_tags(run: DagsterRun, order: int = 0) -> dict[str, str]:
    """``order`` is a bulk run's place in the historical lane (start-time fair queuing): a share
    joins at the service frontier and then follows its own queue, so concurrent backfills
    alternate and one backfill never holds another source's fills back."""
    share = bulk_share(run)
    bulk = share is not None
    short_job = run.job_name in {
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'repair_binance_spot_depth20_projection_job',
        'repair_binance_spot_depth200_projection_job',
        'build_depth_snapshot_store_arrow_job',
        'publish_binance_spot_trades_mount_job',
        MAINTENANCE_JOB,
    }
    daily = re.fullmatch(r'\d{4}-\d{2}-\d{2}', run.tags.get('dagster/partition', '')) is not None
    default_runtime = str(SHORT_JOB_MAX_RUNTIME_SECONDS if short_job and not bulk else DEFAULT_JOB_MAX_RUNTIME_SECONDS)
    requested_runtime = run.tags.get('dagster/max_runtime', default_runtime)
    maintenance = run.job_name == MAINTENANCE_JOB and not bulk
    return {
        WORKLOAD_TAG: 'maintenance' if maintenance else 'backfill' if bulk else 'routine',
        IDENTITY_TAG: request_identity(run),
        **({ROUTINE_JOB_TAG: run.job_name} if not (bulk or maintenance) else {}),
        **({SHARE_TAG: share} if share else {}),
        'dagster/priority': '300' if maintenance else str(-order) if bulk else '200' if daily else '100',
        # Preserve the daily ingestion retry envelope; unlimited jobs can leak slots.
        'dagster/max_runtime': default_runtime if requested_runtime == '0' else requested_runtime,
    }


@contextmanager
def admission_lock(instance: DagsterInstance) -> Iterator[None]:
    path = Path(instance.storage_directory()) / 'orchestration.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def outstanding_partitions(instance: DagsterInstance, job_name: str) -> set[str]:
    return {
        value
        for run in instance.get_runs(RunsFilter(job_name=job_name, statuses=OUTSTANDING))
        if (value := run.tags.get('dagster/partition')) is not None
    }


def has_outstanding(instance: DagsterInstance, job_name: str) -> bool:
    return bool(instance.get_runs(RunsFilter(job_name=job_name, statuses=OUTSTANDING), limit=1))


def reconcile_stale_concurrency_claims(instance: DagsterInstance) -> int:
    """Free pool slots held by runs that can no longer release them.

    A step claims its pool slot when it starts and releases it on step end. When the
    run worker dies in between (kill, crash, deploy), the run goes terminal without
    step-end events and the claim wedges the pool forever: every later run stays
    QUEUED behind `blocked by global concurrency limits`, and a schedule gated on
    `has_outstanding` goes silent. Only claims of runs outside ACTIVE are freed, so
    a live holder is never evicted. Returns the runs freed.
    """
    storage = instance.event_log_storage
    if not storage.supports_global_concurrency_limits:
        return 0
    stale = {
        pending.run_id
        for key in storage.get_concurrency_keys()
        for pending in storage.get_concurrency_info(key).pending_steps
        if (run := instance.get_run_by_id(pending.run_id)) is None or run.status not in ACTIVE
    }
    for run_id in sorted(stale):
        storage.free_concurrency_slots_for_run(run_id)
    return len(stale)


def outstanding_configs(
    instance: DagsterInstance, job_name: str, op_name: str, config_key: str, *, partition: str
) -> set[str]:
    values: set[str] = set()
    for run in instance.get_runs(
        RunsFilter(job_name=job_name, statuses=OUTSTANDING, tags={'dagster/partition': partition})
    ):
        ops: object = run.run_config.get('ops', {})
        if not isinstance(ops, Mapping):
            raise ValueError('Run ops must be a mapping.')
        op: object = cast(Mapping[str, object], ops).get(op_name, {})
        if not isinstance(op, Mapping):
            raise ValueError('Run op must be a mapping.')
        config: object = cast(Mapping[str, object], op).get('config', {})
        if not isinstance(config, Mapping):
            raise ValueError('Run config must be a mapping.')
        value: object = cast(Mapping[str, object], config).get(config_key)
        if isinstance(value, str):
            values.add(value)
    return values
