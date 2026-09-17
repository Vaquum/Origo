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

WORKLOAD_TAG = 'origo/workload'
IDENTITY_TAG = 'origo/request_identity'
ROUTINE_JOB_TAG = 'origo/routine_job'
WORKER_TAG = 'origo/worker_container'
CLAIM_TAG = 'origo/launch_claimed'
REDUNDANT_TAG = 'origo/redundant_run'
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
    'binance_spot_latest_minute_start',
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


def execution_tags(run: DagsterRun) -> dict[str, str]:
    bulk = bool(run.tags.get('dagster/backfill')) or run.job_name.startswith('backfill_')
    short_job = run.job_name in {
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'repair_binance_spot_depth20_projection_job',
        'repair_binance_spot_depth200_projection_job',
        'refresh_binance_spot_latest_data_source_job',
        'build_depth_snapshot_store_arrow_job',
        'build_bar_store_arrow_job',
        'publish_binance_spot_klines_to_mount_job',
        'maintain_operational_metadata_job',
    }
    daily = re.fullmatch(r'\d{4}-\d{2}-\d{2}', run.tags.get('dagster/partition', '')) is not None
    default_runtime = '1800' if short_job and not bulk else '93600'
    requested_runtime = run.tags.get('dagster/max_runtime', default_runtime)
    return {
        WORKLOAD_TAG: 'backfill' if bulk else 'routine',
        IDENTITY_TAG: request_identity(run),
        **({ROUTINE_JOB_TAG: run.job_name} if not bulk else {}),
        'dagster/priority': '0' if bulk else '200' if daily else '100',
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
