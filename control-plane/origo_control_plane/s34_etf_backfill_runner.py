from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient
from dagster import DagsterInstance
from dagster._core.execution.api import create_execution_plan
from dagster._core.remote_representation.code_location import CodeLocation
from dagster._core.remote_representation.external import RemoteJob, RemoteRepository
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES, DagsterRunStatus
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._core.workspace.load import load_workspace_process_context_from_yaml_paths

from origo_control_plane.backfill import (
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    load_last_completed_daily_partition_from_canonical_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.jobs.etf_daily_ingest import origo_etf_daily_ingest_job

_ETF_JOB_NAME = 'origo_etf_daily_ingest_job'
_DATASET = 'etf_daily_metrics'
_RUN_POLL_INTERVAL_SECONDS = 2.0
_DAGSTER_HOME_ENV = 'DAGSTER_HOME'
_TERMINAL_PARTITION_STATES = ('proved_complete', 'empty_proved')


@dataclass(frozen=True)
class _DagsterJobHandle:
    workspace_process_context: Any
    request_context: BaseWorkspaceRequestContext
    code_location: CodeLocation
    remote_repository: RemoteRepository
    remote_job: RemoteJob


@dataclass(frozen=True)
class _CompletedRun:
    run_id: str
    started_at_utc: datetime
    finished_at_utc: datetime


def _default_run_id() -> str:
    return f's34-backfill-etf-{datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")}'


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _workspace_yaml_path_or_raise() -> Path:
    path = _repo_root() / 'control-plane' / 'workspace.yaml'
    if not path.is_file():
        raise RuntimeError(f'Dagster workspace.yaml is missing: {path}')
    return path


def _require_dagster_home_or_raise() -> str:
    import os

    value = os.environ.get(_DAGSTER_HOME_ENV)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{_DAGSTER_HOME_ENV} must be set and non-empty')
    return value


def _load_dagster_job_handle_or_raise(
    *, instance: DagsterInstance, job_name: str
) -> _DagsterJobHandle:
    _require_dagster_home_or_raise()
    workspace_yaml_path = _workspace_yaml_path_or_raise()
    workspace_process_context: Any = load_workspace_process_context_from_yaml_paths(
        instance,
        [str(workspace_yaml_path)],
    )
    workspace_process_context.__enter__()
    request_context = workspace_process_context.create_request_context()
    for entry in request_context.get_code_location_entries().values():
        code_location = entry.code_location
        if code_location is None:
            continue
        for remote_repository in code_location.get_repositories().values():
            if not remote_repository.has_job(job_name):
                continue
            remote_job = remote_repository.get_full_job(job_name)
            return _DagsterJobHandle(
                workspace_process_context=workspace_process_context,
                request_context=request_context,
                code_location=code_location,
                remote_repository=remote_repository,
                remote_job=remote_job,
            )
    workspace_process_context.__exit__(None, None, None)
    raise RuntimeError(f'Failed to resolve Dagster remote job for job_name={job_name}')


def _build_run_tags(*, run_id: str) -> dict[str, str]:
    return {
        'origo.backfill.dataset': _DATASET,
        'origo.backfill.control_run_id': run_id,
    }


def _create_and_submit_etf_run_or_raise(
    *,
    instance: DagsterInstance,
    handle: _DagsterJobHandle,
    run_id: str,
) -> str:
    tags = _build_run_tags(run_id=run_id)
    execution_plan = create_execution_plan(
        origo_etf_daily_ingest_job,
        run_config={},
        instance_ref=instance.get_ref(),
        tags=tags,
    )
    dagster_run = instance.create_run_for_job(
        job_def=origo_etf_daily_ingest_job,
        execution_plan=execution_plan,
        run_id=run_id,
        run_config={},
        tags=tags,
        remote_job_origin=handle.remote_job.get_remote_origin(),
        job_code_origin=handle.remote_job.get_python_origin(),
    )
    instance.submit_run(dagster_run.run_id, handle.request_context)
    return dagster_run.run_id


def _wait_for_run_success_or_raise(
    *,
    instance: DagsterInstance,
    run_id: str,
) -> _CompletedRun:
    submitted_at_utc = datetime.now(UTC)
    while True:
        dagster_run = instance.get_run_by_id(run_id)
        if dagster_run is None:
            raise RuntimeError(f'Dagster ETF run disappeared after submission: {run_id}')
        if dagster_run.status in NOT_FINISHED_STATUSES:
            time.sleep(_RUN_POLL_INTERVAL_SECONDS)
            continue
        if dagster_run.status != DagsterRunStatus.SUCCESS:
            raise RuntimeError(
                'Dagster ETF backfill run failed '
                f'for run_id={run_id} status={dagster_run.status.value}'
            )
        run_record = instance.get_run_record_by_id(run_id)
        if run_record is None:
            raise RuntimeError(f'Dagster ETF run record missing after success: {run_id}')
        start_timestamp = run_record.start_time
        end_timestamp = run_record.end_time
        started_at_utc = (
            datetime.fromtimestamp(start_timestamp, tz=UTC)
            if start_timestamp is not None
            else submitted_at_utc
        )
        finished_at_utc = (
            datetime.fromtimestamp(end_timestamp, tz=UTC)
            if end_timestamp is not None
            else datetime.now(UTC)
        )
        return _CompletedRun(
            run_id=run_id,
            started_at_utc=started_at_utc,
            finished_at_utc=finished_at_utc,
        )


def _build_clickhouse_client_or_raise() -> tuple[ClickHouseClient, str]:
    settings = resolve_clickhouse_native_settings()
    return (
        ClickHouseClient(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            database=settings.database,
            compression=True,
            send_receive_timeout=900,
        ),
        settings.database,
    )


def _load_ambiguous_daily_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> tuple[str, ...]:
    contract = get_s34_dataset_contract(_DATASET)
    rows = client.execute(
        f'''
        SELECT
            event_partitions.partition_id
        FROM
        (
            SELECT DISTINCT partition_id
            FROM {database}.canonical_event_log
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
        ) AS event_partitions
        LEFT JOIN
        (
            SELECT
                partition_id,
                argMax(state, proof_revision) AS state
            FROM {database}.canonical_backfill_partition_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
            GROUP BY partition_id
        ) AS proofs
        ON event_partitions.partition_id = proofs.partition_id
        WHERE proofs.state IS NULL OR proofs.state NOT IN %(terminal_states)s
        ORDER BY event_partitions.partition_id ASC
        ''',
        {
            'source_id': contract.source_id,
            'stream_id': contract.stream_id,
            'terminal_states': _TERMINAL_PARTITION_STATES,
        },
    )
    return tuple(str(row[0]) for row in rows)


def _load_terminal_partition_count_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> int:
    contract = get_s34_dataset_contract(_DATASET)
    rows = client.execute(
        f'''
        SELECT count()
        FROM
        (
            SELECT
                partition_id,
                argMax(state, proof_revision) AS state
            FROM {database}.canonical_backfill_partition_proofs
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
            GROUP BY partition_id
        )
        WHERE state IN %(terminal_states)s
        ''',
        {
            'source_id': contract.source_id,
            'stream_id': contract.stream_id,
            'terminal_states': _TERMINAL_PARTITION_STATES,
        },
    )
    return int(rows[0][0])


def _load_etf_backfill_summary_or_raise() -> dict[str, Any]:
    contract = get_s34_dataset_contract(_DATASET)
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'ETF backfill runner requires daily partition scheme, '
            f'got={contract.partition_scheme}'
        )
    client, database = _build_clickhouse_client_or_raise()
    try:
        last_completed_partition = load_last_completed_daily_partition_from_canonical_or_raise(
            client=client,
            database=database,
            contract=contract,
        )
        if last_completed_partition is None:
            raise RuntimeError(
                'ETF backfill completed without a terminal proof boundary'
            )
        ambiguous_partition_ids = _load_ambiguous_daily_partition_ids_or_raise(
            client=client,
            database=database,
        )
        if ambiguous_partition_ids != ():
            preview = ambiguous_partition_ids[:10]
            raise RuntimeError(
                'ETF backfill completed but ambiguous partitions remain: '
                f'count={len(ambiguous_partition_ids)} preview={preview}'
            )
        terminal_partition_count = _load_terminal_partition_count_or_raise(
            client=client,
            database=database,
        )
        return {
            'dataset': _DATASET,
            'proof_boundary_partition_id': last_completed_partition,
            'terminal_partition_count': terminal_partition_count,
            'ambiguous_partition_count': 0,
        }
    finally:
        client.disconnect()


def run_s34_etf_backfill_or_raise(*, run_id: str | None = None) -> dict[str, Any]:
    assert_s34_backfill_contract_consistency_or_raise()
    normalized_run_id = (run_id or _default_run_id()).strip()
    if normalized_run_id == '':
        raise RuntimeError('run_id must be non-empty')

    instance: DagsterInstance | None = None
    handle: _DagsterJobHandle | None = None
    try:
        instance = DagsterInstance.get()
        handle = _load_dagster_job_handle_or_raise(
            instance=instance,
            job_name=_ETF_JOB_NAME,
        )
        dagster_run_id = _create_and_submit_etf_run_or_raise(
            instance=instance,
            handle=handle,
            run_id=normalized_run_id,
        )
        completed_run = _wait_for_run_success_or_raise(
            instance=instance,
            run_id=dagster_run_id,
        )
        summary = _load_etf_backfill_summary_or_raise()
        return {
            'dataset': _DATASET,
            'job_name': _ETF_JOB_NAME,
            'run_id': normalized_run_id,
            'dagster_run_id': dagster_run_id,
            'started_at_utc': completed_run.started_at_utc.isoformat(),
            'finished_at_utc': completed_run.finished_at_utc.isoformat(),
            'proof_summary': summary,
        }
    finally:
        if handle is not None:
            handle.workspace_process_context.__exit__(None, None, None)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Run the Slice 34 ETF full-history Dagster job and fail loudly unless '
            'ETF proof state is terminally clean on completion.'
        )
    )
    parser.add_argument('--run-id', default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    print(run_s34_etf_backfill_or_raise(run_id=args.run_id))


if __name__ == '__main__':
    main()
