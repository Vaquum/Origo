from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from clickhouse_driver import Client as ClickHouseClient
from dagster import DagsterInstance
from dagster._core.execution.api import create_execution_plan
from dagster._core.remote_representation.code_location import CodeLocation
from dagster._core.remote_representation.external import RemoteJob, RemoteRepository
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES, DagsterRunStatus
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._core.workspace.load import load_workspace_process_context_from_yaml_paths

from origo.events import CanonicalBackfillStateStore
from origo_control_plane.backfill import (
    BACKFILL_EXECUTION_MODE_TAG,
    BACKFILL_PARTITION_IDS_TAG,
    BACKFILL_PROJECTION_MODE_TAG,
    BACKFILL_RUNTIME_AUDIT_MODE_TAG,
    ExecutionMode,
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.jobs.fred_daily_ingest import origo_fred_daily_backfill_job
from origo_control_plane.s34_fred_reconcile_planning import (
    load_fred_reconcile_max_partitions_per_run_or_raise,
    load_fred_reconcile_max_source_window_days_or_raise,
    parse_fred_partition_id_as_date_or_raise,
    select_fred_reconcile_partition_ids_or_raise,
)
from origo_control_plane.s34_partition_authority import (
    load_nonterminal_partition_ids_for_stream_or_raise,
)

_DATASET = 'fred_series_metrics'
_FRED_JOB_NAME = 'origo_fred_daily_backfill_job'
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


@dataclass(frozen=True)
class _PlannedFREDRun:
    execution_mode: ExecutionMode
    partition_ids: tuple[str, ...]
    ambiguous_partition_count: int
    source_window_start: str | None
    source_window_end: str | None
    source_window_days: int | None


def _default_run_id() -> str:
    return f's34-backfill-fred-{datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")}'


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
            send_receive_timeout=settings.send_receive_timeout_seconds,
        ),
        settings.database,
    )


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


def _build_run_tags(
    *,
    control_run_id: str,
    execution_mode: ExecutionMode,
    partition_ids: tuple[str, ...] = (),
) -> dict[str, str]:
    tags = {
        'origo.backfill.dataset': _DATASET,
        'origo.backfill.control_run_id': control_run_id,
        BACKFILL_PROJECTION_MODE_TAG: 'deferred',
        BACKFILL_EXECUTION_MODE_TAG: execution_mode,
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: 'summary',
    }
    if partition_ids != ():
        tags[BACKFILL_PARTITION_IDS_TAG] = ','.join(partition_ids)
    return tags


def _create_and_submit_fred_run_or_raise(
    *,
    instance: DagsterInstance,
    handle: _DagsterJobHandle,
    control_run_id: str,
    execution_mode: ExecutionMode,
    partition_ids: tuple[str, ...] = (),
) -> str:
    tags = _build_run_tags(
        control_run_id=control_run_id,
        execution_mode=execution_mode,
        partition_ids=partition_ids,
    )
    dagster_run_id = str(uuid4())
    execution_plan = create_execution_plan(
        origo_fred_daily_backfill_job,
        run_config={},
        instance_ref=instance.get_ref(),
        tags=tags,
    )
    dagster_run = instance.create_run_for_job(
        job_def=origo_fred_daily_backfill_job,
        execution_plan=execution_plan,
        run_id=dagster_run_id,
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
            raise RuntimeError(f'Dagster FRED run disappeared after submission: {run_id}')
        if dagster_run.status in NOT_FINISHED_STATUSES:
            time.sleep(_RUN_POLL_INTERVAL_SECONDS)
            continue
        if dagster_run.status != DagsterRunStatus.SUCCESS:
            raise RuntimeError(
                'Dagster FRED backfill run failed '
                f'for run_id={run_id} status={dagster_run.status.value}'
            )
        run_record = instance.get_run_record_by_id(run_id)
        if run_record is None:
            raise RuntimeError(f'Dagster FRED run record missing after success: {run_id}')
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


def _load_ambiguous_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> tuple[str, ...]:
    contract = get_s34_dataset_contract(_DATASET)
    return load_nonterminal_partition_ids_for_stream_or_raise(
        client=client,
        database=database,
        source_id=contract.source_id,
        stream_id=contract.stream_id,
        terminal_states=_TERMINAL_PARTITION_STATES,
    )


def _load_fred_backfill_summary_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> dict[str, Any]:
    contract = get_s34_dataset_contract(_DATASET)
    state_store = CanonicalBackfillStateStore(
        client=client,
        database=database,
    )
    terminal_partition_ids = state_store.list_terminal_partition_ids(
        source_id=contract.source_id,
        stream_id=contract.stream_id,
    )
    if terminal_partition_ids == ():
        raise RuntimeError('FRED backfill completed without a terminal proof boundary')
    ambiguous_partition_ids = _load_ambiguous_partition_ids_or_raise(
        client=client,
        database=database,
    )
    if ambiguous_partition_ids != ():
        preview = ambiguous_partition_ids[:10]
        raise RuntimeError(
            'FRED backfill completed but ambiguous partitions remain: '
            f'count={len(ambiguous_partition_ids)} preview={preview}'
        )
    return {
        'dataset': _DATASET,
        'proof_boundary_partition_id': terminal_partition_ids[-1],
        'terminal_partition_count': len(terminal_partition_ids),
        'ambiguous_partition_count': 0,
    }


def _load_reconcile_partition_batch_summary_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    partition_ids: tuple[str, ...],
) -> dict[str, Any]:
    if partition_ids == ():
        raise RuntimeError(
            'FRED reconcile batch summary requires at least one partition id'
        )
    contract = get_s34_dataset_contract(_DATASET)
    rows = client.execute(
        f'''
        SELECT
            partition_id,
            argMax(state, proof_revision) AS state,
            argMax(reason, proof_revision) AS reason,
            argMax(proof_digest_sha256, proof_revision) AS proof_digest_sha256,
            argMax(source_row_count, proof_revision) AS source_row_count,
            argMax(canonical_row_count, proof_revision) AS canonical_row_count
        FROM {database}.canonical_backfill_partition_proofs
        WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id IN %(partition_ids)s
        GROUP BY partition_id
        ORDER BY partition_id ASC
        ''',
        {
            'source_id': contract.source_id,
            'stream_id': contract.stream_id,
            'partition_ids': partition_ids,
        },
    )
    proofs_by_partition: dict[str, dict[str, Any]] = {
        str(row[0]): {
            'state': str(row[1]),
            'reason': str(row[2]),
            'proof_digest_sha256': str(row[3]),
            'source_row_count': int(row[4]),
            'canonical_row_count': int(row[5]),
        }
        for row in rows
    }
    missing_partition_ids = [
        partition_id
        for partition_id in partition_ids
        if partition_id not in proofs_by_partition
    ]
    if missing_partition_ids != []:
        raise RuntimeError(
            'Dagster FRED reconcile run succeeded but latest partition proofs are '
            f'missing for targeted partitions: count={len(missing_partition_ids)} '
            f'preview={missing_partition_ids[:10]}'
        )
    nonterminal_partitions = [
        {
            'partition_id': partition_id,
            'state': proofs_by_partition[partition_id]['state'],
            'reason': proofs_by_partition[partition_id]['reason'],
        }
        for partition_id in partition_ids
        if proofs_by_partition[partition_id]['state'] not in _TERMINAL_PARTITION_STATES
    ]
    if nonterminal_partitions != []:
        raise RuntimeError(
            'Dagster FRED reconcile run succeeded but targeted partitions remain '
            f'non-terminal: count={len(nonterminal_partitions)} '
            f'preview={nonterminal_partitions[:10]}'
        )
    state_counts: dict[str, int] = {}
    for partition_id in partition_ids:
        state = proofs_by_partition[partition_id]['state']
        state_counts[state] = state_counts.get(state, 0) + 1
    return {
        'partition_count': len(partition_ids),
        'first_partition_id': partition_ids[0],
        'last_partition_id': partition_ids[-1],
        'state_counts': state_counts,
    }


def _plan_next_fred_run_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
) -> _PlannedFREDRun:
    ambiguous_partition_ids = _load_ambiguous_partition_ids_or_raise(
        client=client,
        database=database,
    )
    if ambiguous_partition_ids != ():
        reconcile_partition_limit = (
            load_fred_reconcile_max_partitions_per_run_or_raise()
        )
        reconcile_source_window_days = (
            load_fred_reconcile_max_source_window_days_or_raise()
        )
        partition_ids = select_fred_reconcile_partition_ids_or_raise(
            ambiguous_partition_ids=ambiguous_partition_ids,
            max_partitions_per_run=reconcile_partition_limit,
            max_source_window_days=reconcile_source_window_days,
        )
        first_partition_date = parse_fred_partition_id_as_date_or_raise(
            partition_id=partition_ids[0]
        )
        last_partition_date = parse_fred_partition_id_as_date_or_raise(
            partition_id=partition_ids[-1]
        )
        return _PlannedFREDRun(
            execution_mode='reconcile',
            partition_ids=partition_ids,
            ambiguous_partition_count=len(ambiguous_partition_ids),
            source_window_start=partition_ids[0],
            source_window_end=partition_ids[-1],
            source_window_days=(last_partition_date - first_partition_date).days + 1,
        )
    return _PlannedFREDRun(
        execution_mode='backfill',
        partition_ids=(),
        ambiguous_partition_count=0,
        source_window_start=None,
        source_window_end=None,
        source_window_days=None,
    )


def run_s34_fred_backfill_or_raise(*, run_id: str | None = None) -> dict[str, Any]:
    assert_s34_backfill_contract_consistency_or_raise()
    normalized_run_id = (run_id or _default_run_id()).strip()
    if normalized_run_id == '':
        raise RuntimeError('run_id must be non-empty')

    instance: DagsterInstance | None = None
    handle: _DagsterJobHandle | None = None
    planning_client: ClickHouseClient | None = None
    planning_database: str | None = None
    try:
        instance = DagsterInstance.get()
        handle = _load_dagster_job_handle_or_raise(
            instance=instance,
            job_name=_FRED_JOB_NAME,
        )
        planning_client, planning_database = _build_clickhouse_client_or_raise()
        completed_runs: list[dict[str, Any]] = []
        while True:
            plan = _plan_next_fred_run_or_raise(
                client=planning_client,
                database=planning_database,
            )
            dagster_run_id = _create_and_submit_fred_run_or_raise(
                instance=instance,
                handle=handle,
                control_run_id=normalized_run_id,
                execution_mode=plan.execution_mode,
                partition_ids=plan.partition_ids,
            )
            completed_run = _wait_for_run_success_or_raise(
                instance=instance,
                run_id=dagster_run_id,
            )
            reconcile_batch_summary: dict[str, Any] | None = None
            if plan.execution_mode == 'reconcile':
                if plan.partition_ids == ():
                    raise RuntimeError(
                        'FRED reconcile plan must include at least one partition id'
                    )
                reconcile_batch_summary = (
                    _load_reconcile_partition_batch_summary_or_raise(
                        client=planning_client,
                        database=planning_database,
                        partition_ids=plan.partition_ids,
                    )
                )
            completed_runs.append(
                {
                    'execution_mode': plan.execution_mode,
                    'partition_ids': list(plan.partition_ids),
                    'ambiguous_partition_count': plan.ambiguous_partition_count,
                    'source_window_start': plan.source_window_start,
                    'source_window_end': plan.source_window_end,
                    'source_window_days': plan.source_window_days,
                    'dagster_run_id': dagster_run_id,
                    'started_at_utc': completed_run.started_at_utc.isoformat(),
                    'finished_at_utc': completed_run.finished_at_utc.isoformat(),
                    'reconcile_batch_summary': reconcile_batch_summary,
                }
            )
            if plan.execution_mode == 'reconcile':
                continue
            break
        summary = _load_fred_backfill_summary_or_raise(
            client=planning_client,
            database=planning_database,
        )
        return {
            'dataset': _DATASET,
            'job_name': _FRED_JOB_NAME,
            'run_id': normalized_run_id,
            'dagster_run_id': completed_runs[-1]['dagster_run_id'],
            'started_at_utc': completed_runs[0]['started_at_utc'],
            'finished_at_utc': completed_runs[-1]['finished_at_utc'],
            'completed_runs': completed_runs,
            'proof_summary': summary,
        }
    finally:
        if planning_client is not None:
            planning_client.disconnect()
        if handle is not None:
            handle.workspace_process_context.__exit__(None, None, None)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Run the Slice 34 FRED full-history Dagster job and fail loudly unless '
            'FRED proof state is terminally clean on completion.'
        )
    )
    parser.add_argument('--run-id', default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    print(run_s34_fred_backfill_or_raise(run_id=args.run_id))


if __name__ == '__main__':
    main()
