from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient
from dagster import DagsterInstance
from dagster._core.execution.api import create_execution_plan
from dagster._core.remote_representation.code_location import CodeLocation
from dagster._core.remote_representation.external import RemoteJob, RemoteRepository
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES, DagsterRunStatus
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._core.workspace.load import load_workspace_process_context_from_yaml_paths

from origo.events import CanonicalBackfillStateStore, CanonicalStreamKey
from origo_control_plane.backfill import (
    BACKFILL_EXECUTION_MODE_TAG,
    BACKFILL_HEIGHT_END_TAG,
    BACKFILL_HEIGHT_START_TAG,
    BACKFILL_PROJECTION_MODE_TAG,
    BACKFILL_RUNTIME_AUDIT_MODE_TAG,
    ExecutionMode,
    ProjectionMode,
    RuntimeAuditMode,
    S34BackfillDataset,
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    list_s34_dataset_contracts,
)
from origo_control_plane.bitcoin_core import (
    format_bitcoin_height_range_partition_id_or_raise,
    parse_bitcoin_height_range_partition_id_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings

_TERMINAL_PARTITION_STATES = ('proved_complete', 'empty_proved')
_RUN_POLL_INTERVAL_SECONDS = 2.0
_DAGSTER_HOME_ENV = 'DAGSTER_HOME'
_MEMPOOL_DATASET: S34BackfillDataset = 'bitcoin_mempool_state'
_CHAIN_JOB_NAMES: dict[S34BackfillDataset, str] = {
    'bitcoin_block_headers': 'insert_bitcoin_block_headers_to_origo_job',
    'bitcoin_block_transactions': 'insert_bitcoin_block_transactions_to_origo_job',
    'bitcoin_block_fee_totals': 'insert_bitcoin_block_fee_totals_to_origo_job',
    'bitcoin_block_subsidy_schedule': 'insert_bitcoin_block_subsidy_schedule_to_origo_job',
    'bitcoin_network_hashrate_estimate': 'insert_bitcoin_network_hashrate_estimate_to_origo_job',
    'bitcoin_circulating_supply': 'insert_bitcoin_circulating_supply_to_origo_job',
}
_MEMPOOL_JOB_NAME = 'insert_bitcoin_mempool_state_to_origo_job'


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
class BitcoinHeightBatchPlan:
    dataset: S34BackfillDataset
    execution_mode: ExecutionMode
    start_height: int
    end_height: int
    partition_id: str
    run_id: str


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
            send_receive_timeout=900,
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


def _create_and_submit_job_run_or_raise(
    *,
    instance: DagsterInstance,
    handle: _DagsterJobHandle,
    job_def: Any,
    run_id: str,
    tags: dict[str, str],
) -> str:
    execution_plan = create_execution_plan(
        job_def,
        run_config={},
        instance_ref=instance.get_ref(),
        tags=tags,
    )
    dagster_run = instance.create_run_for_job(
        job_def=job_def,
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
            raise RuntimeError(
                f'Dagster Bitcoin backfill run disappeared after submission: {run_id}'
            )
        if dagster_run.status in NOT_FINISHED_STATUSES:
            time.sleep(_RUN_POLL_INTERVAL_SECONDS)
            continue
        if dagster_run.status != DagsterRunStatus.SUCCESS:
            raise RuntimeError(
                'Dagster Bitcoin backfill run failed '
                f'for run_id={run_id} status={dagster_run.status.value}'
            )
        run_record = instance.get_run_record_by_id(run_id)
        if run_record is None:
            raise RuntimeError(
                f'Dagster Bitcoin backfill run record missing after success: {run_id}'
            )
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


def _parse_projection_mode_or_raise(value: str) -> ProjectionMode:
    normalized = value.strip().lower()
    if normalized not in {'inline', 'deferred'}:
        raise RuntimeError(f'Invalid projection_mode={value!r}; expected inline|deferred')
    return cast(ProjectionMode, normalized)


def _parse_runtime_audit_mode_or_raise(value: str) -> RuntimeAuditMode:
    normalized = value.strip().lower()
    if normalized not in {'event', 'summary'}:
        raise RuntimeError(
            f'Invalid runtime_audit_mode={value!r}; expected event|summary'
        )
    return cast(RuntimeAuditMode, normalized)


def _load_chain_job_def_or_raise(dataset: S34BackfillDataset) -> Any:
    from origo_control_plane import definitions

    job_attr_by_dataset: dict[S34BackfillDataset, str] = {
        'bitcoin_block_headers': 'insert_bitcoin_block_headers_job',
        'bitcoin_block_transactions': 'insert_bitcoin_block_transactions_job',
        'bitcoin_block_fee_totals': 'insert_bitcoin_block_fee_totals_job',
        'bitcoin_block_subsidy_schedule': 'insert_bitcoin_block_subsidy_schedule_job',
        'bitcoin_network_hashrate_estimate': 'insert_bitcoin_network_hashrate_estimate_job',
        'bitcoin_circulating_supply': 'insert_bitcoin_circulating_supply_job',
    }
    job_attr = job_attr_by_dataset.get(dataset)
    if job_attr is None:
        raise RuntimeError(
            'Bitcoin chain job definition requires height_range dataset, '
            f'got dataset={dataset}'
        )
    return getattr(definitions, job_attr)


def _load_mempool_job_def_or_raise() -> Any:
    from origo_control_plane import definitions

    return getattr(definitions, 'insert_bitcoin_mempool_state_job')


def _parse_iso_date_or_raise(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise RuntimeError(f'Invalid ISO date={value!r}; expected YYYY-MM-DD') from exc


def _load_ambiguous_height_range_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    dataset: S34BackfillDataset,
) -> tuple[str, ...]:
    contract = get_s34_dataset_contract(dataset)
    if contract.partition_scheme != 'height_range':
        raise RuntimeError(
            'Bitcoin height-range planner requires height_range partition scheme, '
            f'got dataset={dataset} partition_scheme={contract.partition_scheme}'
        )
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
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
        ),
    )
    normalized: list[str] = []
    for row in rows:
        partition_id = str(row[0])
        parse_bitcoin_height_range_partition_id_or_raise(partition_id)
        normalized.append(partition_id)
    return tuple(normalized)


def _load_terminal_height_range_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    dataset: S34BackfillDataset,
) -> tuple[str, ...]:
    contract = get_s34_dataset_contract(dataset)
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
        f'''
        SELECT partition_id
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
        ORDER BY partition_id ASC
        ''',
        {
            'source_id': contract.source_id,
            'stream_id': contract.stream_id,
            'terminal_states': _TERMINAL_PARTITION_STATES,
        },
        ),
    )
    normalized: list[str] = []
    for row in rows:
        partition_id = str(row[0])
        parse_bitcoin_height_range_partition_id_or_raise(partition_id)
        normalized.append(partition_id)
    return tuple(normalized)


def _load_last_contiguous_terminal_height_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    dataset: S34BackfillDataset,
) -> int | None:
    contract = get_s34_dataset_contract(dataset)
    if contract.partition_scheme != 'height_range':
        raise RuntimeError(
            'Bitcoin height-range frontier requires height_range partition scheme, '
            f'got dataset={dataset} partition_scheme={contract.partition_scheme}'
        )
    earliest_height = contract.earliest_height
    if earliest_height is None:
        raise RuntimeError(
            f'S34 height_range contract missing earliest_height for dataset={dataset}'
        )
    terminal_partition_ids = _load_terminal_height_range_partition_ids_or_raise(
        client=client,
        database=database,
        dataset=dataset,
    )
    last_contiguous_end: int | None = None
    expected_start = earliest_height
    for partition_id in terminal_partition_ids:
        start_height, end_height = parse_bitcoin_height_range_partition_id_or_raise(
            partition_id
        )
        if start_height < expected_start:
            raise RuntimeError(
                'Bitcoin height-range proof coverage overlaps or is out of order: '
                f'dataset={dataset} partition_id={partition_id} '
                f'expected_start>={expected_start}'
            )
        if start_height != expected_start:
            break
        last_contiguous_end = end_height
        expected_start = end_height + 1
    return last_contiguous_end


def list_bitcoin_chain_datasets_or_raise() -> tuple[S34BackfillDataset, ...]:
    assert_s34_backfill_contract_consistency_or_raise()
    datasets = cast(
        tuple[S34BackfillDataset, ...],
        tuple(
            contract.dataset
            for contract in list_s34_dataset_contracts()
            if contract.source_id == 'bitcoin_core'
            and contract.partition_scheme == 'height_range'
        ),
    )
    if datasets == ():
        raise RuntimeError('No Bitcoin height_range datasets are configured in S34')
    return datasets


def _resolve_bitcoin_chain_dataset_or_raise(value: str) -> S34BackfillDataset:
    normalized = value.strip()
    for dataset in list_bitcoin_chain_datasets_or_raise():
        if dataset == normalized:
            return dataset
    raise RuntimeError(
        'Bitcoin chain runner supports only height_range Bitcoin datasets, '
        f'got dataset={value!r}'
    )


def plan_next_bitcoin_chain_batch_or_raise(
    *,
    dataset: S34BackfillDataset,
    plan_end_height: int,
    batch_size_blocks: int,
    run_id_prefix: str = 's34-bitcoin-chain',
    client: ClickHouseClient | None = None,
    database: str | None = None,
) -> BitcoinHeightBatchPlan | None:
    assert_s34_backfill_contract_consistency_or_raise()
    if (client is None) != (database is None):
        raise RuntimeError(
            'plan_next_bitcoin_chain_batch_or_raise requires client and database '
            'to both be provided or both be None'
        )
    contract = get_s34_dataset_contract(dataset)
    if contract.partition_scheme != 'height_range':
        raise RuntimeError(
            'Bitcoin chain batch planner requires height_range dataset, '
            f'got dataset={dataset} partition_scheme={contract.partition_scheme}'
        )
    earliest_height = contract.earliest_height
    if earliest_height is None:
        raise RuntimeError(
            f'S34 Bitcoin chain contract missing earliest_height for dataset={dataset}'
        )
    if batch_size_blocks <= 0:
        raise RuntimeError(f'batch_size_blocks must be > 0, got {batch_size_blocks}')
    if plan_end_height < earliest_height:
        raise RuntimeError(
            f'plan_end_height must be >= {earliest_height} for dataset={dataset}, '
            f'got {plan_end_height}'
        )

    owns_client = client is None
    clickhouse_client = client
    clickhouse_database = database
    if clickhouse_client is None or clickhouse_database is None:
        clickhouse_client, clickhouse_database = _build_clickhouse_client_or_raise()

    try:
        ambiguous_partition_ids = _load_ambiguous_height_range_partition_ids_or_raise(
            client=clickhouse_client,
            database=clickhouse_database,
            dataset=dataset,
        )
        if ambiguous_partition_ids != ():
            partition_id = ambiguous_partition_ids[0]
            start_height, end_height = parse_bitcoin_height_range_partition_id_or_raise(
                partition_id
            )
            return BitcoinHeightBatchPlan(
                dataset=dataset,
                execution_mode='reconcile',
                start_height=start_height,
                end_height=end_height,
                partition_id=partition_id,
                run_id=(
                    f'{run_id_prefix}-{dataset}-reconcile-'
                    f'{start_height}-{end_height}'
                ),
            )

        last_contiguous_end = _load_last_contiguous_terminal_height_or_raise(
            client=clickhouse_client,
            database=clickhouse_database,
            dataset=dataset,
        )
        start_height = (
            earliest_height if last_contiguous_end is None else last_contiguous_end + 1
        )
        if start_height > plan_end_height:
            return None
        end_height = min(start_height + batch_size_blocks - 1, plan_end_height)
        partition_id = format_bitcoin_height_range_partition_id_or_raise(
            start_height=start_height,
            end_height=end_height,
        )
        return BitcoinHeightBatchPlan(
            dataset=dataset,
            execution_mode='backfill',
            start_height=start_height,
            end_height=end_height,
            partition_id=partition_id,
            run_id=f'{run_id_prefix}-{dataset}-backfill-{start_height}-{end_height}',
        )
    finally:
        if owns_client:
            clickhouse_client.disconnect()


def _load_partition_proof_summary_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    dataset: S34BackfillDataset,
    partition_id: str,
) -> dict[str, Any]:
    contract = get_s34_dataset_contract(dataset)
    state_store = CanonicalBackfillStateStore(
        client=client,
        database=database,
    )
    proof = state_store.fetch_latest_partition_proof(
        stream_key=CanonicalStreamKey(
            source_id=contract.source_id,
            stream_id=contract.stream_id,
            partition_id=partition_id,
        )
    )
    if proof is None:
        raise RuntimeError(
            'Bitcoin Dagster run succeeded but latest partition proof is missing: '
            f'dataset={dataset} partition_id={partition_id}'
        )
    if proof.state not in _TERMINAL_PARTITION_STATES:
        raise RuntimeError(
            'Bitcoin Dagster run succeeded but partition proof is not terminal: '
            f'dataset={dataset} partition_id={partition_id} state={proof.state}'
        )
    return {
        'partition_id': partition_id,
        'proof_state': proof.state,
        'proof_reason': proof.reason,
        'proof_digest_sha256': proof.proof_digest_sha256,
        'source_row_count': proof.source_row_count,
        'canonical_row_count': proof.canonical_row_count,
        'gap_count': proof.gap_count,
        'duplicate_count': proof.duplicate_count,
    }


def _build_chain_run_tags_or_raise(
    *,
    dataset: S34BackfillDataset,
    execution_mode: ExecutionMode,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    start_height: int,
    end_height: int,
    run_id: str,
) -> dict[str, str]:
    _require_chain_job_name_or_raise(dataset)
    return {
        'origo.backfill.dataset': dataset,
        'origo.backfill.control_run_id': run_id,
        BACKFILL_PROJECTION_MODE_TAG: projection_mode,
        BACKFILL_EXECUTION_MODE_TAG: execution_mode,
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: runtime_audit_mode,
        BACKFILL_HEIGHT_START_TAG: str(start_height),
        BACKFILL_HEIGHT_END_TAG: str(end_height),
    }


def _require_chain_job_name_or_raise(dataset: S34BackfillDataset) -> str:
    job_name = _CHAIN_JOB_NAMES.get(dataset)
    if job_name is None:
        raise RuntimeError(
            'Bitcoin chain runner supports only height_range datasets, '
            f'got dataset={dataset}'
        )
    return job_name


def run_bitcoin_chain_backfill_or_raise(
    *,
    dataset: S34BackfillDataset,
    plan_end_height: int,
    batch_size_blocks: int,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    run_id_prefix: str = 's34-bitcoin-chain',
    max_batches: int | None = None,
) -> dict[str, Any]:
    if max_batches is not None and max_batches <= 0:
        raise RuntimeError(f'max_batches must be > 0 when provided, got {max_batches}')

    job_name = _require_chain_job_name_or_raise(dataset)
    job_def = _load_chain_job_def_or_raise(dataset)
    instance: DagsterInstance | None = None
    handle: _DagsterJobHandle | None = None
    client: ClickHouseClient | None = None
    database: str | None = None
    completed_batches: list[dict[str, Any]] = []
    executed_batches = 0
    controller_stopped_reason = 'no_remaining_work'
    try:
        instance = DagsterInstance.get()
        handle = _load_dagster_job_handle_or_raise(
            instance=instance,
            job_name=job_name,
        )
        client, database = _build_clickhouse_client_or_raise()
        while True:
            if max_batches is not None and executed_batches >= max_batches:
                controller_stopped_reason = 'max_batches_reached'
                break
            batch = plan_next_bitcoin_chain_batch_or_raise(
                dataset=dataset,
                plan_end_height=plan_end_height,
                batch_size_blocks=batch_size_blocks,
                run_id_prefix=run_id_prefix,
                client=client,
                database=database,
            )
            if batch is None:
                controller_stopped_reason = 'no_remaining_work'
                break
            dagster_run_id = _create_and_submit_job_run_or_raise(
                instance=instance,
                handle=handle,
                job_def=job_def,
                run_id=batch.run_id,
                tags=_build_chain_run_tags_or_raise(
                    dataset=batch.dataset,
                    execution_mode=batch.execution_mode,
                    projection_mode=projection_mode,
                    runtime_audit_mode=runtime_audit_mode,
                    start_height=batch.start_height,
                    end_height=batch.end_height,
                    run_id=batch.run_id,
                ),
            )
            completed_run = _wait_for_run_success_or_raise(
                instance=instance,
                run_id=dagster_run_id,
            )
            proof_summary = _load_partition_proof_summary_or_raise(
                client=client,
                database=database,
                dataset=dataset,
                partition_id=batch.partition_id,
            )
            completed_batches.append(
                {
                    'dataset': dataset,
                    'execution_mode': batch.execution_mode,
                    'partition_id': batch.partition_id,
                    'start_height': batch.start_height,
                    'end_height': batch.end_height,
                    'run_id': batch.run_id,
                    'dagster_run_id': dagster_run_id,
                    'started_at_utc': completed_run.started_at_utc.isoformat(),
                    'finished_at_utc': completed_run.finished_at_utc.isoformat(),
                    'proof_summary': proof_summary,
                }
            )
            executed_batches += 1
        return {
            'dataset': dataset,
            'job_name': job_name,
            'plan_end_height': plan_end_height,
            'batch_size_blocks': batch_size_blocks,
            'completed_batch_count': len(completed_batches),
            'completed_batches': completed_batches,
            'controller_stopped_reason': controller_stopped_reason,
        }
    finally:
        if client is not None:
            client.disconnect()
        if handle is not None:
            handle.workspace_process_context.__exit__(None, None, None)


def run_bitcoin_chain_sequence_controller_or_raise(
    *,
    plan_end_height: int,
    batch_size_blocks: int,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    run_id_prefix: str = 's34-bitcoin-sequence',
    datasets: tuple[S34BackfillDataset, ...] | None = None,
    max_batches_per_dataset: int | None = None,
) -> dict[str, Any]:
    if max_batches_per_dataset is not None:
        raise RuntimeError(
            'Bitcoin chain sequence controller requires full dataset completion; '
            'max_batches_per_dataset is unsupported. Use --mode chain for bounded runs.'
        )
    sequence_datasets = (
        list_bitcoin_chain_datasets_or_raise() if datasets is None else datasets
    )
    completed_datasets: list[dict[str, Any]] = []
    for dataset in sequence_datasets:
        _require_chain_job_name_or_raise(dataset)
        result = run_bitcoin_chain_backfill_or_raise(
            dataset=dataset,
            plan_end_height=plan_end_height,
            batch_size_blocks=batch_size_blocks,
            projection_mode=projection_mode,
            runtime_audit_mode=runtime_audit_mode,
            run_id_prefix=run_id_prefix,
            max_batches=max_batches_per_dataset,
        )
        if result['controller_stopped_reason'] != 'no_remaining_work':
            raise RuntimeError(
                'Bitcoin chain sequence controller requires full dataset completion '
                f'before advancing, got dataset={dataset} '
                f"controller_stopped_reason={result['controller_stopped_reason']}"
            )
        completed_datasets.append(result)
    return {
        'datasets': sequence_datasets,
        'plan_end_height': plan_end_height,
        'batch_size_blocks': batch_size_blocks,
        'completed_dataset_count': len(completed_datasets),
        'completed_datasets': completed_datasets,
        'controller_stopped_reason': 'no_remaining_work',
    }


def run_bitcoin_mempool_daily_path_or_raise(
    *,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    requested_partition_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    contract = get_s34_dataset_contract(_MEMPOOL_DATASET)
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'Bitcoin mempool daily path requires daily partition scheme, '
            f'got partition_scheme={contract.partition_scheme}'
        )
    current_partition_id = datetime.now(UTC).date().isoformat()
    if requested_partition_id is not None:
        normalized_requested_partition_id = _parse_iso_date_or_raise(
            requested_partition_id
        )
        if normalized_requested_partition_id != current_partition_id:
            raise RuntimeError(
                'Historical mempool replay is unsupported from first-party Bitcoin '
                'Core RPC; requested_partition_id='
                f'{normalized_requested_partition_id} current_utc_partition_id='
                f'{current_partition_id}'
            )

    normalized_run_id = (
        run_id.strip()
        if run_id is not None and run_id.strip() != ''
        else f's34-bitcoin-mempool-{current_partition_id}'
    )

    instance: DagsterInstance | None = None
    handle: _DagsterJobHandle | None = None
    client: ClickHouseClient | None = None
    database: str | None = None
    try:
        instance = DagsterInstance.get()
        mempool_job_def = _load_mempool_job_def_or_raise()
        handle = _load_dagster_job_handle_or_raise(
            instance=instance,
            job_name=_MEMPOOL_JOB_NAME,
        )
        client, database = _build_clickhouse_client_or_raise()
        dagster_run_id = _create_and_submit_job_run_or_raise(
            instance=instance,
            handle=handle,
            job_def=mempool_job_def,
            run_id=normalized_run_id,
            tags={
                'origo.backfill.dataset': _MEMPOOL_DATASET,
                'origo.backfill.control_run_id': normalized_run_id,
                BACKFILL_PROJECTION_MODE_TAG: projection_mode,
                BACKFILL_EXECUTION_MODE_TAG: 'backfill',
                BACKFILL_RUNTIME_AUDIT_MODE_TAG: runtime_audit_mode,
            },
        )
        completed_run = _wait_for_run_success_or_raise(
            instance=instance,
            run_id=dagster_run_id,
        )
        proof_summary = _load_partition_proof_summary_or_raise(
            client=client,
            database=database,
            dataset=_MEMPOOL_DATASET,
            partition_id=current_partition_id,
        )
        return {
            'dataset': _MEMPOOL_DATASET,
            'job_name': _MEMPOOL_JOB_NAME,
            'requested_partition_id': requested_partition_id,
            'executed_partition_id': current_partition_id,
            'run_id': normalized_run_id,
            'dagster_run_id': dagster_run_id,
            'started_at_utc': completed_run.started_at_utc.isoformat(),
            'finished_at_utc': completed_run.finished_at_utc.isoformat(),
            'proof_summary': proof_summary,
        }
    finally:
        if client is not None:
            client.disconnect()
        if handle is not None:
            handle.workspace_process_context.__exit__(None, None, None)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Run repo-native Slice 34 Bitcoin backfill controllers: '
            'height-range chain batches or the explicit mempool daily path.'
        )
    )
    parser.add_argument(
        '--mode',
        required=True,
        choices=['chain', 'chain-sequence', 'mempool'],
    )
    parser.add_argument('--dataset', default=None)
    parser.add_argument('--plan-end-height', type=int, default=None)
    parser.add_argument('--batch-size-blocks', type=int, default=None)
    parser.add_argument('--projection-mode', required=True, choices=['inline', 'deferred'])
    parser.add_argument('--runtime-audit-mode', required=True, choices=['event', 'summary'])
    parser.add_argument('--run-id-prefix', default='s34-bitcoin')
    parser.add_argument('--run-id', default=None)
    parser.add_argument('--max-batches', type=int, default=None)
    parser.add_argument('--requested-partition-id', default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    projection_mode = _parse_projection_mode_or_raise(args.projection_mode)
    runtime_audit_mode = _parse_runtime_audit_mode_or_raise(args.runtime_audit_mode)

    if args.mode == 'chain':
        if args.dataset is None:
            raise RuntimeError('--dataset is required for --mode chain')
        if args.plan_end_height is None:
            raise RuntimeError('--plan-end-height is required for --mode chain')
        if args.batch_size_blocks is None:
            raise RuntimeError('--batch-size-blocks is required for --mode chain')
        print(
            run_bitcoin_chain_backfill_or_raise(
                dataset=_resolve_bitcoin_chain_dataset_or_raise(args.dataset),
                plan_end_height=args.plan_end_height,
                batch_size_blocks=args.batch_size_blocks,
                projection_mode=projection_mode,
                runtime_audit_mode=runtime_audit_mode,
                run_id_prefix=args.run_id_prefix,
                max_batches=args.max_batches,
            )
        )
        return

    if args.mode == 'chain-sequence':
        if args.plan_end_height is None:
            raise RuntimeError('--plan-end-height is required for --mode chain-sequence')
        if args.batch_size_blocks is None:
            raise RuntimeError('--batch-size-blocks is required for --mode chain-sequence')
        print(
            run_bitcoin_chain_sequence_controller_or_raise(
                plan_end_height=args.plan_end_height,
                batch_size_blocks=args.batch_size_blocks,
                projection_mode=projection_mode,
                runtime_audit_mode=runtime_audit_mode,
                run_id_prefix=args.run_id_prefix,
                max_batches_per_dataset=args.max_batches,
            )
        )
        return

    print(
        run_bitcoin_mempool_daily_path_or_raise(
            projection_mode=projection_mode,
            runtime_audit_mode=runtime_audit_mode,
            requested_partition_id=args.requested_partition_id,
            run_id=args.run_id,
        )
    )


if __name__ == '__main__':
    main()
