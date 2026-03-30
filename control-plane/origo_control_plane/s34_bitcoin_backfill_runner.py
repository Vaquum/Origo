from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from clickhouse_driver import Client as ClickHouseClient
from dagster import DagsterInstance
from dagster._core.execution.api import create_execution_plan
from dagster._core.remote_representation.code_location import CodeLocation
from dagster._core.remote_representation.external import RemoteJob, RemoteRepository
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES, DagsterRunStatus
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._core.workspace.load import load_workspace_process_context_from_yaml_paths

from origo.events import (
    LATEST_PARTITION_PROOF_ARGMAX_KEY_SQL,
    CanonicalBackfillStateStore,
    CanonicalStreamKey,
)
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
from origo_control_plane.backfill.runtime_contract import (
    raise_forbidden_helper_write_path_or_raise,
)
from origo_control_plane.bitcoin_core import (
    format_bitcoin_height_range_partition_id_or_raise,
    parse_bitcoin_height_range_partition_id_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.s34_partition_authority import (
    load_nonterminal_partition_ids_for_stream_or_raise,
)

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


def _create_and_submit_job_run_or_raise(
    *,
    instance: DagsterInstance,
    handle: _DagsterJobHandle,
    job_def: Any,
    control_run_id: str,
    tags: dict[str, str],
) -> str:
    dagster_run_id = str(uuid4())
    execution_plan = create_execution_plan(
        job_def,
        run_config={},
        instance_ref=instance.get_ref(),
        tags=tags,
    )
    dagster_run = instance.create_run_for_job(
        job_def=job_def,
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
    rows = load_nonterminal_partition_ids_for_stream_or_raise(
        client=client,
        database=database,
        source_id=contract.source_id,
        stream_id=contract.stream_id,
        terminal_states=_TERMINAL_PARTITION_STATES,
    )
    normalized: list[str] = []
    for row in rows:
        partition_id = str(row)
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
                argMax(state, {LATEST_PARTITION_PROOF_ARGMAX_KEY_SQL}) AS state
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
    raise_forbidden_helper_write_path_or_raise(
        helper_name='s34_bitcoin_backfill_runner.run_bitcoin_chain_backfill_or_raise'
    )


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
    raise_forbidden_helper_write_path_or_raise(
        helper_name='s34_bitcoin_backfill_runner.run_bitcoin_chain_sequence_controller_or_raise'
    )


def run_bitcoin_mempool_daily_path_or_raise(
    *,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    requested_partition_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    raise_forbidden_helper_write_path_or_raise(
        helper_name='s34_bitcoin_backfill_runner.run_bitcoin_mempool_daily_path_or_raise'
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Historical Slice 34 Bitcoin helper surface only; '
            'write execution is disabled.'
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
    raise_forbidden_helper_write_path_or_raise(
        helper_name='s34_bitcoin_backfill_runner.main'
    )


if __name__ == '__main__':
    main()
