from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickHouseClient
from dagster import DagsterInstance

# NOTE: Slice 34 Dagster partition submission currently depends on private
# dagster._core symbols validated against Dagster 1.12.17. Treat any Dagster
# upgrade as a breaking change for this module until the private imports are removed.
from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.execution.job_backfill import create_backfill_run
from dagster._core.remote_representation.code_location import CodeLocation
from dagster._core.remote_representation.external import (
    RemoteJob,
    RemotePartitionSet,
    RemoteRepository,
)
from dagster._core.remote_representation.external_data import (
    PartitionSetExecutionParamSnap,
)
from dagster._core.storage.dagster_run import NOT_FINISHED_STATUSES, DagsterRunStatus
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._core.workspace.load import load_workspace_process_context_from_yaml_paths
from dagster._time import get_current_timestamp

from origo.events import CanonicalBackfillStateStore, CanonicalStreamKey
from origo_control_plane.backfill import (
    BACKFILL_EXECUTION_MODE_TAG,
    BACKFILL_PROJECTION_MODE_TAG,
    BACKFILL_RUNTIME_AUDIT_MODE_TAG,
    S34BackfillDataset,
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    load_backfill_manifest_log_path,
    load_missing_daily_partitions_from_canonical_or_raise,
    write_backfill_manifest_event,
)
from origo_control_plane.config import resolve_clickhouse_native_settings

_DAILY_JOB_NAMES: dict[S34BackfillDataset, str] = {
    'binance_spot_trades': 'insert_daily_trades_to_origo_job',
    'okx_spot_trades': 'insert_daily_okx_spot_trades_to_origo_job',
    'bybit_spot_trades': 'insert_daily_bybit_spot_trades_to_origo_job',
}
ProjectionMode = Literal['inline', 'deferred']
RuntimeAuditMode = Literal['event', 'summary']
ExecutionMode = Literal['backfill', 'reconcile']
_PATH_ENV_NAMES: tuple[str, str] = (
    'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
    'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
)
_MIN_ENV_BACKFILL_CONCURRENCY: int = 10
_MIN_EXPLICIT_BACKFILL_CONCURRENCY: int = 1
_S34_BACKFILL_CONCURRENCY_ENV = 'ORIGO_S34_BACKFILL_CONCURRENCY'
_DAGSTER_HOME_ENV = 'DAGSTER_HOME'
_RUN_POLL_INTERVAL_SECONDS = 2.0


@dataclass(frozen=True)
class _DagsterPartitionHandles:
    workspace_process_context: Any
    request_context: BaseWorkspaceRequestContext
    code_location: CodeLocation
    remote_repository: RemoteRepository
    remote_job: RemoteJob
    remote_partition_set: RemotePartitionSet


@dataclass(frozen=True)
class _ActiveRun:
    partition_id: str
    run_id: str
    submitted_at_utc: datetime


@dataclass(frozen=True)
class _CompletedRun:
    partition_id: str
    run_id: str
    started_at_utc: datetime
    finished_at_utc: datetime


def _load_json_object_or_raise(*, payload: str, label: str) -> dict[str, Any]:
    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'{label} must be valid JSON object text') from exc
    if not isinstance(loaded, dict):
        raise RuntimeError(f'{label} must decode to JSON object, got {type(loaded).__name__}')
    return cast(dict[str, Any], loaded)


def _build_source_manifest_payload_or_raise(
    source_manifest: Any,
) -> dict[str, Any]:
    return {
        'manifest_revision': source_manifest.manifest_revision,
        'manifest_id': str(source_manifest.manifest_id),
        'offset_ordering': source_manifest.offset_ordering,
        'source_artifact_identity': _load_json_object_or_raise(
            payload=source_manifest.source_artifact_identity_json,
            label='source_artifact_identity_json',
        ),
        'source_row_count': source_manifest.source_row_count,
        'first_offset_or_equivalent': source_manifest.first_offset_or_equivalent,
        'last_offset_or_equivalent': source_manifest.last_offset_or_equivalent,
        'source_offset_digest_sha256': source_manifest.source_offset_digest_sha256,
        'source_identity_digest_sha256': source_manifest.source_identity_digest_sha256,
        'allow_empty_partition': source_manifest.allow_empty_partition,
        'manifested_by_run_id': source_manifest.manifested_by_run_id,
        'manifested_at_utc': source_manifest.manifested_at_utc.isoformat(),
    }


def _build_partition_proof_payload_or_raise(latest_proof: Any) -> dict[str, Any]:
    return {
        'proof_revision': latest_proof.proof_revision,
        'proof_id': str(latest_proof.proof_id),
        'state': latest_proof.state,
        'reason': latest_proof.reason,
        'offset_ordering': latest_proof.offset_ordering,
        'source_row_count': latest_proof.source_row_count,
        'canonical_row_count': latest_proof.canonical_row_count,
        'canonical_unique_offset_count': latest_proof.canonical_unique_offset_count,
        'first_offset_or_equivalent': latest_proof.first_offset_or_equivalent,
        'last_offset_or_equivalent': latest_proof.last_offset_or_equivalent,
        'source_offset_digest_sha256': latest_proof.source_offset_digest_sha256,
        'source_identity_digest_sha256': latest_proof.source_identity_digest_sha256,
        'canonical_offset_digest_sha256': latest_proof.canonical_offset_digest_sha256,
        'canonical_identity_digest_sha256': latest_proof.canonical_identity_digest_sha256,
        'gap_count': latest_proof.gap_count,
        'duplicate_count': latest_proof.duplicate_count,
        'proof_digest_sha256': latest_proof.proof_digest_sha256,
        'proof_details': _load_json_object_or_raise(
            payload=latest_proof.proof_details_json,
            label='proof_details_json',
        ),
        'recorded_by_run_id': latest_proof.recorded_by_run_id,
        'recorded_at_utc': latest_proof.recorded_at_utc.isoformat(),
    }


def _build_range_proof_payload_or_raise(range_proof: Any) -> dict[str, Any]:
    return {
        'range_revision': range_proof.range_revision,
        'range_proof_id': str(range_proof.range_proof_id),
        'range_start_partition_id': range_proof.range_start_partition_id,
        'range_end_partition_id': range_proof.range_end_partition_id,
        'partition_count': range_proof.partition_count,
        'range_digest_sha256': range_proof.range_digest_sha256,
        'range_details': _load_json_object_or_raise(
            payload=range_proof.range_details_json,
            label='range_details_json',
        ),
        'recorded_by_run_id': range_proof.recorded_by_run_id,
        'recorded_at_utc': range_proof.recorded_at_utc.isoformat(),
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _workspace_yaml_path_or_raise() -> Path:
    path = _repo_root() / 'control-plane' / 'workspace.yaml'
    if not path.is_file():
        raise RuntimeError(f'Dagster workspace.yaml is missing: {path}')
    return path


def _normalize_path_envs_or_raise() -> None:
    repo_root = _repo_root()
    for name in _PATH_ENV_NAMES:
        raw_value = os.environ.get(name)
        if raw_value is None or raw_value.strip() == '':
            raise RuntimeError(f'{name} must be set and non-empty')
        path = Path(raw_value)
        if not path.is_absolute():
            path = (repo_root / path).resolve()
        os.environ[name] = str(path)


def _normalize_projection_mode_or_raise(value: str) -> ProjectionMode:
    normalized = value.strip().lower()
    if normalized not in {'inline', 'deferred'}:
        raise RuntimeError(
            f'Invalid projection mode={value!r}; expected inline|deferred'
        )
    return cast(ProjectionMode, normalized)


def _normalize_runtime_audit_mode_or_raise(value: str) -> RuntimeAuditMode:
    normalized = value.strip().lower()
    if normalized not in {'event', 'summary'}:
        raise RuntimeError(
            f'Invalid runtime audit mode={value!r}; expected event|summary'
        )
    return cast(RuntimeAuditMode, normalized)


def _normalize_execution_mode_or_raise(value: str) -> ExecutionMode:
    normalized = value.strip().lower()
    if normalized not in {'backfill', 'reconcile'}:
        raise RuntimeError(
            f'Invalid execution mode={value!r}; expected backfill|reconcile'
        )
    return cast(ExecutionMode, normalized)


def _load_daily_job_name_or_raise(dataset: S34BackfillDataset) -> str:
    job_name = _DAILY_JOB_NAMES.get(dataset)
    if job_name is None:
        raise RuntimeError(
            'S34 exchange backfill runner only supports exchange datasets, '
            f'got dataset={dataset}'
        )
    return job_name


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(
            f'Invalid --end-date value={value!r}; expected YYYY-MM-DD'
        ) from exc


def _parse_daily_partition_id_or_raise(value: str, *, label: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise RuntimeError(
            f'{label} must be YYYY-MM-DD daily partition id, got {value!r}'
        ) from exc


def _normalize_reconcile_partition_ids_or_raise(
    *,
    contract: Any,
    partition_ids: list[str] | None,
) -> list[str]:
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'S34 exchange backfill runner reconcile path supports daily partition '
            f'datasets only, got partition_scheme={contract.partition_scheme}'
        )
    if partition_ids is None or partition_ids == []:
        raise RuntimeError(
            'execution_mode=reconcile requires at least one explicit --partition-id'
        )
    normalized = sorted(
        {
            _parse_daily_partition_id_or_raise(
                partition_id,
                label='partition_id',
            )
            for partition_id in partition_ids
        }
    )
    earliest_partition_date = contract.earliest_partition_date
    if earliest_partition_date is None:
        raise RuntimeError(
            'Daily partition contract must define earliest_partition_date for '
            f'dataset={contract.dataset}'
        )
    earliest_partition_id = earliest_partition_date.isoformat()
    for partition_id in normalized:
        if partition_id < earliest_partition_id:
            raise RuntimeError(
                'Reconcile partition is before contract earliest partition: '
                f'dataset={contract.dataset} '
                f'partition_id={partition_id} earliest={earliest_partition_id}'
            )
    return normalized


def _load_env_backfill_concurrency_or_raise() -> int:
    raw = os.environ.get(_S34_BACKFILL_CONCURRENCY_ENV)
    if raw is None or raw.strip() == '':
        raise RuntimeError(
            '--concurrency was not provided and '
            f'{_S34_BACKFILL_CONCURRENCY_ENV} is missing/empty'
        )
    try:
        concurrency = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f'{_S34_BACKFILL_CONCURRENCY_ENV} must be integer, got {raw!r}'
        ) from exc
    if concurrency < _MIN_ENV_BACKFILL_CONCURRENCY:
        raise RuntimeError(
            f'{_S34_BACKFILL_CONCURRENCY_ENV} must be >= {_MIN_ENV_BACKFILL_CONCURRENCY}, '
            f'got {concurrency}'
        )
    return concurrency


def _default_run_id(dataset: str) -> str:
    return f's34-backfill-{dataset}-{datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")}'


def _assert_requested_concurrency_allowed_or_raise(
    *,
    dataset: S34BackfillDataset,
    concurrency: int,
) -> None:
    contract = get_s34_dataset_contract(dataset)
    if concurrency < _MIN_EXPLICIT_BACKFILL_CONCURRENCY:
        raise RuntimeError(
            f'--concurrency must be >= {_MIN_EXPLICIT_BACKFILL_CONCURRENCY}, '
            f'got {concurrency}'
        )
    max_concurrent_partition_runs = contract.max_concurrent_partition_runs
    if (
        max_concurrent_partition_runs is not None
        and concurrency > max_concurrent_partition_runs
    ):
        raise RuntimeError(
            'Requested backfill concurrency exceeds source-safe partition-run limit: '
            f'dataset={dataset} requested={concurrency} '
            f'source_safe_limit={max_concurrent_partition_runs}'
        )


def _require_dagster_home_or_raise() -> str:
    value = os.environ.get(_DAGSTER_HOME_ENV)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{_DAGSTER_HOME_ENV} must be set and non-empty')
    return value


def _load_dagster_partition_handles_or_raise(
    *, instance: DagsterInstance, job_name: str
) -> _DagsterPartitionHandles:
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
            for remote_partition_set in remote_repository.get_partition_sets():
                if remote_partition_set.job_name == job_name:
                    return _DagsterPartitionHandles(
                        workspace_process_context=workspace_process_context,
                        request_context=request_context,
                        code_location=code_location,
                        remote_repository=remote_repository,
                        remote_job=remote_job,
                        remote_partition_set=remote_partition_set,
                    )
    workspace_process_context.__exit__(None, None, None)
    raise RuntimeError(
        f'Failed to resolve Dagster remote job + partition set for job_name={job_name}'
    )


def _build_partition_run_tags(
    *,
    dataset: S34BackfillDataset,
    run_id: str,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    execution_mode: ExecutionMode,
) -> dict[str, str]:
    return {
        BACKFILL_PROJECTION_MODE_TAG: projection_mode,
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: runtime_audit_mode,
        BACKFILL_EXECUTION_MODE_TAG: execution_mode,
        'origo.backfill.dataset': dataset,
        'origo.backfill.control_run_id': run_id,
    }


def _submit_partition_run_or_raise(
    *,
    handles: _DagsterPartitionHandles,
    instance: DagsterInstance,
    backfill_job: PartitionBackfill,
    partition_data: Any,
) -> _ActiveRun:
    dagster_run = create_backfill_run(
        request_context=handles.request_context,
        instance=instance,
        code_location=handles.code_location,
        remote_job=handles.remote_job,
        remote_partition_set=handles.remote_partition_set,
        backfill_job=backfill_job,
        partition_key_or_range=partition_data.name,
        run_tags=partition_data.tags,
        run_config=partition_data.run_config,
    )
    if dagster_run is None:
        raise RuntimeError(
            'Dagster refused to create partition run '
            f'for partition_id={partition_data.name}'
        )
    instance.submit_run(dagster_run.run_id, handles.request_context)
    return _ActiveRun(
        partition_id=str(partition_data.name),
        run_id=dagster_run.run_id,
        submitted_at_utc=datetime.now(UTC),
    )


def _load_completed_run_or_raise(
    *,
    instance: DagsterInstance,
    active_run: _ActiveRun,
) -> _CompletedRun | None:
    dagster_run = instance.get_run_by_id(active_run.run_id)
    if dagster_run is None:
        raise RuntimeError(
            'Dagster run disappeared after submission '
            f'for partition_id={active_run.partition_id} run_id={active_run.run_id}'
        )
    if dagster_run.status in NOT_FINISHED_STATUSES:
        return None
    if dagster_run.status != DagsterRunStatus.SUCCESS:
        raise RuntimeError(
            'Dagster partition run failed '
            f'for partition_id={active_run.partition_id} run_id={active_run.run_id} '
            f'status={dagster_run.status.value}'
        )
    run_record = instance.get_run_record_by_id(active_run.run_id)
    if run_record is None:
        raise RuntimeError(
            'Dagster run record missing after success '
            f'for partition_id={active_run.partition_id} run_id={active_run.run_id}'
        )
    start_timestamp = run_record.start_time
    end_timestamp = run_record.end_time
    started_at_utc = (
        datetime.fromtimestamp(start_timestamp, tz=UTC)
        if start_timestamp is not None
        else active_run.submitted_at_utc
    )
    finished_at_utc = (
        datetime.fromtimestamp(end_timestamp, tz=UTC)
        if end_timestamp is not None
        else datetime.now(UTC)
    )
    return _CompletedRun(
        partition_id=active_run.partition_id,
        run_id=active_run.run_id,
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
    )


def run_exchange_backfill(
    *,
    dataset: S34BackfillDataset,
    end_date: date | None,
    max_partitions: int | None,
    run_id: str | None,
    dry_run: bool,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    concurrency: int,
    execution_mode: ExecutionMode = 'backfill',
    partition_ids: list[str] | None = None,
) -> dict[str, Any]:
    _normalize_path_envs_or_raise()
    assert_s34_backfill_contract_consistency_or_raise()
    contract = get_s34_dataset_contract(dataset)
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'S34 exchange backfill runner supports daily partition datasets only, '
            f'got partition_scheme={contract.partition_scheme} dataset={dataset}'
        )
    if max_partitions is not None and max_partitions <= 0:
        raise RuntimeError('--max-partitions must be > 0 when set')
    _assert_requested_concurrency_allowed_or_raise(
        dataset=dataset,
        concurrency=concurrency,
    )
    normalized_projection_mode = _normalize_projection_mode_or_raise(projection_mode)
    normalized_runtime_audit_mode = _normalize_runtime_audit_mode_or_raise(
        runtime_audit_mode
    )
    normalized_execution_mode = _normalize_execution_mode_or_raise(execution_mode)
    normalized_run_id = (run_id or _default_run_id(dataset)).strip()
    if normalized_run_id == '':
        raise RuntimeError('run_id must be non-empty')

    settings = resolve_clickhouse_native_settings()
    manifest_path = load_backfill_manifest_log_path()
    processed: list[dict[str, Any]] = []
    range_proof = None
    client: ClickHouseClient | None = None
    dagster_instance: DagsterInstance | None = None
    dagster_handles: _DagsterPartitionHandles | None = None
    try:
        client = ClickHouseClient(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            database=settings.database,
            compression=True,
            send_receive_timeout=900,
        )
        state_store = CanonicalBackfillStateStore(
            client=client,
            database=settings.database,
        )
        last_completed_partition: str | None = None
        if normalized_execution_mode == 'reconcile':
            if max_partitions is not None:
                raise RuntimeError(
                    'execution_mode=reconcile does not allow --max-partitions'
                )
            remaining_partitions = _normalize_reconcile_partition_ids_or_raise(
                contract=contract,
                partition_ids=partition_ids,
            )
        else:
            if partition_ids not in (None, []):
                raise RuntimeError(
                    'execution_mode=backfill does not allow explicit --partition-id '
                    'selection'
                )
            if end_date is None:
                raise RuntimeError(
                    'execution_mode=backfill requires --end-date'
                )
            remaining_partitions = list(
                load_missing_daily_partitions_from_canonical_or_raise(
                    client=client,
                    database=settings.database,
                    contract=contract,
                    plan_end_date=end_date,
                )
            )
            if contract.earliest_partition_date is None:
                raise RuntimeError(
                    'Daily dataset contract must define earliest_partition_date '
                    f'for dataset={dataset}'
                )
            if remaining_partitions == []:
                last_completed_partition = end_date.isoformat()
            else:
                first_remaining_partition = date.fromisoformat(remaining_partitions[0])
                if first_remaining_partition <= contract.earliest_partition_date:
                    last_completed_partition = None
                else:
                    last_completed_partition = (
                        first_remaining_partition - timedelta(days=1)
                    ).isoformat()
            if max_partitions is not None:
                remaining_partitions = remaining_partitions[:max_partitions]

        for partition_id in remaining_partitions:
            state_store.assert_partition_can_execute_or_raise(
                stream_key=CanonicalStreamKey(
                    source_id=contract.source_id,
                    stream_id=contract.stream_id,
                    partition_id=partition_id,
                ),
                execution_mode=normalized_execution_mode,
            )

        if dry_run:
            return {
                'dataset': dataset,
                'run_id': normalized_run_id,
                'dry_run': True,
                'planned_partitions': remaining_partitions,
                'planned_partition_count': len(remaining_partitions),
                'last_completed_partition': last_completed_partition,
                'resume_state_source': 'canonical_backfill_partition_proofs',
                'end_date': None if end_date is None else end_date.isoformat(),
                'partition_ids': remaining_partitions,
                'execution_mode': normalized_execution_mode,
            }

        dagster_instance = DagsterInstance.get()
        dagster_handles = _load_dagster_partition_handles_or_raise(
            instance=dagster_instance,
            job_name=_load_daily_job_name_or_raise(dataset)
        )
        partition_execution_data = (
            dagster_handles.code_location.get_partition_set_execution_params(
                repository_handle=dagster_handles.remote_repository.handle,
                partition_set_name=dagster_handles.remote_partition_set.name,
                partition_names=remaining_partitions,
                instance=dagster_instance,
            )
        )
        if not isinstance(partition_execution_data, PartitionSetExecutionParamSnap):
            raise RuntimeError(
                'Dagster partition execution parameter lookup failed '
                f'for dataset={dataset}: {partition_execution_data.error}'
            )
        partition_data_by_id = {
            str(partition_data.name): partition_data
            for partition_data in partition_execution_data.partition_data
        }
        if sorted(partition_data_by_id) != sorted(remaining_partitions):
            raise RuntimeError(
                'Dagster partition execution params mismatch: '
                f'expected={sorted(remaining_partitions)} '
                f'observed={sorted(partition_data_by_id)}'
            )
        backfill_job = PartitionBackfill(
            backfill_id=normalized_run_id,
            partition_set_origin=dagster_handles.remote_partition_set.get_remote_origin(),
            status=BulkActionStatus.REQUESTED,
            partition_names=remaining_partitions,
            from_failure=False,
            reexecution_steps=None,
            tags=_build_partition_run_tags(
                dataset=dataset,
                run_id=normalized_run_id,
                projection_mode=normalized_projection_mode,
                runtime_audit_mode=normalized_runtime_audit_mode,
                execution_mode=normalized_execution_mode,
            ),
            backfill_timestamp=get_current_timestamp(),
        )

        pending_partition_ids = list(remaining_partitions)
        active_runs: dict[str, _ActiveRun] = {}
        completed_partitions: list[str] = []
        while pending_partition_ids or active_runs:
            while pending_partition_ids and len(active_runs) < concurrency:
                partition_id = pending_partition_ids.pop(0)
                active_run = _submit_partition_run_or_raise(
                    handles=dagster_handles,
                    instance=dagster_instance,
                    backfill_job=backfill_job,
                    partition_data=partition_data_by_id[partition_id],
                )
                active_runs[partition_id] = active_run

            if active_runs == {}:
                break

            time.sleep(_RUN_POLL_INTERVAL_SECONDS)
            for partition_id in tuple(active_runs):
                completed_run = _load_completed_run_or_raise(
                    instance=dagster_instance,
                    active_run=active_runs[partition_id],
                )
                if completed_run is None:
                    continue
                latest_proof = state_store.fetch_latest_partition_proof(
                    stream_key=CanonicalStreamKey(
                        source_id=contract.source_id,
                        stream_id=contract.stream_id,
                        partition_id=partition_id,
                    )
                )
                if latest_proof is None:
                    raise RuntimeError(
                        'Dagster partition run completed but no partition proof exists '
                        f'for dataset={dataset} partition_id={partition_id}'
                    )
                latest_source_manifest = state_store.fetch_latest_source_manifest(
                    stream_key=CanonicalStreamKey(
                        source_id=contract.source_id,
                        stream_id=contract.stream_id,
                        partition_id=partition_id,
                    )
                )
                if latest_source_manifest is None:
                    raise RuntimeError(
                        'Dagster partition run completed but no source manifest exists '
                        f'for dataset={dataset} partition_id={partition_id}'
                    )
                if latest_proof.state not in {'proved_complete', 'empty_proved'}:
                    raise RuntimeError(
                        'Dagster partition run completed without terminal proof state '
                        f'for dataset={dataset} partition_id={partition_id} '
                        f'state={latest_proof.state}'
                    )
                partition_event = {
                    'event_type': 's34_backfill_partition_completed',
                    'dataset': dataset,
                    'source_id': contract.source_id,
                    'stream_id': contract.stream_id,
                    'partition_id': partition_id,
                    'run_id': normalized_run_id,
                    'dagster_run_id': completed_run.run_id,
                    'execution_mode': normalized_execution_mode,
                    'completed_at_utc': completed_run.finished_at_utc.isoformat(),
                    'started_at_utc': completed_run.started_at_utc.isoformat(),
                    'source_manifest': _build_source_manifest_payload_or_raise(
                        latest_source_manifest
                    ),
                    'partition_proof': _build_partition_proof_payload_or_raise(
                        latest_proof
                    ),
                }
                write_backfill_manifest_event(path=manifest_path, payload=partition_event)
                processed.append(partition_event)
                completed_partitions.append(partition_id)
                del active_runs[partition_id]

        if completed_partitions != []:
            range_proof = state_store.record_range_proof(
                source_id=contract.source_id,
                stream_id=contract.stream_id,
                partition_ids=sorted(completed_partitions),
                run_id=normalized_run_id,
                recorded_at_utc=datetime.now(UTC),
            )
            write_backfill_manifest_event(
                path=manifest_path,
                payload={
                    'event_type': 's34_backfill_range_proved',
                    'dataset': dataset,
                    'source_id': contract.source_id,
                    'stream_id': contract.stream_id,
                    'run_id': normalized_run_id,
                    'range_proof': _build_range_proof_payload_or_raise(range_proof),
                },
            )
    finally:
        if dagster_handles is not None:
            dagster_handles.workspace_process_context.__exit__(None, None, None)
        if client is not None:
            client.disconnect()

    response: dict[str, Any] = {
        'dataset': dataset,
        'run_id': normalized_run_id,
        'dry_run': False,
        'processed_partition_count': len(processed),
        'processed_partitions': [item['partition_id'] for item in processed],
        'manifest_path': str(manifest_path),
        'end_date': None if end_date is None else end_date.isoformat(),
        'projection_mode': normalized_projection_mode,
        'runtime_audit_mode': normalized_runtime_audit_mode,
        'concurrency': concurrency,
        'execution_mode': normalized_execution_mode,
        'dagster_orchestration_mode': 'submit_and_poll_partition_runs',
    }
    if range_proof is not None:
        response['range_proof'] = {
            'range_start_partition_id': range_proof.range_start_partition_id,
            'range_end_partition_id': range_proof.range_end_partition_id,
            'partition_count': range_proof.partition_count,
            'range_digest_sha256': range_proof.range_digest_sha256,
        }
    return response


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Run S34 exchange dataset backfill partitions by submitting Dagster '
            'partition runs and validating ClickHouse proof state.'
        ),
    )
    parser.add_argument(
        '--dataset',
        required=True,
        choices=[
            'binance_spot_trades',
            'okx_spot_trades',
            'bybit_spot_trades',
        ],
        help='Dataset to backfill.',
    )
    parser.add_argument(
        '--end-date',
        default=None,
        help='Inclusive end date (YYYY-MM-DD).',
    )
    parser.add_argument(
        '--partition-id',
        action='append',
        default=None,
        help=(
            'Explicit daily partition id (YYYY-MM-DD). Required for reconcile mode; '
            'repeat to reconcile multiple partitions.'
        ),
    )
    parser.add_argument(
        '--max-partitions',
        type=int,
        default=None,
        help='Optional partition cap for this run.',
    )
    parser.add_argument(
        '--run-id',
        default=None,
        help='Optional explicit run id (defaults to generated timestamped id).',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help=(
            'Only print planned partitions without submitting Dagster partition runs; '
            'still requires a live ClickHouse connection to read proof state.'
        ),
    )
    parser.add_argument(
        '--projection-mode',
        choices=['inline', 'deferred'],
        default='deferred',
        help=(
            'Projection mode for partition ingest: inline writes native/aligned per '
            'partition, deferred writes canonical only for later bulk projection rebuild.'
        ),
    )
    parser.add_argument(
        '--runtime-audit-mode',
        choices=['event', 'summary'],
        default='summary',
        help='Runtime audit emission mode passed into Dagster run tags.',
    )
    parser.add_argument(
        '--execution-mode',
        choices=['backfill', 'reconcile'],
        default='backfill',
        help='Normal backfill refuses ambiguous/completed partitions; reconcile is explicit repair mode.',
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=None,
        help=(
            'Max number of Dagster partition runs to keep active at once. '
            f'If omitted, {_S34_BACKFILL_CONCURRENCY_ENV} is required.'
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    resolved_concurrency = (
        int(args.concurrency)
        if args.concurrency is not None
        else _load_env_backfill_concurrency_or_raise()
    )
    result = run_exchange_backfill(
        dataset=cast(S34BackfillDataset, args.dataset),
        end_date=None if args.end_date is None else _parse_iso_date(args.end_date),
        max_partitions=args.max_partitions,
        run_id=args.run_id,
        dry_run=bool(args.dry_run),
        projection_mode=cast(ProjectionMode, args.projection_mode),
        runtime_audit_mode=cast(RuntimeAuditMode, args.runtime_audit_mode),
        concurrency=resolved_concurrency,
        execution_mode=cast(ExecutionMode, args.execution_mode),
        partition_ids=cast(list[str] | None, args.partition_id),
    )
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
