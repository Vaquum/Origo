from __future__ import annotations

import argparse
import importlib
import json
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickHouseClient
from dagster import build_asset_context

from origo.events import StreamQuarantineRegistry, load_stream_quarantine_state_path
from origo_control_plane.backfill import (
    BackfillRunStateStore,
    S34BackfillDataset,
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    load_backfill_manifest_log_path,
    load_backfill_run_state_path,
    record_partition_cursor_and_checkpoint_or_raise,
    remaining_daily_partitions_or_raise,
    write_backfill_manifest_event,
)
from origo_control_plane.config import resolve_clickhouse_native_settings

_DAILY_ASSET_RUNNER_IMPORTS: dict[S34BackfillDataset, tuple[str, str]] = {
    'binance_spot_trades': (
        'origo_control_plane.assets.daily_trades_to_origo',
        'insert_daily_binance_trades_to_origo',
    ),
    'okx_spot_trades': (
        'origo_control_plane.assets.daily_okx_spot_trades_to_origo',
        'insert_daily_okx_spot_trades_to_origo',
    ),
    'bybit_spot_trades': (
        'origo_control_plane.assets.daily_bybit_spot_trades_to_origo',
        'insert_daily_bybit_spot_trades_to_origo',
    ),
}
ProjectionMode = Literal['inline', 'deferred']
RuntimeAuditMode = Literal['event', 'summary']
_PATH_ENV_NAMES: tuple[str, str, str] = (
    'ORIGO_BACKFILL_RUN_STATE_PATH',
    'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
    'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
)
_MIN_DAILY_CONCURRENCY: int = 10


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


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


def _load_daily_asset_runner_or_raise(
    dataset: S34BackfillDataset,
) -> Callable[[Any], dict[str, Any]]:
    import_spec = _DAILY_ASSET_RUNNER_IMPORTS.get(dataset)
    if import_spec is None:
        raise RuntimeError(
            'S34 exchange backfill runner only supports exchange datasets, '
            f'got dataset={dataset}'
        )
    module_name, attribute_name = import_spec
    module = importlib.import_module(module_name)
    runner = getattr(module, attribute_name, None)
    if runner is None or not callable(runner):
        raise RuntimeError(
            'Failed to load exchange asset runner '
            f'for dataset={dataset} module={module_name} attr={attribute_name}'
        )
    return cast(Callable[[Any], dict[str, Any]], runner)


def _run_partition_asset_in_worker(
    payload: tuple[
        S34BackfillDataset,
        str,
        ProjectionMode,
        RuntimeAuditMode,
        str,
        str,
    ],
) -> tuple[str, dict[str, Any], datetime]:
    (
        dataset,
        partition_id,
        projection_mode,
        runtime_audit_mode,
        fast_insert_mode,
        runtime_audit_path,
    ) = payload
    os.environ['ORIGO_BACKFILL_PROJECTION_MODE'] = projection_mode
    os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'] = runtime_audit_mode
    os.environ['ORIGO_CANONICAL_FAST_INSERT_MODE'] = fast_insert_mode
    os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'] = runtime_audit_path
    runtime_audit_module = importlib.import_module('origo.events.runtime_audit')
    setattr(runtime_audit_module, '_runtime_audit_singleton', None)
    asset_runner = _load_daily_asset_runner_or_raise(dataset)
    partition_started_at_utc = datetime.now(UTC)
    context = build_asset_context(partition_key=partition_id)
    return partition_id, asset_runner(context), partition_started_at_utc


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(
            f'Invalid --end-date value={value!r}; expected YYYY-MM-DD'
        ) from exc


def _default_run_id(dataset: str) -> str:
    return (
        f's34-backfill-{dataset}-{datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")}'
    )


def run_exchange_backfill(
    *,
    dataset: S34BackfillDataset,
    end_date: date,
    max_partitions: int | None,
    run_id: str | None,
    dry_run: bool,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    concurrency: int,
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
    if concurrency < _MIN_DAILY_CONCURRENCY:
        raise RuntimeError(
            f'--concurrency must be >= {_MIN_DAILY_CONCURRENCY} for daily backfill'
        )
    normalized_projection_mode = _normalize_projection_mode_or_raise(projection_mode)
    normalized_runtime_audit_mode = _normalize_runtime_audit_mode_or_raise(
        runtime_audit_mode
    )

    normalized_run_id = (run_id or _default_run_id(dataset)).strip()
    if normalized_run_id == '':
        raise RuntimeError('run_id must be non-empty')

    run_state = BackfillRunStateStore(path=load_backfill_run_state_path())
    remaining_partitions = list(
        remaining_daily_partitions_or_raise(
            contract=contract,
            plan_end_date=end_date,
            run_state=run_state,
        )
    )
    if max_partitions is not None:
        remaining_partitions = remaining_partitions[:max_partitions]

    if dry_run:
        return {
            'dataset': dataset,
            'run_id': normalized_run_id,
            'dry_run': True,
            'planned_partitions': remaining_partitions,
            'planned_partition_count': len(remaining_partitions),
            'end_date': end_date.isoformat(),
        }

    settings = resolve_clickhouse_native_settings()
    manifest_path = load_backfill_manifest_log_path()
    quarantine_registry = StreamQuarantineRegistry(
        path=load_stream_quarantine_state_path()
    )

    processed: list[dict[str, Any]] = []
    client: ClickHouseClient | None = None
    previous_projection_mode = os.environ.get('ORIGO_BACKFILL_PROJECTION_MODE')
    previous_runtime_audit_mode = os.environ.get('ORIGO_CANONICAL_RUNTIME_AUDIT_MODE')
    previous_fast_insert_mode = os.environ.get('ORIGO_CANONICAL_FAST_INSERT_MODE')
    previous_runtime_audit_path = os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH']
    base_runtime_audit_path = Path(previous_runtime_audit_path)
    if not base_runtime_audit_path.is_absolute():
        base_runtime_audit_path = Path.cwd() / base_runtime_audit_path
    per_run_runtime_audit_path = base_runtime_audit_path.with_name(
        f'{base_runtime_audit_path.stem}-{normalized_run_id}{base_runtime_audit_path.suffix}'
    )
    os.environ['ORIGO_BACKFILL_PROJECTION_MODE'] = normalized_projection_mode
    os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'] = normalized_runtime_audit_mode
    os.environ['ORIGO_CANONICAL_FAST_INSERT_MODE'] = (
        'assume_new_partition'
        if normalized_projection_mode == 'deferred'
        else 'writer'
    )
    os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'] = str(
        per_run_runtime_audit_path
    )
    runtime_audit_module = importlib.import_module('origo.events.runtime_audit')
    setattr(runtime_audit_module, '_runtime_audit_singleton', None)
    fast_insert_mode = os.environ['ORIGO_CANONICAL_FAST_INSERT_MODE']
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
        partition_results: dict[str, dict[str, Any]] = {}
        partition_start_times: dict[str, datetime] = {}
        worker_payloads = [
            (
                dataset,
                partition_id,
                normalized_projection_mode,
                normalized_runtime_audit_mode,
                fast_insert_mode,
                str(
                    per_run_runtime_audit_path.with_name(
                        f'{per_run_runtime_audit_path.stem}-{partition_id}'
                        f'{per_run_runtime_audit_path.suffix}'
                    )
                ),
            )
            for partition_id in remaining_partitions
        ]
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            for partition_id, asset_result, partition_started_at_utc in executor.map(
                _run_partition_asset_in_worker,
                worker_payloads,
            ):
                partition_results[partition_id] = asset_result
                partition_start_times[partition_id] = partition_started_at_utc

        for partition_id in remaining_partitions:
            partition_started_at_utc = partition_start_times[partition_id]
            asset_result = partition_results[partition_id]
            checkpoint = record_partition_cursor_and_checkpoint_or_raise(
                client=client,
                database=settings.database,
                contract=contract,
                partition_id=partition_id,
                run_id=normalized_run_id,
                checked_at_utc=datetime.now(UTC),
                quarantine_registry=quarantine_registry,
            )
            run_state.mark_completed(
                dataset=dataset,
                partition_id=partition_id,
                run_id=normalized_run_id,
                completed_at_utc=datetime.now(UTC),
            )
            partition_event = {
                'event_type': 's34_backfill_partition_completed',
                'dataset': dataset,
                'source_id': contract.source_id,
                'stream_id': contract.stream_id,
                'partition_id': partition_id,
                'run_id': normalized_run_id,
                'completed_at_utc': datetime.now(UTC).isoformat(),
                'started_at_utc': partition_started_at_utc.isoformat(),
                'checkpoint': {
                    'observed_event_count': checkpoint.observed_event_count,
                    'expected_event_count': checkpoint.expected_event_count,
                    'gap_count': checkpoint.gap_count,
                    'cursor_status': checkpoint.cursor_status,
                    'checkpoint_status': checkpoint.checkpoint_status,
                    'last_source_offset_or_equivalent': (
                        checkpoint.last_source_offset_or_equivalent
                    ),
                },
                'asset_result': asset_result,
            }
            write_backfill_manifest_event(path=manifest_path, payload=partition_event)
            processed.append(partition_event)
    finally:
        if previous_projection_mode is None:
            os.environ.pop('ORIGO_BACKFILL_PROJECTION_MODE', None)
        else:
            os.environ['ORIGO_BACKFILL_PROJECTION_MODE'] = previous_projection_mode
        if previous_runtime_audit_mode is None:
            os.environ.pop('ORIGO_CANONICAL_RUNTIME_AUDIT_MODE', None)
        else:
            os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'] = (
                previous_runtime_audit_mode
            )
        if previous_fast_insert_mode is None:
            os.environ.pop('ORIGO_CANONICAL_FAST_INSERT_MODE', None)
        else:
            os.environ['ORIGO_CANONICAL_FAST_INSERT_MODE'] = previous_fast_insert_mode
        os.environ['ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH'] = (
            previous_runtime_audit_path
        )
        runtime_audit_module = importlib.import_module('origo.events.runtime_audit')
        setattr(runtime_audit_module, '_runtime_audit_singleton', None)
        if client is not None:
            client.disconnect()

    return {
        'dataset': dataset,
        'run_id': normalized_run_id,
        'dry_run': False,
        'processed_partition_count': len(processed),
        'processed_partitions': [item['partition_id'] for item in processed],
        'manifest_path': str(manifest_path),
        'end_date': end_date.isoformat(),
        'projection_mode': normalized_projection_mode,
        'runtime_audit_mode': normalized_runtime_audit_mode,
        'concurrency': concurrency,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Run S34 exchange dataset backfill partitions with resume controls.',
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
        required=True,
        help='Inclusive end date (YYYY-MM-DD).',
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
        help='Only print planned partitions without executing asset ingestion.',
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
        help=(
            'Runtime audit emission mode during backfill: event emits per-event audit '
            'rows, summary emits one immutable batch summary row per write batch.'
        ),
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=_MIN_DAILY_CONCURRENCY,
        help=(
            'Max number of partition workers to run in parallel. '
            'Use carefully to avoid memory pressure.'
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    result = run_exchange_backfill(
        dataset=cast(S34BackfillDataset, args.dataset),
        end_date=_parse_iso_date(args.end_date),
        max_partitions=args.max_partitions,
        run_id=args.run_id,
        dry_run=bool(args.dry_run),
        projection_mode=cast(ProjectionMode, args.projection_mode),
        runtime_audit_mode=cast(RuntimeAuditMode, args.runtime_audit_mode),
        concurrency=int(args.concurrency),
    )
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
