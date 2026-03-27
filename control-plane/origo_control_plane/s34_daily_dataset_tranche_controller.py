from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.backfill import (
    ExecutionMode,
    ProjectionMode,
    RuntimeAuditMode,
    S34BackfillDataset,
    S34DatasetBackfillContract,
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    list_s34_dataset_contracts,
    load_last_completed_daily_partition_from_canonical_or_raise,
    remaining_daily_partitions_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.s34_exchange_backfill_runner import run_exchange_backfill

_TERMINAL_PARTITION_STATES = ('proved_complete', 'empty_proved')


@dataclass(frozen=True)
class DailyBatchPlan:
    dataset: S34BackfillDataset
    execution_mode: ExecutionMode
    partition_ids: tuple[str, ...]
    batch_start_partition_id: str
    batch_end_partition_id: str
    end_date: date | None
    run_id: str


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


def _parse_iso_date_or_raise(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'Invalid ISO date={value!r}; expected YYYY-MM-DD') from exc


def _build_batch_run_id(
    *,
    dataset: S34BackfillDataset,
    execution_mode: ExecutionMode,
    batch_start_partition_id: str,
    batch_end_partition_id: str,
    run_id_prefix: str,
) -> str:
    return (
        f'{run_id_prefix}-{dataset}-{execution_mode}-'
        f'{batch_start_partition_id}-{batch_end_partition_id}'
    )


def _load_ambiguous_daily_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    dataset: S34BackfillDataset,
) -> tuple[str, ...]:
    contract = get_s34_dataset_contract(dataset)
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'Daily tranche controller requires daily partition scheme, '
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
    return tuple(str(row[0]) for row in rows)


def _parse_dataset_or_raise(value: str) -> S34BackfillDataset:
    normalized = value.strip()
    for contract in list_s34_dataset_contracts():
        if contract.dataset == normalized:
            return contract.dataset
    raise RuntimeError(f'Unknown S34 dataset={value!r}')


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


def _require_exchange_daily_dataset_contract_or_raise(
    dataset: S34BackfillDataset,
) -> S34DatasetBackfillContract:
    contract = get_s34_dataset_contract(dataset)
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'Daily tranche controller requires daily partition scheme, '
            f'got dataset={dataset} partition_scheme={contract.partition_scheme}'
        )
    if contract.phase not in {'exchange_primary', 'exchange_parallel'}:
        raise RuntimeError(
            'Daily tranche controller supports exchange datasets only, '
            f'got dataset={dataset} phase={contract.phase}'
        )
    return contract


def plan_next_daily_batch_or_raise(
    *,
    dataset: S34BackfillDataset,
    plan_end_date: date,
    batch_size_days: int,
    run_id_prefix: str = 's34-tranche',
    client: ClickHouseClient | None = None,
    database: str | None = None,
) -> DailyBatchPlan | None:
    assert_s34_backfill_contract_consistency_or_raise()
    if batch_size_days <= 0:
        raise RuntimeError(f'batch_size_days must be > 0, got {batch_size_days}')
    contract = _require_exchange_daily_dataset_contract_or_raise(dataset)

    if (client is None) != (database is None):
        raise RuntimeError(
            'plan_next_daily_batch_or_raise requires client and database to both '
            'be provided or both be None'
        )

    owns_client = client is None
    clickhouse_client = client
    clickhouse_database = database
    if clickhouse_client is None or clickhouse_database is None:
        clickhouse_client, clickhouse_database = _build_clickhouse_client_or_raise()

    try:
        ambiguous_partition_ids = _load_ambiguous_daily_partition_ids_or_raise(
            client=clickhouse_client,
            database=clickhouse_database,
            dataset=dataset,
        )
        if ambiguous_partition_ids != ():
            batch_partition_ids = ambiguous_partition_ids[:batch_size_days]
            return DailyBatchPlan(
                dataset=dataset,
                execution_mode='reconcile',
                partition_ids=batch_partition_ids,
                batch_start_partition_id=batch_partition_ids[0],
                batch_end_partition_id=batch_partition_ids[-1],
                end_date=None,
                run_id=_build_batch_run_id(
                    dataset=dataset,
                    execution_mode='reconcile',
                    batch_start_partition_id=batch_partition_ids[0],
                    batch_end_partition_id=batch_partition_ids[-1],
                    run_id_prefix=run_id_prefix,
                ),
            )

        last_completed_partition = load_last_completed_daily_partition_from_canonical_or_raise(
            client=clickhouse_client,
            database=clickhouse_database,
            contract=contract,
        )
        remaining_partitions = remaining_daily_partitions_or_raise(
            contract=contract,
            plan_end_date=plan_end_date,
            last_completed_partition=last_completed_partition,
        )
        if remaining_partitions == ():
            return None
        batch_partition_ids = remaining_partitions[:batch_size_days]
        batch_end_date = _parse_iso_date_or_raise(batch_partition_ids[-1])
        return DailyBatchPlan(
            dataset=dataset,
            execution_mode='backfill',
            partition_ids=batch_partition_ids,
            batch_start_partition_id=batch_partition_ids[0],
            batch_end_partition_id=batch_partition_ids[-1],
            end_date=batch_end_date,
            run_id=_build_batch_run_id(
                dataset=dataset,
                execution_mode='backfill',
                batch_start_partition_id=batch_partition_ids[0],
                batch_end_partition_id=batch_partition_ids[-1],
                run_id_prefix=run_id_prefix,
            ),
        )
    finally:
        if owns_client:
            clickhouse_client.disconnect()


def run_daily_dataset_tranche_controller_or_raise(
    *,
    dataset: S34BackfillDataset,
    plan_end_date: date,
    batch_size_days: int,
    concurrency: int,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    run_id_prefix: str = 's34-tranche',
    max_batches: int | None = None,
) -> dict[str, Any]:
    if max_batches is not None and max_batches <= 0:
        raise RuntimeError(f'max_batches must be > 0 when provided, got {max_batches}')

    completed_batches: list[dict[str, Any]] = []
    executed_batches = 0
    while True:
        if max_batches is not None and executed_batches >= max_batches:
            break
        batch = plan_next_daily_batch_or_raise(
            dataset=dataset,
            plan_end_date=plan_end_date,
            batch_size_days=batch_size_days,
            run_id_prefix=run_id_prefix,
        )
        if batch is None:
            break
        result = run_exchange_backfill(
            dataset=batch.dataset,
            end_date=batch.end_date,
            max_partitions=None,
            run_id=batch.run_id,
            dry_run=False,
            projection_mode=projection_mode,
            runtime_audit_mode=runtime_audit_mode,
            concurrency=concurrency,
            execution_mode=batch.execution_mode,
            partition_ids=(None if batch.execution_mode == 'backfill' else list(batch.partition_ids)),
        )
        completed_batches.append(
            {
                'dataset': batch.dataset,
                'execution_mode': batch.execution_mode,
                'batch_start_partition_id': batch.batch_start_partition_id,
                'batch_end_partition_id': batch.batch_end_partition_id,
                'run_id': batch.run_id,
                'result': result,
            }
        )
        executed_batches += 1

    return {
        'dataset': dataset,
        'plan_end_date': plan_end_date.isoformat(),
        'batch_size_days': batch_size_days,
        'completed_batch_count': len(completed_batches),
        'completed_batches': completed_batches,
        'controller_stopped_reason': (
            'max_batches_reached'
            if max_batches is not None and executed_batches >= max_batches
            else 'no_remaining_work'
        ),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Run contiguous daily backfill/reconcile tranches for a single S34 daily dataset '
            'using authoritative proof state.'
        )
    )
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--plan-end-date', required=True)
    parser.add_argument('--batch-size-days', type=int, required=True)
    parser.add_argument('--concurrency', type=int, required=True)
    parser.add_argument('--projection-mode', choices=['inline', 'deferred'], required=True)
    parser.add_argument('--runtime-audit-mode', choices=['event', 'summary'], required=True)
    parser.add_argument('--run-id-prefix', default='s34-tranche')
    parser.add_argument('--max-batches', type=int, default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run_daily_dataset_tranche_controller_or_raise(
        dataset=_parse_dataset_or_raise(args.dataset),
        plan_end_date=_parse_iso_date_or_raise(args.plan_end_date),
        batch_size_days=args.batch_size_days,
        concurrency=args.concurrency,
        projection_mode=_parse_projection_mode_or_raise(args.projection_mode),
        runtime_audit_mode=_parse_runtime_audit_mode_or_raise(args.runtime_audit_mode),
        run_id_prefix=args.run_id_prefix,
        max_batches=args.max_batches,
    )
    print(result)


if __name__ == '__main__':
    main()
