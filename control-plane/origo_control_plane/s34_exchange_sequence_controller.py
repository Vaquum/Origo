from __future__ import annotations

import argparse
from datetime import date
from typing import Any, cast

from origo_control_plane.backfill import (
    ProjectionMode,
    RuntimeAuditMode,
    S34BackfillDataset,
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    list_s34_dataset_contracts,
)
from origo_control_plane.s34_daily_dataset_tranche_controller import (
    run_daily_dataset_tranche_controller_or_raise,
)


def _parse_iso_date_or_raise(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'Invalid ISO date={value!r}; expected YYYY-MM-DD') from exc


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


def list_exchange_parallel_datasets_or_raise() -> tuple[S34BackfillDataset, ...]:
    assert_s34_backfill_contract_consistency_or_raise()
    datasets = cast(
        tuple[S34BackfillDataset, ...],
        tuple(
            contract.dataset
            for contract in list_s34_dataset_contracts()
            if contract.phase == 'exchange_parallel'
        ),
    )
    if datasets == ():
        raise RuntimeError('No S34 exchange_parallel datasets are configured')
    for dataset in datasets:
        contract = get_s34_dataset_contract(dataset)
        if contract.partition_scheme != 'daily':
            raise RuntimeError(
                'S34 exchange sequence controller requires daily partition datasets, '
                f'got dataset={dataset} partition_scheme={contract.partition_scheme}'
            )
    return datasets


def _resolve_exchange_sequence_datasets_or_raise(
    dataset_values: tuple[str, ...],
) -> tuple[S34BackfillDataset, ...]:
    default_datasets = list_exchange_parallel_datasets_or_raise()
    if dataset_values == ():
        return default_datasets

    rank_by_dataset = {
        contract.dataset: contract.execution_rank for contract in list_s34_dataset_contracts()
    }
    resolved: list[S34BackfillDataset] = []
    seen: set[S34BackfillDataset] = set()
    for raw_value in dataset_values:
        normalized = raw_value.strip()
        dataset = cast(S34BackfillDataset, normalized)
        contract = get_s34_dataset_contract(dataset)
        if contract.phase != 'exchange_parallel':
            raise RuntimeError(
                'S34 exchange sequence controller supports only exchange_parallel '
                f'datasets, got dataset={dataset} phase={contract.phase}'
            )
        if dataset in seen:
            raise RuntimeError(
                f'S34 exchange sequence controller received duplicate dataset={dataset}'
            )
        seen.add(dataset)
        resolved.append(dataset)

    expected_order = tuple(sorted(resolved, key=lambda dataset: rank_by_dataset[dataset]))
    actual_order = tuple(resolved)
    if actual_order != expected_order:
        raise RuntimeError(
            'S34 exchange sequence controller requires datasets in S34 contract order, '
            f'expected={expected_order} actual={actual_order}'
        )
    return actual_order


def run_exchange_sequence_controller_or_raise(
    *,
    plan_end_date: date,
    batch_size_days: int,
    concurrency: int,
    projection_mode: ProjectionMode,
    runtime_audit_mode: RuntimeAuditMode,
    run_id_prefix: str = 's34-exchange-sequence',
    datasets: tuple[S34BackfillDataset, ...] | None = None,
) -> dict[str, Any]:
    if batch_size_days <= 0:
        raise RuntimeError(f'batch_size_days must be > 0, got {batch_size_days}')
    if concurrency <= 0:
        raise RuntimeError(f'concurrency must be > 0, got {concurrency}')

    sequence_datasets = (
        list_exchange_parallel_datasets_or_raise() if datasets is None else datasets
    )
    completed_datasets: list[dict[str, Any]] = []
    for dataset in sequence_datasets:
        result = run_daily_dataset_tranche_controller_or_raise(
            dataset=dataset,
            plan_end_date=plan_end_date,
            batch_size_days=batch_size_days,
            concurrency=concurrency,
            projection_mode=projection_mode,
            runtime_audit_mode=runtime_audit_mode,
            run_id_prefix=run_id_prefix,
        )
        if result['controller_stopped_reason'] != 'no_remaining_work':
            raise RuntimeError(
                'S34 exchange sequence controller requires full dataset completion '
                f'before advancing, got dataset={dataset} '
                f"controller_stopped_reason={result['controller_stopped_reason']}"
            )
        completed_datasets.append(result)

    return {
        'datasets': sequence_datasets,
        'plan_end_date': plan_end_date.isoformat(),
        'batch_size_days': batch_size_days,
        'concurrency': concurrency,
        'completed_dataset_count': len(completed_datasets),
        'completed_datasets': completed_datasets,
        'controller_stopped_reason': 'no_remaining_work',
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Run S34 post-Binance exchange backfills in contract order '
            '(OKX then Bybit) using the daily tranche controller.'
        )
    )
    parser.add_argument('--dataset', action='append', default=[])
    parser.add_argument('--plan-end-date', required=True)
    parser.add_argument('--batch-size-days', type=int, required=True)
    parser.add_argument('--concurrency', type=int, required=True)
    parser.add_argument('--projection-mode', choices=['inline', 'deferred'], required=True)
    parser.add_argument('--runtime-audit-mode', choices=['event', 'summary'], required=True)
    parser.add_argument('--run-id-prefix', default='s34-exchange-sequence')
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run_exchange_sequence_controller_or_raise(
        plan_end_date=_parse_iso_date_or_raise(args.plan_end_date),
        batch_size_days=args.batch_size_days,
        concurrency=args.concurrency,
        projection_mode=_parse_projection_mode_or_raise(args.projection_mode),
        runtime_audit_mode=_parse_runtime_audit_mode_or_raise(args.runtime_audit_mode),
        run_id_prefix=args.run_id_prefix,
        datasets=_resolve_exchange_sequence_datasets_or_raise(tuple(args.dataset)),
    )
    print(result)


if __name__ == '__main__':
    main()
