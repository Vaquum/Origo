# pyright: reportUnusedFunction=false
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
from origo_control_plane.backfill.runtime_contract import (
    raise_forbidden_helper_write_path_or_raise,
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
    raise_forbidden_helper_write_path_or_raise(
        helper_name='s34_exchange_sequence_controller.run_exchange_sequence_controller_or_raise'
    )


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
    raise_forbidden_helper_write_path_or_raise(
        helper_name='s34_exchange_sequence_controller.main'
    )


if __name__ == '__main__':
    main()
