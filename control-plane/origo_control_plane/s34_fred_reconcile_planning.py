from __future__ import annotations

import os
from datetime import date, timedelta

_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN_ENV = (
    'ORIGO_S34_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN'
)
_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS_ENV = (
    'ORIGO_S34_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS'
)


def load_fred_reconcile_max_partitions_per_run_or_raise() -> int:
    raw = os.environ.get(_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN_ENV)
    if raw is None or raw.strip() == '':
        raise RuntimeError(
            f'{_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN_ENV} must be set and non-empty'
        )
    normalized = raw.strip()
    try:
        value = int(normalized)
    except ValueError as exc:
        raise RuntimeError(
            f'{_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN_ENV} must be an integer, '
            f"got '{normalized}'"
        ) from exc
    if value <= 0:
        raise RuntimeError(
            f'{_FRED_RECONCILE_MAX_PARTITIONS_PER_RUN_ENV} must be > 0, got {value}'
        )
    return value


def load_fred_reconcile_max_source_window_days_or_raise() -> int:
    raw = os.environ.get(_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS_ENV)
    if raw is None or raw.strip() == '':
        raise RuntimeError(
            f'{_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS_ENV} must be set and non-empty'
        )
    normalized = raw.strip()
    try:
        value = int(normalized)
    except ValueError as exc:
        raise RuntimeError(
            f'{_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS_ENV} must be an integer, '
            f"got '{normalized}'"
        ) from exc
    if value <= 0:
        raise RuntimeError(
            f'{_FRED_RECONCILE_MAX_SOURCE_WINDOW_DAYS_ENV} must be > 0, got {value}'
        )
    return value


def parse_fred_partition_id_as_date_or_raise(*, partition_id: str) -> date:
    try:
        return date.fromisoformat(partition_id)
    except ValueError as exc:
        raise RuntimeError(
            'FRED partition ids must be ISO dates, '
            f"got partition_id='{partition_id}'"
        ) from exc


def select_fred_reconcile_partition_ids_or_raise(
    *,
    ambiguous_partition_ids: tuple[str, ...],
    max_partitions_per_run: int,
    max_source_window_days: int,
) -> tuple[str, ...]:
    if ambiguous_partition_ids == ():
        raise RuntimeError('ambiguous_partition_ids must be non-empty')
    if max_partitions_per_run <= 0:
        raise RuntimeError('max_partitions_per_run must be > 0')
    if max_source_window_days <= 0:
        raise RuntimeError('max_source_window_days must be > 0')

    first_partition_date = parse_fred_partition_id_as_date_or_raise(
        partition_id=ambiguous_partition_ids[0]
    )
    max_partition_date = first_partition_date + timedelta(
        days=max_source_window_days - 1
    )
    selected_partition_ids: list[str] = []
    previous_partition_date = first_partition_date
    for partition_id in ambiguous_partition_ids:
        partition_date = parse_fred_partition_id_as_date_or_raise(
            partition_id=partition_id
        )
        if partition_date < previous_partition_date:
            raise RuntimeError(
                'FRED ambiguous partition ids must be sorted ascending, '
                f"got previous='{previous_partition_date.isoformat()}' "
                f"current='{partition_id}'"
            )
        if len(selected_partition_ids) >= max_partitions_per_run:
            break
        if partition_date > max_partition_date:
            break
        selected_partition_ids.append(partition_id)
        previous_partition_date = partition_date

    if selected_partition_ids == []:
        raise RuntimeError(
            'FRED reconcile tranche selection produced zero partition ids'
        )
    return tuple(selected_partition_ids)


def select_fred_reconcile_partition_ids_from_env_or_raise(
    *,
    ambiguous_partition_ids: tuple[str, ...],
) -> tuple[str, ...]:
    return select_fred_reconcile_partition_ids_or_raise(
        ambiguous_partition_ids=ambiguous_partition_ids,
        max_partitions_per_run=load_fred_reconcile_max_partitions_per_run_or_raise(),
        max_source_window_days=load_fred_reconcile_max_source_window_days_or_raise(),
    )
