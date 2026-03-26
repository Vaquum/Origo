from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events import CanonicalBackfillStateStore
from origo_control_plane.config import require_env

from .s34_contract import S34DatasetBackfillContract

_BACKFILL_MANIFEST_LOG_PATH_ENV = 'ORIGO_BACKFILL_MANIFEST_LOG_PATH'
_NUMERIC_GAP_PREVIEW_LIMIT = 20


def _resolve_path_from_env(name: str) -> Path:
    raw = require_env(name)
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def load_backfill_manifest_log_path() -> Path:
    return _resolve_path_from_env(_BACKFILL_MANIFEST_LOG_PATH_ENV)


@dataclass(frozen=True)
class NumericGapSummary:
    min_offset: int
    max_offset: int
    expected_event_count: int
    observed_event_count: int
    gap_count: int
    duplicate_offset_count: int
    missing_offset_preview: tuple[int, ...]


def _parse_partition_date_or_raise(value: str, *, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be ISO date (YYYY-MM-DD), got {value!r}') from exc


def build_daily_partitions(
    *,
    start_date: date,
    end_date: date,
) -> tuple[str, ...]:
    if end_date < start_date:
        raise RuntimeError(
            'Daily partition window is invalid: '
            f'start_date={start_date.isoformat()} end_date={end_date.isoformat()}'
        )

    days: list[str] = []
    current = start_date
    while current <= end_date:
        days.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(days)


def remaining_daily_partitions_or_raise(
    *,
    contract: S34DatasetBackfillContract,
    plan_end_date: date,
    last_completed_partition: str | None,
) -> tuple[str, ...]:
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'remaining_daily_partitions_or_raise requires daily partition scheme, '
            f'got={contract.partition_scheme} for dataset={contract.dataset}'
        )
    if contract.earliest_partition_date is None:
        raise RuntimeError(
            'Daily partition contract must define earliest_partition_date for '
            f'dataset={contract.dataset}'
        )
    if plan_end_date < contract.earliest_partition_date:
        raise RuntimeError(
            'plan_end_date must be >= earliest_partition_date for '
            f'dataset={contract.dataset}, '
            f'earliest={contract.earliest_partition_date.isoformat()} '
            f'end={plan_end_date.isoformat()}'
        )

    planned = list(
        build_daily_partitions(
            start_date=contract.earliest_partition_date,
            end_date=plan_end_date,
        )
    )
    if last_completed_partition is None:
        return tuple(planned)

    last_day = _parse_partition_date_or_raise(
        last_completed_partition,
        label='last_completed_partition',
    )
    if last_day < contract.earliest_partition_date:
        raise RuntimeError(
            'last_completed_partition is before contract earliest_partition_date '
            f'for dataset={contract.dataset}: '
            f'last={last_day.isoformat()} '
            f'earliest={contract.earliest_partition_date.isoformat()}'
        )
    if last_day >= plan_end_date:
        return tuple()

    start_index = (last_day - contract.earliest_partition_date).days + 1
    return tuple(planned[start_index:])


def load_last_completed_daily_partition_from_canonical_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    contract: S34DatasetBackfillContract,
) -> str | None:
    if contract.partition_scheme != 'daily':
        raise RuntimeError(
            'load_last_completed_daily_partition_from_canonical_or_raise requires '
            f'daily partition scheme, got={contract.partition_scheme} '
            f'for dataset={contract.dataset}'
        )
    if contract.earliest_partition_date is None:
        raise RuntimeError(
            'Daily partition contract must define earliest_partition_date for '
            f'dataset={contract.dataset}'
        )
    return CanonicalBackfillStateStore(
        client=client,
        database=database,
    ).load_last_completed_daily_partition_or_raise(
        source_id=contract.source_id,
        stream_id=contract.stream_id,
        earliest_partition_date=contract.earliest_partition_date,
    )


def evaluate_numeric_offset_gaps_or_raise(
    *,
    offsets: list[str],
    missing_preview_limit: int = _NUMERIC_GAP_PREVIEW_LIMIT,
) -> NumericGapSummary:
    if offsets == []:
        raise RuntimeError('offsets must be non-empty')
    if missing_preview_limit <= 0:
        raise RuntimeError('missing_preview_limit must be > 0')

    parsed: list[int] = []
    for offset in offsets:
        normalized = offset.strip()
        if normalized == '':
            raise RuntimeError('offsets must not contain empty values')
        try:
            parsed.append(int(normalized))
        except ValueError as exc:
            raise RuntimeError(f'offset must be integer text, got {offset!r}') from exc

    sorted_offsets = sorted(parsed)
    unique_offsets = sorted(set(parsed))
    duplicates = len(sorted_offsets) - len(unique_offsets)
    min_offset = unique_offsets[0]
    max_offset = unique_offsets[-1]
    expected = (max_offset - min_offset) + 1
    observed = len(unique_offsets)
    gap_count = expected - observed

    missing_preview: list[int] = []
    previous = unique_offsets[0]
    for current in unique_offsets[1:]:
        delta = current - previous
        if delta > 1:
            for missing in range(previous + 1, current):
                missing_preview.append(missing)
                if len(missing_preview) >= missing_preview_limit:
                    break
        if len(missing_preview) >= missing_preview_limit:
            break
        previous = current

    return NumericGapSummary(
        min_offset=min_offset,
        max_offset=max_offset,
        expected_event_count=expected,
        observed_event_count=observed,
        gap_count=gap_count,
        duplicate_offset_count=duplicates,
        missing_offset_preview=tuple(missing_preview),
    )


def write_backfill_manifest_event(
    *,
    path: Path,
    payload: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + '\n')
