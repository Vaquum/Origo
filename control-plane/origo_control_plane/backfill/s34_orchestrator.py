from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
    CursorAdvanceInput,
    StreamQuarantineRegistry,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)
from origo_control_plane.config import require_env

from .s34_contract import S34DatasetBackfillContract

_BACKFILL_RUN_STATE_PATH_ENV = 'ORIGO_BACKFILL_RUN_STATE_PATH'
_BACKFILL_MANIFEST_LOG_PATH_ENV = 'ORIGO_BACKFILL_MANIFEST_LOG_PATH'
_NUMERIC_GAP_PREVIEW_LIMIT = 20


def _resolve_path_from_env(name: str) -> Path:
    raw = require_env(name)
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def load_backfill_run_state_path() -> Path:
    return _resolve_path_from_env(_BACKFILL_RUN_STATE_PATH_ENV)


def load_backfill_manifest_log_path() -> Path:
    return _resolve_path_from_env(_BACKFILL_MANIFEST_LOG_PATH_ENV)


def _ensure_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware UTC datetime')
    return value.astimezone(UTC)


def _parse_partition_date_or_raise(value: str, *, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be ISO date (YYYY-MM-DD), got {value!r}') from exc


@dataclass(frozen=True)
class NumericGapSummary:
    min_offset: int
    max_offset: int
    expected_event_count: int
    observed_event_count: int
    gap_count: int
    duplicate_offset_count: int
    missing_offset_preview: tuple[int, ...]


@dataclass(frozen=True)
class PartitionCheckpointSummary:
    dataset: str
    partition_id: str
    observed_event_count: int
    expected_event_count: int
    gap_count: int
    cursor_status: str
    checkpoint_status: str
    last_source_offset_or_equivalent: str


class BackfillRunStateStore:
    def __init__(self, *, path: Path) -> None:
        self._path = path

    def read(self) -> dict[str, dict[str, str]]:
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f'Backfill run-state JSON is invalid at {self._path}: {exc.msg}'
            ) from exc
        if not isinstance(raw, dict):
            raise RuntimeError('Backfill run-state root must be an object')

        normalized: dict[str, dict[str, str]] = {}
        raw_map = cast(dict[Any, Any], raw)
        for dataset_raw, entry_raw in raw_map.items():
            if not isinstance(dataset_raw, str):
                raise RuntimeError('Backfill run-state dataset keys must be strings')
            if not isinstance(entry_raw, dict):
                raise RuntimeError(
                    f'Backfill run-state entry must be an object for dataset={dataset_raw!r}'
                )
            entry_map = cast(dict[Any, Any], entry_raw)
            entry_normalized: dict[str, str] = {}
            for key in ('last_completed_partition', 'updated_by_run_id', 'updated_at_utc'):
                value = entry_map.get(key)
                if not isinstance(value, str) or value.strip() == '':
                    raise RuntimeError(
                        f'Backfill run-state {key} must be non-empty string '
                        f'for dataset={dataset_raw!r}'
                    )
                entry_normalized[key] = value
            normalized[dataset_raw] = entry_normalized
        return normalized

    def last_completed_partition(self, *, dataset: str) -> str | None:
        payload = self.read()
        entry = payload.get(dataset)
        if entry is None:
            return None
        return entry['last_completed_partition']

    def mark_completed(
        self,
        *,
        dataset: str,
        partition_id: str,
        run_id: str,
        completed_at_utc: datetime,
    ) -> None:
        normalized_dataset = dataset.strip()
        normalized_partition_id = partition_id.strip()
        normalized_run_id = run_id.strip()
        if normalized_dataset == '':
            raise RuntimeError('dataset must be non-empty')
        if normalized_partition_id == '':
            raise RuntimeError('partition_id must be non-empty')
        if normalized_run_id == '':
            raise RuntimeError('run_id must be non-empty')

        payload = self.read()
        previous = payload.get(normalized_dataset)
        if previous is not None:
            previous_partition_id = previous['last_completed_partition']
            if normalized_partition_id < previous_partition_id:
                raise RuntimeError(
                    'Backfill run-state regression is not allowed: '
                    f'dataset={normalized_dataset!r} previous={previous_partition_id!r} '
                    f'next={normalized_partition_id!r}'
                )
            if normalized_partition_id == previous_partition_id:
                return

        payload[normalized_dataset] = {
            'last_completed_partition': normalized_partition_id,
            'updated_by_run_id': normalized_run_id,
            'updated_at_utc': _ensure_utc(
                completed_at_utc, label='completed_at_utc'
            ).isoformat(),
        }
        self._write(payload)

    def _write(self, payload: dict[str, dict[str, str]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(f'{self._path.suffix}.tmp')
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + '\n',
            encoding='utf-8',
        )
        tmp_path.replace(self._path)


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
    run_state: BackfillRunStateStore,
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
    last_completed = run_state.last_completed_partition(dataset=contract.dataset)
    if last_completed is None:
        return tuple(planned)

    last_day = _parse_partition_date_or_raise(
        last_completed, label='run_state.last_completed_partition'
    )
    if last_day < contract.earliest_partition_date:
        raise RuntimeError(
            'Run-state last_completed_partition is before contract earliest_partition_date '
            f'for dataset={contract.dataset}: '
            f'last={last_day.isoformat()} '
            f'earliest={contract.earliest_partition_date.isoformat()}'
        )
    if last_day >= plan_end_date:
        return tuple()

    start_index = (last_day - contract.earliest_partition_date).days + 1
    return tuple(planned[start_index:])


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


def _fetch_partition_offsets_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
) -> list[str]:
    rows = client.execute(
        f'''
        SELECT source_offset_or_equivalent
        FROM {database}.canonical_event_log
        WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id = %(partition_id)s
        ORDER BY source_offset_or_equivalent ASC
        ''',
        {
            'source_id': stream_key.source_id,
            'stream_id': stream_key.stream_id,
            'partition_id': stream_key.partition_id,
        },
    )
    offsets = [str(row[0]) for row in rows]
    if offsets == []:
        raise RuntimeError(
            'No canonical events found for backfill partition '
            f'{stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
        )
    return offsets


def record_partition_cursor_and_checkpoint_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    contract: S34DatasetBackfillContract,
    partition_id: str,
    run_id: str,
    checked_at_utc: datetime,
    quarantine_registry: StreamQuarantineRegistry,
) -> PartitionCheckpointSummary:
    normalized_partition_id = partition_id.strip()
    if normalized_partition_id == '':
        raise RuntimeError('partition_id must be non-empty')
    normalized_run_id = run_id.strip()
    if normalized_run_id == '':
        raise RuntimeError('run_id must be non-empty')
    normalized_checked_at_utc = _ensure_utc(
        checked_at_utc, label='checked_at_utc'
    )

    stream_key = CanonicalStreamKey(
        source_id=contract.source_id,
        stream_id=contract.stream_id,
        partition_id=normalized_partition_id,
    )
    offsets = _fetch_partition_offsets_or_raise(
        client=client,
        database=database,
        stream_key=stream_key,
    )

    expected_event_count: int
    observed_event_count: int
    gap_count: int
    min_offset = offsets[0]
    max_offset = offsets[-1]
    gap_details: dict[str, Any]
    if contract.offset_ordering == 'numeric':
        gap_summary = evaluate_numeric_offset_gaps_or_raise(
            offsets=offsets,
            missing_preview_limit=_NUMERIC_GAP_PREVIEW_LIMIT,
        )
        expected_event_count = gap_summary.expected_event_count
        observed_event_count = gap_summary.observed_event_count
        gap_count = gap_summary.gap_count
        min_offset = str(gap_summary.min_offset)
        max_offset = str(gap_summary.max_offset)
        gap_details = {
            'missing_offset_preview': list(gap_summary.missing_offset_preview),
            'duplicate_offset_count': gap_summary.duplicate_offset_count,
            'missing_offset_preview_truncated': (
                len(gap_summary.missing_offset_preview)
                == _NUMERIC_GAP_PREVIEW_LIMIT
            ),
        }
    else:
        unique_offsets = sorted(set(offsets))
        observed_event_count = len(unique_offsets)
        expected_event_count = observed_event_count
        gap_count = 0
        min_offset = unique_offsets[0]
        max_offset = unique_offsets[-1]
        gap_details = {
            'non_numeric_offset_ordering': contract.offset_ordering,
        }

    state_store = CanonicalIngestStateStore(
        client=client,
        database=database,
        quarantine_registry=quarantine_registry,
    )
    max_event_id = canonical_event_id_from_key(
        canonical_event_idempotency_key(
            source_id=contract.source_id,
            stream_id=contract.stream_id,
            partition_id=normalized_partition_id,
            source_offset_or_equivalent=max_offset,
        )
    )
    cursor_result = state_store.advance_cursor(
        CursorAdvanceInput(
            stream_key=stream_key,
            next_source_offset_or_equivalent=max_offset,
            event_id=max_event_id,
            offset_ordering=contract.offset_ordering,
            run_id=normalized_run_id,
            change_reason='s34_backfill_partition_checkpoint',
            ingested_at_utc=normalized_checked_at_utc,
            source_event_time_utc=None,
            updated_at_utc=normalized_checked_at_utc,
            expected_previous_source_offset_or_equivalent=None,
        )
    )

    checkpoint_result = state_store.record_completeness_checkpoint(
        CompletenessCheckpointInput(
            stream_key=stream_key,
            offset_ordering=contract.offset_ordering,
            check_scope_start_offset=min_offset,
            check_scope_end_offset=max_offset,
            last_checked_source_offset_or_equivalent=max_offset,
            expected_event_count=expected_event_count,
            observed_event_count=observed_event_count,
            gap_count=gap_count,
            status='gap_detected' if gap_count > 0 else 'ok',
            gap_details=gap_details,
            checked_by_run_id=normalized_run_id,
            checked_at_utc=normalized_checked_at_utc,
        )
    )

    if gap_count > 0:
        raise RuntimeError(
            'Backfill completeness gap detected and quarantined for '
            f'dataset={contract.dataset} partition_id={normalized_partition_id} '
            f'gap_count={gap_count} details={gap_details}'
        )

    return PartitionCheckpointSummary(
        dataset=contract.dataset,
        partition_id=normalized_partition_id,
        observed_event_count=observed_event_count,
        expected_event_count=expected_event_count,
        gap_count=gap_count,
        cursor_status=cursor_result.status,
        checkpoint_status=checkpoint_result.status,
        last_source_offset_or_equivalent=max_offset,
    )


def write_backfill_manifest_event(
    *,
    path: Path,
    payload: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + '\n')
