from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date

from .client import (
    FREDClient,
    normalize_fred_series_metadata_payload_or_raise,
    normalize_fred_series_observations_payload_or_raise,
)
from .contracts import FREDSeriesRegistryEntry, FREDSeriesSnapshot
from .normalize import FREDLongMetricRow, normalize_fred_snapshots_to_long_metrics
from .persistence import FREDRawSeriesBundle


def _stable_hash_rows(rows: list[FREDLongMetricRow]) -> str:
    canonical_rows = [
        {
            'metric_id': row.metric_id,
            'source_id': row.source_id,
            'metric_name': row.metric_name,
            'metric_unit': row.metric_unit,
            'metric_value_string': row.metric_value_string,
            'metric_value_int': row.metric_value_int,
            'metric_value_float': row.metric_value_float,
            'metric_value_bool': row.metric_value_bool,
            'observed_at_utc': row.observed_at_utc.isoformat(),
            'dimensions_json': row.dimensions_json,
            'provenance_json': row.provenance_json,
        }
        for row in rows
    ]
    payload = json.dumps(canonical_rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class FREDBackfillSeriesResult:
    series_id: str
    source_id: str
    observation_start: date
    observation_end: date
    observation_count: int
    normalized_row_count: int

    def __post_init__(self) -> None:
        if self.series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        if self.source_id.strip() == '':
            raise ValueError('source_id must be non-empty')
        if self.observation_start > self.observation_end:
            raise ValueError(
                'observation_start must be <= observation_end, got '
                f'{self.observation_start.isoformat()} > {self.observation_end.isoformat()}'
            )
        if self.observation_count <= 0:
            raise ValueError('observation_count must be > 0')
        if self.normalized_row_count <= 0:
            raise ValueError('normalized_row_count must be > 0')


@dataclass(frozen=True)
class FREDBackfillResult:
    registry_version: str
    requested_start: date | None
    requested_end: date | None
    per_series: tuple[FREDBackfillSeriesResult, ...]
    rows: tuple[FREDLongMetricRow, ...]
    rows_hash_sha256: str

    def __post_init__(self) -> None:
        if self.registry_version.strip() == '':
            raise ValueError('registry_version must be non-empty')
        if len(self.per_series) == 0:
            raise ValueError('per_series must be non-empty')
        if len(self.rows) == 0:
            raise ValueError('rows must be non-empty')
        if self.rows_hash_sha256.strip() == '':
            raise ValueError('rows_hash_sha256 must be non-empty')


def run_fred_historical_backfill(
    *,
    client: FREDClient,
    registry_entries: list[FREDSeriesRegistryEntry],
    registry_version: str,
    observation_start: date | None = None,
    observation_end: date | None = None,
) -> FREDBackfillResult:
    if len(registry_entries) == 0:
        raise ValueError('registry_entries must be non-empty')
    if registry_version.strip() == '':
        raise ValueError('registry_version must be non-empty')
    if (
        observation_start is not None
        and observation_end is not None
        and observation_start > observation_end
    ):
        raise ValueError(
            'observation_start must be <= observation_end, got '
            f'{observation_start.isoformat()} > {observation_end.isoformat()}'
        )

    snapshots: list[FREDSeriesSnapshot] = []
    per_series: list[FREDBackfillSeriesResult] = []

    for entry in registry_entries:
        metadata = client.fetch_series_metadata(series_id=entry.series_id)

        series_start = metadata.observation_start
        series_end = metadata.observation_end

        effective_start = (
            series_start
            if observation_start is None
            else max(series_start, observation_start)
        )
        effective_end = (
            series_end if observation_end is None else min(series_end, observation_end)
        )
        if effective_start > effective_end:
            raise RuntimeError(
                'No overlapping backfill window for series_id='
                f'{entry.series_id} effective_start={effective_start.isoformat()} '
                f'effective_end={effective_end.isoformat()}'
            )

        observations = client.fetch_series_observations(
            series_id=entry.series_id,
            observation_start=effective_start,
            observation_end=effective_end,
            sort_order='asc',
            limit=None,
        )
        snapshots.append(
            FREDSeriesSnapshot(
                registry_entry=entry,
                metadata=metadata,
                observations=observations,
            )
        )

    normalized_rows = normalize_fred_snapshots_to_long_metrics(
        snapshots=snapshots,
        registry_version=registry_version,
    )

    rows_by_source: dict[str, int] = {}
    for row in normalized_rows:
        rows_by_source[row.source_id] = rows_by_source.get(row.source_id, 0) + 1

    for snapshot in snapshots:
        source_id = snapshot.registry_entry.source_id
        source_rows = rows_by_source.get(source_id, 0)
        if source_rows <= 0:
            raise RuntimeError(
                'Historical backfill produced zero normalized rows for source_id='
                f'{source_id}'
            )

        observed_dates = [observation.observation_date for observation in snapshot.observations]
        per_series.append(
            FREDBackfillSeriesResult(
                series_id=snapshot.registry_entry.series_id,
                source_id=source_id,
                observation_start=min(observed_dates),
                observation_end=max(observed_dates),
                observation_count=len(snapshot.observations),
                normalized_row_count=source_rows,
            )
        )

    per_series.sort(key=lambda item: (item.source_id, item.series_id))
    rows_hash = _stable_hash_rows(normalized_rows)
    return FREDBackfillResult(
        registry_version=registry_version,
        requested_start=observation_start,
        requested_end=observation_end,
        per_series=tuple(per_series),
        rows=tuple(normalized_rows),
        rows_hash_sha256=rows_hash,
    )


def normalize_fred_raw_bundles_to_long_metrics_or_raise(
    *,
    bundles: list[FREDRawSeriesBundle],
    registry_entries: list[FREDSeriesRegistryEntry],
    registry_version: str,
) -> list[FREDLongMetricRow]:
    if bundles == []:
        raise ValueError('bundles must be non-empty')
    if registry_entries == []:
        raise ValueError('registry_entries must be non-empty')
    if registry_version.strip() == '':
        raise ValueError('registry_version must be non-empty')

    registry_by_series_id = {entry.series_id: entry for entry in registry_entries}
    snapshots: list[FREDSeriesSnapshot] = []
    for bundle in bundles:
        if bundle.registry_version != registry_version:
            raise RuntimeError(
                'FRED raw bundle registry version mismatch: '
                f'bundle={bundle.registry_version} expected={registry_version} '
                f'series_id={bundle.series_id}'
            )
        registry_entry = registry_by_series_id.get(bundle.series_id)
        if registry_entry is None:
            raise RuntimeError(
                'FRED raw bundle series_id is missing from registry: '
                f'series_id={bundle.series_id}'
            )
        metadata = normalize_fred_series_metadata_payload_or_raise(
            series_id=bundle.series_id,
            payload=bundle.metadata_payload,
        )
        if (
            registry_entry.metric_unit is not None
            and registry_entry.metric_unit != metadata.units
        ):
            raise RuntimeError(
                'Registry metric_unit must match FRED metadata units, '
                f'series_id={bundle.series_id} registry={registry_entry.metric_unit} '
                f'metadata={metadata.units}'
            )
        observations = normalize_fred_series_observations_payload_or_raise(
            series_id=bundle.series_id,
            payload=bundle.observations_payload,
        )
        snapshots.append(
            FREDSeriesSnapshot(
                registry_entry=registry_entry,
                metadata=metadata,
                observations=observations,
            )
        )
    return normalize_fred_snapshots_to_long_metrics(
        snapshots=snapshots,
        registry_version=registry_version,
    )


@dataclass(frozen=True)
class FREDIncrementalSeriesResult:
    series_id: str
    source_id: str
    cursor_date: date | None
    effective_start: date
    effective_end: date
    status: str
    observation_count: int
    normalized_row_count: int

    def __post_init__(self) -> None:
        if self.series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        if self.source_id.strip() == '':
            raise ValueError('source_id must be non-empty')
        if self.status not in {'updated', 'no_new_data'}:
            raise ValueError(
                f"status must be 'updated' or 'no_new_data', got {self.status}"
            )
        if self.effective_start > self.effective_end:
            raise ValueError(
                'effective_start must be <= effective_end, got '
                f'{self.effective_start.isoformat()} > {self.effective_end.isoformat()}'
            )
        if self.observation_count < 0:
            raise ValueError('observation_count must be >= 0')
        if self.normalized_row_count < 0:
            raise ValueError('normalized_row_count must be >= 0')
        if self.status == 'updated':
            if self.observation_count <= 0:
                raise ValueError('updated status requires observation_count > 0')
            if self.normalized_row_count <= 0:
                raise ValueError('updated status requires normalized_row_count > 0')
        if self.status == 'no_new_data':
            if self.observation_count != 0:
                raise ValueError('no_new_data status requires observation_count == 0')
            if self.normalized_row_count != 0:
                raise ValueError(
                    'no_new_data status requires normalized_row_count == 0'
                )


@dataclass(frozen=True)
class FREDIncrementalResult:
    registry_version: str
    as_of_date: date
    per_series: tuple[FREDIncrementalSeriesResult, ...]
    rows: tuple[FREDLongMetricRow, ...]
    rows_hash_sha256: str

    def __post_init__(self) -> None:
        if self.registry_version.strip() == '':
            raise ValueError('registry_version must be non-empty')
        if len(self.per_series) == 0:
            raise ValueError('per_series must be non-empty')
        if self.rows_hash_sha256.strip() == '':
            raise ValueError('rows_hash_sha256 must be non-empty')


def run_fred_incremental_update(
    *,
    client: FREDClient,
    registry_entries: list[FREDSeriesRegistryEntry],
    registry_version: str,
    last_observed_by_source: dict[str, date],
    as_of_date: date,
) -> FREDIncrementalResult:
    if len(registry_entries) == 0:
        raise ValueError('registry_entries must be non-empty')
    if registry_version.strip() == '':
        raise ValueError('registry_version must be non-empty')

    snapshots: list[FREDSeriesSnapshot] = []
    per_series: list[FREDIncrementalSeriesResult] = []

    for entry in registry_entries:
        metadata = client.fetch_series_metadata(series_id=entry.series_id)

        cursor_date = last_observed_by_source.get(entry.source_id)
        if cursor_date is None:
            effective_start = metadata.observation_start
        else:
            effective_start = date.fromordinal(cursor_date.toordinal() + 1)
        effective_end = min(metadata.observation_end, as_of_date)

        if effective_start > effective_end:
            per_series.append(
                FREDIncrementalSeriesResult(
                    series_id=entry.series_id,
                    source_id=entry.source_id,
                    cursor_date=cursor_date,
                    effective_start=effective_end,
                    effective_end=effective_end,
                    status='no_new_data',
                    observation_count=0,
                    normalized_row_count=0,
                )
            )
            continue

        observations = client.fetch_series_observations(
            series_id=entry.series_id,
            observation_start=effective_start,
            observation_end=effective_end,
            sort_order='asc',
            limit=None,
        )
        if len(observations) == 0:
            per_series.append(
                FREDIncrementalSeriesResult(
                    series_id=entry.series_id,
                    source_id=entry.source_id,
                    cursor_date=cursor_date,
                    effective_start=effective_start,
                    effective_end=effective_end,
                    status='no_new_data',
                    observation_count=0,
                    normalized_row_count=0,
                )
            )
            continue

        snapshots.append(
            FREDSeriesSnapshot(
                registry_entry=entry,
                metadata=metadata,
                observations=observations,
            )
        )

    normalized_rows = normalize_fred_snapshots_to_long_metrics(
        snapshots=snapshots,
        registry_version=registry_version,
    )
    rows_by_source: dict[str, int] = {}
    for row in normalized_rows:
        rows_by_source[row.source_id] = rows_by_source.get(row.source_id, 0) + 1

    for snapshot in snapshots:
        source_id = snapshot.registry_entry.source_id
        observation_dates = [item.observation_date for item in snapshot.observations]
        normalized_row_count = rows_by_source.get(source_id, 0)
        if normalized_row_count <= 0:
            raise RuntimeError(
                'Incremental update produced zero normalized rows for source_id='
                f'{source_id}'
            )
        per_series.append(
            FREDIncrementalSeriesResult(
                series_id=snapshot.registry_entry.series_id,
                source_id=source_id,
                cursor_date=last_observed_by_source.get(source_id),
                effective_start=min(observation_dates),
                effective_end=max(observation_dates),
                status='updated',
                observation_count=len(snapshot.observations),
                normalized_row_count=normalized_row_count,
            )
        )

    per_series.sort(key=lambda item: (item.source_id, item.series_id))
    rows_hash = _stable_hash_rows(normalized_rows)
    return FREDIncrementalResult(
        registry_version=registry_version,
        as_of_date=as_of_date,
        per_series=tuple(per_series),
        rows=tuple(normalized_rows),
        rows_hash_sha256=rows_hash,
    )
