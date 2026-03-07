from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from .contracts import FREDSeriesSnapshot


def _require_non_empty(value: str, label: str) -> str:
    if value.strip() == '':
        raise ValueError(f'{label} must be non-empty')
    return value


def _to_utc_midnight(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def _stable_hash_text(parts: list[str]) -> str:
    digest = hashlib.sha256()
    for index, part in enumerate(parts):
        if index > 0:
            digest.update(b'|')
        digest.update(part.encode('utf-8'))
    return digest.hexdigest()


@dataclass(frozen=True)
class FREDLongMetricRow:
    metric_id: str
    source_id: str
    metric_name: str
    metric_unit: str | None
    metric_value_string: str | None
    metric_value_int: int | None
    metric_value_float: float | None
    metric_value_bool: int | None
    observed_at_utc: datetime
    dimensions_json: str
    provenance_json: str | None

    def __post_init__(self) -> None:
        _require_non_empty(self.metric_id, 'metric_id')
        _require_non_empty(self.source_id, 'source_id')
        _require_non_empty(self.metric_name, 'metric_name')
        if self.metric_unit is not None:
            _require_non_empty(self.metric_unit, 'metric_unit')
        if self.metric_value_bool is not None:
            if self.metric_value_bool not in {0, 1}:
                raise ValueError(
                    f'metric_value_bool must be 0 or 1 when set, got {self.metric_value_bool}'
                )
        if self.observed_at_utc.tzinfo is None:
            raise ValueError('observed_at_utc must include timezone information')
        if self.observed_at_utc.astimezone(UTC) != self.observed_at_utc:
            raise ValueError('observed_at_utc must be normalized to UTC')
        _require_non_empty(self.dimensions_json, 'dimensions_json')
        if self.provenance_json is not None:
            _require_non_empty(self.provenance_json, 'provenance_json')


def _build_metric_id(
    *,
    source_id: str,
    metric_name: str,
    observation_date: date,
    realtime_start: date,
    realtime_end: date,
    raw_value: str,
) -> str:
    return _stable_hash_text(
        [
            source_id,
            metric_name,
            observation_date.isoformat(),
            realtime_start.isoformat(),
            realtime_end.isoformat(),
            raw_value,
        ]
    )


def _build_dimensions_json(snapshot: FREDSeriesSnapshot, *, observation_date: date) -> str:
    dimensions = {
        'frequency': snapshot.metadata.frequency,
        'observation_date': observation_date.isoformat(),
        'seasonal_adjustment': snapshot.metadata.seasonal_adjustment,
        'series_id': snapshot.registry_entry.series_id,
    }
    return json.dumps(dimensions, sort_keys=True, separators=(',', ':'))


def _build_provenance_json(
    snapshot: FREDSeriesSnapshot,
    *,
    registry_version: str,
    realtime_start: date,
    realtime_end: date,
) -> str:
    provenance = {
        'fetched_from': 'fred_api',
        'frequency': snapshot.metadata.frequency,
        'last_updated_utc': snapshot.metadata.last_updated_utc.isoformat(),
        'realtime_end': realtime_end.isoformat(),
        'realtime_start': realtime_start.isoformat(),
        'registry_version': registry_version,
        'seasonal_adjustment': snapshot.metadata.seasonal_adjustment,
        'series_id': snapshot.registry_entry.series_id,
        'source_uri': f'fred://series/{snapshot.registry_entry.series_id}',
        'units': snapshot.metadata.units,
    }
    return json.dumps(provenance, sort_keys=True, separators=(',', ':'))


def normalize_fred_snapshots_to_long_metrics(
    *,
    snapshots: list[FREDSeriesSnapshot],
    registry_version: str,
) -> list[FREDLongMetricRow]:
    if len(snapshots) == 0:
        raise ValueError('snapshots must be non-empty')
    _require_non_empty(registry_version, 'registry_version')

    normalized_rows: list[FREDLongMetricRow] = []
    for snapshot in snapshots:
        ordered_observations = sorted(
            snapshot.observations,
            key=lambda observation: (
                observation.observation_date,
                observation.realtime_start,
                observation.realtime_end,
                observation.raw_value,
            ),
        )
        for observation in ordered_observations:
            metric_id = _build_metric_id(
                source_id=snapshot.registry_entry.source_id,
                metric_name=snapshot.registry_entry.metric_name,
                observation_date=observation.observation_date,
                realtime_start=observation.realtime_start,
                realtime_end=observation.realtime_end,
                raw_value=observation.raw_value,
            )
            normalized_rows.append(
                FREDLongMetricRow(
                    metric_id=metric_id,
                    source_id=snapshot.registry_entry.source_id,
                    metric_name=snapshot.registry_entry.metric_name,
                    metric_unit=snapshot.registry_entry.metric_unit
                    if snapshot.registry_entry.metric_unit is not None
                    else snapshot.metadata.units,
                    metric_value_string=observation.raw_value,
                    metric_value_int=None,
                    metric_value_float=observation.value,
                    metric_value_bool=None,
                    observed_at_utc=_to_utc_midnight(observation.observation_date),
                    dimensions_json=_build_dimensions_json(
                        snapshot, observation_date=observation.observation_date
                    ),
                    provenance_json=_build_provenance_json(
                        snapshot,
                        registry_version=registry_version,
                        realtime_start=observation.realtime_start,
                        realtime_end=observation.realtime_end,
                    ),
                )
            )

    normalized_rows.sort(
        key=lambda row: (
            row.source_id,
            row.observed_at_utc,
            row.metric_name,
            row.metric_id,
        )
    )
    return normalized_rows


def long_metric_rows_to_json_rows(
    *,
    rows: list[FREDLongMetricRow],
) -> list[dict[str, Any]]:
    json_rows: list[dict[str, Any]] = []
    for row in rows:
        json_rows.append(
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
        )
    return json_rows
