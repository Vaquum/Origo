from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime


def _require_non_empty(value: str, label: str) -> str:
    if value.strip() == '':
        raise ValueError(f'{label} must be non-empty')
    return value


def _require_utc_datetime(value: datetime, label: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f'{label} must include timezone information')
    return value.astimezone(UTC)


@dataclass(frozen=True)
class FREDSeriesRegistryEntry:
    series_id: str
    source_id: str
    metric_name: str
    metric_unit: str | None = None
    frequency_hint: str | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty(self.series_id, 'series_id')
        _require_non_empty(self.source_id, 'source_id')
        _require_non_empty(self.metric_name, 'metric_name')
        if self.metric_unit is not None:
            _require_non_empty(self.metric_unit, 'metric_unit')
        if self.frequency_hint is not None:
            _require_non_empty(self.frequency_hint, 'frequency_hint')
        if self.notes is not None:
            _require_non_empty(self.notes, 'notes')


@dataclass(frozen=True)
class FREDSeriesMetadata:
    series_id: str
    title: str
    units: str
    frequency: str
    seasonal_adjustment: str
    observation_start: date
    observation_end: date
    last_updated_utc: datetime
    popularity: int
    notes: str

    def __post_init__(self) -> None:
        _require_non_empty(self.series_id, 'series_id')
        _require_non_empty(self.title, 'title')
        _require_non_empty(self.units, 'units')
        _require_non_empty(self.frequency, 'frequency')
        _require_non_empty(self.seasonal_adjustment, 'seasonal_adjustment')
        object.__setattr__(
            self,
            'last_updated_utc',
            _require_utc_datetime(self.last_updated_utc, 'last_updated_utc'),
        )
        if self.observation_start > self.observation_end:
            raise ValueError(
                'observation_start must be <= observation_end, got '
                f'{self.observation_start.isoformat()} > {self.observation_end.isoformat()}'
            )
        if self.popularity < 0:
            raise ValueError(f'popularity must be >= 0, got {self.popularity}')


@dataclass(frozen=True)
class FREDObservation:
    series_id: str
    realtime_start: date
    realtime_end: date
    observation_date: date
    value: float | None
    raw_value: str

    def __post_init__(self) -> None:
        _require_non_empty(self.series_id, 'series_id')
        _require_non_empty(self.raw_value, 'raw_value')
        if self.realtime_start > self.realtime_end:
            raise ValueError(
                'realtime_start must be <= realtime_end, got '
                f'{self.realtime_start.isoformat()} > {self.realtime_end.isoformat()}'
            )


@dataclass(frozen=True)
class FREDSeriesSnapshot:
    registry_entry: FREDSeriesRegistryEntry
    metadata: FREDSeriesMetadata
    observations: Sequence[FREDObservation]

    def __post_init__(self) -> None:
        if self.registry_entry.series_id != self.metadata.series_id:
            raise ValueError(
                'registry_entry.series_id must match metadata.series_id, got '
                f'{self.registry_entry.series_id} != {self.metadata.series_id}'
            )
        if len(self.observations) == 0:
            raise ValueError('observations must be non-empty')
        for observation in self.observations:
            if observation.series_id != self.registry_entry.series_id:
                raise ValueError(
                    'observation series_id must match registry series_id, got '
                    f'{observation.series_id} != {self.registry_entry.series_id}'
                )
