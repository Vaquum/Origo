from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable

FetchMethod = Literal['http', 'browser']
ArtifactFormat = Literal['html', 'json', 'csv', 'pdf', 'binary']
MetricValue = str | int | float | bool | None


def _require_non_empty(value: str, label: str) -> str:
    if value.strip() == '':
        raise ValueError(f'{label} must be non-empty')
    return value


def _require_utc_datetime(value: datetime, label: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f'{label} must include timezone information')
    return value.astimezone(UTC)


def _empty_str_dict() -> dict[str, str]:
    return {}


@dataclass(frozen=True)
class ScrapeRunContext:
    run_id: str
    started_at_utc: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.run_id, 'run_id')
        object.__setattr__(
            self,
            'started_at_utc',
            _require_utc_datetime(self.started_at_utc, 'started_at_utc'),
        )


@dataclass(frozen=True)
class SourceDescriptor:
    source_id: str
    source_name: str
    source_uri: str
    discovered_at_utc: datetime
    metadata: dict[str, str] = field(default_factory=_empty_str_dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.source_id, 'source_id')
        _require_non_empty(self.source_name, 'source_name')
        _require_non_empty(self.source_uri, 'source_uri')
        object.__setattr__(
            self,
            'discovered_at_utc',
            _require_utc_datetime(self.discovered_at_utc, 'discovered_at_utc'),
        )
        for key, value in self.metadata.items():
            _require_non_empty(key, 'metadata key')
            _require_non_empty(value, f'metadata[{key}]')


@dataclass(frozen=True)
class RawArtifact:
    artifact_id: str
    source_id: str
    source_uri: str
    fetched_at_utc: datetime
    fetch_method: FetchMethod
    artifact_format: ArtifactFormat
    content_sha256: str
    content: bytes
    metadata: dict[str, str] = field(default_factory=_empty_str_dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.artifact_id, 'artifact_id')
        _require_non_empty(self.source_id, 'source_id')
        _require_non_empty(self.source_uri, 'source_uri')
        _require_non_empty(self.content_sha256, 'content_sha256')
        object.__setattr__(
            self,
            'fetched_at_utc',
            _require_utc_datetime(self.fetched_at_utc, 'fetched_at_utc'),
        )
        if len(self.content) == 0:
            raise ValueError('content must be non-empty')
        for key, value in self.metadata.items():
            _require_non_empty(key, 'metadata key')
            _require_non_empty(value, f'metadata[{key}]')


@dataclass(frozen=True)
class PersistedRawArtifact:
    artifact_id: str
    storage_uri: str
    manifest_uri: str
    persisted_at_utc: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.artifact_id, 'artifact_id')
        _require_non_empty(self.storage_uri, 'storage_uri')
        _require_non_empty(self.manifest_uri, 'manifest_uri')
        object.__setattr__(
            self,
            'persisted_at_utc',
            _require_utc_datetime(self.persisted_at_utc, 'persisted_at_utc'),
        )


@dataclass(frozen=True)
class ParsedRecord:
    record_id: str
    artifact_id: str
    payload: dict[str, MetricValue]
    parser_name: str
    parser_version: str
    parsed_at_utc: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.record_id, 'record_id')
        _require_non_empty(self.artifact_id, 'artifact_id')
        _require_non_empty(self.parser_name, 'parser_name')
        _require_non_empty(self.parser_version, 'parser_version')
        object.__setattr__(
            self,
            'parsed_at_utc',
            _require_utc_datetime(self.parsed_at_utc, 'parsed_at_utc'),
        )
        if len(self.payload) == 0:
            raise ValueError('payload must contain at least one field')
        for key in self.payload:
            _require_non_empty(key, 'payload key')


@dataclass(frozen=True)
class ProvenanceMetadata:
    source_id: str
    source_uri: str
    artifact_id: str
    artifact_sha256: str
    fetch_method: FetchMethod
    parser_name: str
    parser_version: str
    fetched_at_utc: datetime
    parsed_at_utc: datetime
    normalized_at_utc: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.source_id, 'source_id')
        _require_non_empty(self.source_uri, 'source_uri')
        _require_non_empty(self.artifact_id, 'artifact_id')
        _require_non_empty(self.artifact_sha256, 'artifact_sha256')
        _require_non_empty(self.parser_name, 'parser_name')
        _require_non_empty(self.parser_version, 'parser_version')
        object.__setattr__(
            self,
            'fetched_at_utc',
            _require_utc_datetime(self.fetched_at_utc, 'fetched_at_utc'),
        )
        object.__setattr__(
            self,
            'parsed_at_utc',
            _require_utc_datetime(self.parsed_at_utc, 'parsed_at_utc'),
        )
        object.__setattr__(
            self,
            'normalized_at_utc',
            _require_utc_datetime(self.normalized_at_utc, 'normalized_at_utc'),
        )


@dataclass(frozen=True)
class NormalizedMetricRecord:
    metric_id: str
    source_id: str
    metric_name: str
    metric_value: MetricValue
    metric_unit: str | None
    observed_at_utc: datetime
    dimensions: dict[str, str] = field(default_factory=_empty_str_dict)
    provenance: ProvenanceMetadata | None = None

    def __post_init__(self) -> None:
        _require_non_empty(self.metric_id, 'metric_id')
        _require_non_empty(self.source_id, 'source_id')
        _require_non_empty(self.metric_name, 'metric_name')
        object.__setattr__(
            self,
            'observed_at_utc',
            _require_utc_datetime(self.observed_at_utc, 'observed_at_utc'),
        )
        if self.metric_unit is not None:
            _require_non_empty(self.metric_unit, 'metric_unit')
        for key, value in self.dimensions.items():
            _require_non_empty(key, 'dimensions key')
            _require_non_empty(value, f'dimensions[{key}]')


@runtime_checkable
class ScraperAdapter(Protocol):
    """Source-agnostic contract for scraper adapters."""

    adapter_name: str

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]: ...

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact: ...

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]: ...

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]: ...
