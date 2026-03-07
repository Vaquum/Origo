from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from .contracts import (
    MetricValue,
    NormalizedMetricRecord,
    ParsedRecord,
    ProvenanceMetadata,
    RawArtifact,
    SourceDescriptor,
)

_DEFAULT_OBSERVED_AT_UTC = datetime(1970, 1, 1, tzinfo=UTC)


def _value_to_string(value: MetricValue) -> str:
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


def _build_metric_id(
    *,
    source_id: str,
    record_id: str,
    metric_name: str,
    metric_value: MetricValue,
) -> str:
    digest = hashlib.sha256()
    digest.update(source_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(record_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(metric_name.encode('utf-8'))
    digest.update(b'|')
    digest.update(_value_to_string(metric_value).encode('utf-8'))
    return digest.hexdigest()


def _build_provenance(
    *,
    source: SourceDescriptor,
    artifact: RawArtifact,
    parsed_record: ParsedRecord,
    normalized_at_utc: datetime,
) -> ProvenanceMetadata:
    return ProvenanceMetadata(
        source_id=source.source_id,
        source_uri=source.source_uri,
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact.content_sha256,
        fetch_method=artifact.fetch_method,
        parser_name=parsed_record.parser_name,
        parser_version=parsed_record.parser_version,
        fetched_at_utc=artifact.fetched_at_utc,
        parsed_at_utc=parsed_record.parsed_at_utc,
        normalized_at_utc=normalized_at_utc,
    )


def _extract_observed_at_utc(payload: dict[str, MetricValue]) -> datetime:
    raw_observed = payload.get('observed_at_utc')
    if isinstance(raw_observed, str) and raw_observed.strip() != '':
        normalized = raw_observed.replace('Z', '+00:00')
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            raise RuntimeError(
                'observed_at_utc payload field must include timezone information'
            )
        return parsed.astimezone(UTC)
    return _DEFAULT_OBSERVED_AT_UTC


def _extract_dimensions(payload: dict[str, MetricValue]) -> dict[str, str]:
    dimensions: dict[str, str] = {}
    for key in ('table_index', 'row_index', 'line_index', 'page_index'):
        value = payload.get(key)
        if value is None:
            continue
        dimensions[key] = _value_to_string(value)
    return dimensions


def normalize_parsed_records(
    *,
    parsed_records: list[ParsedRecord],
    source: SourceDescriptor,
    artifact: RawArtifact,
) -> list[NormalizedMetricRecord]:
    if len(parsed_records) == 0:
        raise ValueError('parsed_records must be non-empty')
    if source.source_id != artifact.source_id:
        raise RuntimeError(
            'source.source_id must match artifact.source_id for normalization'
        )

    normalized_records: list[NormalizedMetricRecord] = []

    for parsed_record in parsed_records:
        normalized_at_utc = datetime.now(UTC)
        provenance = _build_provenance(
            source=source,
            artifact=artifact,
            parsed_record=parsed_record,
            normalized_at_utc=normalized_at_utc,
        )
        observed_at_utc = _extract_observed_at_utc(parsed_record.payload)
        dimensions = _extract_dimensions(parsed_record.payload)

        for metric_name, metric_value in parsed_record.payload.items():
            if metric_name in {'table_index', 'row_index', 'line_index', 'page_index'}:
                continue
            metric_id = _build_metric_id(
                source_id=source.source_id,
                record_id=parsed_record.record_id,
                metric_name=metric_name,
                metric_value=metric_value,
            )
            normalized_records.append(
                NormalizedMetricRecord(
                    metric_id=metric_id,
                    source_id=source.source_id,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    metric_unit=None,
                    observed_at_utc=observed_at_utc,
                    dimensions=dict(dimensions),
                    provenance=provenance,
                )
            )

    if len(normalized_records) == 0:
        raise RuntimeError(
            'Normalization produced zero metrics. '
            'Ensure parsed payload includes metric fields.'
        )

    return normalized_records


def normalized_records_to_json_rows(
    *,
    records: list[NormalizedMetricRecord],
) -> list[dict[str, Any]]:
    if len(records) == 0:
        return []

    rows: list[dict[str, Any]] = []
    for record in records:
        provenance_payload = None
        if record.provenance is not None:
            provenance_payload = {
                'source_id': record.provenance.source_id,
                'source_uri': record.provenance.source_uri,
                'artifact_id': record.provenance.artifact_id,
                'artifact_sha256': record.provenance.artifact_sha256,
                'fetch_method': record.provenance.fetch_method,
                'parser_name': record.provenance.parser_name,
                'parser_version': record.provenance.parser_version,
                'fetched_at_utc': record.provenance.fetched_at_utc.isoformat(),
                'parsed_at_utc': record.provenance.parsed_at_utc.isoformat(),
                'normalized_at_utc': record.provenance.normalized_at_utc.isoformat(),
            }

        metric_value = record.metric_value
        value_string: str | None = None
        value_int: int | None = None
        value_float: float | None = None
        value_bool: int | None = None

        if metric_value is None:
            pass
        elif isinstance(metric_value, bool):
            value_bool = 1 if metric_value else 0
            value_string = 'true' if metric_value else 'false'
        elif isinstance(metric_value, int):
            value_int = metric_value
            value_float = float(metric_value)
            value_string = str(metric_value)
        elif isinstance(metric_value, float):
            value_float = metric_value
            value_string = str(metric_value)
        else:
            value_string = metric_value

        rows.append(
            {
                'metric_id': record.metric_id,
                'source_id': record.source_id,
                'metric_name': record.metric_name,
                'metric_unit': record.metric_unit,
                'metric_value_string': value_string,
                'metric_value_int': value_int,
                'metric_value_float': value_float,
                'metric_value_bool': value_bool,
                'observed_at_utc': record.observed_at_utc,
                'dimensions_json': json.dumps(
                    record.dimensions, sort_keys=True, separators=(',', ':')
                ),
                'provenance_json': (
                    json.dumps(
                        provenance_payload, sort_keys=True, separators=(',', ':')
                    )
                    if provenance_payload is not None
                    else None
                ),
            }
        )
    return rows
