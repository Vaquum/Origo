from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter

from .contracts import MetricValue, NormalizedMetricRecord, ProvenanceMetadata

_CANONICAL_SOURCE_ID = 'etf'
_CANONICAL_STREAM_ID = 'etf_daily_metrics'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware UTC datetime')
    return value.astimezone(UTC)


def _metric_value_kind_and_repr(value: MetricValue) -> tuple[str, str | None, bool | None]:
    if value is None:
        return ('null', None, None)
    if isinstance(value, bool):
        return ('bool', 'true' if value else 'false', value)
    if isinstance(value, int):
        return ('int', str(value), None)
    if isinstance(value, float):
        return ('float', format(value, '.17g'), None)
    return ('string', value, None)


def _provenance_payload(provenance: ProvenanceMetadata | None) -> dict[str, str] | None:
    if provenance is None:
        return None
    return {
        'source_id': provenance.source_id,
        'source_uri': provenance.source_uri,
        'artifact_id': provenance.artifact_id,
        'artifact_sha256': provenance.artifact_sha256,
        'fetch_method': provenance.fetch_method,
        'parser_name': provenance.parser_name,
        'parser_version': provenance.parser_version,
        'fetched_at_utc': _require_utc(
            provenance.fetched_at_utc,
            label='provenance.fetched_at_utc',
        ).isoformat(),
        'parsed_at_utc': _require_utc(
            provenance.parsed_at_utc,
            label='provenance.parsed_at_utc',
        ).isoformat(),
        'normalized_at_utc': _require_utc(
            provenance.normalized_at_utc,
            label='provenance.normalized_at_utc',
        ).isoformat(),
    }


def build_etf_canonical_payload(
    *,
    record: NormalizedMetricRecord,
) -> dict[str, Any]:
    observed_at_utc = _require_utc(record.observed_at_utc, label='record.observed_at_utc')
    metric_value_kind, metric_value_text, metric_value_bool = _metric_value_kind_and_repr(
        record.metric_value
    )
    return {
        'record_source_id': record.source_id,
        'metric_id': record.metric_id,
        'metric_name': record.metric_name,
        'metric_unit': record.metric_unit,
        'metric_value_kind': metric_value_kind,
        'metric_value_text': metric_value_text,
        'metric_value_bool': metric_value_bool,
        'observed_at_utc': observed_at_utc.isoformat(),
        'dimensions': {key: value for key, value in sorted(record.dimensions.items())},
        'provenance': _provenance_payload(record.provenance),
    }


@dataclass(frozen=True)
class ETFCanonicalWriteSummary:
    rows_processed: int
    rows_inserted: int
    rows_duplicate: int

    def to_dict(self) -> dict[str, int]:
        return {
            'rows_processed': self.rows_processed,
            'rows_inserted': self.rows_inserted,
            'rows_duplicate': self.rows_duplicate,
        }


def write_etf_normalized_records_to_canonical(
    *,
    client: ClickHouseClient,
    database: str,
    records: list[NormalizedMetricRecord],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> ETFCanonicalWriteSummary:
    if len(records) == 0:
        return ETFCanonicalWriteSummary(
            rows_processed=0,
            rows_inserted=0,
            rows_duplicate=0,
        )

    writer = CanonicalEventWriter(client=client, database=database)
    normalized_ingested_at_utc = _require_utc(
        ingested_at_utc,
        label='ingested_at_utc',
    )

    rows_inserted = 0
    rows_duplicate = 0
    for record in records:
        observed_at_utc = _require_utc(
            record.observed_at_utc,
            label='record.observed_at_utc',
        )
        payload_raw = json.dumps(
            build_etf_canonical_payload(record=record),
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        ).encode(_PAYLOAD_ENCODING)

        write_result = writer.write_event(
            CanonicalEventWriteInput(
                source_id=_CANONICAL_SOURCE_ID,
                stream_id=_CANONICAL_STREAM_ID,
                partition_id=observed_at_utc.date().isoformat(),
                source_offset_or_equivalent=record.metric_id,
                source_event_time_utc=observed_at_utc,
                ingested_at_utc=normalized_ingested_at_utc,
                payload_content_type=_PAYLOAD_CONTENT_TYPE,
                payload_encoding=_PAYLOAD_ENCODING,
                payload_raw=payload_raw,
                run_id=run_id,
            )
        )
        if write_result.status == 'inserted':
            rows_inserted += 1
        elif write_result.status == 'duplicate':
            rows_duplicate += 1
        else:
            raise RuntimeError(f'Unexpected canonical writer status: {write_result.status}')

    if rows_inserted + rows_duplicate != len(records):
        raise RuntimeError(
            'ETF canonical writer summary mismatch: '
            f'rows_inserted={rows_inserted} rows_duplicate={rows_duplicate} '
            f'rows_processed={len(records)}'
        )

    return ETFCanonicalWriteSummary(
        rows_processed=len(records),
        rows_inserted=rows_inserted,
        rows_duplicate=rows_duplicate,
    )
