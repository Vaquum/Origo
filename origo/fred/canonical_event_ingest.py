from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter

from .normalize import FREDLongMetricRow

_CANONICAL_SOURCE_ID = 'fred'
_CANONICAL_STREAM_ID = 'fred_series_metrics'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware UTC datetime')
    return value.astimezone(UTC)


def _parse_json_object_or_none(*, value: str | None, label: str) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        raw = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'{label} must be valid JSON') from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f'{label} must decode to an object')
    object_value = cast(dict[Any, Any], raw)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in object_value.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} object keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _metric_value_kind_and_repr(
    row: FREDLongMetricRow,
) -> tuple[str, str | None, bool | None]:
    if row.metric_value_bool is not None:
        return ('bool', 'true' if row.metric_value_bool == 1 else 'false', row.metric_value_bool == 1)
    if row.metric_value_int is not None:
        return ('int', str(row.metric_value_int), None)
    if row.metric_value_float is not None:
        if row.metric_value_string is not None:
            return ('float', row.metric_value_string, None)
        return ('float', format(Decimal(str(row.metric_value_float)), 'f'), None)
    if row.metric_value_string is not None:
        return ('string', row.metric_value_string, None)
    return ('null', None, None)


def build_fred_canonical_payload(*, row: FREDLongMetricRow) -> dict[str, Any]:
    observed_at_utc = _require_utc(row.observed_at_utc, label='row.observed_at_utc')
    metric_value_kind, metric_value_text, metric_value_bool = _metric_value_kind_and_repr(
        row
    )
    dimensions = _parse_json_object_or_none(
        value=row.dimensions_json,
        label='row.dimensions_json',
    )
    if dimensions is None:
        raise RuntimeError('row.dimensions_json must be non-null object JSON')
    provenance = _parse_json_object_or_none(
        value=row.provenance_json,
        label='row.provenance_json',
    )
    return {
        'record_source_id': row.source_id,
        'metric_id': row.metric_id,
        'metric_name': row.metric_name,
        'metric_unit': row.metric_unit,
        'metric_value_kind': metric_value_kind,
        'metric_value_text': metric_value_text,
        'metric_value_bool': metric_value_bool,
        'observed_at_utc': observed_at_utc.isoformat(),
        'dimensions': {key: value for key, value in sorted(dimensions.items())},
        'provenance': (
            None
            if provenance is None
            else {key: value for key, value in sorted(provenance.items())}
        ),
    }


@dataclass(frozen=True)
class FREDCanonicalWriteSummary:
    rows_processed: int
    rows_inserted: int
    rows_duplicate: int

    def to_dict(self) -> dict[str, int]:
        return {
            'rows_processed': self.rows_processed,
            'rows_inserted': self.rows_inserted,
            'rows_duplicate': self.rows_duplicate,
        }


def write_fred_long_metrics_to_canonical(
    *,
    client: ClickHouseClient,
    database: str,
    rows: list[FREDLongMetricRow],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> FREDCanonicalWriteSummary:
    if len(rows) == 0:
        return FREDCanonicalWriteSummary(
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
    for row in rows:
        observed_at_utc = _require_utc(
            row.observed_at_utc,
            label='row.observed_at_utc',
        )
        payload_raw = json.dumps(
            build_fred_canonical_payload(row=row),
            ensure_ascii=True,
            sort_keys=True,
            separators=(',', ':'),
        ).encode(_PAYLOAD_ENCODING)
        write_result = writer.write_event(
            CanonicalEventWriteInput(
                source_id=_CANONICAL_SOURCE_ID,
                stream_id=_CANONICAL_STREAM_ID,
                partition_id=observed_at_utc.date().isoformat(),
                source_offset_or_equivalent=row.metric_id,
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

    if rows_inserted + rows_duplicate != len(rows):
        raise RuntimeError(
            'FRED canonical writer summary mismatch: '
            f'rows_inserted={rows_inserted} rows_duplicate={rows_duplicate} '
            f'rows_processed={len(rows)}'
        )

    return FREDCanonicalWriteSummary(
        rows_processed=len(rows),
        rows_inserted=rows_inserted,
        rows_duplicate=rows_duplicate,
    )
