from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from clickhouse_driver import Client as ClickhouseClient

from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime, ProjectorEvent

_CANONICAL_SOURCE_ID = 'etf'
_CANONICAL_STREAM_ID = 'etf_daily_metrics'
_PROJECTOR_ID = 'etf_daily_metrics_native_v1'
_TARGET_TABLE = 'canonical_etf_daily_metrics_native_v1'
_INSERT_COLUMNS = (
    'metric_id, source_id, metric_name, metric_unit, metric_value_string, '
    'metric_value_int, metric_value_float, metric_value_bool, observed_at_utc, '
    'dimensions_json, provenance_json, ingested_at_utc, event_id, '
    'source_offset_or_equivalent, source_event_time_utc'
)


def _require_non_empty(value: str, *, label: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be set and non-empty')
    return normalized


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware UTC datetime')
    return value.astimezone(UTC)


def _require_payload_object(payload_json: str) -> dict[str, Any]:
    payload_raw = json.loads(payload_json)
    if not isinstance(payload_raw, dict):
        raise RuntimeError('Canonical payload_json must decode to object')
    payload_map = cast(dict[Any, Any], payload_raw)
    payload: dict[str, Any] = {}
    for key, value in payload_map.items():
        if not isinstance(key, str):
            raise RuntimeError('Canonical payload_json keys must be strings')
        payload[key] = value
    return payload


def _require_str(payload: dict[str, Any], *, key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f'Payload key {key} must be string')
    return _require_non_empty(value, label=f'payload.{key}')


def _optional_str(payload: dict[str, Any], *, key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f'Payload key {key} must be string or null')
    normalized = value.strip()
    if normalized == '':
        return None
    return normalized


def _require_iso_datetime(payload: dict[str, Any], *, key: str) -> datetime:
    value = _require_str(payload, key=key)
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    return _require_utc(parsed, label=f'payload.{key}')


def _canonical_json_or_null(value: Any, *, key: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeError(f'Payload key {key} must be object or null')
    value_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in value_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'Payload key {key} object keys must be strings')
        normalized[raw_key] = raw_value
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    )


def _decode_metric_value(payload: dict[str, Any]) -> tuple[str | None, int | None, float | None, int | None]:
    metric_value_kind = _require_str(payload, key='metric_value_kind')
    metric_value_text = _optional_str(payload, key='metric_value_text')
    metric_value_bool_raw = payload.get('metric_value_bool')

    if metric_value_bool_raw is None:
        metric_value_bool: int | None = None
    elif isinstance(metric_value_bool_raw, bool):
        metric_value_bool = 1 if metric_value_bool_raw else 0
    else:
        raise RuntimeError('Payload key metric_value_bool must be bool or null')

    if metric_value_kind == 'null':
        return (None, None, None, None)
    if metric_value_kind == 'bool':
        if metric_value_bool is None or metric_value_text not in {'true', 'false'}:
            raise RuntimeError(
                'Boolean ETF payload requires metric_value_bool and metric_value_text=true|false'
            )
        return (metric_value_text, None, None, metric_value_bool)
    if metric_value_kind == 'int':
        if metric_value_text is None:
            raise RuntimeError('Integer ETF payload requires metric_value_text')
        try:
            value_int = int(metric_value_text)
        except ValueError as exc:
            raise RuntimeError('Integer ETF payload metric_value_text must parse as int') from exc
        return (metric_value_text, value_int, float(value_int), None)
    if metric_value_kind == 'float':
        if metric_value_text is None:
            raise RuntimeError('Float ETF payload requires metric_value_text')
        try:
            value_float = float(Decimal(metric_value_text))
        except InvalidOperation as exc:
            raise RuntimeError('Float ETF payload metric_value_text must parse as decimal') from exc
        return (metric_value_text, None, value_float, None)
    if metric_value_kind == 'string':
        if metric_value_text is None:
            raise RuntimeError('String ETF payload requires metric_value_text')
        return (metric_value_text, None, None, None)
    raise RuntimeError(f'Unsupported ETF metric_value_kind: {metric_value_kind}')


def _build_insert_row(event: ProjectorEvent) -> tuple[object, ...]:
    payload = _require_payload_object(event.payload_json)
    metric_id = _require_str(payload, key='metric_id')
    source_id = _require_str(payload, key='record_source_id')
    metric_name = _require_str(payload, key='metric_name')
    metric_unit = _optional_str(payload, key='metric_unit')
    observed_at_utc = _require_iso_datetime(payload, key='observed_at_utc')
    metric_value_string, metric_value_int, metric_value_float, metric_value_bool = (
        _decode_metric_value(payload)
    )
    dimensions_json = _canonical_json_or_null(payload.get('dimensions'), key='dimensions')
    if dimensions_json is None:
        raise RuntimeError('Payload key dimensions must be object')
    provenance_json = _canonical_json_or_null(payload.get('provenance'), key='provenance')

    return (
        metric_id,
        source_id,
        metric_name,
        metric_unit,
        metric_value_string,
        metric_value_int,
        metric_value_float,
        metric_value_bool,
        observed_at_utc,
        dimensions_json,
        provenance_json,
        _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
        event.event_id,
        event.source_offset_or_equivalent,
        event.source_event_time_utc,
    )


@dataclass(frozen=True)
class ETFNativeProjectorSummary:
    partitions_processed: int
    batches_processed: int
    events_processed: int
    rows_written: int

    def to_dict(self) -> dict[str, int]:
        return {
            'partitions_processed': self.partitions_processed,
            'batches_processed': self.batches_processed,
            'events_processed': self.events_processed,
            'rows_written': self.rows_written,
        }


def _aggregate_summaries(
    summaries: list[ETFNativeProjectorSummary],
) -> ETFNativeProjectorSummary:
    return ETFNativeProjectorSummary(
        partitions_processed=sum(summary.partitions_processed for summary in summaries),
        batches_processed=sum(summary.batches_processed for summary in summaries),
        events_processed=sum(summary.events_processed for summary in summaries),
        rows_written=sum(summary.rows_written for summary in summaries),
    )


def _project_partition(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int,
) -> ETFNativeProjectorSummary:
    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_PROJECTOR_ID,
        stream_key=CanonicalStreamKey(
            source_id=_CANONICAL_SOURCE_ID,
            stream_id=_CANONICAL_STREAM_ID,
            partition_id=partition_id,
        ),
        batch_size=batch_size,
    )

    runtime.start()
    batches_processed = 0
    events_processed = 0
    rows_written = 0
    try:
        while True:
            batch = runtime.fetch_next_batch()
            if batch == []:
                break
            batches_processed += 1
            events_processed += len(batch)
            insert_rows = [_build_insert_row(event) for event in batch]
            client.execute(
                f'''
                INSERT INTO {database}.{_TARGET_TABLE}
                ({_INSERT_COLUMNS})
                VALUES
                ''',
                insert_rows,
            )
            rows_written += len(insert_rows)
            runtime.commit_checkpoint(
                last_event=batch[-1],
                run_id=run_id,
                checkpointed_at_utc=projected_at_utc,
                state={
                    'projection': _TARGET_TABLE,
                    'rows_written': len(insert_rows),
                },
            )
    finally:
        runtime.stop()

    return ETFNativeProjectorSummary(
        partitions_processed=1,
        batches_processed=batches_processed,
        events_processed=events_processed,
        rows_written=rows_written,
    )


def project_etf_daily_metrics_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ETFNativeProjectorSummary:
    normalized_database = _require_non_empty(database, label='database')
    normalized_run_id = _require_non_empty(run_id, label='run_id')
    normalized_projected_at_utc = _require_utc(
        projected_at_utc,
        label='projected_at_utc',
    )
    normalized_partition_ids = sorted(
        {_require_non_empty(partition_id, label='partition_id') for partition_id in partition_ids}
    )
    if normalized_partition_ids == []:
        raise RuntimeError('partition_ids must contain at least one partition_id')

    summaries: list[ETFNativeProjectorSummary] = []
    for partition_id in normalized_partition_ids:
        summaries.append(
            _project_partition(
                client=client,
                database=normalized_database,
                partition_id=partition_id,
                run_id=normalized_run_id,
                projected_at_utc=normalized_projected_at_utc,
                batch_size=batch_size,
            )
        )
    return _aggregate_summaries(summaries)
