from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from clickhouse_driver import Client as ClickhouseClient

from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime, ProjectorEvent
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)

_CANONICAL_SOURCE_ID = 'okx'
_CANONICAL_STREAM_ID = 'okx_spot_trades'
_PROJECTOR_ID = 'okx_spot_trades_native_v1'
_TARGET_TABLE = 'canonical_okx_spot_trades_native_v1'
_INSERT_COLUMNS = (
    'instrument_name, trade_id, side, price, size, quote_quantity, timestamp, datetime, '
    'event_id, source_offset_or_equivalent, source_event_time_utc, ingested_at_utc'
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


def _require_payload(payload_json: str) -> dict[str, Any]:
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
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'Payload key {key} must be non-empty string')
    return normalized


def _require_int(payload: dict[str, Any], *, key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise RuntimeError(f'Payload key {key} must not be bool')
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(f'Payload key {key} must be integer') from exc
    raise RuntimeError(f'Payload key {key} must be int|string')


def _require_decimal_float(payload: dict[str, Any], *, key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool):
        raise RuntimeError(f'Payload key {key} must not be bool')
    if isinstance(value, (int, str)):
        try:
            return float(Decimal(str(value)))
        except InvalidOperation as exc:
            raise RuntimeError(f'Payload key {key} must be decimal') from exc
    raise RuntimeError(f'Payload key {key} must be int|string decimal')


def _require_source_event_time(event: ProjectorEvent) -> datetime:
    if event.source_event_time_utc is None:
        raise RuntimeError(
            'OKX native projector requires source_event_time_utc for every event'
        )
    return _require_utc(event.source_event_time_utc, label='event.source_event_time_utc')


def _build_insert_row(event: ProjectorEvent) -> tuple[tuple[object, ...], tuple[object, ...]]:
    payload = _require_payload(event.payload_json)
    source_event_time_utc = _require_source_event_time(event)
    instrument_name = _require_str(payload, key='instrument_name')
    trade_id = _require_int(payload, key='trade_id')
    side = _require_str(payload, key='side').lower()
    if side not in {'buy', 'sell'}:
        raise RuntimeError(f'Payload key side must be buy|sell, got {side!r}')
    price = _require_decimal_float(payload, key='price')
    size = _require_decimal_float(payload, key='size')
    quote_quantity = price * size
    timestamp = _require_int(payload, key='timestamp')

    integrity_row: tuple[object, ...] = (
        instrument_name,
        trade_id,
        side,
        price,
        size,
        quote_quantity,
        timestamp,
        source_event_time_utc,
    )
    insert_row: tuple[object, ...] = (
        instrument_name,
        trade_id,
        side,
        price,
        size,
        quote_quantity,
        timestamp,
        source_event_time_utc,
        event.event_id,
        event.source_offset_or_equivalent,
        source_event_time_utc,
        _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
    )
    return integrity_row, insert_row


@dataclass(frozen=True)
class OKXNativeProjectorSummary:
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


def _aggregate_summaries(summaries: list[OKXNativeProjectorSummary]) -> OKXNativeProjectorSummary:
    return OKXNativeProjectorSummary(
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
) -> OKXNativeProjectorSummary:
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
            projected_rows = [_build_insert_row(event) for event in batch]
            projected_rows_sorted = sorted(
                projected_rows,
                key=lambda row: int(cast(int, row[0][1])),
            )
            integrity_rows = [row[0] for row in projected_rows_sorted]
            run_exchange_integrity_suite_rows(
                dataset='okx_spot_trades',
                rows=integrity_rows,
            )
            insert_rows = [row[1] for row in projected_rows_sorted]
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

    return OKXNativeProjectorSummary(
        partitions_processed=1,
        batches_processed=batches_processed,
        events_processed=events_processed,
        rows_written=rows_written,
    )


def project_okx_spot_trades_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> OKXNativeProjectorSummary:
    normalized_database = _require_non_empty(database, label='database')
    normalized_run_id = _require_non_empty(run_id, label='run_id')
    normalized_projected_at_utc = _require_utc(
        projected_at_utc,
        label='projected_at_utc',
    )
    normalized_partitions = sorted(
        {_require_non_empty(partition_id, label='partition_id') for partition_id in partition_ids}
    )
    if normalized_partitions == []:
        raise RuntimeError('partition_ids must contain at least one partition_id')

    summaries: list[OKXNativeProjectorSummary] = []
    for partition_id in normalized_partitions:
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
