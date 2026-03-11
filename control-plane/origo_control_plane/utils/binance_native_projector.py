from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickhouseClient

from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime, ProjectorEvent
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)

BinanceNativeDataset = Literal['spot_trades', 'spot_agg_trades', 'futures_trades']


def _require_non_empty(value: str, *, label: str) -> str:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be set and non-empty')
    return normalized


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


def _require_int_payload(payload: dict[str, Any], key: str) -> int:
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


def _require_decimal_float_payload(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool):
        raise RuntimeError(f'Payload key {key} must not be bool')
    if isinstance(value, (int, str)):
        try:
            return float(Decimal(str(value)))
        except InvalidOperation as exc:
            raise RuntimeError(f'Payload key {key} must be decimal') from exc
    raise RuntimeError(f'Payload key {key} must be int|string decimal')


def _require_bool_payload(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return value == 1
    raise RuntimeError(f'Payload key {key} must be bool or 0/1 integer')


def _require_source_event_time(event: ProjectorEvent) -> datetime:
    if event.source_event_time_utc is None:
        raise RuntimeError(
            'Projector event source_event_time_utc must be present for Binance native projections'
        )
    if event.source_event_time_utc.tzinfo is None:
        return event.source_event_time_utc.replace(tzinfo=UTC)
    return event.source_event_time_utc.astimezone(UTC)


@dataclass(frozen=True)
class ProjectorSummary:
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


@dataclass(frozen=True)
class ProjectionInsert:
    integrity_row: tuple[object, ...]
    insert_row: tuple[object, ...]


def _project_partition(
    *,
    client: ClickhouseClient,
    database: str,
    dataset: BinanceNativeDataset,
    stream_key: CanonicalStreamKey,
    projector_id: str,
    target_table: str,
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int,
    build_insert_row: Callable[[ProjectorEvent], ProjectionInsert],
    insert_columns: str,
) -> ProjectorSummary:
    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=projector_id,
        stream_key=stream_key,
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
            projection_rows = [build_insert_row(event) for event in batch]
            integrity_rows = [row.integrity_row for row in projection_rows]
            run_exchange_integrity_suite_rows(
                dataset=dataset,
                rows=integrity_rows,
            )
            insert_rows = [row.insert_row for row in projection_rows]
            client.execute(
                f'''
                INSERT INTO {database}.{target_table}
                ({insert_columns})
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
                    'projection': target_table,
                    'rows_written': len(insert_rows),
                },
            )
    finally:
        runtime.stop()

    return ProjectorSummary(
        partitions_processed=1,
        batches_processed=batches_processed,
        events_processed=events_processed,
        rows_written=rows_written,
    )


def _aggregate_summaries(summaries: list[ProjectorSummary]) -> ProjectorSummary:
    return ProjectorSummary(
        partitions_processed=sum(summary.partitions_processed for summary in summaries),
        batches_processed=sum(summary.batches_processed for summary in summaries),
        events_processed=sum(summary.events_processed for summary in summaries),
        rows_written=sum(summary.rows_written for summary in summaries),
    )


def project_binance_spot_trades_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    stream_id = 'spot_trades'
    projector_id = 'binance_spot_trades_native_v1'
    target_table = 'canonical_binance_spot_trades_native_v1'
    insert_columns = (
        'trade_id, timestamp, price, quantity, quote_quantity, '
        'is_buyer_maker, is_best_match, datetime, event_id, '
        'source_offset_or_equivalent, source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        trade_id = _require_int_payload(payload, 'trade_id')
        timestamp = int(source_event_time_utc.timestamp() * 1000)
        price = _require_decimal_float_payload(payload, 'price')
        quantity = _require_decimal_float_payload(payload, 'qty')
        quote_quantity = _require_decimal_float_payload(payload, 'quote_qty')
        is_buyer_maker = 1 if _require_bool_payload(payload, 'is_buyer_maker') else 0
        is_best_match = 1 if _require_bool_payload(payload, 'is_best_match') else 0
        return ProjectionInsert(
            integrity_row=(
                trade_id,
                price,
                quantity,
                quote_quantity,
                timestamp,
                is_buyer_maker,
                is_best_match,
                source_event_time_utc,
            ),
            insert_row=(
                trade_id,
                timestamp,
                price,
                quantity,
                quote_quantity,
                is_buyer_maker,
                is_best_match,
                source_event_time_utc,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                event.ingested_at_utc.astimezone(UTC),
            ),
        )

    summaries: list[ProjectorSummary] = []
    for partition_id in sorted({_require_non_empty(p, label='partition_id') for p in partition_ids}):
        summaries.append(
            _project_partition(
                client=client,
                database=database,
                dataset=stream_id,
                stream_key=CanonicalStreamKey(
                    source_id='binance',
                    stream_id=stream_id,
                    partition_id=partition_id,
                ),
                projector_id=projector_id,
                target_table=target_table,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
                batch_size=batch_size,
                build_insert_row=_build_insert_row,
                insert_columns=insert_columns,
            )
        )
    return _aggregate_summaries(summaries)


def project_binance_spot_agg_trades_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    stream_id = 'spot_agg_trades'
    projector_id = 'binance_spot_agg_trades_native_v1'
    target_table = 'canonical_binance_spot_agg_trades_native_v1'
    insert_columns = (
        'agg_trade_id, timestamp, price, quantity, first_trade_id, '
        'last_trade_id, is_buyer_maker, datetime, event_id, '
        'source_offset_or_equivalent, source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        agg_trade_id = _require_int_payload(payload, 'agg_trade_id')
        timestamp = int(source_event_time_utc.timestamp() * 1000)
        price = _require_decimal_float_payload(payload, 'price')
        quantity = _require_decimal_float_payload(payload, 'qty')
        first_trade_id = _require_int_payload(payload, 'first_trade_id')
        last_trade_id = _require_int_payload(payload, 'last_trade_id')
        is_buyer_maker = 1 if _require_bool_payload(payload, 'is_buyer_maker') else 0
        return ProjectionInsert(
            integrity_row=(
                agg_trade_id,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                timestamp,
                is_buyer_maker,
                source_event_time_utc,
            ),
            insert_row=(
                agg_trade_id,
                timestamp,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                is_buyer_maker,
                source_event_time_utc,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                event.ingested_at_utc.astimezone(UTC),
            ),
        )

    summaries: list[ProjectorSummary] = []
    for partition_id in sorted({_require_non_empty(p, label='partition_id') for p in partition_ids}):
        summaries.append(
            _project_partition(
                client=client,
                database=database,
                dataset=stream_id,
                stream_key=CanonicalStreamKey(
                    source_id='binance',
                    stream_id=stream_id,
                    partition_id=partition_id,
                ),
                projector_id=projector_id,
                target_table=target_table,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
                batch_size=batch_size,
                build_insert_row=_build_insert_row,
                insert_columns=insert_columns,
            )
        )
    return _aggregate_summaries(summaries)


def project_binance_futures_trades_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    stream_id = 'futures_trades'
    projector_id = 'binance_futures_trades_native_v1'
    target_table = 'canonical_binance_futures_trades_native_v1'
    insert_columns = (
        'futures_trade_id, timestamp, price, quantity, quote_quantity, '
        'is_buyer_maker, datetime, event_id, source_offset_or_equivalent, '
        'source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        trade_id = _require_int_payload(payload, 'trade_id')
        timestamp = int(source_event_time_utc.timestamp() * 1000)
        price = _require_decimal_float_payload(payload, 'price')
        quantity = _require_decimal_float_payload(payload, 'qty')
        quote_quantity = _require_decimal_float_payload(payload, 'quote_qty')
        is_buyer_maker = 1 if _require_bool_payload(payload, 'is_buyer_maker') else 0
        return ProjectionInsert(
            integrity_row=(
                trade_id,
                price,
                quantity,
                quote_quantity,
                timestamp,
                is_buyer_maker,
                source_event_time_utc,
            ),
            insert_row=(
                trade_id,
                timestamp,
                price,
                quantity,
                quote_quantity,
                is_buyer_maker,
                source_event_time_utc,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                event.ingested_at_utc.astimezone(UTC),
            ),
        )

    summaries: list[ProjectorSummary] = []
    for partition_id in sorted({_require_non_empty(p, label='partition_id') for p in partition_ids}):
        summaries.append(
            _project_partition(
                client=client,
                database=database,
                dataset=stream_id,
                stream_key=CanonicalStreamKey(
                    source_id='binance',
                    stream_id=stream_id,
                    partition_id=partition_id,
                ),
                projector_id=projector_id,
                target_table=target_table,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
                batch_size=batch_size,
                build_insert_row=_build_insert_row,
                insert_columns=insert_columns,
            )
        )
    return _aggregate_summaries(summaries)
