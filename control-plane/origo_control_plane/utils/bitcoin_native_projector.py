from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickhouseClient

from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime, ProjectorEvent
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_block_fee_total_integrity,
    run_bitcoin_block_header_integrity,
    run_bitcoin_block_transaction_integrity,
    run_bitcoin_circulating_supply_integrity,
    run_bitcoin_mempool_state_integrity,
    run_bitcoin_network_hashrate_integrity,
    run_bitcoin_subsidy_schedule_integrity,
)

BitcoinNativeStreamId = Literal[
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]


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


def _require_int(
    payload: dict[str, Any], key: str, *, minimum: int | None = None
) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise RuntimeError(f'Payload key {key} must not be bool')
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value)
        except ValueError as exc:
            raise RuntimeError(f'Payload key {key} must be integer') from exc
    else:
        raise RuntimeError(f'Payload key {key} must be int|string')
    if minimum is not None and parsed < minimum:
        raise RuntimeError(f'Payload key {key} must be >= {minimum}')
    return parsed


def _require_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'Payload key {key} must be non-empty string')
    return value


def _require_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return value == 1
    raise RuntimeError(f'Payload key {key} must be bool or 0/1 integer')


def _require_decimal_float(payload: dict[str, Any], key: str) -> float:
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
            'Projector event source_event_time_utc must be present for Bitcoin native projections'
        )
    return _require_utc(event.source_event_time_utc, label='event.source_event_time_utc')


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
    integrity_row: Mapping[str, Any]
    insert_row: tuple[object, ...]


def _run_integrity(*, dataset: BitcoinNativeStreamId, rows: list[dict[str, Any]]) -> None:
    if dataset == 'bitcoin_block_headers':
        run_bitcoin_block_header_integrity(rows=rows)
        return
    if dataset == 'bitcoin_block_transactions':
        run_bitcoin_block_transaction_integrity(rows=rows)
        return
    if dataset == 'bitcoin_mempool_state':
        run_bitcoin_mempool_state_integrity(rows=rows)
        return
    if dataset == 'bitcoin_block_fee_totals':
        run_bitcoin_block_fee_total_integrity(rows=rows)
        return
    if dataset == 'bitcoin_block_subsidy_schedule':
        run_bitcoin_subsidy_schedule_integrity(rows=rows)
        return
    if dataset == 'bitcoin_network_hashrate_estimate':
        run_bitcoin_network_hashrate_integrity(rows=rows)
        return
    if dataset == 'bitcoin_circulating_supply':
        run_bitcoin_circulating_supply_integrity(rows=rows)
        return
    raise RuntimeError(f'Unsupported bitcoin dataset: {dataset}')


def _projection_sort_key(
    *, dataset: BitcoinNativeStreamId, projection: ProjectionInsert
) -> tuple[object, ...]:
    row = projection.integrity_row
    if dataset == 'bitcoin_block_headers':
        return (cast(int, row['height']),)
    if dataset in {
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }:
        return (cast(int, row['block_height']),)
    if dataset == 'bitcoin_block_transactions':
        return (
            cast(int, row['block_height']),
            cast(int, row['transaction_index']),
            cast(str, row['txid']),
        )
    if dataset == 'bitcoin_mempool_state':
        return (
            cast(int, row['snapshot_at_unix_ms']),
            cast(str, row['txid']),
        )
    raise RuntimeError(f'Unsupported bitcoin dataset for sort key: {dataset}')


def _project_partition(
    *,
    client: ClickhouseClient,
    database: str,
    dataset: BitcoinNativeStreamId,
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
        require_terminal_partition_proof=True,
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
            ordered_projection_rows = sorted(
                projection_rows,
                key=lambda row: _projection_sort_key(dataset=dataset, projection=row),
            )
            integrity_rows = [
                dict(row.integrity_row) for row in ordered_projection_rows
            ]
            if dataset == 'bitcoin_mempool_state':
                grouped_rows: dict[int, list[dict[str, Any]]] = {}
                for row in integrity_rows:
                    snapshot_at_unix_ms_raw = row.get('snapshot_at_unix_ms')
                    if not isinstance(snapshot_at_unix_ms_raw, int):
                        raise RuntimeError(
                            'bitcoin_mempool_state integrity rows must include '
                            'integer snapshot_at_unix_ms'
                        )
                    existing = grouped_rows.get(snapshot_at_unix_ms_raw)
                    if existing is None:
                        grouped_rows[snapshot_at_unix_ms_raw] = [row]
                    else:
                        existing.append(row)
                for snapshot_rows in grouped_rows.values():
                    _run_integrity(dataset=dataset, rows=snapshot_rows)
            else:
                _run_integrity(dataset=dataset, rows=integrity_rows)
            insert_rows = [row.insert_row for row in ordered_projection_rows]
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


def _project_stream_native(
    *,
    client: ClickhouseClient,
    database: str,
    stream_id: BitcoinNativeStreamId,
    projector_id: str,
    target_table: str,
    insert_columns: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int,
    build_insert_row: Callable[[ProjectorEvent], ProjectionInsert],
) -> ProjectorSummary:
    normalized_database = _require_non_empty(database, label='database')
    normalized_run_id = _require_non_empty(run_id, label='run_id')
    normalized_projector_id = _require_non_empty(projector_id, label='projector_id')
    normalized_projected_at_utc = _require_utc(
        projected_at_utc,
        label='projected_at_utc',
    )
    normalized_partitions = sorted(
        {_require_non_empty(partition_id, label='partition_id') for partition_id in partition_ids}
    )
    if normalized_partitions == []:
        raise RuntimeError('partition_ids must contain at least one partition_id')

    summaries: list[ProjectorSummary] = []
    for partition_id in normalized_partitions:
        summaries.append(
            _project_partition(
                client=client,
                database=normalized_database,
                dataset=stream_id,
                stream_key=CanonicalStreamKey(
                    source_id='bitcoin_core',
                    stream_id=stream_id,
                    partition_id=partition_id,
                ),
                projector_id=normalized_projector_id,
                target_table=target_table,
                run_id=normalized_run_id,
                projected_at_utc=normalized_projected_at_utc,
                batch_size=batch_size,
                build_insert_row=build_insert_row,
                insert_columns=insert_columns,
            )
        )
    return _aggregate_summaries(summaries)


def project_bitcoin_block_headers_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'height, block_hash, prev_hash, merkle_root, version, nonce, difficulty, '
        'timestamp, datetime, source_chain, event_id, source_offset_or_equivalent, '
        'source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        height = _require_int(payload, 'height', minimum=0)
        block_hash = _require_str(payload, 'block_hash')
        prev_hash = _require_str(payload, 'prev_hash') if payload.get('prev_hash') is not None else ''
        merkle_root = _require_str(payload, 'merkle_root')
        version = _require_int(payload, 'version')
        nonce = _require_int(payload, 'nonce', minimum=0)
        difficulty = _require_decimal_float(payload, 'difficulty')
        timestamp_ms = _require_int(payload, 'timestamp_ms', minimum=1)
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'height': height,
                'block_hash': block_hash,
                'prev_hash': prev_hash,
                'merkle_root': merkle_root,
                'version': version,
                'nonce': nonce,
                'difficulty': difficulty,
                'timestamp_ms': timestamp_ms,
                'source_chain': source_chain,
            },
            insert_row=(
                height,
                block_hash,
                prev_hash,
                merkle_root,
                version,
                nonce,
                difficulty,
                timestamp_ms,
                source_event_time_utc,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_block_headers',
        projector_id='bitcoin_block_headers_native_v1',
        target_table='canonical_bitcoin_block_headers_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )


def project_bitcoin_block_transactions_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'block_height, block_hash, block_timestamp, transaction_index, txid, '
        'inputs, outputs, values, scripts, witness_data, coinbase, datetime, '
        'source_chain, event_id, source_offset_or_equivalent, '
        'source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        block_height = _require_int(payload, 'block_height', minimum=0)
        block_hash = _require_str(payload, 'block_hash')
        block_timestamp_ms = _require_int(payload, 'block_timestamp_ms', minimum=1)
        transaction_index = _require_int(payload, 'transaction_index', minimum=0)
        txid = _require_str(payload, 'txid')
        inputs_json = _require_str(payload, 'inputs_json')
        outputs_json = _require_str(payload, 'outputs_json')
        values_json = _require_str(payload, 'values_json')
        scripts_json = _require_str(payload, 'scripts_json')
        witness_data_json = _require_str(payload, 'witness_data_json')
        coinbase = _require_bool(payload, 'coinbase')
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'block_height': block_height,
                'block_hash': block_hash,
                'block_timestamp_ms': block_timestamp_ms,
                'transaction_index': transaction_index,
                'txid': txid,
                'inputs_json': inputs_json,
                'outputs_json': outputs_json,
                'values_json': values_json,
                'scripts_json': scripts_json,
                'witness_data_json': witness_data_json,
                'coinbase': coinbase,
                'source_chain': source_chain,
            },
            insert_row=(
                block_height,
                block_hash,
                block_timestamp_ms,
                transaction_index,
                txid,
                inputs_json,
                outputs_json,
                values_json,
                scripts_json,
                witness_data_json,
                1 if coinbase else 0,
                source_event_time_utc,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_block_transactions',
        projector_id='bitcoin_block_transactions_native_v1',
        target_table='canonical_bitcoin_block_transactions_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )


def project_bitcoin_mempool_state_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'snapshot_at, snapshot_at_unix_ms, txid, fee_rate_sat_vb, vsize, '
        'first_seen_timestamp, rbf_flag, source_chain, event_id, '
        'source_offset_or_equivalent, source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        snapshot_at_unix_ms = _require_int(payload, 'snapshot_at_unix_ms', minimum=0)
        txid = _require_str(payload, 'txid')
        fee_rate_sat_vb = _require_decimal_float(payload, 'fee_rate_sat_vb')
        vsize = _require_int(payload, 'vsize', minimum=1)
        first_seen_timestamp = _require_int(payload, 'first_seen_timestamp', minimum=0)
        rbf_flag = _require_bool(payload, 'rbf_flag')
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'snapshot_at_unix_ms': snapshot_at_unix_ms,
                'txid': txid,
                'fee_rate_sat_vb': fee_rate_sat_vb,
                'vsize': vsize,
                'first_seen_timestamp': first_seen_timestamp,
                'rbf_flag': rbf_flag,
                'source_chain': source_chain,
            },
            insert_row=(
                source_event_time_utc,
                snapshot_at_unix_ms,
                txid,
                fee_rate_sat_vb,
                vsize,
                first_seen_timestamp,
                1 if rbf_flag else 0,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_mempool_state',
        projector_id='bitcoin_mempool_state_native_v1',
        target_table='canonical_bitcoin_mempool_state_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )


def project_bitcoin_block_fee_totals_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'block_height, block_hash, block_timestamp, fee_total_btc, datetime, source_chain, '
        'event_id, source_offset_or_equivalent, source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        block_height = _require_int(payload, 'block_height', minimum=0)
        block_hash = _require_str(payload, 'block_hash')
        block_timestamp_ms = _require_int(payload, 'block_timestamp_ms', minimum=1)
        fee_total_btc = _require_decimal_float(payload, 'fee_total_btc')
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'block_height': block_height,
                'block_hash': block_hash,
                'block_timestamp_ms': block_timestamp_ms,
                'fee_total_btc': fee_total_btc,
                'source_chain': source_chain,
            },
            insert_row=(
                block_height,
                block_hash,
                block_timestamp_ms,
                fee_total_btc,
                source_event_time_utc,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_block_fee_totals',
        projector_id='bitcoin_block_fee_totals_native_v1',
        target_table='canonical_bitcoin_block_fee_totals_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )


def project_bitcoin_block_subsidy_schedule_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'block_height, block_hash, block_timestamp, halving_interval, subsidy_sats, '
        'subsidy_btc, datetime, source_chain, event_id, source_offset_or_equivalent, '
        'source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        block_height = _require_int(payload, 'block_height', minimum=0)
        block_hash = _require_str(payload, 'block_hash')
        block_timestamp_ms = _require_int(payload, 'block_timestamp_ms', minimum=1)
        halving_interval = _require_int(payload, 'halving_interval', minimum=0)
        subsidy_sats = _require_int(payload, 'subsidy_sats', minimum=0)
        subsidy_btc = _require_decimal_float(payload, 'subsidy_btc')
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'block_height': block_height,
                'block_hash': block_hash,
                'block_timestamp_ms': block_timestamp_ms,
                'halving_interval': halving_interval,
                'subsidy_sats': subsidy_sats,
                'subsidy_btc': subsidy_btc,
                'source_chain': source_chain,
            },
            insert_row=(
                block_height,
                block_hash,
                block_timestamp_ms,
                halving_interval,
                subsidy_sats,
                subsidy_btc,
                source_event_time_utc,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_block_subsidy_schedule',
        projector_id='bitcoin_block_subsidy_schedule_native_v1',
        target_table='canonical_bitcoin_block_subsidy_schedule_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )


def project_bitcoin_network_hashrate_estimate_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'block_height, block_hash, block_timestamp, difficulty, observed_interval_seconds, '
        'hashrate_hs, datetime, source_chain, event_id, source_offset_or_equivalent, '
        'source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        block_height = _require_int(payload, 'block_height', minimum=0)
        block_hash = _require_str(payload, 'block_hash')
        block_timestamp_ms = _require_int(payload, 'block_timestamp_ms', minimum=1)
        difficulty = _require_decimal_float(payload, 'difficulty')
        observed_interval_seconds = _require_int(
            payload,
            'observed_interval_seconds',
            minimum=1,
        )
        hashrate_hs = _require_decimal_float(payload, 'hashrate_hs')
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'block_height': block_height,
                'block_hash': block_hash,
                'block_timestamp_ms': block_timestamp_ms,
                'difficulty': difficulty,
                'observed_interval_seconds': observed_interval_seconds,
                'hashrate_hs': hashrate_hs,
                'source_chain': source_chain,
            },
            insert_row=(
                block_height,
                block_hash,
                block_timestamp_ms,
                difficulty,
                observed_interval_seconds,
                hashrate_hs,
                source_event_time_utc,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_network_hashrate_estimate',
        projector_id='bitcoin_network_hashrate_estimate_native_v1',
        target_table='canonical_bitcoin_network_hashrate_estimate_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )


def project_bitcoin_circulating_supply_native(
    *,
    client: ClickhouseClient,
    database: str,
    partition_ids: list[str] | set[str],
    run_id: str,
    projected_at_utc: datetime,
    batch_size: int = 10_000,
) -> ProjectorSummary:
    insert_columns = (
        'block_height, block_hash, block_timestamp, circulating_supply_sats, '
        'circulating_supply_btc, datetime, source_chain, event_id, '
        'source_offset_or_equivalent, source_event_time_utc, ingested_at_utc'
    )

    def _build_insert_row(event: ProjectorEvent) -> ProjectionInsert:
        payload = _require_payload(event.payload_json)
        source_event_time_utc = _require_source_event_time(event)
        block_height = _require_int(payload, 'block_height', minimum=0)
        block_hash = _require_str(payload, 'block_hash')
        block_timestamp_ms = _require_int(payload, 'block_timestamp_ms', minimum=1)
        circulating_supply_sats = _require_int(
            payload, 'circulating_supply_sats', minimum=0
        )
        circulating_supply_btc = _require_decimal_float(
            payload, 'circulating_supply_btc'
        )
        source_chain = _require_str(payload, 'source_chain')
        return ProjectionInsert(
            integrity_row={
                'block_height': block_height,
                'block_hash': block_hash,
                'block_timestamp_ms': block_timestamp_ms,
                'circulating_supply_sats': circulating_supply_sats,
                'circulating_supply_btc': circulating_supply_btc,
                'source_chain': source_chain,
            },
            insert_row=(
                block_height,
                block_hash,
                block_timestamp_ms,
                circulating_supply_sats,
                circulating_supply_btc,
                source_event_time_utc,
                source_chain,
                event.event_id,
                event.source_offset_or_equivalent,
                source_event_time_utc,
                _require_utc(event.ingested_at_utc, label='event.ingested_at_utc'),
            ),
        )

    return _project_stream_native(
        client=client,
        database=database,
        stream_id='bitcoin_circulating_supply',
        projector_id='bitcoin_circulating_supply_native_v1',
        target_table='canonical_bitcoin_circulating_supply_native_v1',
        insert_columns=insert_columns,
        partition_ids=partition_ids,
        run_id=run_id,
        projected_at_utc=projected_at_utc,
        batch_size=batch_size,
        build_insert_row=_build_insert_row,
    )
