import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from origo_control_plane.backfill import (
    apply_runtime_audit_mode_or_raise,
    load_backfill_height_window_or_raise,
    load_backfill_runtime_contract_or_raise,
)
from origo_control_plane.bitcoin_core import (
    BitcoinCoreNodeContract,
    BitcoinCoreNodeSettings,
    BitcoinCoreRpcClient,
    format_bitcoin_height_range_partition_id_or_raise,
    resolve_bitcoin_core_node_settings_with_height_range_or_raise,
    validate_bitcoin_core_node_contract_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    bitcoin_decimal_text,
    build_bitcoin_partition_source_proof_or_raise,
    execute_bitcoin_partition_backfill_or_raise,
)
from origo_control_plane.utils.bitcoin_derived_aligned_projector import (
    project_bitcoin_block_fee_totals_aligned,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_block_fee_total_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_block_fee_totals_native,
)

_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')


@dataclass(frozen=True)
class _ClickHouseTarget:
    host: str
    port: int
    user: str
    password: str
    database: str
    send_receive_timeout_seconds: int


def _resolve_clickhouse_target() -> _ClickHouseTarget:
    settings = resolve_clickhouse_native_settings()
    return _ClickHouseTarget(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
        send_receive_timeout_seconds=settings.send_receive_timeout_seconds,
    )


@dataclass(frozen=True)
class _NormalizedBlockFeeTotal:
    block_height: int
    block_hash: str
    block_timestamp_ms: int
    fee_total_btc: float
    datetime_utc: datetime
    source_chain: str

    def as_insert_row(self) -> tuple[int, str, int, float, datetime, str]:
        return (
            self.block_height,
            self.block_hash,
            self.block_timestamp_ms,
            self.fee_total_btc,
            self.datetime_utc,
            self.source_chain,
        )

    def as_canonical_map(self) -> dict[str, Any]:
        return {
            'block_height': self.block_height,
            'block_hash': self.block_hash,
            'block_timestamp_ms': self.block_timestamp_ms,
            'fee_total_btc': self.fee_total_btc,
            'source_chain': self.source_chain,
        }


def _require_dict(raw: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuntimeError(f'{label} must be object')
    raw_map = cast(dict[Any, Any], raw)
    normalized: dict[str, Any] = {}
    for raw_key, value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = value
    return normalized


def _require_list(raw: Any, *, label: str) -> list[Any]:
    if not isinstance(raw, list):
        raise RuntimeError(f'{label} must be list')
    return cast(list[Any], raw)


def _require_hash_hex_64(raw: Any, *, label: str) -> str:
    if not isinstance(raw, str):
        raise RuntimeError(f'{label} must be string')
    if _HASH_HEX_64_PATTERN.fullmatch(raw) is None:
        raise RuntimeError(
            f'{label} must be a 64-char lowercase hexadecimal hash, got={raw}'
        )
    return raw


def _require_int(raw: Any, *, label: str, minimum: int | None = None) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise RuntimeError(f'{label} must be int')
    if minimum is not None and raw < minimum:
        raise RuntimeError(f'{label} must be >= {minimum}, got={raw}')
    return raw


def _require_float(raw: Any, *, label: str, minimum: float | None = None) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise RuntimeError(f'{label} must be numeric')
    value = float(raw)
    if minimum is not None and value < minimum:
        raise RuntimeError(f'{label} must be >= {minimum}, got={value}')
    return value


def _is_coinbase_input(vin: dict[str, Any]) -> bool:
    return vin.get('coinbase') is not None


def _sum_transaction_input_btc_or_raise(
    *, tx: dict[str, Any], tx_label: str
) -> Decimal:
    vin_payload = _require_list(tx.get('vin'), label=f'{tx_label}.vin')
    total = Decimal('0')
    for input_index, raw_vin in enumerate(vin_payload):
        vin = _require_dict(raw_vin, label=f'{tx_label}.vin[{input_index}]')
        if _is_coinbase_input(vin):
            continue
        prevout = _require_dict(
            vin.get('prevout'),
            label=f'{tx_label}.vin[{input_index}].prevout',
        )
        prevout_value_btc = _require_float(
            prevout.get('value'),
            label=f'{tx_label}.vin[{input_index}].prevout.value',
            minimum=0.0,
        )
        total += Decimal(str(prevout_value_btc))
    return total


def _sum_transaction_output_btc_or_raise(
    *, tx: dict[str, Any], tx_label: str
) -> Decimal:
    vout_payload = _require_list(tx.get('vout'), label=f'{tx_label}.vout')
    total = Decimal('0')
    for output_index, raw_vout in enumerate(vout_payload):
        vout = _require_dict(raw_vout, label=f'{tx_label}.vout[{output_index}]')
        value_btc = _require_float(
            vout.get('value'),
            label=f'{tx_label}.vout[{output_index}].value',
            minimum=0.0,
        )
        total += Decimal(str(value_btc))
    return total


def _compute_block_fee_total_btc_or_raise(*, tx_payload: list[Any]) -> float:
    block_fee_total = Decimal('0')
    for tx_index, raw_tx in enumerate(tx_payload):
        tx_label = f'block.tx[{tx_index}]'
        tx = _require_dict(raw_tx, label=tx_label)
        vin_payload = _require_list(tx.get('vin'), label=f'{tx_label}.vin')
        has_coinbase_input = any(
            _is_coinbase_input(_require_dict(vin, label=f'{tx_label}.vin[*]'))
            for vin in vin_payload
        )
        if has_coinbase_input:
            continue

        input_total = _sum_transaction_input_btc_or_raise(tx=tx, tx_label=tx_label)
        output_total = _sum_transaction_output_btc_or_raise(tx=tx, tx_label=tx_label)
        tx_fee = input_total - output_total
        if tx_fee < Decimal('0'):
            raise RuntimeError(
                f'Negative transaction fee computed for {tx_label}: '
                f'input_total={input_total} output_total={output_total}'
            )
        block_fee_total += tx_fee
    return float(block_fee_total)


def normalize_block_fee_total_or_raise(
    *,
    block_payload: dict[str, Any],
    expected_height: int,
    expected_block_hash: str,
    source_chain: str,
) -> _NormalizedBlockFeeTotal:
    block_hash = _require_hash_hex_64(block_payload.get('hash'), label='block.hash')
    if block_hash != expected_block_hash:
        raise RuntimeError(
            'block hash mismatch: '
            f'expected={expected_block_hash} actual={block_hash}'
        )

    block_height = _require_int(block_payload.get('height'), label='block.height', minimum=0)
    if block_height != expected_height:
        raise RuntimeError(
            'block height mismatch: '
            f'expected={expected_height} actual={block_height}'
        )

    block_timestamp_seconds = _require_int(
        block_payload.get('time'),
        label='block.time',
        minimum=1,
    )
    tx_payload = _require_list(block_payload.get('tx'), label='block.tx')
    if len(tx_payload) == 0:
        raise RuntimeError(f'Block has no transactions at height={block_height}')

    fee_total_btc = _compute_block_fee_total_btc_or_raise(tx_payload=tx_payload)
    block_timestamp_ms = block_timestamp_seconds * 1000
    return _NormalizedBlockFeeTotal(
        block_height=block_height,
        block_hash=block_hash,
        block_timestamp_ms=block_timestamp_ms,
        fee_total_btc=fee_total_btc,
        datetime_utc=datetime.fromtimestamp(block_timestamp_seconds, tz=UTC),
        source_chain=source_chain,
    )


def _fetch_block_fee_totals_or_raise(
    *,
    client: BitcoinCoreRpcClient,
    node_contract: BitcoinCoreNodeContract,
    settings: BitcoinCoreNodeSettings,
) -> list[_NormalizedBlockFeeTotal]:
    rows: list[_NormalizedBlockFeeTotal] = []
    for height in range(settings.headers_start_height, settings.headers_end_height + 1):
        block_hash = client.get_block_hash(height)
        raw_block = client.get_block(block_hash, 3)
        block_payload = _require_dict(raw_block, label=f'getblock(height={height})')
        rows.append(
            normalize_block_fee_total_or_raise(
                block_payload=block_payload,
                expected_height=height,
                expected_block_hash=block_hash,
                source_chain=node_contract.chain,
            )
        )
    return rows


def _canonical_rows_sha256(rows: list[_NormalizedBlockFeeTotal]) -> str:
    payload = json.dumps(
        [row.as_canonical_map() for row in rows],
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


@asset(
    group_name='bitcoin_core_data',
    description='Computes deterministic Bitcoin block fee totals from canonical block transaction payloads and loads to ClickHouse.',
)
def insert_bitcoin_block_fee_totals_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    runtime_contract = load_backfill_runtime_contract_or_raise(context)
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    clickhouse_target = _resolve_clickhouse_target()
    height_window = load_backfill_height_window_or_raise(context)
    settings = resolve_bitcoin_core_node_settings_with_height_range_or_raise(
        headers_start_height=height_window.start_height,
        headers_end_height=height_window.end_height,
    )
    context.log.info(
        'Fetching Bitcoin block fee totals '
        f'for range=[{settings.headers_start_height}, {settings.headers_end_height}]'
    )

    rpc_client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=rpc_client,
        settings=settings,
    )
    rows = _fetch_block_fee_totals_or_raise(
        client=rpc_client,
        node_contract=node_contract,
        settings=settings,
    )
    expected_rows = settings.headers_end_height - settings.headers_start_height + 1
    if len(rows) != expected_rows:
        raise RuntimeError(
            'Unexpected block fee-total row count after fetch: '
            f'expected={expected_rows} actual={len(rows)}'
        )
    integrity_report = run_bitcoin_block_fee_total_integrity(
        rows=[row.as_canonical_map() for row in rows]
    )
    rows_sha256 = _canonical_rows_sha256(rows)
    partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=settings.headers_start_height,
        end_height=settings.headers_end_height,
    )
    canonical_events = [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_block_fee_totals',
            partition_id=partition_id,
            source_offset_or_equivalent=str(row.block_height),
            source_event_time_utc=row.datetime_utc,
            payload={
                'block_height': row.block_height,
                'block_hash': row.block_hash,
                'block_timestamp_ms': row.block_timestamp_ms,
                'fee_total_btc': bitcoin_decimal_text(
                    row.fee_total_btc,
                    label='row.fee_total_btc',
                ),
                'metric_name': 'block_fee_total_btc',
                'metric_unit': 'BTC',
                'metric_value': bitcoin_decimal_text(
                    row.fee_total_btc,
                    label='row.metric_value',
                ),
                'source_chain': row.source_chain,
                'provenance': {
                    'source_id': 'bitcoin_core',
                    'stream_id': 'bitcoin_block_headers',
                    'source_offset_or_equivalent': str(row.block_height),
                    'block_hash': row.block_hash,
                },
            },
        )
        for row in rows
    ]

    client: ClickhouseClient | None = None
    try:
        client = ClickhouseClient(
            host=clickhouse_target.host,
            port=clickhouse_target.port,
            user=clickhouse_target.user,
            password=clickhouse_target.password,
            database=clickhouse_target.database,
            compression=True,
            send_receive_timeout=clickhouse_target.send_receive_timeout_seconds,
        )
        source_proof = build_bitcoin_partition_source_proof_or_raise(
            stream_id='bitcoin_block_fee_totals',
            partition_id=partition_id,
            offset_ordering='numeric',
            source_artifact_identity={
                'source_kind': 'bitcoin_core_rpc_height_range',
                'source_chain': node_contract.chain,
                'range_start_height': settings.headers_start_height,
                'range_end_height': settings.headers_end_height,
                'rows_sha256': rows_sha256,
                'node_best_block_height': node_contract.best_block_height,
                'node_best_block_hash': node_contract.best_block_hash,
            },
            events=canonical_events,
            allow_empty_partition=False,
        )
        backfill_summary = execute_bitcoin_partition_backfill_or_raise(
            client=client,
            database=clickhouse_target.database,
            source_proof=source_proof,
            events=canonical_events,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
            execution_mode=runtime_contract.execution_mode,
        )
        rows_processed = backfill_summary.rows_processed
        rows_inserted = backfill_summary.rows_inserted
        rows_duplicate = backfill_summary.rows_duplicate

        partition_ids = {event.partition_id for event in canonical_events}
        if runtime_contract.projection_mode == 'inline':
            native_projection_summary = project_bitcoin_block_fee_totals_native(
                client=client,
                database=clickhouse_target.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            ).to_dict()
            aligned_projection_summary = project_bitcoin_block_fee_totals_aligned(
                client=client,
                database=clickhouse_target.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            ).to_dict()
        else:
            native_projection_summary = {
                'partitions_processed': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }
            aligned_projection_summary = {
                'partitions_processed': 0,
                'policies_recorded': 0,
                'policies_duplicate': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }

        result_data: dict[str, Any] = {
            'range_start_height': settings.headers_start_height,
            'range_end_height': settings.headers_end_height,
            'projection_mode': runtime_contract.projection_mode,
            'write_path': backfill_summary.write_path,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'rows_sha256': rows_sha256,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary,
            'aligned_projection_summary': aligned_projection_summary,
            'partition_proof_state': backfill_summary.partition_proof_state,
            'partition_proof_digest_sha256': backfill_summary.partition_proof_digest_sha256,
            'source_chain': node_contract.chain,
            'node_best_block_height': node_contract.best_block_height,
            'node_best_block_hash': node_contract.best_block_hash,
            'generated_at_utc': datetime.now(UTC).isoformat(),
        }
        context.log.info(
            'Successfully ingested Bitcoin block fee totals: '
            + json.dumps(result_data, sort_keys=True)
        )
        return result_data
    finally:
        if client is not None:
            try:
                client.disconnect()
            except Exception as exc:
                active_exception = sys.exc_info()[1]
                if active_exception is not None:
                    active_exception.add_note(
                        f'ClickHouse disconnect failed during cleanup: {exc}'
                    )
                    context.log.warning(
                        f'Failed to disconnect ClickHouse client cleanly: {exc}'
                    )
                else:
                    raise RuntimeError(
                        f'Failed to disconnect ClickHouse client cleanly: {exc}'
                    ) from exc
