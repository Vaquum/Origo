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
    build_bitcoin_partition_source_proof_or_raise,
    execute_bitcoin_partition_backfill_or_raise,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_block_transaction_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_block_transactions_native,
)
from origo_control_plane.utils.bitcoin_stream_aligned_projector import (
    project_bitcoin_block_transactions_aligned,
)

_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_HEX_PATTERN = re.compile(r'^[0-9a-f]*$')


@dataclass(frozen=True)
class _ClickHouseTarget:
    host: str
    port: int
    user: str
    password: str
    database: str


def _resolve_clickhouse_target() -> _ClickHouseTarget:
    settings = resolve_clickhouse_native_settings()
    return _ClickHouseTarget(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
    )


@dataclass(frozen=True)
class _NormalizedTransactionRow:
    block_height: int
    block_hash: str
    block_timestamp_ms: int
    transaction_index: int
    txid: str
    inputs_json: str
    outputs_json: str
    values_json: str
    scripts_json: str
    witness_data_json: str
    coinbase: bool
    datetime_utc: datetime
    source_chain: str

    def as_insert_row(
        self,
    ) -> tuple[
        int,
        str,
        int,
        int,
        str,
        str,
        str,
        str,
        str,
        str,
        int,
        datetime,
        str,
    ]:
        return (
            self.block_height,
            self.block_hash,
            self.block_timestamp_ms,
            self.transaction_index,
            self.txid,
            self.inputs_json,
            self.outputs_json,
            self.values_json,
            self.scripts_json,
            self.witness_data_json,
            1 if self.coinbase else 0,
            self.datetime_utc,
            self.source_chain,
        )

    def as_canonical_map(self) -> dict[str, Any]:
        return {
            'block_height': self.block_height,
            'block_hash': self.block_hash,
            'block_timestamp_ms': self.block_timestamp_ms,
            'transaction_index': self.transaction_index,
            'txid': self.txid,
            'inputs_json': self.inputs_json,
            'outputs_json': self.outputs_json,
            'values_json': self.values_json,
            'scripts_json': self.scripts_json,
            'witness_data_json': self.witness_data_json,
            'coinbase': self.coinbase,
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


def _require_str(raw: Any, *, label: str) -> str:
    if not isinstance(raw, str):
        raise RuntimeError(f'{label} must be string')
    value = raw.strip()
    if value == '':
        raise RuntimeError(f'{label} must be non-empty string')
    return value


def _read_optional_str(raw: Any, *, label: str) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise RuntimeError(f'{label} must be string when provided')
    return raw


def _require_hash_hex_64(raw: Any, *, label: str) -> str:
    value = _require_str(raw, label=label)
    if _HASH_HEX_64_PATTERN.fullmatch(value) is None:
        raise RuntimeError(
            f'{label} must be a 64-char lowercase hexadecimal hash, got={value}'
        )
    return value


def _require_hex(raw: Any, *, label: str) -> str:
    value = _require_str(raw, label=label)
    if _HEX_PATTERN.fullmatch(value) is None:
        raise RuntimeError(f'{label} must be lowercase hexadecimal, got={value}')
    return value


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


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _sum_btc(values: list[float]) -> float:
    total = Decimal('0')
    for value in values:
        total += Decimal(str(value))
    return float(total)


def _normalize_addresses_or_raise(
    *, script_pubkey: dict[str, Any], label: str
) -> list[str]:
    resolved: list[str] = []

    address_raw = script_pubkey.get('address')
    if address_raw is not None:
        resolved.append(_require_str(address_raw, label=f'{label}.address'))

    addresses_raw = script_pubkey.get('addresses')
    if addresses_raw is not None:
        for idx, entry in enumerate(_require_list(addresses_raw, label=f'{label}.addresses')):
            resolved.append(
                _require_str(entry, label=f'{label}.addresses[{idx}]')
            )

    deduped: list[str] = []
    seen: set[str] = set()
    for address in resolved:
        if address not in seen:
            deduped.append(address)
            seen.add(address)
    return deduped


def _normalize_transaction_or_raise(
    *,
    tx_payload: dict[str, Any],
    block_height: int,
    block_hash: str,
    block_timestamp_seconds: int,
    transaction_index: int,
    source_chain: str,
) -> _NormalizedTransactionRow:
    txid = _require_hash_hex_64(tx_payload.get('txid'), label='tx.txid')
    vin_items = _require_list(tx_payload.get('vin'), label='tx.vin')
    vout_items = _require_list(tx_payload.get('vout'), label='tx.vout')
    if len(vin_items) == 0:
        raise RuntimeError('tx.vin must contain at least one input')
    if len(vout_items) == 0:
        raise RuntimeError('tx.vout must contain at least one output')

    normalized_inputs: list[dict[str, Any]] = []
    normalized_outputs: list[dict[str, Any]] = []
    input_values_btc: list[float] = []
    output_values_btc: list[float] = []
    normalized_input_scripts: list[dict[str, Any]] = []
    normalized_output_scripts: list[dict[str, Any]] = []
    normalized_witness_data: list[dict[str, Any]] = []
    is_coinbase_transaction = False

    for input_index, raw_input in enumerate(vin_items):
        vin = _require_dict(raw_input, label=f'tx.vin[{input_index}]')
        sequence = _require_int(
            vin.get('sequence'),
            label=f'tx.vin[{input_index}].sequence',
            minimum=0,
        )
        coinbase_raw = vin.get('coinbase')
        input_payload: dict[str, Any] = {
            'input_index': input_index,
            'sequence': sequence,
        }
        if coinbase_raw is not None:
            input_payload['coinbase_hex'] = _require_hex(
                coinbase_raw,
                label=f'tx.vin[{input_index}].coinbase',
            )
            is_coinbase_transaction = True
        else:
            input_payload['prev_txid'] = _require_hash_hex_64(
                vin.get('txid'),
                label=f'tx.vin[{input_index}].txid',
            )
            input_payload['prev_vout'] = _require_int(
                vin.get('vout'),
                label=f'tx.vin[{input_index}].vout',
                minimum=0,
            )

        prevout_raw = vin.get('prevout')
        if prevout_raw is not None:
            prevout = _require_dict(prevout_raw, label=f'tx.vin[{input_index}].prevout')
            prevout_value = _require_float(
                prevout.get('value'),
                label=f'tx.vin[{input_index}].prevout.value',
                minimum=0.0,
            )
            input_payload['prevout_value_btc'] = prevout_value
            input_values_btc.append(prevout_value)

        script_sig_raw = vin.get('scriptSig')
        if script_sig_raw is not None:
            script_sig = _require_dict(
                script_sig_raw,
                label=f'tx.vin[{input_index}].scriptSig',
            )
            normalized_input_scripts.append(
                {
                    'input_index': input_index,
                    'asm': _read_optional_str(
                        script_sig.get('asm'),
                        label=f'tx.vin[{input_index}].scriptSig.asm',
                    ),
                    'hex': _read_optional_str(
                        script_sig.get('hex'),
                        label=f'tx.vin[{input_index}].scriptSig.hex',
                    ),
                }
            )

        witness_raw = vin.get('txinwitness')
        if witness_raw is not None:
            witness_stack = _require_list(
                witness_raw, label=f'tx.vin[{input_index}].txinwitness'
            )
            normalized_stack: list[str] = []
            for witness_index, witness_element in enumerate(witness_stack):
                normalized_stack.append(
                    _require_hex(
                        witness_element,
                        label=(
                            'tx.vin['
                            f'{input_index}].txinwitness[{witness_index}]'
                        ),
                    )
                )
            normalized_witness_data.append(
                {
                    'input_index': input_index,
                    'stack': normalized_stack,
                }
            )

        normalized_inputs.append(input_payload)

    for output_index, raw_output in enumerate(vout_items):
        vout = _require_dict(raw_output, label=f'tx.vout[{output_index}]')
        vout_n = _require_int(
            vout.get('n'),
            label=f'tx.vout[{output_index}].n',
            minimum=0,
        )
        if vout_n != output_index:
            raise RuntimeError(
                'tx.vout index mismatch: '
                f'expected={output_index} actual={vout_n}'
            )
        value_btc = _require_float(
            vout.get('value'),
            label=f'tx.vout[{output_index}].value',
            minimum=0.0,
        )
        output_values_btc.append(value_btc)

        script_pubkey = _require_dict(
            vout.get('scriptPubKey'),
            label=f'tx.vout[{output_index}].scriptPubKey',
        )
        addresses = _normalize_addresses_or_raise(
            script_pubkey=script_pubkey,
            label=f'tx.vout[{output_index}].scriptPubKey',
        )
        script_type = _read_optional_str(
            script_pubkey.get('type'),
            label=f'tx.vout[{output_index}].scriptPubKey.type',
        )
        script_hex = _read_optional_str(
            script_pubkey.get('hex'),
            label=f'tx.vout[{output_index}].scriptPubKey.hex',
        )
        if script_hex is not None and _HEX_PATTERN.fullmatch(script_hex) is None:
            raise RuntimeError(
                'tx.vout['
                f'{output_index}].scriptPubKey.hex must be lowercase hexadecimal'
            )
        script_asm = _read_optional_str(
            script_pubkey.get('asm'),
            label=f'tx.vout[{output_index}].scriptPubKey.asm',
        )

        normalized_outputs.append(
            {
                'output_index': output_index,
                'value_btc': value_btc,
                'script_type': script_type,
                'addresses': addresses,
            }
        )
        normalized_output_scripts.append(
            {
                'output_index': output_index,
                'asm': script_asm,
                'hex': script_hex,
                'type': script_type,
                'addresses': addresses,
            }
        )

    values_payload = {
        'input_values_btc': input_values_btc,
        'output_values_btc': output_values_btc,
        'input_value_btc_sum': _sum_btc(input_values_btc),
        'output_value_btc_sum': _sum_btc(output_values_btc),
    }
    scripts_payload = {
        'input_script_sig': normalized_input_scripts,
        'output_script_pub_key': normalized_output_scripts,
    }

    block_timestamp_ms = block_timestamp_seconds * 1000
    block_datetime_utc = datetime.fromtimestamp(block_timestamp_seconds, tz=UTC)
    return _NormalizedTransactionRow(
        block_height=block_height,
        block_hash=block_hash,
        block_timestamp_ms=block_timestamp_ms,
        transaction_index=transaction_index,
        txid=txid,
        inputs_json=_canonical_json(normalized_inputs),
        outputs_json=_canonical_json(normalized_outputs),
        values_json=_canonical_json(values_payload),
        scripts_json=_canonical_json(scripts_payload),
        witness_data_json=_canonical_json(normalized_witness_data),
        coinbase=is_coinbase_transaction,
        datetime_utc=block_datetime_utc,
        source_chain=source_chain,
    )


def normalize_block_transactions_or_raise(
    *,
    block_payload: dict[str, Any],
    expected_height: int,
    expected_block_hash: str,
    source_chain: str,
) -> list[_NormalizedTransactionRow]:
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
        raise RuntimeError(f'Block has no transactions at height={expected_height}')

    rows: list[_NormalizedTransactionRow] = []
    for transaction_index, raw_transaction in enumerate(tx_payload):
        tx = _require_dict(raw_transaction, label=f'block.tx[{transaction_index}]')
        rows.append(
            _normalize_transaction_or_raise(
                tx_payload=tx,
                block_height=block_height,
                block_hash=block_hash,
                block_timestamp_seconds=block_timestamp_seconds,
                transaction_index=transaction_index,
                source_chain=source_chain,
            )
        )
    return rows


def _canonical_rows_sha256(rows: list[_NormalizedTransactionRow]) -> str:
    canonical = [row.as_canonical_map() for row in rows]
    payload = _canonical_json(canonical)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _fetch_transaction_rows_or_raise(
    *,
    client: BitcoinCoreRpcClient,
    node_contract: BitcoinCoreNodeContract,
    settings: BitcoinCoreNodeSettings,
) -> list[_NormalizedTransactionRow]:
    rows: list[_NormalizedTransactionRow] = []
    for height in range(settings.headers_start_height, settings.headers_end_height + 1):
        block_hash = client.get_block_hash(height)
        raw_block = client.get_block(block_hash, 3)
        block_payload = _require_dict(raw_block, label=f'getblock(height={height})')
        block_rows = normalize_block_transactions_or_raise(
            block_payload=block_payload,
            expected_height=height,
            expected_block_hash=block_hash,
            source_chain=node_contract.chain,
        )
        rows.extend(block_rows)
    return rows


@asset(
    group_name='bitcoin_core_data',
    description='Fetches deterministic Bitcoin block transaction rows from a self-hosted unpruned node and loads to ClickHouse.',
)
def insert_bitcoin_block_transactions_to_origo(
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
        'Fetching Bitcoin block transactions '
        f'for range=[{settings.headers_start_height}, {settings.headers_end_height}]'
    )

    rpc_client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=rpc_client,
        settings=settings,
    )
    rows = _fetch_transaction_rows_or_raise(
        client=rpc_client,
        node_contract=node_contract,
        settings=settings,
    )
    if len(rows) == 0:
        raise RuntimeError(
            'No transaction rows fetched for requested range: '
            f'[{settings.headers_start_height}, {settings.headers_end_height}]'
        )
    integrity_report = run_bitcoin_block_transaction_integrity(
        rows=[row.as_canonical_map() for row in rows]
    )
    rows_sha256 = _canonical_rows_sha256(rows)
    partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=settings.headers_start_height,
        end_height=settings.headers_end_height,
    )
    canonical_events = [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_block_transactions',
            partition_id=partition_id,
            source_offset_or_equivalent=(
                f'{row.block_height}:{row.transaction_index}:{row.txid}'
            ),
            source_event_time_utc=row.datetime_utc,
            payload=row.as_canonical_map(),
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
            send_receive_timeout=900,
        )
        source_proof = build_bitcoin_partition_source_proof_or_raise(
            stream_id='bitcoin_block_transactions',
            partition_id=partition_id,
            offset_ordering='lexicographic',
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

        partition_ids = {partition_id}
        if runtime_contract.projection_mode == 'inline':
            native_projection_summary_dict = project_bitcoin_block_transactions_native(
                client=client,
                database=clickhouse_target.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            ).to_dict()
            aligned_projection_summary_dict = project_bitcoin_block_transactions_aligned(
                client=client,
                database=clickhouse_target.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            ).to_dict()
        else:
            native_projection_summary_dict = {
                'partitions_processed': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }
            aligned_projection_summary_dict = {
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
            'blocks_processed': (
                settings.headers_end_height - settings.headers_start_height + 1
            ),
            'projection_mode': runtime_contract.projection_mode,
            'write_path': backfill_summary.write_path,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'rows_sha256': rows_sha256,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary_dict,
            'aligned_projection_summary': aligned_projection_summary_dict,
            'partition_proof_state': backfill_summary.partition_proof_state,
            'partition_proof_digest_sha256': backfill_summary.partition_proof_digest_sha256,
            'source_chain': node_contract.chain,
            'node_best_block_height': node_contract.best_block_height,
            'node_best_block_hash': node_contract.best_block_hash,
            'generated_at_utc': datetime.now(UTC).isoformat(),
        }
        context.log.info(
            'Successfully ingested Bitcoin block transaction range: '
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
