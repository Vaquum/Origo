import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from origo_control_plane.backfill import (
    apply_runtime_audit_mode_or_raise,
    build_backfill_height_window_config_schema,
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
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_block_header_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_block_headers_native,
)
from origo_control_plane.utils.bitcoin_stream_aligned_projector import (
    project_bitcoin_block_headers_aligned,
)

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database
_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')


@dataclass(frozen=True)
class _NormalizedHeader:
    height: int
    block_hash: str
    prev_hash: str
    merkle_root: str
    version: int
    nonce: int
    difficulty: float
    timestamp_ms: int
    datetime_utc: datetime
    source_chain: str

    def as_insert_row(
        self,
    ) -> tuple[int, str, str, str, int, int, float, int, datetime, str]:
        return (
            self.height,
            self.block_hash,
            self.prev_hash,
            self.merkle_root,
            self.version,
            self.nonce,
            self.difficulty,
            self.timestamp_ms,
            self.datetime_utc,
            self.source_chain,
        )

    def as_canonical_map(self) -> dict[str, Any]:
        return {
            'height': self.height,
            'block_hash': self.block_hash,
            'prev_hash': self.prev_hash,
            'merkle_root': self.merkle_root,
            'version': self.version,
            'nonce': self.nonce,
            'difficulty': self.difficulty,
            'timestamp_ms': self.timestamp_ms,
            'source_chain': self.source_chain,
        }


def _require_str(raw: Any, *, label: str) -> str:
    if not isinstance(raw, str):
        raise RuntimeError(f'{label} must be string')
    value = raw.strip()
    if value == '':
        raise RuntimeError(f'{label} must be non-empty string')
    return value


def _require_hash_hex_64(raw: Any, *, label: str) -> str:
    value = _require_str(raw, label=label)
    if _HASH_HEX_64_PATTERN.fullmatch(value) is None:
        raise RuntimeError(
            f'{label} must be a 64-char lowercase hexadecimal hash, got={value}'
        )
    return value


def _require_int(raw: Any, *, label: str, minimum: int | None = None) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise RuntimeError(f'{label} must be int')
    if minimum is not None and raw < minimum:
        raise RuntimeError(f'{label} must be >= {minimum}, got={raw}')
    return raw


def _require_float(raw: Any, *, label: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise RuntimeError(f'{label} must be numeric')
    value = float(raw)
    if value <= 0:
        raise RuntimeError(f'{label} must be > 0, got={value}')
    return value


def _normalize_header_or_raise(
    *,
    raw_header: dict[str, Any],
    expected_height: int,
    source_chain: str,
) -> _NormalizedHeader:
    block_hash = _require_hash_hex_64(raw_header.get('hash'), label='header.hash')
    height = _require_int(raw_header.get('height'), label='header.height', minimum=0)
    if height != expected_height:
        raise RuntimeError(
            f'header.height mismatch: expected={expected_height} actual={height}'
        )

    prev_hash_raw = raw_header.get('previousblockhash')
    prev_hash = ''
    if prev_hash_raw is not None:
        prev_hash = _require_hash_hex_64(
            prev_hash_raw, label='header.previousblockhash'
        )
    merkle_root = _require_hash_hex_64(
        raw_header.get('merkleroot'), label='header.merkleroot'
    )
    version = _require_int(raw_header.get('version'), label='header.version')
    nonce = _require_int(raw_header.get('nonce'), label='header.nonce', minimum=0)
    difficulty = _require_float(raw_header.get('difficulty'), label='header.difficulty')
    timestamp_seconds = _require_int(raw_header.get('time'), label='header.time', minimum=1)
    timestamp_ms = timestamp_seconds * 1000
    dt = datetime.fromtimestamp(timestamp_seconds, tz=UTC)

    return _NormalizedHeader(
        height=height,
        block_hash=block_hash,
        prev_hash=prev_hash,
        merkle_root=merkle_root,
        version=version,
        nonce=nonce,
        difficulty=difficulty,
        timestamp_ms=timestamp_ms,
        datetime_utc=dt,
        source_chain=source_chain,
    )


def _canonical_headers_sha256(headers: list[_NormalizedHeader]) -> str:
    canonical = [header.as_canonical_map() for header in headers]
    payload = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _fetch_headers_or_raise(
    *,
    client: BitcoinCoreRpcClient,
    node_contract: BitcoinCoreNodeContract,
    settings: BitcoinCoreNodeSettings,
) -> list[_NormalizedHeader]:
    start_height = settings.headers_start_height
    end_height = settings.headers_end_height

    previous_block_hash: str | None = None
    if start_height > 0:
        previous_block_hash = client.get_block_hash(start_height - 1)

    headers: list[_NormalizedHeader] = []
    for height in range(start_height, end_height + 1):
        block_hash = client.get_block_hash(height)
        raw_header = client.get_block_header(block_hash)
        normalized = _normalize_header_or_raise(
            raw_header=raw_header,
            expected_height=height,
            source_chain=node_contract.chain,
        )

        if normalized.block_hash != block_hash:
            raise RuntimeError(
                f'header.hash mismatch at height={height}: '
                f'expected={block_hash} actual={normalized.block_hash}'
            )
        if previous_block_hash is not None and normalized.prev_hash != previous_block_hash:
            raise RuntimeError(
                f'header.previousblockhash linkage mismatch at height={height}: '
                f'expected={previous_block_hash} actual={normalized.prev_hash}'
            )
        if previous_block_hash is None and height == 0 and normalized.prev_hash != '':
            raise RuntimeError(
                'genesis header previousblockhash must be omitted/empty '
                f'but got={normalized.prev_hash}'
            )
        previous_block_hash = normalized.block_hash
        headers.append(normalized)

    return headers


@asset(
    config_schema=build_backfill_height_window_config_schema(
        default_projection_mode='deferred'
    ),
    group_name='bitcoin_core_data',
    description='Fetches deterministic Bitcoin block header range from a self-hosted unpruned node and loads to ClickHouse.',
)
def insert_bitcoin_block_headers_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    runtime_contract = load_backfill_runtime_contract_or_raise(context)
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    height_window = load_backfill_height_window_or_raise(context)
    settings = resolve_bitcoin_core_node_settings_with_height_range_or_raise(
        headers_start_height=height_window.start_height,
        headers_end_height=height_window.end_height,
    )
    context.log.info(
        'Fetching Bitcoin block headers '
        f'for range=[{settings.headers_start_height}, {settings.headers_end_height}]'
    )

    rpc_client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=rpc_client,
        settings=settings,
    )
    headers = _fetch_headers_or_raise(
        client=rpc_client,
        node_contract=node_contract,
        settings=settings,
    )
    expected_rows = settings.headers_end_height - settings.headers_start_height + 1
    if len(headers) != expected_rows:
        raise RuntimeError(
            'Unexpected header row count after fetch: '
            f'expected={expected_rows} actual={len(headers)}'
        )
    integrity_report = run_bitcoin_block_header_integrity(
        rows=[header.as_canonical_map() for header in headers]
    )
    headers_sha256 = _canonical_headers_sha256(headers)
    partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=settings.headers_start_height,
        end_height=settings.headers_end_height,
    )

    canonical_events = [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_block_headers',
            partition_id=partition_id,
            source_offset_or_equivalent=str(header.height),
            source_event_time_utc=header.datetime_utc,
            payload={
                'height': header.height,
                'block_hash': header.block_hash,
                'prev_hash': header.prev_hash,
                'merkle_root': header.merkle_root,
                'version': header.version,
                'nonce': header.nonce,
                'difficulty': bitcoin_decimal_text(
                    header.difficulty,
                    label='header.difficulty',
                ),
                'timestamp_ms': header.timestamp_ms,
                'source_chain': header.source_chain,
            },
        )
        for header in headers
    ]

    client: ClickhouseClient | None = None
    try:
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            compression=True,
            send_receive_timeout=_CLICKHOUSE.send_receive_timeout_seconds,
        )
        source_proof = build_bitcoin_partition_source_proof_or_raise(
            stream_id='bitcoin_block_headers',
            partition_id=partition_id,
            offset_ordering='numeric',
            source_artifact_identity={
                'source_kind': 'bitcoin_core_rpc_height_range',
                'source_chain': node_contract.chain,
                'range_start_height': settings.headers_start_height,
                'range_end_height': settings.headers_end_height,
                'headers_sha256': headers_sha256,
                'node_best_block_height': node_contract.best_block_height,
                'node_best_block_hash': node_contract.best_block_hash,
            },
            events=canonical_events,
            allow_empty_partition=False,
        )
        backfill_summary = execute_bitcoin_partition_backfill_or_raise(
            client=client,
            database=CLICKHOUSE_DATABASE,
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
            native_projection_summary_dict = project_bitcoin_block_headers_native(
                client=client,
                database=CLICKHOUSE_DATABASE,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            ).to_dict()
            aligned_projection_summary_dict = project_bitcoin_block_headers_aligned(
                client=client,
                database=CLICKHOUSE_DATABASE,
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
            'projection_mode': runtime_contract.projection_mode,
            'write_path': backfill_summary.write_path,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'headers_sha256': headers_sha256,
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
            'Successfully ingested Bitcoin block header range: '
            + json.dumps(result_data, sort_keys=True)
        )
        return result_data
    finally:
        if client is not None:
            active_exception = sys.exc_info()[1]
            try:
                client.disconnect()
            except Exception as exc:
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
