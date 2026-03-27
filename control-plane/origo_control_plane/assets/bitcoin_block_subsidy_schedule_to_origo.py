import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
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
    project_bitcoin_block_subsidy_schedule_aligned,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_subsidy_schedule_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_block_subsidy_schedule_native,
)

_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_SATS_PER_BTC = 100_000_000
_INITIAL_SUBSIDY_SATS = 50 * _SATS_PER_BTC
_HALVING_INTERVAL_BLOCKS = 210_000
_MAX_HALVINGS = 64


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
class _NormalizedBlockSubsidy:
    block_height: int
    block_hash: str
    block_timestamp_ms: int
    halving_interval: int
    subsidy_sats: int
    subsidy_btc: float
    datetime_utc: datetime
    source_chain: str

    def as_insert_row(
        self,
    ) -> tuple[int, str, int, int, int, float, datetime, str]:
        return (
            self.block_height,
            self.block_hash,
            self.block_timestamp_ms,
            self.halving_interval,
            self.subsidy_sats,
            self.subsidy_btc,
            self.datetime_utc,
            self.source_chain,
        )

    def as_canonical_map(self) -> dict[str, Any]:
        return {
            'block_height': self.block_height,
            'block_hash': self.block_hash,
            'block_timestamp_ms': self.block_timestamp_ms,
            'halving_interval': self.halving_interval,
            'subsidy_sats': self.subsidy_sats,
            'subsidy_btc': self.subsidy_btc,
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


def _subsidy_sats_for_height(height: int) -> tuple[int, int]:
    halving_interval = height // _HALVING_INTERVAL_BLOCKS
    if halving_interval >= _MAX_HALVINGS:
        return halving_interval, 0
    return halving_interval, _INITIAL_SUBSIDY_SATS >> halving_interval


def normalize_block_subsidy_or_raise(
    *,
    block_hash: str,
    block_header: dict[str, Any],
    expected_height: int,
    source_chain: str,
) -> _NormalizedBlockSubsidy:
    normalized_block_hash = _require_hash_hex_64(block_hash, label='block_hash')
    header_hash = _require_hash_hex_64(block_header.get('hash'), label='header.hash')
    if header_hash != normalized_block_hash:
        raise RuntimeError(
            'header hash mismatch: '
            f'expected={normalized_block_hash} actual={header_hash}'
        )
    header_height = _require_int(
        block_header.get('height'), label='header.height', minimum=0
    )
    if header_height != expected_height:
        raise RuntimeError(
            'header height mismatch: '
            f'expected={expected_height} actual={header_height}'
        )
    block_timestamp_seconds = _require_int(
        block_header.get('time'), label='header.time', minimum=1
    )
    halving_interval, subsidy_sats = _subsidy_sats_for_height(expected_height)
    return _NormalizedBlockSubsidy(
        block_height=expected_height,
        block_hash=normalized_block_hash,
        block_timestamp_ms=block_timestamp_seconds * 1000,
        halving_interval=halving_interval,
        subsidy_sats=subsidy_sats,
        subsidy_btc=float(subsidy_sats) / float(_SATS_PER_BTC),
        datetime_utc=datetime.fromtimestamp(block_timestamp_seconds, tz=UTC),
        source_chain=source_chain,
    )


def _fetch_block_subsidy_rows_or_raise(
    *,
    client: BitcoinCoreRpcClient,
    node_contract: BitcoinCoreNodeContract,
    settings: BitcoinCoreNodeSettings,
) -> list[_NormalizedBlockSubsidy]:
    rows: list[_NormalizedBlockSubsidy] = []
    for height in range(settings.headers_start_height, settings.headers_end_height + 1):
        block_hash = client.get_block_hash(height)
        header = _require_dict(
            client.get_block_header(block_hash),
            label=f'getblockheader(height={height})',
        )
        rows.append(
            normalize_block_subsidy_or_raise(
                block_hash=block_hash,
                block_header=header,
                expected_height=height,
                source_chain=node_contract.chain,
            )
        )
    return rows


def _canonical_rows_sha256(rows: list[_NormalizedBlockSubsidy]) -> str:
    payload = json.dumps(
        [row.as_canonical_map() for row in rows],
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


@asset(
    group_name='bitcoin_core_data',
    description='Computes deterministic Bitcoin subsidy schedule rows and loads to ClickHouse.',
)
def insert_bitcoin_block_subsidy_schedule_to_origo(
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
        'Fetching Bitcoin subsidy schedule '
        f'for range=[{settings.headers_start_height}, {settings.headers_end_height}]'
    )

    rpc_client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=rpc_client,
        settings=settings,
    )
    rows = _fetch_block_subsidy_rows_or_raise(
        client=rpc_client,
        node_contract=node_contract,
        settings=settings,
    )
    expected_rows = settings.headers_end_height - settings.headers_start_height + 1
    if len(rows) != expected_rows:
        raise RuntimeError(
            'Unexpected subsidy row count after fetch: '
            f'expected={expected_rows} actual={len(rows)}'
        )
    integrity_report = run_bitcoin_subsidy_schedule_integrity(
        rows=[row.as_canonical_map() for row in rows]
    )
    rows_sha256 = _canonical_rows_sha256(rows)
    partition_id = format_bitcoin_height_range_partition_id_or_raise(
        start_height=settings.headers_start_height,
        end_height=settings.headers_end_height,
    )
    canonical_events = [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_block_subsidy_schedule',
            partition_id=partition_id,
            source_offset_or_equivalent=str(row.block_height),
            source_event_time_utc=row.datetime_utc,
            payload={
                'block_height': row.block_height,
                'block_hash': row.block_hash,
                'block_timestamp_ms': row.block_timestamp_ms,
                'halving_interval': row.halving_interval,
                'subsidy_sats': row.subsidy_sats,
                'subsidy_btc': bitcoin_decimal_text(
                    row.subsidy_btc,
                    label='row.subsidy_btc',
                ),
                'metric_name': 'block_subsidy_btc',
                'metric_unit': 'BTC',
                'metric_value': bitcoin_decimal_text(
                    row.subsidy_btc,
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
            send_receive_timeout=900,
        )
        source_proof = build_bitcoin_partition_source_proof_or_raise(
            stream_id='bitcoin_block_subsidy_schedule',
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
            native_projection_summary = project_bitcoin_block_subsidy_schedule_native(
                client=client,
                database=clickhouse_target.database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            ).to_dict()
            aligned_projection_summary = project_bitcoin_block_subsidy_schedule_aligned(
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
            'Successfully ingested Bitcoin subsidy schedule: '
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
