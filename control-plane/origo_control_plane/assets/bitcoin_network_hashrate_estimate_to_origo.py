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
    load_backfill_height_window_or_raise,
)
from origo_control_plane.bitcoin_core import (
    BitcoinCoreNodeContract,
    BitcoinCoreNodeSettings,
    BitcoinCoreRpcClient,
    resolve_bitcoin_core_node_settings_with_height_range_or_raise,
    validate_bitcoin_core_node_contract_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    bitcoin_decimal_text,
    write_bitcoin_events_to_canonical,
)
from origo_control_plane.utils.bitcoin_derived_aligned_projector import (
    project_bitcoin_network_hashrate_estimate_aligned,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_network_hashrate_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    project_bitcoin_network_hashrate_estimate_native,
)

_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_HASHRATE_CONSTANT = float(2**32)


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
class _HeaderPoint:
    height: int
    block_hash: str
    timestamp_seconds: int
    difficulty: float


@dataclass(frozen=True)
class _NormalizedHashrateRow:
    block_height: int
    block_hash: str
    block_timestamp_ms: int
    difficulty: float
    observed_interval_seconds: int
    hashrate_hs: float
    datetime_utc: datetime
    source_chain: str

    def as_insert_row(
        self,
    ) -> tuple[int, str, int, float, int, float, datetime, str]:
        return (
            self.block_height,
            self.block_hash,
            self.block_timestamp_ms,
            self.difficulty,
            self.observed_interval_seconds,
            self.hashrate_hs,
            self.datetime_utc,
            self.source_chain,
        )

    def as_canonical_map(self) -> dict[str, Any]:
        return {
            'block_height': self.block_height,
            'block_hash': self.block_hash,
            'block_timestamp_ms': self.block_timestamp_ms,
            'difficulty': self.difficulty,
            'observed_interval_seconds': self.observed_interval_seconds,
            'hashrate_hs': self.hashrate_hs,
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


def _require_float(raw: Any, *, label: str, minimum: float | None = None) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise RuntimeError(f'{label} must be numeric')
    value = float(raw)
    if minimum is not None and value < minimum:
        raise RuntimeError(f'{label} must be >= {minimum}, got={value}')
    return value


def _fetch_header_point_or_raise(
    *, client: BitcoinCoreRpcClient, height: int
) -> _HeaderPoint:
    if height < 0:
        raise RuntimeError(f'Header height must be >= 0, got={height}')
    block_hash = client.get_block_hash(height)
    header = _require_dict(
        client.get_block_header(block_hash),
        label=f'getblockheader(height={height})',
    )
    header_hash = _require_hash_hex_64(header.get('hash'), label='header.hash')
    if header_hash != block_hash:
        raise RuntimeError(
            f'header.hash mismatch for height={height}: '
            f'expected={block_hash} actual={header_hash}'
        )
    header_height = _require_int(header.get('height'), label='header.height', minimum=0)
    if header_height != height:
        raise RuntimeError(
            f'header.height mismatch for height={height}: actual={header_height}'
        )
    timestamp_seconds = _require_int(header.get('time'), label='header.time', minimum=1)
    difficulty = _require_float(
        header.get('difficulty'),
        label='header.difficulty',
        minimum=0.0,
    )
    return _HeaderPoint(
        height=height,
        block_hash=block_hash,
        timestamp_seconds=timestamp_seconds,
        difficulty=difficulty,
    )


def _observed_interval_seconds_or_raise(
    *,
    index: int,
    headers: list[_HeaderPoint],
    client: BitcoinCoreRpcClient,
) -> int:
    current = headers[index]
    if index > 0:
        interval = current.timestamp_seconds - headers[index - 1].timestamp_seconds
        if interval <= 0:
            raise RuntimeError(
                'Observed block interval must be positive: '
                f'height={current.height} interval={interval}'
            )
        return interval

    if current.height > 0:
        previous = _fetch_header_point_or_raise(client=client, height=current.height - 1)
        interval = current.timestamp_seconds - previous.timestamp_seconds
        if interval <= 0:
            raise RuntimeError(
                'Observed block interval must be positive using previous header: '
                f'height={current.height} interval={interval}'
            )
        return interval

    next_header: _HeaderPoint
    if len(headers) > 1:
        next_header = headers[1]
    else:
        next_header = _fetch_header_point_or_raise(client=client, height=current.height + 1)
    interval = next_header.timestamp_seconds - current.timestamp_seconds
    if interval <= 0:
        raise RuntimeError(
            'Observed block interval must be positive using next header: '
            f'height={current.height} interval={interval}'
        )
    return interval


def normalize_network_hashrate_rows_or_raise(
    *,
    headers: list[_HeaderPoint],
    source_chain: str,
    client: BitcoinCoreRpcClient,
) -> list[_NormalizedHashrateRow]:
    if len(headers) == 0:
        raise RuntimeError('Header list for hashrate normalization cannot be empty')

    rows: list[_NormalizedHashrateRow] = []
    for index, header in enumerate(headers):
        observed_interval_seconds = _observed_interval_seconds_or_raise(
            index=index,
            headers=headers,
            client=client,
        )
        hashrate_hs = (
            header.difficulty * _HASHRATE_CONSTANT / float(observed_interval_seconds)
        )
        rows.append(
            _NormalizedHashrateRow(
                block_height=header.height,
                block_hash=header.block_hash,
                block_timestamp_ms=header.timestamp_seconds * 1000,
                difficulty=header.difficulty,
                observed_interval_seconds=observed_interval_seconds,
                hashrate_hs=hashrate_hs,
                datetime_utc=datetime.fromtimestamp(header.timestamp_seconds, tz=UTC),
                source_chain=source_chain,
            )
        )
    return rows


def _fetch_hashrate_rows_or_raise(
    *,
    client: BitcoinCoreRpcClient,
    node_contract: BitcoinCoreNodeContract,
    settings: BitcoinCoreNodeSettings,
) -> list[_NormalizedHashrateRow]:
    headers: list[_HeaderPoint] = []
    for height in range(settings.headers_start_height, settings.headers_end_height + 1):
        headers.append(_fetch_header_point_or_raise(client=client, height=height))
    return normalize_network_hashrate_rows_or_raise(
        headers=headers,
        source_chain=node_contract.chain,
        client=client,
    )


def _canonical_rows_sha256(rows: list[_NormalizedHashrateRow]) -> str:
    payload = json.dumps(
        [row.as_canonical_map() for row in rows],
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


@asset(
    group_name='bitcoin_core_data',
    description='Computes deterministic Bitcoin network hashrate estimate rows and loads to ClickHouse.',
)
def insert_bitcoin_network_hashrate_estimate_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    clickhouse_target = _resolve_clickhouse_target()
    height_window = load_backfill_height_window_or_raise(context)
    settings = resolve_bitcoin_core_node_settings_with_height_range_or_raise(
        headers_start_height=height_window.start_height,
        headers_end_height=height_window.end_height,
    )
    context.log.info(
        'Fetching Bitcoin network hashrate estimate rows '
        f'for range=[{settings.headers_start_height}, {settings.headers_end_height}]'
    )

    rpc_client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=rpc_client,
        settings=settings,
    )
    rows = _fetch_hashrate_rows_or_raise(
        client=rpc_client,
        node_contract=node_contract,
        settings=settings,
    )
    expected_rows = settings.headers_end_height - settings.headers_start_height + 1
    if len(rows) != expected_rows:
        raise RuntimeError(
            'Unexpected hashrate row count after fetch: '
            f'expected={expected_rows} actual={len(rows)}'
        )
    integrity_report = run_bitcoin_network_hashrate_integrity(
        rows=[row.as_canonical_map() for row in rows]
    )
    rows_sha256 = _canonical_rows_sha256(rows)
    canonical_events = [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_network_hashrate_estimate',
            partition_id=row.datetime_utc.date().isoformat(),
            source_offset_or_equivalent=str(row.block_height),
            source_event_time_utc=row.datetime_utc,
            payload={
                'block_height': row.block_height,
                'block_hash': row.block_hash,
                'block_timestamp_ms': row.block_timestamp_ms,
                'difficulty': bitcoin_decimal_text(
                    row.difficulty,
                    label='row.difficulty',
                ),
                'observed_interval_seconds': row.observed_interval_seconds,
                'hashrate_hs': bitcoin_decimal_text(
                    row.hashrate_hs,
                    label='row.hashrate_hs',
                ),
                'metric_name': 'network_hashrate_hs',
                'metric_unit': 'H/s',
                'metric_value': bitcoin_decimal_text(
                    row.hashrate_hs,
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
        write_summary = write_bitcoin_events_to_canonical(
            client=client,
            database=clickhouse_target.database,
            events=canonical_events,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
        )
        rows_processed = int(write_summary['rows_processed'])
        rows_inserted = int(write_summary['rows_inserted'])
        rows_duplicate = int(write_summary['rows_duplicate'])
        if rows_processed != len(canonical_events):
            raise RuntimeError(
                'Bitcoin hashrate canonical writer summary mismatch: '
                f'rows_processed={rows_processed} expected={len(canonical_events)}'
            )
        if rows_inserted + rows_duplicate != rows_processed:
            raise RuntimeError(
                'Bitcoin hashrate canonical writer summary mismatch: '
                f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
                f'rows_processed={rows_processed}'
            )

        partition_ids = {event.partition_id for event in canonical_events}
        native_projection_summary = project_bitcoin_network_hashrate_estimate_native(
            client=client,
            database=clickhouse_target.database,
            partition_ids=partition_ids,
            run_id=context.run_id,
            projected_at_utc=datetime.now(UTC),
        )
        aligned_projection_summary = project_bitcoin_network_hashrate_estimate_aligned(
            client=client,
            database=clickhouse_target.database,
            partition_ids=partition_ids,
            run_id=context.run_id,
            projected_at_utc=datetime.now(UTC),
        )

        result_data: dict[str, Any] = {
            'range_start_height': settings.headers_start_height,
            'range_end_height': settings.headers_end_height,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'rows_sha256': rows_sha256,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary.to_dict(),
            'aligned_projection_summary': aligned_projection_summary.to_dict(),
            'source_chain': node_contract.chain,
            'node_best_block_height': node_contract.best_block_height,
            'node_best_block_hash': node_contract.best_block_hash,
            'generated_at_utc': datetime.now(UTC).isoformat(),
        }
        context.log.info(
            'Successfully ingested Bitcoin network hashrate estimate rows: '
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
