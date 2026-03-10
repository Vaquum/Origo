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

from origo_control_plane.bitcoin_core import (
    BitcoinCoreRpcClient,
    resolve_bitcoin_core_node_settings,
    validate_bitcoin_core_node_contract_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    bitcoin_decimal_text,
    write_bitcoin_events_to_canonical,
)
from origo_control_plane.utils.bitcoin_integrity import (
    run_bitcoin_mempool_state_integrity,
)
from origo_control_plane.utils.bitcoin_native_projector import (
    ProjectorSummary,
    project_bitcoin_mempool_state_native,
)

_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')


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
class _NormalizedMempoolRow:
    snapshot_at_utc: datetime
    snapshot_at_unix_ms: int
    txid: str
    fee_rate_sat_vb: float
    vsize: int
    first_seen_timestamp: int
    rbf_flag: bool
    source_chain: str

    def as_insert_row(
        self,
    ) -> tuple[datetime, int, str, float, int, int, int, str]:
        return (
            self.snapshot_at_utc,
            self.snapshot_at_unix_ms,
            self.txid,
            self.fee_rate_sat_vb,
            self.vsize,
            self.first_seen_timestamp,
            1 if self.rbf_flag else 0,
            self.source_chain,
        )

    def as_canonical_map(self) -> dict[str, Any]:
        return {
            'snapshot_at_unix_ms': self.snapshot_at_unix_ms,
            'txid': self.txid,
            'fee_rate_sat_vb': self.fee_rate_sat_vb,
            'vsize': self.vsize,
            'first_seen_timestamp': self.first_seen_timestamp,
            'rbf_flag': self.rbf_flag,
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


def _require_bool(raw: Any, *, label: str) -> bool:
    if not isinstance(raw, bool):
        raise RuntimeError(f'{label} must be bool')
    return raw


def _fee_rate_sat_vb_or_raise(*, fee_btc: float, vsize: int) -> float:
    if vsize <= 0:
        raise RuntimeError(f'vsize must be > 0, got={vsize}')
    fee_sats = Decimal(str(fee_btc)) * Decimal('100000000')
    fee_rate = fee_sats / Decimal(vsize)
    return float(fee_rate)


def normalize_mempool_state_or_raise(
    *,
    mempool_payload: dict[str, Any],
    snapshot_at_utc: datetime,
    source_chain: str,
) -> list[_NormalizedMempoolRow]:
    if snapshot_at_utc.tzinfo is None:
        raise RuntimeError('snapshot_at_utc must be timezone-aware')
    snapshot_at_utc = snapshot_at_utc.astimezone(UTC)
    snapshot_at_unix_ms = int(snapshot_at_utc.timestamp() * 1000)

    rows: list[_NormalizedMempoolRow] = []
    for txid in sorted(mempool_payload.keys()):
        txid_normalized = _require_hash_hex_64(txid, label='mempool txid')
        entry = _require_dict(mempool_payload[txid], label=f'mempool[{txid}]')
        fees = _require_dict(entry.get('fees'), label=f'mempool[{txid}].fees')
        fee_btc = _require_float(
            fees.get('base'),
            label=f'mempool[{txid}].fees.base',
            minimum=0.0,
        )
        vsize = _require_int(entry.get('vsize'), label=f'mempool[{txid}].vsize', minimum=1)
        first_seen_timestamp = _require_int(
            entry.get('time'),
            label=f'mempool[{txid}].time',
            minimum=0,
        )
        rbf_flag = _require_bool(
            entry.get('bip125-replaceable'),
            label=f'mempool[{txid}].bip125-replaceable',
        )
        rows.append(
            _NormalizedMempoolRow(
                snapshot_at_utc=snapshot_at_utc,
                snapshot_at_unix_ms=snapshot_at_unix_ms,
                txid=txid_normalized,
                fee_rate_sat_vb=_fee_rate_sat_vb_or_raise(fee_btc=fee_btc, vsize=vsize),
                vsize=vsize,
                first_seen_timestamp=first_seen_timestamp,
                rbf_flag=rbf_flag,
                source_chain=source_chain,
            )
        )
    return rows


def _canonical_rows_sha256(rows: list[_NormalizedMempoolRow]) -> str:
    payload = json.dumps(
        [row.as_canonical_map() for row in rows],
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


@asset(
    group_name='bitcoin_core_data',
    description='Fetches deterministic Bitcoin mempool snapshot rows from a self-hosted node and loads to ClickHouse.',
)
def insert_bitcoin_mempool_state_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    clickhouse_target = _resolve_clickhouse_target()
    settings = resolve_bitcoin_core_node_settings()
    snapshot_at_utc = datetime.now(UTC)
    context.log.info(
        'Fetching Bitcoin mempool snapshot '
        f'at={snapshot_at_utc.isoformat()} for chain validation and normalization'
    )

    rpc_client = BitcoinCoreRpcClient(settings=settings)
    node_contract = validate_bitcoin_core_node_contract_or_raise(
        client=rpc_client,
        settings=settings,
    )
    raw_mempool = rpc_client.get_raw_mempool(verbose=True)
    mempool_payload = _require_dict(raw_mempool, label='getrawmempool(verbose=true)')
    rows = normalize_mempool_state_or_raise(
        mempool_payload=mempool_payload,
        snapshot_at_utc=snapshot_at_utc,
        source_chain=node_contract.chain,
    )
    integrity_report = run_bitcoin_mempool_state_integrity(
        rows=[row.as_canonical_map() for row in rows]
    )
    rows_sha256 = _canonical_rows_sha256(rows)
    snapshot_at_unix_ms = int(snapshot_at_utc.timestamp() * 1000)

    canonical_events = [
        BitcoinCanonicalEvent(
            stream_id='bitcoin_mempool_state',
            partition_id=row.snapshot_at_utc.date().isoformat(),
            source_offset_or_equivalent=f'{row.snapshot_at_unix_ms}:{row.txid}',
            source_event_time_utc=row.snapshot_at_utc,
            payload={
                'snapshot_at_unix_ms': row.snapshot_at_unix_ms,
                'txid': row.txid,
                'fee_rate_sat_vb': bitcoin_decimal_text(
                    row.fee_rate_sat_vb,
                    label='row.fee_rate_sat_vb',
                ),
                'vsize': row.vsize,
                'first_seen_timestamp': row.first_seen_timestamp,
                'rbf_flag': row.rbf_flag,
                'source_chain': row.source_chain,
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
                'Bitcoin mempool canonical writer summary mismatch: '
                f'rows_processed={rows_processed} expected={len(canonical_events)}'
            )
        if rows_inserted + rows_duplicate != rows_processed:
            raise RuntimeError(
                'Bitcoin mempool canonical writer summary mismatch: '
                f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
                f'rows_processed={rows_processed}'
            )

        native_projection_summary: ProjectorSummary
        if canonical_events == []:
            native_projection_summary = ProjectorSummary(
                partitions_processed=0,
                batches_processed=0,
                events_processed=0,
                rows_written=0,
            )
        else:
            native_projection_summary = project_bitcoin_mempool_state_native(
                client=client,
                database=clickhouse_target.database,
                partition_ids={event.partition_id for event in canonical_events},
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            )

        result_data: dict[str, Any] = {
            'snapshot_at_utc': snapshot_at_utc.isoformat(),
            'snapshot_at_unix_ms': snapshot_at_unix_ms,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'rows_sha256': rows_sha256,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary.to_dict(),
            'source_chain': node_contract.chain,
            'node_best_block_height': node_contract.best_block_height,
            'node_best_block_hash': node_contract.best_block_hash,
            'generated_at_utc': datetime.now(UTC).isoformat(),
        }
        context.log.info(
            'Successfully ingested Bitcoin mempool snapshot: '
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
