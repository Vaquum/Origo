from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast

import polars as pl
from clickhouse_connect import get_client as _raw_get_client

from .binance_aligned_1s import assert_canonical_aligned_1s_storage_contract
from .native_core import (
    LatestRowsWindow,
    MonthWindow,
    QueryWindow,
    RandomRowsWindow,
    TimeRangeWindow,
    resolve_clickhouse_http_settings,
)

logger = logging.getLogger(__name__)
get_client = cast(Any, _raw_get_client)

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_ALIGNED_VIEW_ID = 'aligned_1s_raw'
_ALIGNED_VIEW_VERSION = 1
_CANONICAL_SOURCE_ID = 'bitcoin_core'
_CANONICAL_ALIGNED_TABLE = 'canonical_aligned_1s_aggregates'

BitcoinStreamAlignedDataset = Literal[
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
]

_BITCOIN_STREAM_ALIGNED_ALLOWED_COLUMNS: dict[BitcoinStreamAlignedDataset, frozenset[str]] = {
    'bitcoin_block_headers': frozenset(
        {
            'aligned_at_utc',
            'records_in_bucket',
            'latest_height',
            'latest_block_hash',
            'latest_prev_hash',
            'latest_merkle_root',
            'latest_version',
            'latest_nonce',
            'latest_difficulty',
            'latest_timestamp_ms',
            'latest_source_chain',
            'latest_ingested_at_utc',
            'first_source_offset_or_equivalent',
            'last_source_offset_or_equivalent',
            'bucket_sha256',
        }
    ),
    'bitcoin_block_transactions': frozenset(
        {
            'aligned_at_utc',
            'records_in_bucket',
            'tx_count',
            'coinbase_tx_count',
            'total_input_value_btc',
            'total_output_value_btc',
            'first_block_height',
            'last_block_height',
            'latest_block_timestamp_ms',
            'latest_txid',
            'latest_source_chain',
            'latest_ingested_at_utc',
            'first_source_offset_or_equivalent',
            'last_source_offset_or_equivalent',
            'bucket_sha256',
        }
    ),
    'bitcoin_mempool_state': frozenset(
        {
            'aligned_at_utc',
            'records_in_bucket',
            'tx_count',
            'fee_rate_sat_vb_min',
            'fee_rate_sat_vb_max',
            'fee_rate_sat_vb_avg',
            'total_vsize',
            'first_seen_timestamp_min',
            'first_seen_timestamp_max',
            'rbf_true_count',
            'latest_snapshot_at_unix_ms',
            'latest_txid',
            'latest_source_chain',
            'latest_ingested_at_utc',
            'first_source_offset_or_equivalent',
            'last_source_offset_or_equivalent',
            'bucket_sha256',
        }
    ),
}


@dataclass(frozen=True)
class _AlignedEvent:
    order_key: tuple[int, ...]
    payload: dict[str, Any]


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_clickhouse_datetime64_literal(value: str) -> str:
    parsed = _parse_iso_datetime(value)
    return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _to_utc_datetime(value: Any, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise RuntimeError(f'{label} must be datetime')
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _to_decimal(value: Any, *, label: str) -> Decimal:
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must be decimal-compatible')
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise RuntimeError(f'{label} must be decimal-compatible') from exc
    raise RuntimeError(f'{label} must be decimal-compatible')


def _expect_json_object(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must decode to object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_json_list(value: Any, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must decode to list')
    return cast(list[Any], value)


def _parse_header_source_offset(value: Any) -> tuple[int, ...]:
    if isinstance(value, bool):
        raise RuntimeError('header source_offset_or_equivalent must not be bool')
    if isinstance(value, int):
        return (value,)
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(
            'header source_offset_or_equivalent must be int-or-string height'
        )
    normalized = value.strip()
    if not normalized.isdigit():
        raise RuntimeError(
            'header source_offset_or_equivalent must be numeric height string'
        )
    return (int(normalized),)


def _parse_transaction_source_offset(value: Any) -> tuple[int, ...]:
    if not isinstance(value, str):
        raise RuntimeError(
            'transaction source_offset_or_equivalent must be string '
            'format=height:transaction_index:txid'
        )
    parts = value.split(':')
    if len(parts) != 3:
        raise RuntimeError(
            'transaction source_offset_or_equivalent must be '
            'height:transaction_index:txid'
        )
    height_raw, tx_index_raw, txid_raw = parts
    if not height_raw.isdigit() or not tx_index_raw.isdigit():
        raise RuntimeError(
            'transaction source_offset_or_equivalent must contain numeric '
            'height and transaction_index'
        )
    if txid_raw.strip() == '':
        raise RuntimeError('transaction source_offset_or_equivalent txid is empty')
    # Hash ordering token for deterministic lexical tie-break.
    txid_order = int.from_bytes(txid_raw.encode('utf-8'), 'big', signed=False)
    return (int(height_raw), int(tx_index_raw), txid_order)


def _parse_mempool_source_offset(value: Any) -> tuple[int, ...]:
    if not isinstance(value, str):
        raise RuntimeError(
            'mempool source_offset_or_equivalent must be string '
            'format=snapshot_at_unix_ms:txid'
        )
    parts = value.split(':')
    if len(parts) != 2:
        raise RuntimeError(
            'mempool source_offset_or_equivalent must be snapshot_at_unix_ms:txid'
        )
    snapshot_raw, txid_raw = parts
    if not snapshot_raw.isdigit():
        raise RuntimeError(
            'mempool source_offset_or_equivalent snapshot_at_unix_ms must be numeric'
        )
    if txid_raw.strip() == '':
        raise RuntimeError('mempool source_offset_or_equivalent txid is empty')
    txid_order = int.from_bytes(txid_raw.encode('utf-8'), 'big', signed=False)
    return (int(snapshot_raw), txid_order)


def _extract_events(
    *,
    dataset: BitcoinStreamAlignedDataset,
    payload_rows_json: str,
) -> list[_AlignedEvent]:
    try:
        decoded_payload = json.loads(payload_rows_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError('aligned payload_rows_json must be valid JSON') from exc

    payload_object = _expect_json_object(
        decoded_payload,
        label='aligned payload_rows_json',
    )
    rows = _expect_json_list(
        payload_object.get('rows'),
        label='aligned payload_rows_json.rows',
    )
    if rows == []:
        raise RuntimeError('aligned payload_rows_json.rows must be non-empty list')

    events: list[_AlignedEvent] = []
    for index, row_any in enumerate(rows):
        row = _expect_json_object(
            row_any,
            label=f'aligned payload_rows_json.rows[{index}]',
        )
        payload_json_raw = row.get('payload_json')
        if not isinstance(payload_json_raw, str):
            raise RuntimeError(
                'aligned payload row must include payload_json string '
                f'at index={index}'
            )
        try:
            decoded_payload_json = json.loads(payload_json_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f'aligned payload_json must be valid JSON at index={index}'
            ) from exc
        payload = _expect_json_object(
            decoded_payload_json,
            label=f'aligned payload_json[{index}]',
        )

        source_offset = row.get('source_offset_or_equivalent')
        if dataset == 'bitcoin_block_headers':
            order_key = _parse_header_source_offset(source_offset)
        elif dataset == 'bitcoin_block_transactions':
            order_key = _parse_transaction_source_offset(source_offset)
        else:
            order_key = _parse_mempool_source_offset(source_offset)

        events.append(_AlignedEvent(order_key=order_key, payload=payload))

    return sorted(events, key=lambda event: event.order_key)


def _build_where_clause(*, stream_id: str, window: QueryWindow) -> str:
    base_predicates = (
        f"source_id = '{_CANONICAL_SOURCE_ID}' "
        f"AND stream_id = '{stream_id}' "
        f"AND view_id = '{_ALIGNED_VIEW_ID}' "
        f'AND view_version = {_ALIGNED_VIEW_VERSION}'
    )

    if isinstance(window, MonthWindow):
        month_start = f'{window.year:04d}-{window.month:02d}-01'
        return (
            f'WHERE {base_predicates} '
            f"AND aligned_at_utc >= toDateTime64('{month_start} 00:00:00.000', 3, 'UTC') "
            f"AND aligned_at_utc < addMonths(toDateTime64('{month_start} 00:00:00.000', 3, 'UTC'), 1)"
        )

    if isinstance(window, TimeRangeWindow):
        start_ch = _to_clickhouse_datetime64_literal(window.start_iso)
        end_ch = _to_clickhouse_datetime64_literal(window.end_iso)
        return (
            f'WHERE {base_predicates} '
            f"AND aligned_at_utc >= toDateTime64('{start_ch}', 3, 'UTC') "
            f"AND aligned_at_utc < toDateTime64('{end_ch}', 3, 'UTC')"
        )

    return f'WHERE {base_predicates}'


def build_bitcoin_stream_aligned_1s_sql(
    *,
    dataset: BitcoinStreamAlignedDataset,
    window: QueryWindow,
    database: str,
) -> str:
    if not _IDENTIFIER_PATTERN.match(database):
        raise ValueError(f'Invalid database identifier: {database}')
    if not _IDENTIFIER_PATTERN.match(dataset):
        raise ValueError(f'Invalid dataset identifier: {dataset}')

    where_clause = _build_where_clause(stream_id=dataset, window=window)
    base_query = (
        'SELECT '
        'aligned_at_utc, '
        'payload_rows_json, '
        'first_source_offset_or_equivalent, '
        'last_source_offset_or_equivalent, '
        'latest_ingested_at_utc, '
        'bucket_sha256 '
        f'FROM {database}.{_CANONICAL_ALIGNED_TABLE} '
        f'{where_clause}'
    )

    if isinstance(window, LatestRowsWindow):
        return (
            'SELECT aligned_at_utc, payload_rows_json, first_source_offset_or_equivalent, '
            'last_source_offset_or_equivalent, latest_ingested_at_utc, bucket_sha256 '
            'FROM ('
            'SELECT aligned_at_utc, payload_rows_json, first_source_offset_or_equivalent, '
            'last_source_offset_or_equivalent, latest_ingested_at_utc, bucket_sha256 '
            f'FROM ({base_query}) '
            'ORDER BY aligned_at_utc DESC, first_source_offset_or_equivalent DESC '
            f'LIMIT {window.rows}'
            ') '
            'ORDER BY aligned_at_utc ASC, first_source_offset_or_equivalent ASC'
        )

    if isinstance(window, RandomRowsWindow):
        return (
            'SELECT aligned_at_utc, payload_rows_json, first_source_offset_or_equivalent, '
            'last_source_offset_or_equivalent, latest_ingested_at_utc, bucket_sha256 '
            'FROM ('
            'SELECT aligned_at_utc, payload_rows_json, first_source_offset_or_equivalent, '
            'last_source_offset_or_equivalent, latest_ingested_at_utc, bucket_sha256 '
            f'FROM ({base_query}) '
            'ORDER BY sipHash64(toUnixTimestamp64Milli(aligned_at_utc), bucket_sha256) '
            f'LIMIT {window.rows}'
            ') '
            'ORDER BY aligned_at_utc ASC, first_source_offset_or_equivalent ASC'
        )

    return (
        f'{base_query} '
        'ORDER BY aligned_at_utc ASC, first_source_offset_or_equivalent ASC'
    )


def _shape_header_frame(
    *,
    rows: list[tuple[Any, ...]],
    datetime_iso_output: bool,
) -> pl.DataFrame:
    shaped_rows: list[dict[str, Any]] = []
    for row in rows:
        aligned_at_utc = _to_utc_datetime(row[0], label='aligned_at_utc')
        payload_rows_json = str(row[1])
        first_offset = str(row[2])
        last_offset = str(row[3])
        latest_ingested_at_utc = _to_utc_datetime(row[4], label='latest_ingested_at_utc')
        bucket_sha256 = str(row[5])
        ordered_events = _extract_events(
            dataset='bitcoin_block_headers',
            payload_rows_json=payload_rows_json,
        )
        latest_payload = ordered_events[-1].payload
        shaped_rows.append(
            {
                'aligned_at_utc': aligned_at_utc,
                'records_in_bucket': len(ordered_events),
                'latest_height': int(latest_payload['height']),
                'latest_block_hash': str(latest_payload['block_hash']),
                'latest_prev_hash': str(latest_payload['prev_hash']),
                'latest_merkle_root': str(latest_payload['merkle_root']),
                'latest_version': int(latest_payload['version']),
                'latest_nonce': int(latest_payload['nonce']),
                'latest_difficulty': float(
                    _to_decimal(
                        latest_payload['difficulty'],
                        label='bitcoin_block_headers.latest_payload.difficulty',
                    )
                ),
                'latest_timestamp_ms': int(latest_payload['timestamp_ms']),
                'latest_source_chain': str(latest_payload['source_chain']),
                'latest_ingested_at_utc': latest_ingested_at_utc,
                'first_source_offset_or_equivalent': first_offset,
                'last_source_offset_or_equivalent': last_offset,
                'bucket_sha256': bucket_sha256,
            }
        )

    frame = pl.DataFrame(shaped_rows)
    if frame.is_empty():
        frame = pl.DataFrame(
            schema={
                'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'records_in_bucket': pl.Int64,
                'latest_height': pl.Int64,
                'latest_block_hash': pl.Utf8,
                'latest_prev_hash': pl.Utf8,
                'latest_merkle_root': pl.Utf8,
                'latest_version': pl.Int64,
                'latest_nonce': pl.Int64,
                'latest_difficulty': pl.Float64,
                'latest_timestamp_ms': pl.Int64,
                'latest_source_chain': pl.Utf8,
                'latest_ingested_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'first_source_offset_or_equivalent': pl.Utf8,
                'last_source_offset_or_equivalent': pl.Utf8,
                'bucket_sha256': pl.Utf8,
            }
        )

    if not frame.is_empty():
        frame = frame.sort(
            ['aligned_at_utc', 'first_source_offset_or_equivalent'],
            descending=[False, False],
        )

    if datetime_iso_output:
        frame = frame.with_columns(
            [
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc'),
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc'),
            ]
        )
    return frame


def _shape_transaction_frame(
    *,
    rows: list[tuple[Any, ...]],
    datetime_iso_output: bool,
) -> pl.DataFrame:
    shaped_rows: list[dict[str, Any]] = []
    for row in rows:
        aligned_at_utc = _to_utc_datetime(row[0], label='aligned_at_utc')
        payload_rows_json = str(row[1])
        first_offset = str(row[2])
        last_offset = str(row[3])
        latest_ingested_at_utc = _to_utc_datetime(row[4], label='latest_ingested_at_utc')
        bucket_sha256 = str(row[5])
        ordered_events = _extract_events(
            dataset='bitcoin_block_transactions',
            payload_rows_json=payload_rows_json,
        )

        total_input_value_btc = Decimal('0')
        total_output_value_btc = Decimal('0')
        coinbase_tx_count = 0
        for event in ordered_events:
            payload = event.payload
            values_json_raw = payload.get('values_json')
            if not isinstance(values_json_raw, str):
                raise RuntimeError(
                    'bitcoin_block_transactions payload.values_json must be string'
                )
            try:
                values_payload = json.loads(values_json_raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    'bitcoin_block_transactions payload.values_json must be valid JSON'
                ) from exc
            values_map = _expect_json_object(
                values_payload,
                label='bitcoin_block_transactions payload.values_json',
            )
            total_input_value_btc += _to_decimal(
                values_map.get('input_value_btc_sum'),
                label='bitcoin_block_transactions.input_value_btc_sum',
            )
            total_output_value_btc += _to_decimal(
                values_map.get('output_value_btc_sum'),
                label='bitcoin_block_transactions.output_value_btc_sum',
            )
            coinbase_raw = payload.get('coinbase')
            if not isinstance(coinbase_raw, bool):
                raise RuntimeError('bitcoin_block_transactions payload.coinbase must be bool')
            if coinbase_raw:
                coinbase_tx_count += 1

        first_payload = ordered_events[0].payload
        latest_payload = ordered_events[-1].payload
        shaped_rows.append(
            {
                'aligned_at_utc': aligned_at_utc,
                'records_in_bucket': len(ordered_events),
                'tx_count': len(ordered_events),
                'coinbase_tx_count': coinbase_tx_count,
                'total_input_value_btc': float(total_input_value_btc),
                'total_output_value_btc': float(total_output_value_btc),
                'first_block_height': int(first_payload['block_height']),
                'last_block_height': int(latest_payload['block_height']),
                'latest_block_timestamp_ms': int(latest_payload['block_timestamp_ms']),
                'latest_txid': str(latest_payload['txid']),
                'latest_source_chain': str(latest_payload['source_chain']),
                'latest_ingested_at_utc': latest_ingested_at_utc,
                'first_source_offset_or_equivalent': first_offset,
                'last_source_offset_or_equivalent': last_offset,
                'bucket_sha256': bucket_sha256,
            }
        )

    frame = pl.DataFrame(shaped_rows)
    if frame.is_empty():
        frame = pl.DataFrame(
            schema={
                'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'records_in_bucket': pl.Int64,
                'tx_count': pl.Int64,
                'coinbase_tx_count': pl.Int64,
                'total_input_value_btc': pl.Float64,
                'total_output_value_btc': pl.Float64,
                'first_block_height': pl.Int64,
                'last_block_height': pl.Int64,
                'latest_block_timestamp_ms': pl.Int64,
                'latest_txid': pl.Utf8,
                'latest_source_chain': pl.Utf8,
                'latest_ingested_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'first_source_offset_or_equivalent': pl.Utf8,
                'last_source_offset_or_equivalent': pl.Utf8,
                'bucket_sha256': pl.Utf8,
            }
        )

    if not frame.is_empty():
        frame = frame.sort(
            ['aligned_at_utc', 'first_source_offset_or_equivalent'],
            descending=[False, False],
        )

    if datetime_iso_output:
        frame = frame.with_columns(
            [
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc'),
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc'),
            ]
        )
    return frame


def _shape_mempool_frame(
    *,
    rows: list[tuple[Any, ...]],
    datetime_iso_output: bool,
) -> pl.DataFrame:
    shaped_rows: list[dict[str, Any]] = []
    for row in rows:
        aligned_at_utc = _to_utc_datetime(row[0], label='aligned_at_utc')
        payload_rows_json = str(row[1])
        first_offset = str(row[2])
        last_offset = str(row[3])
        latest_ingested_at_utc = _to_utc_datetime(row[4], label='latest_ingested_at_utc')
        bucket_sha256 = str(row[5])
        ordered_events = _extract_events(
            dataset='bitcoin_mempool_state',
            payload_rows_json=payload_rows_json,
        )

        fee_rates: list[Decimal] = []
        total_vsize = 0
        first_seen_values: list[int] = []
        rbf_true_count = 0
        for event in ordered_events:
            payload = event.payload
            fee_rates.append(
                _to_decimal(
                    payload.get('fee_rate_sat_vb'),
                    label='bitcoin_mempool_state.payload.fee_rate_sat_vb',
                )
            )
            total_vsize += int(payload['vsize'])
            first_seen_values.append(int(payload['first_seen_timestamp']))
            rbf_flag_raw = payload.get('rbf_flag')
            if not isinstance(rbf_flag_raw, bool):
                raise RuntimeError('bitcoin_mempool_state payload.rbf_flag must be bool')
            if rbf_flag_raw:
                rbf_true_count += 1

        latest_payload = ordered_events[-1].payload
        fee_rate_sum = sum(fee_rates, Decimal('0'))
        fee_rate_avg = fee_rate_sum / Decimal(len(fee_rates))

        shaped_rows.append(
            {
                'aligned_at_utc': aligned_at_utc,
                'records_in_bucket': len(ordered_events),
                'tx_count': len(ordered_events),
                'fee_rate_sat_vb_min': float(min(fee_rates)),
                'fee_rate_sat_vb_max': float(max(fee_rates)),
                'fee_rate_sat_vb_avg': float(fee_rate_avg),
                'total_vsize': total_vsize,
                'first_seen_timestamp_min': min(first_seen_values),
                'first_seen_timestamp_max': max(first_seen_values),
                'rbf_true_count': rbf_true_count,
                'latest_snapshot_at_unix_ms': int(latest_payload['snapshot_at_unix_ms']),
                'latest_txid': str(latest_payload['txid']),
                'latest_source_chain': str(latest_payload['source_chain']),
                'latest_ingested_at_utc': latest_ingested_at_utc,
                'first_source_offset_or_equivalent': first_offset,
                'last_source_offset_or_equivalent': last_offset,
                'bucket_sha256': bucket_sha256,
            }
        )

    frame = pl.DataFrame(shaped_rows)
    if frame.is_empty():
        frame = pl.DataFrame(
            schema={
                'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'records_in_bucket': pl.Int64,
                'tx_count': pl.Int64,
                'fee_rate_sat_vb_min': pl.Float64,
                'fee_rate_sat_vb_max': pl.Float64,
                'fee_rate_sat_vb_avg': pl.Float64,
                'total_vsize': pl.Int64,
                'first_seen_timestamp_min': pl.Int64,
                'first_seen_timestamp_max': pl.Int64,
                'rbf_true_count': pl.Int64,
                'latest_snapshot_at_unix_ms': pl.Int64,
                'latest_txid': pl.Utf8,
                'latest_source_chain': pl.Utf8,
                'latest_ingested_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'first_source_offset_or_equivalent': pl.Utf8,
                'last_source_offset_or_equivalent': pl.Utf8,
                'bucket_sha256': pl.Utf8,
            }
        )

    if not frame.is_empty():
        frame = frame.sort(
            ['aligned_at_utc', 'first_source_offset_or_equivalent'],
            descending=[False, False],
        )

    if datetime_iso_output:
        frame = frame.with_columns(
            [
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc'),
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc'),
            ]
        )
    return frame


def query_bitcoin_stream_aligned_1s_data(
    *,
    dataset: BitcoinStreamAlignedDataset,
    window: QueryWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    sql = build_bitcoin_stream_aligned_1s_sql(
        dataset=dataset,
        window=window,
        database=settings.database,
    )
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )

    started_at = time.time()
    try:
        assert_canonical_aligned_1s_storage_contract(
            client=client,
            database=settings.database,
        )
        rows = client.query(sql).result_rows
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)

    if dataset == 'bitcoin_block_headers':
        shaped = _shape_header_frame(
            rows=rows,
            datetime_iso_output=datetime_iso_output,
        )
    elif dataset == 'bitcoin_block_transactions':
        shaped = _shape_transaction_frame(
            rows=rows,
            datetime_iso_output=datetime_iso_output,
        )
    else:
        shaped = _shape_mempool_frame(
            rows=rows,
            datetime_iso_output=datetime_iso_output,
        )

    if show_summary:
        elapsed = time.time() - started_at
        logger.info(
            '%s | dataset=%s | rows=%d | cols=%d | %.2f GB RAM',
            f'{elapsed:.2f}s',
            dataset,
            shaped.shape[0],
            shaped.shape[1],
            shaped.estimated_size() / (1024**3),
        )
    return shaped


def validate_bitcoin_stream_aligned_selected_columns(
    *,
    dataset: BitcoinStreamAlignedDataset,
    selected_columns: list[str] | tuple[str, ...] | None,
) -> None:
    if selected_columns is None:
        return
    if selected_columns == []:
        raise ValueError('selected_columns cannot be empty when provided')
    allowed_columns = _BITCOIN_STREAM_ALIGNED_ALLOWED_COLUMNS[dataset]
    invalid_columns = sorted(set(selected_columns).difference(allowed_columns))
    if invalid_columns:
        raise ValueError(
            'Unsupported aligned projection fields for dataset='
            f'{dataset}: {invalid_columns}'
        )
