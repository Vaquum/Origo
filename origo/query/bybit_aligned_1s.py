from __future__ import annotations

import json
import logging
import re
import sys
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
_CANONICAL_SOURCE_ID = 'bybit'

BybitAlignedDataset = Literal['bybit_spot_trades']


@dataclass(frozen=True)
class BybitAligned1sMaterialization:
    stream_id: str

    def __post_init__(self) -> None:
        if not _IDENTIFIER_PATTERN.match(self.stream_id):
            raise ValueError(f'Invalid stream_id: {self.stream_id}')


@dataclass(frozen=True)
class _AlignedTradeEvent:
    order_key: tuple[int, int | str]
    price: Decimal
    size: Decimal
    quote_quantity: Decimal


_BYBIT_ALIGNED_1S_MATERIALIZATIONS: dict[
    BybitAlignedDataset, BybitAligned1sMaterialization
] = {
    'bybit_spot_trades': BybitAligned1sMaterialization(
        stream_id='bybit_spot_trades',
    )
}


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_clickhouse_datetime64_literal(value: str) -> str:
    parsed = _parse_iso_datetime(value)
    return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


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


def build_bybit_aligned_1s_sql(
    *,
    dataset: BybitAlignedDataset,
    window: QueryWindow,
    database: str,
) -> str:
    if not _IDENTIFIER_PATTERN.match(database):
        raise ValueError(f'Invalid database identifier: {database}')

    materialization = _BYBIT_ALIGNED_1S_MATERIALIZATIONS[dataset]
    where_clause = _build_where_clause(
        stream_id=materialization.stream_id,
        window=window,
    )

    base_query = (
        'SELECT '
        'aligned_at_utc, '
        'payload_rows_json, '
        'first_source_offset_or_equivalent, '
        'bucket_sha256 '
        f'FROM {database}.canonical_aligned_1s_aggregates '
        f'{where_clause}'
    )

    if isinstance(window, LatestRowsWindow):
        return (
            'SELECT '
            'aligned_at_utc, '
            'payload_rows_json, '
            'first_source_offset_or_equivalent, '
            'bucket_sha256 '
            'FROM ('
            'SELECT '
            'aligned_at_utc, '
            'payload_rows_json, '
            'first_source_offset_or_equivalent, '
            'bucket_sha256 '
            f'FROM ({base_query}) '
            'ORDER BY aligned_at_utc DESC, first_source_offset_or_equivalent DESC '
            f'LIMIT {window.rows}'
            ') '
            'ORDER BY aligned_at_utc ASC, first_source_offset_or_equivalent ASC'
        )

    if isinstance(window, RandomRowsWindow):
        return (
            'SELECT '
            'aligned_at_utc, '
            'payload_rows_json, '
            'first_source_offset_or_equivalent, '
            'bucket_sha256 '
            'FROM ('
            'SELECT '
            'aligned_at_utc, '
            'payload_rows_json, '
            'first_source_offset_or_equivalent, '
            'bucket_sha256 '
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


def _require_decimal(value: Any, *, label: str) -> Decimal:
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must not be bool')
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise RuntimeError(f'{label} must be decimal-compatible') from exc
    raise RuntimeError(f'{label} must be int|float|string|Decimal')


def _parse_order_key(value: Any, *, label: str) -> tuple[int, int | str]:
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must not be bool')
    if isinstance(value, int):
        return (0, value)
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be string or int')
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label} must be non-empty')
    if normalized.isdigit():
        return (0, int(normalized))
    return (1, normalized)


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


def _extract_trade_events(payload_rows_json: str) -> list[_AlignedTradeEvent]:
    try:
        payload_raw = json.loads(payload_rows_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError('aligned payload_rows_json must be valid JSON') from exc

    payload_object = _expect_json_object(payload_raw, label='aligned payload_rows_json')
    rows_raw = _expect_json_list(
        payload_object.get('rows'),
        label='aligned payload_rows_json.rows',
    )
    if rows_raw == []:
        raise RuntimeError('aligned payload_rows_json.rows must be non-empty list')

    events: list[_AlignedTradeEvent] = []
    for index, row_raw_any in enumerate(rows_raw):
        row_raw = _expect_json_object(
            row_raw_any,
            label=f'aligned payload row[{index}]',
        )
        payload_json_raw = row_raw.get('payload_json')
        if not isinstance(payload_json_raw, str):
            raise RuntimeError(
                f'aligned payload row[{index}].payload_json must be string'
            )
        try:
            payload_obj = json.loads(payload_json_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f'aligned payload row[{index}] payload_json must be valid JSON'
            ) from exc
        payload = _expect_json_object(
            payload_obj,
            label=f'aligned payload row[{index}] payload_json',
        )

        price = _require_decimal(payload.get('price'), label='payload.price')
        size = _require_decimal(payload.get('size'), label='payload.size')
        quote_quantity = price * size
        order_key = _parse_order_key(
            row_raw.get('source_offset_or_equivalent'),
            label='row.source_offset_or_equivalent',
        )
        events.append(
            _AlignedTradeEvent(
                order_key=order_key,
                price=price,
                size=size,
                quote_quantity=quote_quantity,
            )
        )

    if events == []:
        raise RuntimeError('aligned payload_rows_json.rows must include at least one event')
    return events


def _to_utc_datetime(value: Any, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise RuntimeError(f'{label} must be datetime')
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _shape_aligned_frame(*, rows: list[tuple[Any, ...]], datetime_iso_output: bool) -> pl.DataFrame:
    buckets: dict[datetime, list[_AlignedTradeEvent]] = {}
    for row in rows:
        aligned_at_utc = _to_utc_datetime(row[0], label='aligned_at_utc')
        payload_rows_json = str(row[1])
        bucket_events = _extract_trade_events(payload_rows_json)
        existing = buckets.get(aligned_at_utc)
        if existing is None:
            buckets[aligned_at_utc] = bucket_events
        else:
            existing.extend(bucket_events)

    output_rows: list[dict[str, Any]] = []
    for aligned_at_utc in sorted(buckets):
        ordered_events = sorted(buckets[aligned_at_utc], key=lambda event: event.order_key)
        prices = [event.price for event in ordered_events]
        quantity_sum = sum((event.size for event in ordered_events), Decimal('0'))
        quote_volume_sum = sum(
            (event.quote_quantity for event in ordered_events),
            Decimal('0'),
        )
        output_rows.append(
            {
                'aligned_at_utc': aligned_at_utc,
                'open_price': float(prices[0]),
                'high_price': float(max(prices)),
                'low_price': float(min(prices)),
                'close_price': float(prices[-1]),
                'quantity_sum': float(quantity_sum),
                'quote_volume_sum': float(quote_volume_sum),
                'trade_count': len(ordered_events),
            }
        )

    frame = pl.DataFrame(output_rows)
    if frame.is_empty():
        empty_frame = pl.DataFrame(
            schema={
                'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'open_price': pl.Float64,
                'high_price': pl.Float64,
                'low_price': pl.Float64,
                'close_price': pl.Float64,
                'quantity_sum': pl.Float64,
                'quote_volume_sum': pl.Float64,
                'trade_count': pl.Int64,
            }
        )
        if datetime_iso_output:
            return empty_frame.with_columns(
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc')
            )
        return empty_frame

    frame = frame.sort('aligned_at_utc')
    if datetime_iso_output:
        frame = frame.with_columns(
            pl.col('aligned_at_utc')
            .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
            .alias('aligned_at_utc')
        )
    return frame


def query_bybit_aligned_1s_data(
    *,
    dataset: BybitAlignedDataset,
    window: QueryWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    sql = build_bybit_aligned_1s_sql(
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
        rows = cast(list[tuple[Any, ...]], client.query(sql).result_rows)
    finally:
        try:
            client.close()
        except Exception as exc:
            active_exception = sys.exc_info()[1]
            if active_exception is not None:
                active_exception.add_note(
                    f'ClickHouse client close failed during cleanup: {exc}'
                )
                logger.warning('Failed to close ClickHouse client cleanly: %s', exc)
            else:
                raise RuntimeError(
                    f'Failed to close ClickHouse client cleanly: {exc}'
                ) from exc

    shaped = _shape_aligned_frame(
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
