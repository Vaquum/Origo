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

BitcoinDerivedAlignedDataset = Literal[
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]

_BITCOIN_DERIVED_ALIGNED_OBS_COLUMNS: tuple[str, ...] = (
    'aligned_at_utc',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'dimensions_json',
    'provenance_json',
    'latest_ingested_at_utc',
    'records_in_bucket',
)


@dataclass(frozen=True)
class BitcoinDerivedAligned1sMaterialization:
    stream_id: str
    metric_name: str
    metric_unit: str
    metric_value_payload_key: str

    def __post_init__(self) -> None:
        for value, label in [
            (self.stream_id, 'stream_id'),
            (self.metric_name, 'metric_name'),
            (self.metric_unit, 'metric_unit'),
            (self.metric_value_payload_key, 'metric_value_payload_key'),
        ]:
            if value.strip() == '':
                raise ValueError(f'{label} must be non-empty')
        if not _IDENTIFIER_PATTERN.match(self.stream_id):
            raise ValueError(f'Invalid stream_id: {self.stream_id}')


_BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS: dict[
    BitcoinDerivedAlignedDataset, BitcoinDerivedAligned1sMaterialization
] = {
    'bitcoin_block_fee_totals': BitcoinDerivedAligned1sMaterialization(
        stream_id='bitcoin_block_fee_totals',
        metric_name='block_fee_total_btc',
        metric_unit='BTC',
        metric_value_payload_key='fee_total_btc',
    ),
    'bitcoin_block_subsidy_schedule': BitcoinDerivedAligned1sMaterialization(
        stream_id='bitcoin_block_subsidy_schedule',
        metric_name='block_subsidy_btc',
        metric_unit='BTC',
        metric_value_payload_key='subsidy_btc',
    ),
    'bitcoin_network_hashrate_estimate': BitcoinDerivedAligned1sMaterialization(
        stream_id='bitcoin_network_hashrate_estimate',
        metric_name='network_hashrate_hs',
        metric_unit='H/s',
        metric_value_payload_key='hashrate_hs',
    ),
    'bitcoin_circulating_supply': BitcoinDerivedAligned1sMaterialization(
        stream_id='bitcoin_circulating_supply',
        metric_name='btc_circulating_supply_btc',
        metric_unit='BTC',
        metric_value_payload_key='circulating_supply_btc',
    ),
}


@dataclass(frozen=True)
class _BucketEvent:
    source_offset_or_equivalent: str
    source_event_time_utc: datetime
    ingested_at_utc: datetime
    event_id: str
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


def _coerce_iso_datetime(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be ISO datetime string')
    return _parse_iso_datetime(value)


def _parse_decimal_float(value: Any, *, label: str) -> float:
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must be decimal-compatible, got bool')
    if isinstance(value, (int, float, str, Decimal)):
        try:
            return float(Decimal(str(value)))
        except InvalidOperation as exc:
            raise RuntimeError(f'{label} must be decimal-compatible') from exc
    raise RuntimeError(f'{label} must be decimal-compatible')


def _canonical_json_or_null(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be object or null')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return json.dumps(
        normalized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )


def _offset_sort_key(value: str) -> tuple[int, int | str]:
    try:
        return (0, int(value))
    except ValueError:
        return (1, value)


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


def build_bitcoin_derived_aligned_1s_sql(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    window: QueryWindow,
    database: str,
) -> str:
    if not _IDENTIFIER_PATTERN.match(database):
        raise ValueError(f'Invalid database identifier: {database}')

    stream_id = _BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS[dataset].stream_id
    where_clause = _build_where_clause(stream_id=stream_id, window=window)
    base_query = (
        'SELECT '
        'aligned_at_utc, '
        'payload_rows_json, '
        'first_source_offset_or_equivalent, '
        'bucket_sha256 '
        f'FROM {database}.{_CANONICAL_ALIGNED_TABLE} '
        f'{where_clause}'
    )

    if isinstance(window, LatestRowsWindow):
        return (
            'SELECT aligned_at_utc, payload_rows_json, '
            'first_source_offset_or_equivalent, bucket_sha256 '
            'FROM ('
            'SELECT aligned_at_utc, payload_rows_json, '
            'first_source_offset_or_equivalent, bucket_sha256 '
            f'FROM ({base_query}) '
            'ORDER BY aligned_at_utc DESC, first_source_offset_or_equivalent DESC '
            f'LIMIT {window.rows}'
            ') '
            'ORDER BY aligned_at_utc ASC, first_source_offset_or_equivalent ASC'
        )

    if isinstance(window, RandomRowsWindow):
        return (
            'SELECT aligned_at_utc, payload_rows_json, '
            'first_source_offset_or_equivalent, bucket_sha256 '
            'FROM ('
            'SELECT aligned_at_utc, payload_rows_json, '
            'first_source_offset_or_equivalent, bucket_sha256 '
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


def _parse_bucket_events(payload_rows_json: Any) -> list[_BucketEvent]:
    if not isinstance(payload_rows_json, str):
        raise RuntimeError('payload_rows_json must be string')
    try:
        decoded_payload = json.loads(payload_rows_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError('payload_rows_json must be valid JSON') from exc
    if not isinstance(decoded_payload, dict):
        raise RuntimeError('payload_rows_json must decode to object')
    decoded_map = cast(dict[Any, Any], decoded_payload)
    rows_any = decoded_map.get('rows')
    if not isinstance(rows_any, list) or rows_any == []:
        raise RuntimeError('payload_rows_json.rows must be non-empty list')

    rows = cast(list[Any], rows_any)
    events: list[_BucketEvent] = []
    for index, row_any in enumerate(rows):
        if not isinstance(row_any, dict):
            raise RuntimeError(f'payload_rows_json.rows[{index}] must be object')
        row = cast(dict[Any, Any], row_any)

        payload_json_value = row.get('payload_json')
        if not isinstance(payload_json_value, str):
            raise RuntimeError(f'payload_rows_json.rows[{index}].payload_json must be string')
        try:
            payload_object = json.loads(payload_json_value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f'payload_rows_json.rows[{index}].payload_json must be valid JSON'
            ) from exc
        if not isinstance(payload_object, dict):
            raise RuntimeError(
                f'payload_rows_json.rows[{index}].payload_json must decode to object'
            )

        source_offset = row.get('source_offset_or_equivalent')
        if not isinstance(source_offset, str) or source_offset.strip() == '':
            raise RuntimeError(
                f'payload_rows_json.rows[{index}].source_offset_or_equivalent must be string'
            )
        source_event_time_utc = _coerce_iso_datetime(
            row.get('source_event_time_utc'),
            label=f'payload_rows_json.rows[{index}].source_event_time_utc',
        )
        ingested_at_utc = _coerce_iso_datetime(
            row.get('ingested_at_utc'),
            label=f'payload_rows_json.rows[{index}].ingested_at_utc',
        )
        event_id = row.get('event_id')
        if not isinstance(event_id, str) or event_id.strip() == '':
            raise RuntimeError(
                f'payload_rows_json.rows[{index}].event_id must be non-empty string'
            )

        payload_map = cast(dict[Any, Any], payload_object)
        normalized_payload: dict[str, Any] = {}
        for raw_key, raw_value in payload_map.items():
            if not isinstance(raw_key, str):
                raise RuntimeError(
                    f'payload_rows_json.rows[{index}].payload_json keys must be strings'
                )
            normalized_payload[raw_key] = raw_value

        events.append(
            _BucketEvent(
                source_offset_or_equivalent=source_offset,
                source_event_time_utc=source_event_time_utc,
                ingested_at_utc=ingested_at_utc,
                event_id=event_id,
                payload=normalized_payload,
            )
        )
    return events


def _bucket_to_observation_row(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    aligned_at_utc: Any,
    payload_rows_json: Any,
) -> dict[str, Any]:
    bucket_aligned_at_utc = _to_utc_datetime(aligned_at_utc, label='aligned_at_utc')
    materialization = _BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS[dataset]
    events = _parse_bucket_events(payload_rows_json)

    selected_event = max(
        events,
        key=lambda event: (
            event.source_event_time_utc,
            _offset_sort_key(event.source_offset_or_equivalent),
            event.event_id,
        ),
    )

    payload = selected_event.payload
    metric_name_raw = payload.get('metric_name')
    metric_unit_raw = payload.get('metric_unit')
    if isinstance(metric_name_raw, str) and metric_name_raw.strip() != '':
        metric_name = metric_name_raw
    else:
        metric_name = materialization.metric_name
    if isinstance(metric_unit_raw, str) and metric_unit_raw.strip() != '':
        metric_unit = metric_unit_raw
    else:
        metric_unit = materialization.metric_unit

    metric_value_raw = payload.get('metric_value')
    if metric_value_raw is None:
        metric_value_raw = payload.get(materialization.metric_value_payload_key)
    metric_value_float = _parse_decimal_float(
        metric_value_raw,
        label='metric_value',
    )

    provenance_json = _canonical_json_or_null(payload.get('provenance'), label='provenance')

    return {
        'aligned_at_utc': bucket_aligned_at_utc,
        'source_id': _CANONICAL_SOURCE_ID,
        'metric_name': metric_name,
        'metric_unit': metric_unit,
        'metric_value_string': None,
        'metric_value_int': None,
        'metric_value_float': metric_value_float,
        'metric_value_bool': None,
        'dimensions_json': '{}',
        'provenance_json': provenance_json,
        'latest_ingested_at_utc': max(event.ingested_at_utc for event in events),
        'records_in_bucket': len(events),
    }


def _observation_schema() -> dict[str, Any]:
    return {
        'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
        'source_id': pl.Utf8,
        'metric_name': pl.Utf8,
        'metric_unit': pl.Utf8,
        'metric_value_string': pl.Utf8,
        'metric_value_int': pl.Int64,
        'metric_value_float': pl.Float64,
        'metric_value_bool': pl.UInt8,
        'dimensions_json': pl.Utf8,
        'provenance_json': pl.Utf8,
        'latest_ingested_at_utc': pl.Datetime('ms', time_zone='UTC'),
        'records_in_bucket': pl.UInt64,
    }


def _shape_aligned_frame(*, frame: pl.DataFrame, datetime_iso_output: bool) -> pl.DataFrame:
    if frame.height == 0:
        empty = pl.DataFrame(schema=_observation_schema())
        if datetime_iso_output:
            empty = empty.with_columns(
                [
                    pl.col('aligned_at_utc')
                    .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                    .alias('aligned_at_utc'),
                    pl.col('latest_ingested_at_utc')
                    .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                    .alias('latest_ingested_at_utc'),
                ]
            )
        return empty

    shaped = frame.sort(['aligned_at_utc', 'source_id', 'metric_name'])
    if datetime_iso_output:
        shaped = shaped.with_columns(
            [
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc'),
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc'),
            ]
        )
    return shaped


def _query_bitcoin_derived_aligned_prior_state(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    start_iso: str,
    auth_token: str | None,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    materialization = _BITCOIN_DERIVED_ALIGNED_1S_MATERIALIZATIONS[dataset]
    start_ch = _to_clickhouse_datetime64_literal(start_iso)

    sql = (
        'SELECT aligned_at_utc, payload_rows_json '
        f'FROM {settings.database}.{_CANONICAL_ALIGNED_TABLE} '
        f"WHERE source_id = '{_CANONICAL_SOURCE_ID}' "
        f"AND stream_id = '{materialization.stream_id}' "
        f"AND view_id = '{_ALIGNED_VIEW_ID}' "
        f'AND view_version = {_ALIGNED_VIEW_VERSION} '
        f"AND aligned_at_utc < toDateTime64('{start_ch}', 3, 'UTC') "
        'ORDER BY aligned_at_utc DESC, first_source_offset_or_equivalent DESC '
        'LIMIT 1'
    )

    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )
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

    if rows == []:
        return pl.DataFrame(schema=_observation_schema())

    observations = [
        _bucket_to_observation_row(
            dataset=dataset,
            aligned_at_utc=row[0],
            payload_rows_json=row[1],
        )
        for row in rows
    ]
    return _shape_aligned_frame(
        frame=pl.DataFrame(observations, schema=_observation_schema()),
        datetime_iso_output=False,
    )


def _build_bitcoin_derived_forward_fill_intervals(
    *,
    observations: pl.DataFrame,
    window_start: datetime,
    window_end: datetime,
    datetime_iso_output: bool,
) -> pl.DataFrame:
    if window_start >= window_end:
        raise ValueError(
            'forward-fill window must satisfy start < end '
            f'(start={window_start.isoformat()}, end={window_end.isoformat()})'
        )

    if observations.height == 0:
        return pl.DataFrame(
            schema={
                'source_id': pl.Utf8,
                'metric_name': pl.Utf8,
                'metric_unit': pl.Utf8,
                'metric_value_string': pl.Utf8,
                'metric_value_int': pl.Int64,
                'metric_value_float': pl.Float64,
                'metric_value_bool': pl.UInt8,
                'dimensions_json': pl.Utf8,
                'provenance_json': pl.Utf8,
                'latest_ingested_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'records_in_bucket': pl.UInt64,
                'aligned_at_utc': pl.Datetime('ms', time_zone='UTC'),
                'valid_from_utc': pl.Datetime('ms', time_zone='UTC'),
                'valid_to_utc_exclusive': pl.Datetime('ms', time_zone='UTC'),
            }
        )

    for column in _BITCOIN_DERIVED_ALIGNED_OBS_COLUMNS:
        if column not in observations.columns:
            raise RuntimeError(
                f'forward-fill observations are missing required column: {column}'
            )

    normalized = (
        observations.select(_BITCOIN_DERIVED_ALIGNED_OBS_COLUMNS)
        .sort(['source_id', 'metric_name', 'aligned_at_utc', 'latest_ingested_at_utc'])
        .unique(
            subset=['source_id', 'metric_name', 'aligned_at_utc'],
            keep='last',
            maintain_order=True,
        )
    )

    intervals = normalized.with_columns(
        pl.col('aligned_at_utc')
        .shift(-1)
        .over(['source_id', 'metric_name'])
        .alias('_next_aligned_at_utc')
    ).with_columns(
        [
            pl.when(pl.col('aligned_at_utc') < pl.lit(window_start))
            .then(pl.lit(window_start))
            .otherwise(pl.col('aligned_at_utc'))
            .alias('valid_from_utc'),
            pl.when(
                pl.col('_next_aligned_at_utc').is_null()
                | (pl.col('_next_aligned_at_utc') > pl.lit(window_end))
            )
            .then(pl.lit(window_end))
            .otherwise(pl.col('_next_aligned_at_utc'))
            .alias('valid_to_utc_exclusive'),
        ]
    ).drop('_next_aligned_at_utc')

    intervals = intervals.filter(
        (pl.col('valid_to_utc_exclusive') > pl.col('valid_from_utc'))
        & (pl.col('valid_to_utc_exclusive') > pl.lit(window_start))
        & (pl.col('valid_from_utc') < pl.lit(window_end))
    ).sort(['source_id', 'metric_name', 'valid_from_utc'])

    if datetime_iso_output:
        intervals = intervals.with_columns(
            [
                pl.col('aligned_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('aligned_at_utc'),
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc'),
                pl.col('valid_from_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('valid_from_utc'),
                pl.col('valid_to_utc_exclusive')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('valid_to_utc_exclusive'),
            ]
        )

    return intervals


def query_bitcoin_derived_aligned_1s_data(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    window: QueryWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    sql = build_bitcoin_derived_aligned_1s_sql(
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

    observations = [
        _bucket_to_observation_row(
            dataset=dataset,
            aligned_at_utc=row[0],
            payload_rows_json=row[1],
        )
        for row in rows
    ]
    shaped = _shape_aligned_frame(
        frame=pl.DataFrame(observations, schema=_observation_schema()),
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


def query_bitcoin_derived_forward_fill_intervals(
    *,
    dataset: BitcoinDerivedAlignedDataset,
    window: TimeRangeWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    window_start = _parse_iso_datetime(window.start_iso)
    window_end = _parse_iso_datetime(window.end_iso)

    in_window = query_bitcoin_derived_aligned_1s_data(
        dataset=dataset,
        window=window,
        auth_token=auth_token,
        show_summary=False,
        datetime_iso_output=False,
    )
    prior_state = _query_bitcoin_derived_aligned_prior_state(
        dataset=dataset,
        start_iso=window.start_iso,
        auth_token=auth_token,
    )

    combined = pl.concat([prior_state, in_window], how='vertical_relaxed')
    started_at = time.time()
    intervals = _build_bitcoin_derived_forward_fill_intervals(
        observations=combined,
        window_start=window_start,
        window_end=window_end,
        datetime_iso_output=datetime_iso_output,
    )
    if show_summary:
        elapsed = time.time() - started_at
        logger.info(
            '%s | dataset=%s | intervals=%d | cols=%d | %.2f GB RAM',
            f'{elapsed:.2f}s',
            dataset,
            intervals.shape[0],
            intervals.shape[1],
            intervals.estimated_size() / (1024**3),
        )
    return intervals
