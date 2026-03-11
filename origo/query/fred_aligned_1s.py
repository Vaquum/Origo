from __future__ import annotations

import hashlib
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
_CANONICAL_ALIGNED_TABLE = 'canonical_aligned_1s_aggregates'
_CANONICAL_SOURCE_ID = 'fred'
_FRED_ALIGNED_OBS_COLUMNS: tuple[str, ...] = (
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
FREDAlignedDataset = Literal['fred_series_metrics']


@dataclass(frozen=True)
class FREDAligned1sMaterialization:
    stream_id: str

    def __post_init__(self) -> None:
        if not _IDENTIFIER_PATTERN.match(self.stream_id):
            raise ValueError(f'Invalid stream_id: {self.stream_id}')


@dataclass(frozen=True)
class _AlignedMetricEvent:
    aligned_at_utc: datetime
    source_id: str
    metric_name: str
    metric_unit: str | None
    metric_value_string: str | None
    metric_value_int: int | None
    metric_value_float: float | None
    metric_value_bool: int | None
    dimensions_json: str
    provenance_json: str | None
    latest_ingested_at_utc: datetime
    source_offset_or_equivalent: str
    event_id: str


_FRED_ALIGNED_1S_MATERIALIZATIONS: dict[
    FREDAlignedDataset, FREDAligned1sMaterialization
] = {
    'fred_series_metrics': FREDAligned1sMaterialization(
        stream_id='fred_series_metrics',
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


def _to_utc_datetime(value: Any, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise RuntimeError(f'{label} must be datetime')
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _build_where_clause(
    *,
    stream_id: str,
    window: QueryWindow,
) -> str:
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


def _expect_str(payload: dict[str, Any], *, key: str, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f'{label}.{key} must be string')
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError(f'{label}.{key} must be non-empty string')
    return normalized


def _optional_str(payload: dict[str, Any], *, key: str, label: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f'{label}.{key} must be string or null')
    normalized = value.strip()
    if normalized == '':
        return None
    return normalized


def _canonical_json_or_null(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be object or null')
    value_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in value_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} object keys must be strings')
        normalized[raw_key] = raw_value
    return json.dumps(
        normalized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )


def _decode_metric_value(
    *,
    payload: dict[str, Any],
    label: str,
) -> tuple[str | None, int | None, float | None, int | None]:
    metric_value_kind = _expect_str(payload, key='metric_value_kind', label=label)
    metric_value_text = _optional_str(payload, key='metric_value_text', label=label)
    metric_value_bool_raw = payload.get('metric_value_bool')

    if metric_value_bool_raw is None:
        metric_value_bool: int | None = None
    elif isinstance(metric_value_bool_raw, bool):
        metric_value_bool = 1 if metric_value_bool_raw else 0
    else:
        raise RuntimeError(f'{label}.metric_value_bool must be bool or null')

    if metric_value_kind == 'null':
        return (None, None, None, None)

    if metric_value_kind == 'bool':
        if metric_value_bool is None or metric_value_text not in {'true', 'false'}:
            raise RuntimeError(
                f'{label} boolean payload requires '
                'metric_value_bool and metric_value_text=true|false'
            )
        return (metric_value_text, None, None, metric_value_bool)

    if metric_value_kind == 'int':
        if metric_value_text is None:
            raise RuntimeError(f'{label} integer payload requires metric_value_text')
        try:
            metric_value_int = int(metric_value_text)
        except ValueError as exc:
            raise RuntimeError(
                f'{label} integer payload metric_value_text must parse as int'
            ) from exc
        return (metric_value_text, metric_value_int, float(metric_value_int), None)

    if metric_value_kind == 'float':
        if metric_value_text is None:
            raise RuntimeError(f'{label} float payload requires metric_value_text')
        try:
            metric_value_float = float(Decimal(metric_value_text))
        except InvalidOperation as exc:
            raise RuntimeError(
                f'{label} float payload metric_value_text must parse as decimal'
            ) from exc
        return (metric_value_text, None, metric_value_float, None)

    if metric_value_kind == 'string':
        if metric_value_text is None:
            raise RuntimeError(f'{label} string payload requires metric_value_text')
        return (metric_value_text, None, None, None)

    raise RuntimeError(f'{label} has unsupported metric_value_kind={metric_value_kind}')


def _extract_metric_events_from_bucket(
    *,
    aligned_at_utc: Any,
    payload_rows_json: Any,
) -> list[_AlignedMetricEvent]:
    bucket_aligned_at_utc = _to_utc_datetime(aligned_at_utc, label='aligned_at_utc')
    if not isinstance(payload_rows_json, str):
        raise RuntimeError('payload_rows_json must be string')

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

    events: list[_AlignedMetricEvent] = []
    for index, row_any in enumerate(rows_raw):
        row = _expect_json_object(
            row_any,
            label=f'aligned payload row[{index}]',
        )
        payload_json_raw = row.get('payload_json')
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

        source_id = _expect_str(
            payload,
            key='record_source_id',
            label=f'aligned payload row[{index}] payload_json',
        )
        metric_name = _expect_str(
            payload,
            key='metric_name',
            label=f'aligned payload row[{index}] payload_json',
        )
        metric_unit = _optional_str(
            payload,
            key='metric_unit',
            label=f'aligned payload row[{index}] payload_json',
        )
        (
            metric_value_string,
            metric_value_int,
            metric_value_float,
            metric_value_bool,
        ) = _decode_metric_value(
            payload=payload,
            label=f'aligned payload row[{index}] payload_json',
        )
        dimensions_json = _canonical_json_or_null(
            payload.get('dimensions'),
            label=f'aligned payload row[{index}] payload_json.dimensions',
        )
        if dimensions_json is None:
            raise RuntimeError(
                f'aligned payload row[{index}] payload_json.dimensions must be object'
            )
        provenance_json = _canonical_json_or_null(
            payload.get('provenance'),
            label=f'aligned payload row[{index}] payload_json.provenance',
        )

        ingested_at_raw = _expect_str(
            row,
            key='ingested_at_utc',
            label=f'aligned payload row[{index}]',
        )
        ingested_at_utc = _parse_iso_datetime(ingested_at_raw)
        source_offset_or_equivalent = _expect_str(
            row,
            key='source_offset_or_equivalent',
            label=f'aligned payload row[{index}]',
        )
        event_id = _expect_str(
            row,
            key='event_id',
            label=f'aligned payload row[{index}]',
        )

        events.append(
            _AlignedMetricEvent(
                aligned_at_utc=bucket_aligned_at_utc,
                source_id=source_id,
                metric_name=metric_name,
                metric_unit=metric_unit,
                metric_value_string=metric_value_string,
                metric_value_int=metric_value_int,
                metric_value_float=metric_value_float,
                metric_value_bool=metric_value_bool,
                dimensions_json=dimensions_json,
                provenance_json=provenance_json,
                latest_ingested_at_utc=ingested_at_utc,
                source_offset_or_equivalent=source_offset_or_equivalent,
                event_id=event_id,
            )
        )

    return events


def _events_to_observation_frame(
    *,
    events: list[_AlignedMetricEvent],
) -> pl.DataFrame:
    if events == []:
        return pl.DataFrame(
            schema={
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
        )

    grouped: dict[tuple[datetime, str, str], list[_AlignedMetricEvent]] = {}
    for event in events:
        key = (event.aligned_at_utc, event.source_id, event.metric_name)
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = [event]
        else:
            existing.append(event)

    output_rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        aligned_at_utc, source_id, metric_name = key
        group = sorted(
            grouped[key],
            key=lambda item: (
                item.latest_ingested_at_utc,
                item.source_offset_or_equivalent,
                item.event_id,
            ),
        )
        latest = group[-1]
        output_rows.append(
            {
                'aligned_at_utc': aligned_at_utc,
                'source_id': source_id,
                'metric_name': metric_name,
                'metric_unit': latest.metric_unit,
                'metric_value_string': latest.metric_value_string,
                'metric_value_int': latest.metric_value_int,
                'metric_value_float': latest.metric_value_float,
                'metric_value_bool': latest.metric_value_bool,
                'dimensions_json': latest.dimensions_json,
                'provenance_json': latest.provenance_json,
                'latest_ingested_at_utc': latest.latest_ingested_at_utc,
                'records_in_bucket': len(group),
            }
        )

    return pl.DataFrame(output_rows).sort(['aligned_at_utc', 'source_id', 'metric_name'])


def _row_random_hash(row: dict[str, Any]) -> int:
    aligned_at_utc = row.get('aligned_at_utc')
    latest_ingested_at_utc = row.get('latest_ingested_at_utc')
    if not isinstance(aligned_at_utc, datetime):
        raise RuntimeError('aligned_at_utc must be datetime for random-row hash')
    if not isinstance(latest_ingested_at_utc, datetime):
        raise RuntimeError('latest_ingested_at_utc must be datetime for random-row hash')
    payload = (
        f"{aligned_at_utc.astimezone(UTC).isoformat()}|"
        f"{row.get('source_id')}|"
        f"{row.get('metric_name')}|"
        f"{latest_ingested_at_utc.astimezone(UTC).isoformat()}|"
        f"{row.get('records_in_bucket')}"
    )
    digest = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    return int(digest[:16], 16)


def _apply_window(
    *,
    frame: pl.DataFrame,
    window: QueryWindow,
) -> pl.DataFrame:
    sorted_frame = frame.sort(['aligned_at_utc', 'source_id', 'metric_name'])
    if isinstance(window, LatestRowsWindow):
        return sorted_frame.tail(window.rows)

    if isinstance(window, RandomRowsWindow):
        if window.rows >= sorted_frame.height:
            return sorted_frame
        row_dicts = sorted_frame.to_dicts()
        ranked = sorted(
            ((_row_random_hash(row), row) for row in row_dicts),
            key=lambda item: item[0],
        )
        selected = [item[1] for item in ranked[: window.rows]]
        return pl.DataFrame(selected, schema=sorted_frame.schema)

    return sorted_frame


def _query_bucket_rows(
    *,
    dataset: FREDAlignedDataset,
    window: QueryWindow,
    auth_token: str | None,
) -> list[tuple[Any, ...]]:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    sql = build_fred_aligned_1s_sql(
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
    try:
        assert_canonical_aligned_1s_storage_contract(
            client=client,
            database=settings.database,
        )
        return client.query(sql).result_rows
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning('Failed to close ClickHouse client cleanly: %s', exc)


def _parse_bucket_rows(
    *,
    rows: list[tuple[Any, ...]],
) -> list[_AlignedMetricEvent]:
    events: list[_AlignedMetricEvent] = []
    for row in rows:
        if len(row) != 2:
            raise RuntimeError(
                'FRED aligned query expected exactly two columns '
                '(aligned_at_utc, payload_rows_json)'
            )
        events.extend(
            _extract_metric_events_from_bucket(
                aligned_at_utc=row[0],
                payload_rows_json=row[1],
            )
        )
    return events


def build_fred_aligned_1s_sql(
    *,
    dataset: FREDAlignedDataset,
    window: QueryWindow,
    database: str,
) -> str:
    if not _IDENTIFIER_PATTERN.match(database):
        raise ValueError(f'Invalid database identifier: {database}')

    materialization = _FRED_ALIGNED_1S_MATERIALIZATIONS[dataset]
    where_clause = _build_where_clause(
        stream_id=materialization.stream_id,
        window=window,
    )
    return (
        'SELECT '
        'aligned_at_utc, '
        'payload_rows_json '
        f'FROM {database}.{_CANONICAL_ALIGNED_TABLE} '
        f'{where_clause} '
        'ORDER BY aligned_at_utc ASC'
    )


def _shape_aligned_frame(
    *, frame: pl.DataFrame, datetime_iso_output: bool
) -> pl.DataFrame:
    shaped = frame.sort(['aligned_at_utc', 'source_id', 'metric_name'])

    if datetime_iso_output:
        shaped = shaped.with_columns(
            pl.col('aligned_at_utc')
            .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
            .alias('aligned_at_utc')
        )
        if 'latest_ingested_at_utc' in shaped.columns:
            shaped = shaped.with_columns(
                pl.col('latest_ingested_at_utc')
                .dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ')
                .alias('latest_ingested_at_utc')
            )

    return shaped


def _query_fred_aligned_prior_state(
    *,
    dataset: FREDAlignedDataset,
    start_iso: str,
    auth_token: str | None,
) -> pl.DataFrame:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    materialization = _FRED_ALIGNED_1S_MATERIALIZATIONS[dataset]
    start_ch = _to_clickhouse_datetime64_literal(start_iso)
    sql = (
        'SELECT '
        'aligned_at_utc, '
        'payload_rows_json '
        f'FROM {settings.database}.{_CANONICAL_ALIGNED_TABLE} '
        f"WHERE source_id = '{_CANONICAL_SOURCE_ID}' "
        f"AND stream_id = '{materialization.stream_id}' "
        f"AND view_id = '{_ALIGNED_VIEW_ID}' "
        f'AND view_version = {_ALIGNED_VIEW_VERSION} '
        f"AND aligned_at_utc < toDateTime64('{start_ch}', 3, 'UTC') "
        'ORDER BY aligned_at_utc ASC'
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

    observations = _events_to_observation_frame(events=_parse_bucket_rows(rows=rows))
    if observations.height == 0:
        return observations

    latest = observations.sort(
        ['source_id', 'metric_name', 'aligned_at_utc', 'latest_ingested_at_utc']
    ).unique(
        subset=['source_id', 'metric_name'],
        keep='last',
        maintain_order=True,
    )
    return _shape_aligned_frame(frame=latest, datetime_iso_output=False)


def _build_fred_forward_fill_intervals(
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

    for column in _FRED_ALIGNED_OBS_COLUMNS:
        if column not in observations.columns:
            raise RuntimeError(
                f'forward-fill observations are missing required column: {column}'
            )

    normalized = (
        observations.select(_FRED_ALIGNED_OBS_COLUMNS)
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


def query_fred_aligned_1s_data(
    *,
    dataset: FREDAlignedDataset,
    window: QueryWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    started_at = time.time()
    bucket_rows = _query_bucket_rows(
        dataset=dataset,
        window=window,
        auth_token=auth_token,
    )
    observations = _events_to_observation_frame(
        events=_parse_bucket_rows(rows=bucket_rows)
    )
    windowed = _apply_window(frame=observations, window=window)
    shaped = _shape_aligned_frame(
        frame=windowed,
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


def query_fred_forward_fill_intervals(
    *,
    dataset: FREDAlignedDataset,
    window: TimeRangeWindow,
    auth_token: str | None = None,
    show_summary: bool = False,
    datetime_iso_output: bool = False,
) -> pl.DataFrame:
    window_start = _parse_iso_datetime(window.start_iso)
    window_end = _parse_iso_datetime(window.end_iso)

    in_window = query_fred_aligned_1s_data(
        dataset=dataset,
        window=window,
        auth_token=auth_token,
        show_summary=False,
        datetime_iso_output=False,
    )
    prior_state = _query_fred_aligned_prior_state(
        dataset=dataset,
        start_iso=window.start_iso,
        auth_token=auth_token,
    )

    combined = pl.concat([prior_state, in_window], how='vertical_relaxed')
    started_at = time.time()
    intervals = _build_fred_forward_fill_intervals(
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
