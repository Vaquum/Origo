"""Monthly kline rollups for the local Parquet mirror.

Reuses the same aggregation contracts as the Hugging Face publishers:
- time klines roll the 1-minute Origo projection up to N-minute buckets with
  epoch bucketing and the law-of-total-variance std combine (PR #227's
  ``_get_binance_spot_klines_from_1m_projection``);
- dollar klines roll the day-scoped 1M Origo projection up by
  ``intDiv(dollar_bar_id, ratio)`` (PR #211's ``_get_binance_spot_dollar_klines``).

The mirror adds two things on top of those contracts so a month file is always
current to the latest closed minute:
- time months read ``base <= base_cut`` UNION ``latest > base_cut`` in one query
  (``base_cut`` resolved as a single scalar CTE, so the daily refresh cannot land
  a partition between two reads);
- dollar months read finalized days from the base and recompute the still-open
  day(s) from the rolling raw trades with the same within-day running-$-sum the
  base build uses.
"""

import os
import re
from collections.abc import Mapping
from datetime import datetime
from importlib import import_module
from typing import Protocol, cast

import polars as pl
import pyarrow as pa

DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
# The 1M dollar-bar base size; mirrors refresh_binance_spot_dollar_klines_origo.DOLLAR_KLINE_SIZE.
# Kept local so this query layer does not import the assets package.
DOLLAR_KLINE_SIZE = 1_000_000.0
# Prod ClickHouse runs with max_execution_time=0 (no limit); bound each export
# read so a wedged query cannot pile up across 10-minute ticks.
QUERY_TIMEOUT_SECONDS = 300

TIME_KLINE_COLUMNS = [
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "mean",
    "std",
    "volume",
    "maker_ratio",
    "no_of_trades",
    "open_liquidity",
    "high_liquidity",
    "low_liquidity",
    "close_liquidity",
    "liquidity_sum",
    "maker_volume",
    "maker_liquidity",
]
DOLLAR_KLINE_COLUMNS = [
    "start_datetime",
    "end_datetime",
    "dollar_bar_id",
    "open",
    "high",
    "low",
    "close",
    "mean",
    "std",
    "volume",
    "maker_ratio",
    "no_of_trades",
    "open_liquidity",
    "high_liquidity",
    "low_liquidity",
    "close_liquidity",
    "liquidity_sum",
    "maker_volume",
    "maker_liquidity",
]


class _ClickHouseArrowClientProtocol(Protocol):
    def query_arrow(
        self,
        query: str,
        parameters: Mapping[str, object] | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> pa.Table:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def _get_clickhouse_http_port() -> int:
    value = os.environ.get("CLICKHOUSE_HTTP_PORT", str(DEFAULT_CLICKHOUSE_HTTP_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError("CLICKHOUSE_HTTP_PORT environment variable must be an integer.") from exc


def _make_clickhouse_arrow_client() -> _ClickHouseArrowClientProtocol:
    client_factory = getattr(import_module("clickhouse_connect"), "get_client")
    return cast(
        _ClickHouseArrowClientProtocol,
        client_factory(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=_get_clickhouse_http_port(),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ["CLICKHOUSE_PASSWORD"],
        ),
    )


def _validate_identifier(value: str, field_name: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_]+", value) is None:
        raise ValueError(f"Invalid ClickHouse {field_name}: {value}")
    return value


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    if month < 1 or month > 12:
        raise ValueError(f"month must be in 1..12, got {month}")
    start = datetime(year, month, 1)
    end = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def _run_arrow(query: str, parameters: Mapping[str, object]) -> pa.Table:
    client = _make_clickhouse_arrow_client()
    try:
        return client.query_arrow(
            query,
            parameters=parameters,
            settings={"max_execution_time": QUERY_TIMEOUT_SECONDS},
        )
    finally:
        client.close()


def time_month(
    *,
    interval_minutes: int,
    year: int,
    month: int,
    base_table: str = "binance_spot_klines",
    latest_table: str = "binance_spot_klines_latest",
    database: str = "origo",
    source_after: str | None = None,
    source_before: str | None = None,
    base_cut: str | None = None,
) -> pl.DataFrame:
    """Roll the 1-minute base + rolling-latest projection up to ``interval_minutes``
    bars for one calendar month. ``interval_minutes == 1`` is a passthrough.

    ``source_after``/``source_before`` prune the base scan to a ``source_date``
    window (a superset of the month keeps boundary rows exact while the monthly
    parts do the skipping). ``base_cut`` reuses a cut computed once per render
    instead of re-scanning for it per month."""
    if interval_minutes < 1:
        raise ValueError("interval_minutes must be at least 1.")
    if (source_after is None) != (source_before is None):
        raise ValueError("source_after and source_before must be given together.")
    base_table = _validate_identifier(base_table, "table name")
    latest_table = _validate_identifier(latest_table, "table name")
    database = _validate_identifier(database, "database name")
    month_start, month_end = _month_bounds(year, month)
    bucket_seconds = interval_minutes * 60
    columns = ", ".join(TIME_KLINE_COLUMNS)
    cut_expr = (
        "toDateTime({cut:String})"
        if base_cut is not None
        else f"(SELECT max(datetime) FROM {database}.{base_table})"
    )
    prune_expr = (
        " AND source_date >= toDate({source_after:String})"
        " AND source_date < toDate({source_before:String})"
        if source_after is not None and source_before is not None
        else ""
    )

    query = f"""
        WITH {cut_expr} AS base_cut
        SELECT
            kline_datetime AS datetime,
            argMin(source_open, source_datetime) AS open,
            max(source_high) AS high,
            min(source_low) AS low,
            argMax(source_close, source_datetime) AS close,
            sum(source_no_of_trades * source_mean) / sum(source_no_of_trades) AS mean,
            if(
                {{bucket_seconds:UInt32}} = 60,
                argMin(source_std, source_datetime),
                sqrt(
                    greatest(
                        sum(source_no_of_trades * ((source_std * source_std) + (source_mean * source_mean))) / sum(source_no_of_trades)
                        - pow(sum(source_no_of_trades * source_mean) / sum(source_no_of_trades), 2),
                        0
                    )
                )
            ) AS std,
            sumKahan(source_volume) AS volume,
            sum(source_no_of_trades * source_maker_ratio) / sum(source_no_of_trades) AS maker_ratio,
            sum(source_no_of_trades) AS no_of_trades,
            argMin(source_open_liquidity, source_datetime) AS open_liquidity,
            max(source_high_liquidity) AS high_liquidity,
            min(source_low_liquidity) AS low_liquidity,
            argMax(source_close_liquidity, source_datetime) AS close_liquidity,
            sum(source_liquidity_sum) AS liquidity_sum,
            sumKahan(source_maker_volume) AS maker_volume,
            sum(source_maker_liquidity) AS maker_liquidity
        FROM (
            SELECT
                datetime AS source_datetime,
                open AS source_open,
                high AS source_high,
                low AS source_low,
                close AS source_close,
                mean AS source_mean,
                std AS source_std,
                volume AS source_volume,
                maker_ratio AS source_maker_ratio,
                no_of_trades AS source_no_of_trades,
                open_liquidity AS source_open_liquidity,
                high_liquidity AS source_high_liquidity,
                low_liquidity AS source_low_liquidity,
                close_liquidity AS source_close_liquidity,
                liquidity_sum AS source_liquidity_sum,
                maker_volume AS source_maker_volume,
                maker_liquidity AS source_maker_liquidity,
                toDateTime({{bucket_seconds:UInt32}} * intDiv(toUnixTimestamp(datetime), {{bucket_seconds:UInt32}})) AS kline_datetime
            FROM (
                SELECT {columns}
                FROM {database}.{base_table}
                WHERE datetime >= toDateTime({{start_dt:String}})
                  AND datetime < toDateTime({{end_dt:String}})
                  AND datetime <= base_cut{prune_expr}
                UNION ALL
                SELECT {columns}
                FROM {database}.{latest_table}
                WHERE datetime >= toDateTime({{start_dt:String}})
                  AND datetime < toDateTime({{end_dt:String}})
                  AND datetime > base_cut
            )
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime ASC
        """

    parameters: dict[str, object] = {
        "bucket_seconds": bucket_seconds,
        "start_dt": month_start,
        "end_dt": month_end,
    }
    if base_cut is not None:
        parameters["cut"] = base_cut
    if source_after is not None and source_before is not None:
        parameters["source_after"] = source_after
        parameters["source_before"] = source_before
    arrow_table = _run_arrow(query, parameters)
    data = cast(pl.DataFrame, pl.from_arrow(arrow_table)).select(TIME_KLINE_COLUMNS)
    if data.height == 0:
        return data

    return data.with_columns([
        (pl.col("datetime").cast(pl.Int64) * 1000)
        .cast(pl.Datetime("ms", time_zone="UTC"))
        .alias("datetime"),
        pl.col("mean").round(5),
        pl.col("std").round(6),
        pl.col("volume").round(9),
        pl.col("liquidity_sum").round(1),
        pl.col("maker_liquidity").round(1),
    ]).sort("datetime")


def dollar_month(
    *,
    ratio: int,
    year: int,
    month: int,
    base_table: str = "binance_spot_dollar_klines",
    raw_latest_table: str = "binance_spot_trades_latest",
    database: str = "origo",
    id_column: str = "trade_id",
    quote_expr: str = "quote_quantity",
    source_after: str | None = None,
    source_before: str | None = None,
    base_day: str | None = None,
) -> pl.DataFrame:
    """Roll the day-scoped 1M dollar base up by ``ratio`` for one calendar month.

    Finalized days (``toDate(start_datetime) <= base_day``) come from the base;
    still-open day(s) are recomputed from the rolling raw trades with the same
    within-day running-$-sum the base build uses. ``ratio == 1`` is a passthrough.

    ``source_after``/``source_before`` prune the base scan to a ``source_date``
    window (a superset of the month keeps boundary rows exact while the monthly
    parts do the skipping). ``base_day`` reuses a watermark computed once per
    render instead of re-scanning for it per month.
    """
    if ratio < 1:
        raise ValueError("ratio must be at least 1.")
    if (source_after is None) != (source_before is None):
        raise ValueError("source_after and source_before must be given together.")
    base_table = _validate_identifier(base_table, "table name")
    raw_latest_table = _validate_identifier(raw_latest_table, "table name")
    database = _validate_identifier(database, "database name")
    id_column = _validate_identifier(id_column, "column name")
    month_start, month_end = _month_bounds(year, month)
    columns = ", ".join(DOLLAR_KLINE_COLUMNS)
    day_expr = (
        "toDate({day:String})"
        if base_day is not None
        else f"(SELECT max(toDate(start_datetime)) FROM {database}.{base_table})"
    )
    prune_expr = (
        " AND source_date >= toDate({source_after:String})"
        " AND source_date < toDate({source_before:String})"
        if source_after is not None and source_before is not None
        else ""
    )

    query = f"""
        WITH {day_expr} AS base_day
        SELECT
            toDateTime64(min(source_start_datetime), 3) AS start_datetime,
            toDateTime64(max(source_end_datetime), 3) AS end_datetime,
            target_dollar_bar_id AS dollar_bar_id,
            argMin(source_open, tuple(source_start_datetime, source_dollar_bar_id)) AS open,
            max(source_high) AS high,
            min(source_low) AS low,
            argMax(source_close, tuple(source_end_datetime, source_dollar_bar_id)) AS close,
            sum(source_no_of_trades * source_mean) / sum(source_no_of_trades) AS mean,
            sqrt(
                greatest(
                    sum(source_no_of_trades * ((source_std * source_std) + (source_mean * source_mean))) / sum(source_no_of_trades)
                    - pow(sum(source_no_of_trades * source_mean) / sum(source_no_of_trades), 2),
                    0
                )
            ) AS std,
            sumKahan(source_volume) AS volume,
            sum(source_no_of_trades * source_maker_ratio) / sum(source_no_of_trades) AS maker_ratio,
            sum(source_no_of_trades) AS no_of_trades,
            argMin(source_open_liquidity, tuple(source_start_datetime, source_dollar_bar_id)) AS open_liquidity,
            max(source_high_liquidity) AS high_liquidity,
            min(source_low_liquidity) AS low_liquidity,
            argMax(source_close_liquidity, tuple(source_end_datetime, source_dollar_bar_id)) AS close_liquidity,
            sum(source_liquidity_sum) AS liquidity_sum,
            sumKahan(source_maker_volume) AS maker_volume,
            sum(source_maker_liquidity) AS maker_liquidity
        FROM (
            SELECT
                start_datetime AS source_start_datetime,
                end_datetime AS source_end_datetime,
                dollar_bar_id AS source_dollar_bar_id,
                open AS source_open,
                high AS source_high,
                low AS source_low,
                close AS source_close,
                mean AS source_mean,
                std AS source_std,
                volume AS source_volume,
                maker_ratio AS source_maker_ratio,
                no_of_trades AS source_no_of_trades,
                open_liquidity AS source_open_liquidity,
                high_liquidity AS source_high_liquidity,
                low_liquidity AS source_low_liquidity,
                close_liquidity AS source_close_liquidity,
                liquidity_sum AS source_liquidity_sum,
                maker_volume AS source_maker_volume,
                maker_liquidity AS source_maker_liquidity,
                toDate(start_datetime) AS bar_date,
                intDiv(dollar_bar_id, {{base_bar_count:UInt64}}) AS target_dollar_bar_id
            FROM (
                SELECT {columns}
                FROM {database}.{base_table}
                WHERE start_datetime >= toDateTime({{start_dt:String}})
                  AND start_datetime < toDateTime({{end_dt:String}})
                  AND toDate(start_datetime) <= base_day{prune_expr}
                UNION ALL
                SELECT
                    min(datetime) AS start_datetime,
                    max(datetime) AS end_datetime,
                    open_dollar_bar_id AS dollar_bar_id,
                    argMin(price, {id_column}) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    argMax(price, {id_column}) AS close,
                    avg(price) AS mean,
                    stddevPopStable(price) AS std,
                    sumKahan(quantity) AS volume,
                    avg(is_buyer_maker) AS maker_ratio,
                    count() AS no_of_trades,
                    argMin(price * quantity, {id_column}) AS open_liquidity,
                    max(price * quantity) AS high_liquidity,
                    min(price * quantity) AS low_liquidity,
                    argMax(price * quantity, {id_column}) AS close_liquidity,
                    sum(price * quantity) AS liquidity_sum,
                    sumKahan(is_buyer_maker * quantity) AS maker_volume,
                    sum(is_buyer_maker * price * quantity) AS maker_liquidity
                FROM (
                    SELECT
                        *,
                        toUInt64(floor(running_quote_before / {DOLLAR_KLINE_SIZE})) AS open_dollar_bar_id
                    FROM (
                        SELECT
                            *,
                            greatest(
                                sum({quote_expr}) OVER (
                                    PARTITION BY toDate(datetime)
                                    ORDER BY datetime, {id_column}
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                ) - {quote_expr},
                                0.0
                            ) AS running_quote_before
                        FROM {database}.{raw_latest_table}
                        WHERE datetime >= toDateTime64({{start_dt:String}}, 3)
                          AND datetime < toDateTime64({{end_dt:String}}, 3)
                          AND toDate(datetime) > base_day
                    )
                )
                GROUP BY toDate(datetime), open_dollar_bar_id
            )
        )
        GROUP BY bar_date, target_dollar_bar_id
        ORDER BY start_datetime ASC, dollar_bar_id ASC
        """

    parameters: dict[str, object] = {
        "base_bar_count": ratio,
        "start_dt": month_start,
        "end_dt": month_end,
    }
    if base_day is not None:
        parameters["day"] = base_day
    if source_after is not None and source_before is not None:
        parameters["source_after"] = source_after
        parameters["source_before"] = source_before
    arrow_table = _run_arrow(query, parameters)
    data = cast(pl.DataFrame, pl.from_arrow(arrow_table)).select(DOLLAR_KLINE_COLUMNS)
    if data.height == 0:
        return data

    return data.with_columns([
        pl.col("start_datetime").cast(pl.Datetime("ms", time_zone="UTC")),
        pl.col("end_datetime").cast(pl.Datetime("ms", time_zone="UTC")),
        pl.col("mean").round(5),
        pl.col("std").round(6),
        pl.col("volume").round(9),
        pl.col("liquidity_sum").round(1),
        pl.col("maker_liquidity").round(1),
    ]).sort(["start_datetime", "dollar_bar_id"])


def dollar_open_day_gap_days(
    *,
    base_table: str = "binance_spot_dollar_klines",
    raw_latest_table: str = "binance_spot_trades_latest",
    database: str = "origo",
) -> int:
    """Days after the dollar base watermark that the rolling raw table cannot cover.

    Returns 0 in steady state (the open day is within the raw 2-day window). A
    positive value means refresh_binance_spot_dollar_klines_origo has stalled past
    that window, so those days are absent from BOTH the base and the rolling raw and
    the month file develops a hole -- meant to be surfaced as a warning rather than a
    silent gap. (Reported only for a non-empty base, so a fresh deploy is not flagged.)
    """
    base_table = _validate_identifier(base_table, "table name")
    raw_latest_table = _validate_identifier(raw_latest_table, "table name")
    database = _validate_identifier(database, "database name")

    query = f"""
        SELECT toInt64(
            if(
                base_day >= toDate('2017-08-01') AND raw_min_open > base_day,
                greatest(dateDiff('day', base_day, raw_min_open) - 1, 0),
                0
            )
        ) AS gap_days
        FROM (
            SELECT
                (SELECT max(toDate(start_datetime)) FROM {database}.{base_table}) AS base_day,
                (
                    SELECT min(toDate(datetime))
                    FROM {database}.{raw_latest_table}
                    WHERE toDate(datetime) > (SELECT max(toDate(start_datetime)) FROM {database}.{base_table})
                ) AS raw_min_open
        )
        """
    arrow_table = _run_arrow(query, {})
    value = cast(pl.DataFrame, pl.from_arrow(arrow_table)).item()
    if value is None:
        return 0
    return int(value)
