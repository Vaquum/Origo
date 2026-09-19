"""Generic bar math shared by every market-structure source (PRD-0013 row 4).

The SQL and arithmetic below are the relocated spot formulas, unchanged; table
names, dataset literals, bar sizes, the raw id column, and the dollar quote
expression arrive as parameters. Per-source formula modules keep their
table-name constants and delegate their entry points here.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import cast

import numpy as np
import numpy.typing as npt
import pyarrow as pa
from clickhouse_driver import Client as ClickhouseClient

from origo.assets.create_origo_database import ClickHouseClientProtocol

DOLLAR_IMBALANCE_KLINE_SIZE = 100_000.0
DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP = 512


def _partition_datetime_bounds(partition_date: str) -> tuple[str, str]:
    start_datetime = datetime.strptime(partition_date, '%Y-%m-%d')
    end_datetime = start_datetime + timedelta(days=1)
    return (
        start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
    )


def _minute_bounds(minute_start: datetime) -> tuple[str, str]:
    minute_end = minute_start + timedelta(minutes=1)
    return (
        minute_start.strftime('%Y-%m-%d %H:%M:%S'),
        minute_end.strftime('%Y-%m-%d %H:%M:%S'),
    )


def insert_time_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
    *,
    klines_table: str,
    raw_table: str,
    id_column: str = 'trade_id',
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{klines_table}
        SELECT
            kline_datetime AS datetime,
            argMin(price, {id_column}) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, {id_column}) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
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
                toDateTime(60 * intDiv(toUnixTimestamp(datetime), 60)) AS kline_datetime
            FROM {database}.{raw_table}
            WHERE toDate(datetime) = toDate('{partition_date}')
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )


def insert_time_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
    *,
    klines_table: str,
    raw_table: str,
    id_column: str = 'trade_id',
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        INSERT INTO {database}.{klines_table}
        SELECT
            kline_datetime AS datetime,
            argMin(price, {id_column}) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, {id_column}) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
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
                toDateTime(60 * intDiv(toUnixTimestamp(datetime), 60)) AS kline_datetime
            FROM {database}.{raw_table}
            WHERE datetime >= toDateTime64('{start_datetime}', 3)
              AND datetime < toDateTime64('{end_datetime}', 3)
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )


def insert_dollar_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
    *,
    klines_table: str,
    raw_table: str,
    kline_size: float,
    id_column: str = 'trade_id',
    quote_expr: str = 'quote_quantity',
) -> None:
    start_datetime, end_datetime = _partition_datetime_bounds(partition_date)
    client.execute(
        f"""
        INSERT INTO {database}.{klines_table}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            dollar_bar_id,
            argMin(price, {id_column}) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, {id_column}) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
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
                toUInt64(floor(running_quote_before / {kline_size})) AS dollar_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum({quote_expr}) OVER (
                            ORDER BY datetime, {id_column}
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - {quote_expr},
                        0.0
                    ) AS running_quote_before
                FROM {database}.{raw_table}
                WHERE datetime >= toDateTime64('{start_datetime}', 6)
                  AND datetime < toDateTime64('{end_datetime}', 6)
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
    )


def insert_dollar_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
    *,
    klines_table: str,
    raw_table: str,
    kline_size: float,
    id_column: str = 'trade_id',
    quote_expr: str = 'quote_quantity',
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        INSERT INTO {database}.{klines_table}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            dollar_bar_id,
            argMin(price, {id_column}) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, {id_column}) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
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
                toUInt64(floor(running_quote_before / {kline_size})) AS dollar_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum({quote_expr}) OVER (
                            ORDER BY datetime, {id_column}
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - {quote_expr},
                        0.0
                    ) AS running_quote_before
                FROM {database}.{raw_table}
                WHERE datetime >= toDateTime64('{start_datetime}', 3)
                  AND datetime < toDateTime64('{end_datetime}', 3)
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
    )


def insert_volume_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
    *,
    klines_table: str,
    raw_table: str,
    kline_size: float,
    id_column: str = 'trade_id',
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{klines_table}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            volume_bar_id,
            argMin(price, {id_column}) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, {id_column}) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
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
                toUInt64(floor(running_volume_before / {kline_size})) AS volume_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum(quantity) OVER (
                            ORDER BY datetime, {id_column}
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - quantity,
                        0.0
                    ) AS running_volume_before
                FROM {database}.{raw_table}
                WHERE toDate(datetime) = toDate('{partition_date}')
            )
        )
        GROUP BY volume_bar_id
        ORDER BY volume_bar_id
        """
    )


def insert_tick_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
    *,
    klines_table: str,
    raw_table: str,
    kline_size: int,
    id_column: str = 'trade_id',
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{klines_table}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            tick_bar_id,
            argMin(price, {id_column}) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, {id_column}) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
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
                toUInt64(intDiv(running_trade_count_before, {kline_size})) AS tick_bar_id
            FROM (
                SELECT
                    *,
                    row_number() OVER (ORDER BY datetime, {id_column}) - 1 AS running_trade_count_before
                FROM {database}.{raw_table}
                WHERE toDate(datetime) = toDate('{partition_date}')
            )
        )
        GROUP BY tick_bar_id
        ORDER BY tick_bar_id
        """
    )


def insert_aligned_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
    *,
    aligned_table: str,
    klines_table: str,
    dataset_source: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{aligned_table}
        SELECT
            '{dataset_source}' AS dataset_source,
            datetime,
            open,
            high,
            low,
            close,
            mean,
            std,
            median,
            iqr,
            volume,
            maker_ratio,
            no_of_trades,
            open_liquidity,
            high_liquidity,
            low_liquidity,
            close_liquidity,
            liquidity_sum,
            maker_volume,
            maker_liquidity
        FROM {database}.{klines_table}
        WHERE toDate(datetime) = toDate('{partition_date}')
        ORDER BY datetime
        """
    )


def _float64_column(table: pa.Table, column_name: str) -> npt.NDArray[np.float64]:
    return np.asarray(table.column(column_name), dtype=np.float64)


def _uint8_column(table: pa.Table, column_name: str) -> npt.NDArray[np.uint8]:
    return np.asarray(table.column(column_name), dtype=np.uint8)


def _datetime_column(table: pa.Table, column_name: str) -> npt.NDArray[np.datetime64]:
    return cast(npt.NDArray[np.datetime64], np.asarray(table.column(column_name)))


def _bar_boundaries(
    quote_quantities: npt.NDArray[np.float64],
    maker_flags: npt.NDArray[np.uint8],
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    signed_quotes = quote_quantities.copy()
    signed_quotes[maker_flags == 1] *= -1.0
    cumulative_quotes = np.cumsum(signed_quotes, out=signed_quotes)

    starts: list[int] = []
    ends: list[int] = []
    start_index = 0
    row_count = len(cumulative_quotes)

    while start_index < row_count:
        base_quote = cumulative_quotes[start_index - 1] if start_index > 0 else 0.0
        end_index = min(start_index + DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP, row_count)

        while True:
            window = np.abs(cumulative_quotes[start_index:end_index] - base_quote)
            hits = np.flatnonzero(window >= DOLLAR_IMBALANCE_KLINE_SIZE)
            if len(hits) > 0:
                starts.append(start_index)
                ends.append(start_index + int(hits[0]))
                break

            if end_index == row_count:
                starts.append(start_index)
                ends.append(row_count - 1)
                break

            end_index = min(end_index + DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP, row_count)

        start_index = ends[-1] + 1

    return np.array(starts, dtype=np.int64), np.array(ends, dtype=np.int64)


def _quantile_index(length: int, level: float) -> int:
    index = int(level * length)
    if index == length:
        index -= 1
    return index


def _price_distribution_columns(
    prices: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
    ends: npt.NDArray[np.int64],
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    stds: list[float] = []
    medians: list[float] = []
    iqrs: list[float] = []

    for start, end in zip(starts, ends, strict=True):
        bar_prices = prices[start : end + 1]
        row_count = len(bar_prices)
        q25_index = _quantile_index(row_count, 0.25)
        median_index = _quantile_index(row_count, 0.5)
        q75_index = _quantile_index(row_count, 0.75)
        partitioned_prices = np.partition(bar_prices, (q25_index, median_index, q75_index))

        stds.append(float(bar_prices.std()))
        medians.append(float(partitioned_prices[median_index]))
        iqrs.append(float(partitioned_prices[q75_index] - partitioned_prices[q25_index]))

    return (
        np.array(stds, dtype=np.float64),
        np.array(medians, dtype=np.float64),
        np.array(iqrs, dtype=np.float64),
    )


def _sum_by_bar(
    values: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
) -> npt.NDArray[np.float64]:
    return cast(npt.NDArray[np.float64], np.add.reduceat(values, starts))


def _max_by_bar(
    values: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
) -> npt.NDArray[np.float64]:
    return cast(npt.NDArray[np.float64], np.maximum.reduceat(values, starts))


def _min_by_bar(
    values: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
) -> npt.NDArray[np.float64]:
    return cast(npt.NDArray[np.float64], np.minimum.reduceat(values, starts))


def _arrow_array(values: Sequence[object] | npt.NDArray[np.generic]) -> pa.Array:
    return pa.array(values)


def imbalance_kline_rows(table: pa.Table) -> pa.Table:
    prices = _float64_column(table, 'price')
    quantities = _float64_column(table, 'quantity')
    quote_quantities = _float64_column(table, 'quote_quantity')
    maker_flags = _uint8_column(table, 'is_buyer_maker')
    datetimes = _datetime_column(table, 'datetime')

    starts, ends = _bar_boundaries(quote_quantities, maker_flags)
    lengths = (ends - starts + 1).astype(np.uint64)

    liquidities = prices * quantities
    taker_buy_quotes = quote_quantities.copy()
    taker_buy_quotes[maker_flags == 1] = 0.0
    taker_buy_liquidity = _sum_by_bar(taker_buy_quotes, starts)
    del taker_buy_quotes

    taker_sell_quotes = quote_quantities.copy()
    taker_sell_quotes[maker_flags == 0] = 0.0
    taker_sell_liquidity = _sum_by_bar(taker_sell_quotes, starts)
    del taker_sell_quotes

    maker_float = maker_flags.astype(np.float64)
    maker_volume = _sum_by_bar(maker_float * quantities, starts)
    maker_liquidity = _sum_by_bar(maker_float * liquidities, starts)
    stds, medians, iqrs = _price_distribution_columns(prices, starts, ends)

    return pa.table(
        {
            'start_datetime': _arrow_array(datetimes[starts]),
            'end_datetime': _arrow_array(datetimes[ends]),
            'dollar_imbalance_bar_id': _arrow_array(np.arange(len(starts), dtype=np.uint64)),
            'open': _arrow_array(prices[starts]),
            'high': _arrow_array(_max_by_bar(prices, starts)),
            'low': _arrow_array(_min_by_bar(prices, starts)),
            'close': _arrow_array(prices[ends]),
            'mean': _arrow_array(_sum_by_bar(prices, starts) / lengths),
            'std': _arrow_array(stds),
            'median': _arrow_array(medians),
            'iqr': _arrow_array(iqrs),
            'volume': _arrow_array(_sum_by_bar(quantities, starts)),
            'maker_ratio': _arrow_array(_sum_by_bar(maker_float, starts) / lengths),
            'no_of_trades': _arrow_array(lengths),
            'open_liquidity': _arrow_array(liquidities[starts]),
            'high_liquidity': _arrow_array(_max_by_bar(liquidities, starts)),
            'low_liquidity': _arrow_array(_min_by_bar(liquidities, starts)),
            'close_liquidity': _arrow_array(liquidities[ends]),
            'liquidity_sum': _arrow_array(_sum_by_bar(liquidities, starts)),
            'maker_volume': _arrow_array(maker_volume),
            'maker_liquidity': _arrow_array(maker_liquidity),
            'taker_buy_liquidity': _arrow_array(taker_buy_liquidity),
            'taker_sell_liquidity': _arrow_array(taker_sell_liquidity),
            'dollar_imbalance': _arrow_array(taker_buy_liquidity - taker_sell_liquidity),
        }
    )
