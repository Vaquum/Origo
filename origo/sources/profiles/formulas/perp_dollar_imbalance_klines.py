"""Dollar-imbalance bars ported from the spot formula; the arithmetic is unchanged.

Only the revisioned entry point (`_kline_rows`) is carried over; the retired
asset's ClickHouse wiring is not.
"""

from collections.abc import Sequence
from typing import cast

import numpy as np
import numpy.typing as npt
import pyarrow as pa

DOLLAR_IMBALANCE_KLINE_SIZE = 100_000.0


DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP = 512


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


def _kline_rows(table: pa.Table) -> pa.Table:
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
