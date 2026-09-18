"""Spot dollar-imbalance formula: table names over the generic bar math."""

import pyarrow as pa

from .generic_bars import imbalance_kline_rows

DOLLAR_IMBALANCE_KLINES_TABLE_NAME = 'binance_spot_dollar_imbalance_klines'
RAW_TABLE_NAME = 'binance_daily_spot_trades'

DOLLAR_IMBALANCE_KLINE_SIZE = 100_000.0


def _kline_rows(table: pa.Table) -> pa.Table:
    return imbalance_kline_rows(table)
