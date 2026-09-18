"""Spot aggregate dollar-imbalance formula: table names over the generic bar math."""

import pyarrow as pa
import pyarrow.compute as pc

from .generic_bars import imbalance_kline_rows

DOLLAR_IMBALANCE_KLINES_TABLE_NAME = 'binance_spot_aggtrades_dollar_imbalance_klines'
RAW_TABLE_NAME = 'binance_daily_spot_aggtrades'


def _kline_rows(table: pa.Table) -> pa.Table:
    # Aggregates carry no quote column; the imbalance math runs on the defined
    # aggregate notional (price times quantity per aggregate).
    quotes = pc.multiply(table.column('price'), table.column('quantity'))
    return imbalance_kline_rows(table.append_column('quote_quantity', quotes))
