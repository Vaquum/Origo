"""Perp dollar-imbalance formula: table names over the generic bar math."""

import pyarrow as pa

from .generic_bars import imbalance_kline_rows


def _kline_rows(table: pa.Table) -> pa.Table:
    return imbalance_kline_rows(table)
