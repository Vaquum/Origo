"""Validated native parsing of Binance perpetual-trade daily archives."""

from __future__ import annotations

import io
from typing import cast

import polars as pl

from ..arrow_types import ArrowTable
from ..contracts import Partition

COLUMNS = (
    'trade_id',
    'price',
    'quantity',
    'quote_quantity',
    'timestamp',
    'is_buyer_maker',
)

# Current archives carry this header line; 2019 archives have none. Either shape is
# accepted; anything else in the first row must parse as a data row.
HEADER = 'id,price,qty,quote_qty,time,is_buyer_maker'


def perp_table(body: bytes, partition: Partition) -> ArrowTable:
    frame = pl.read_csv(
        io.BytesIO(body), has_header=False, schema={c: pl.String for c in COLUMNS}, n_threads=2
    )
    if frame.row(0) == tuple(HEADER.split(',')):
        frame = frame.slice(1)
    if not frame.height or frame.null_count().sum_horizontal().item():
        raise ValueError('Perp archive must contain non-empty complete rows.')
    integers = ['trade_id', 'timestamp']
    flags = ['is_buyer_maker']
    valid = frame.select(
        pl.all_horizontal(
            *(pl.col(c).str.contains(r'^[0-9]+$') for c in integers),
            *(pl.col(c).is_in(['True', 'False', 'true', 'false']) for c in flags),
        ).all()
    ).item()
    if not valid:
        raise ValueError('Invalid perp unsigned integer or boolean field.')
    frame = frame.with_columns(
        *(pl.col(c).cast(pl.UInt64) for c in integers),
        *(pl.col(c).cast(pl.Float64) for c in ('price', 'quantity', 'quote_quantity')),
        *((pl.col(c).str.to_lowercase() == 'true').cast(pl.UInt8) for c in flags),
    )
    stamp = pl.col('timestamp')
    millis = stamp.is_between(10**12, 10**13 - 1)
    micros = stamp.is_between(10**15, 10**16 - 1)
    frame = frame.with_columns(
        pl.when(millis)
        .then(stamp * 1000)
        .otherwise(stamp)
        .cast(pl.Datetime('us', 'UTC'))
        .alias('datetime')
    )
    valid = frame.select(
        pl.all_horizontal(
            millis | micros,
            *(
                pl.col(c).is_finite() & (pl.col(c) > 0)
                for c in ('price', 'quantity', 'quote_quantity')
            ),
            (pl.col('trade_id') > pl.col('trade_id').shift(1)).fill_null(True),
            (pl.col('datetime') >= pl.col('datetime').shift(1)).fill_null(True),
            pl.col('datetime').is_between(partition.start, partition.end, closed='left'),
        ).all()
    ).item()
    if not valid:
        raise ValueError(
            'Perp rows must be positive, finite, ordered, unique and inside their partition.'
        )
    return cast(ArrowTable, frame.to_arrow())
