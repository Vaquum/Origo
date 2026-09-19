"""Validated native parsing of Binance perp aggregate-trade daily archives."""

from __future__ import annotations

import io
from typing import cast

import polars as pl

from ..arrow_types import ArrowTable
from ..contracts import Partition


COLUMNS = (
    'agg_trade_id',
    'price',
    'quantity',
    'first_trade_id',
    'last_trade_id',
    'timestamp',
    'is_buyer_maker',
)

# Modern archives carry this header; the 2019 days are headerless. The archive
# base skips the first line only when it matches, so both shapes parse.
HEADER = 'agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker'


def agg_table(body: bytes, partition: Partition) -> ArrowTable:
    frame = pl.read_csv(
        io.BytesIO(body), has_header=False, schema={c: pl.String for c in COLUMNS}, n_threads=2
    )
    if frame.height and frame.row(0) == tuple(HEADER.split(',')):
        frame = frame.slice(1)
    if not frame.height or frame.null_count().sum_horizontal().item():
        raise ValueError('Perp aggregate archive must contain non-empty complete rows.')
    integers = ['agg_trade_id', 'timestamp']
    signed = ['first_trade_id', 'last_trade_id']
    flags = ['is_buyer_maker']
    valid = frame.select(
        pl.all_horizontal(
            *(pl.col(c).str.contains(r'^[0-9]+$') for c in integers),
            *(pl.col(c).str.contains(r'^-?[0-9]+$') for c in signed),
            *(pl.col(c).is_in(['True', 'False', 'true', 'false']) for c in flags),
        ).all()
    ).item()
    if not valid:
        raise ValueError('Invalid perp aggregate integer or boolean field.')
    frame = frame.with_columns(
        *(pl.col(c).cast(pl.UInt64) for c in integers),
        *(pl.col(c).cast(pl.Int64) for c in signed),
        *(pl.col(c).cast(pl.Float64) for c in ('price', 'quantity')),
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
                for c in ('price', 'quantity')
            ),
            (pl.col('agg_trade_id') > pl.col('agg_trade_id').shift(1)).fill_null(True),
            (pl.col('datetime') >= pl.col('datetime').shift(1)).fill_null(True),
            pl.col('datetime').is_between(partition.start, partition.end, closed='left'),
        ).all()
    ).item()
    if not valid:
        raise ValueError(
            'Perp aggregate rows must be positive, finite, ordered, unique'
            ' and inside their partition.'
        )
    return cast(ArrowTable, frame.to_arrow())
