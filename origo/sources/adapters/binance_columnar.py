"""Validated native parsing of Binance individual-trade daily archives."""

from __future__ import annotations

import hashlib
import io
from importlib import import_module
from typing import cast

import polars as pl

from ..arrow_types import ArrowIPC, ArrowModule, ArrowTable
from ..contracts import Partition

pa = cast(ArrowModule, import_module('pyarrow'))
ipc = cast(ArrowIPC, import_module('pyarrow.ipc'))


COLUMNS = (
    'trade_id',
    'price',
    'quantity',
    'quote_quantity',
    'timestamp',
    'is_buyer_maker',
    'is_best_match',
)


def spot_table(body: bytes, partition: Partition) -> ArrowTable:
    frame = pl.read_csv(
        io.BytesIO(body), has_header=False, schema={c: pl.String for c in COLUMNS}, n_threads=2
    )
    if not frame.height or frame.null_count().sum_horizontal().item():
        raise ValueError('Spot archive must contain non-empty complete rows.')
    integers = ['trade_id', 'timestamp']
    flags = ['is_buyer_maker', 'is_best_match']
    valid = frame.select(
        pl.all_horizontal(
            *(pl.col(c).str.contains(r'^[0-9]+$') for c in integers),
            *(pl.col(c).is_in(['True', 'False', 'true', 'false']) for c in flags),
        ).all()
    ).item()
    if not valid:
        raise ValueError('Invalid spot unsigned integer or boolean field.')
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
            'Spot rows must be positive, finite, ordered, unique and inside their partition.'
        )
    return cast(ArrowTable, frame.to_arrow())


def table_digest(table: ArrowTable) -> str:
    stream = pa.BufferOutputStream()
    with ipc.new_stream(stream, table.schema) as writer:
        writer.write_table(table.combine_chunks(), max_chunksize=65536)
    return 'arrow-v1:' + hashlib.sha256(stream.getvalue().to_pybytes()).hexdigest()
