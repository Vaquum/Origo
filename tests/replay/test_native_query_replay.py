from __future__ import annotations

import polars as pl

from origo.query.binance_aligned_1s import build_binance_aligned_1s_sql
from origo.query.bitcoin_derived_aligned_1s import (
    build_bitcoin_derived_aligned_1s_sql,
)
from origo.query.bitcoin_native import build_bitcoin_native_query_spec
from origo.query.bybit_aligned_1s import build_bybit_aligned_1s_sql
from origo.query.bybit_native import build_bybit_native_query_spec
from origo.query.native_core import (
    NativeQuerySpec,
    TimeRangeWindow,
    _compile_native_query,
    _shape_native_frame,
)
from origo.query.okx_aligned_1s import build_okx_aligned_1s_sql
from origo.query.okx_native import build_okx_native_query_spec


def test_compile_native_query_is_deterministic() -> None:
    spec = NativeQuerySpec(
        table_name='binance_trades',
        id_column='trade_id',
        select_columns=('trade_id', 'price', 'quantity', 'trade_id'),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
    )

    compiled_run_1 = _compile_native_query(spec, 'origo')
    compiled_run_2 = _compile_native_query(spec, 'origo')

    assert compiled_run_1 == compiled_run_2
    assert compiled_run_1.sql == compiled_run_2.sql


def test_shape_native_frame_is_deterministic() -> None:
    spec = NativeQuerySpec(
        table_name='binance_trades',
        id_column='trade_id',
        select_columns=('trade_id', 'price'),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
    )
    compiled = _compile_native_query(spec, 'origo')

    frame = pl.DataFrame(
        {
            '__origo_datetime_ms': [1704067201000, 1704067200000],
            'trade_id': [2, 1],
            'price': [100.0, 99.0],
        }
    )

    shaped_run_1 = _shape_native_frame(frame, spec.id_column, compiled)
    shaped_run_2 = _shape_native_frame(frame, spec.id_column, compiled)

    assert shaped_run_1.to_dict(as_series=False) == shaped_run_2.to_dict(as_series=False)


def test_compile_okx_native_query_is_deterministic() -> None:
    spec = build_okx_native_query_spec(
        dataset='okx_spot_trades',
        select_columns=('trade_id', 'price', 'size', 'trade_id'),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
    )

    compiled_run_1 = _compile_native_query(spec, 'origo')
    compiled_run_2 = _compile_native_query(spec, 'origo')

    assert compiled_run_1 == compiled_run_2
    assert compiled_run_1.sql == compiled_run_2.sql


def test_compile_okx_aligned_sql_is_deterministic() -> None:
    sql_run_1 = build_okx_aligned_1s_sql(
        dataset='okx_spot_trades',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    sql_run_2 = build_okx_aligned_1s_sql(
        dataset='okx_spot_trades',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    assert sql_run_1 == sql_run_2


def test_compile_binance_aligned_sql_is_deterministic() -> None:
    sql_run_1 = build_binance_aligned_1s_sql(
        dataset='spot_trades',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    sql_run_2 = build_binance_aligned_1s_sql(
        dataset='spot_trades',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    assert sql_run_1 == sql_run_2


def test_compile_bybit_native_query_is_deterministic() -> None:
    spec = build_bybit_native_query_spec(
        dataset='bybit_spot_trades',
        select_columns=('trade_id', 'price', 'size', 'trade_id'),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
    )

    compiled_run_1 = _compile_native_query(spec, 'origo')
    compiled_run_2 = _compile_native_query(spec, 'origo')

    assert compiled_run_1 == compiled_run_2
    assert compiled_run_1.sql == compiled_run_2.sql


def test_compile_bybit_aligned_sql_is_deterministic() -> None:
    sql_run_1 = build_bybit_aligned_1s_sql(
        dataset='bybit_spot_trades',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    sql_run_2 = build_bybit_aligned_1s_sql(
        dataset='bybit_spot_trades',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    assert sql_run_1 == sql_run_2


def test_compile_bitcoin_native_query_is_deterministic() -> None:
    spec = build_bitcoin_native_query_spec(
        dataset='bitcoin_block_headers',
        select_columns=('height', 'difficulty', 'height'),
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
    )

    compiled_run_1 = _compile_native_query(spec, 'origo')
    compiled_run_2 = _compile_native_query(spec, 'origo')

    assert compiled_run_1 == compiled_run_2
    assert compiled_run_1.sql == compiled_run_2.sql


def test_compile_bitcoin_derived_aligned_sql_is_deterministic() -> None:
    sql_run_1 = build_bitcoin_derived_aligned_1s_sql(
        dataset='bitcoin_block_fee_totals',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    sql_run_2 = build_bitcoin_derived_aligned_1s_sql(
        dataset='bitcoin_block_fee_totals',
        window=TimeRangeWindow(
            start_iso='2024-01-01T00:00:00Z',
            end_iso='2024-01-01T01:00:00Z',
        ),
        database='origo',
    )
    assert sql_run_1 == sql_run_2
