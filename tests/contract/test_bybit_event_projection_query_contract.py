from __future__ import annotations

from origo.query.bybit_aligned_1s import build_bybit_aligned_1s_sql
from origo.query.bybit_native import build_bybit_native_query_spec
from origo.query.native_core import LatestRowsWindow


def test_bybit_native_query_spec_targets_canonical_projection_table() -> None:
    spec = build_bybit_native_query_spec(
        dataset='bybit_spot_trades',
        select_columns=('trade_id', 'price', 'size'),
        window=LatestRowsWindow(rows=10),
    )
    assert spec.table_name == 'canonical_bybit_spot_trades_native_v1'


def test_bybit_aligned_query_targets_canonical_aligned_table() -> None:
    sql = build_bybit_aligned_1s_sql(
        dataset='bybit_spot_trades',
        window=LatestRowsWindow(rows=5),
        database='origo',
    )
    assert 'FROM origo.canonical_aligned_1s_aggregates' in sql
    assert "source_id = 'bybit'" in sql
    assert "stream_id = 'bybit_spot_trades'" in sql
    assert 'FROM origo.bybit_spot_trades' not in sql
