from __future__ import annotations

from origo.query.native_core import LatestRowsWindow
from origo.query.okx_aligned_1s import build_okx_aligned_1s_sql
from origo.query.okx_native import build_okx_native_query_spec


def test_okx_native_query_spec_targets_canonical_projection_table() -> None:
    spec = build_okx_native_query_spec(
        dataset='okx_spot_trades',
        select_columns=('trade_id', 'price', 'size'),
        window=LatestRowsWindow(rows=10),
    )
    assert spec.table_name == 'canonical_okx_spot_trades_native_v1'


def test_okx_aligned_query_targets_canonical_aligned_table() -> None:
    sql = build_okx_aligned_1s_sql(
        dataset='okx_spot_trades',
        window=LatestRowsWindow(rows=5),
        database='origo',
    )
    assert 'FROM origo.canonical_aligned_1s_aggregates' in sql
    assert "source_id = 'okx'" in sql
    assert "stream_id = 'okx_spot_trades'" in sql
    assert 'FROM origo.okx_spot_trades' not in sql
