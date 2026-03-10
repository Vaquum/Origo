from __future__ import annotations

from origo.query.etf_aligned_1s import build_etf_aligned_1s_sql
from origo.query.native_core import LatestRowsWindow


def test_etf_aligned_query_targets_canonical_aligned_table() -> None:
    sql = build_etf_aligned_1s_sql(
        dataset='etf_daily_metrics',
        window=LatestRowsWindow(rows=5),
        database='origo',
    )
    assert 'FROM origo.canonical_aligned_1s_aggregates' in sql
    assert "source_id = 'etf'" in sql
    assert "stream_id = 'etf_daily_metrics'" in sql
