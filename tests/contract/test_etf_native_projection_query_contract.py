from __future__ import annotations

from origo.query.etf_native import build_etf_native_query_spec
from origo.query.native_core import LatestRowsWindow


def test_etf_native_query_spec_targets_canonical_projection_table() -> None:
    spec = build_etf_native_query_spec(
        dataset='etf_daily_metrics',
        select_columns=('metric_id', 'source_id'),
        window=LatestRowsWindow(rows=10),
    )
    assert spec.table_name == 'canonical_etf_daily_metrics_native_v1'
