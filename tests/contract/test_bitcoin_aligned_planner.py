from __future__ import annotations

import pytest

from origo.query.aligned_core import (
    _validate_selected_columns,
    build_aligned_query_plan,
)
from origo.query.native_core import LatestRowsWindow, MonthWindow, TimeRangeWindow


def test_bitcoin_derived_aligned_plan_uses_forward_fill_for_month_window() -> None:
    plan = build_aligned_query_plan(
        dataset='bitcoin_block_fee_totals',
        window=MonthWindow(month=1, year=2024),
    )
    assert plan.execution_path == 'bitcoin_derived_aligned_forward_fill'
    assert isinstance(plan.window, TimeRangeWindow)


def test_bitcoin_derived_aligned_plan_uses_observation_for_latest_rows_window() -> None:
    plan = build_aligned_query_plan(
        dataset='bitcoin_block_fee_totals',
        window=LatestRowsWindow(rows=10),
    )
    assert plan.execution_path == 'bitcoin_derived_aligned_observation'


def test_bitcoin_derived_aligned_query_rejects_invalid_selected_columns() -> None:
    with pytest.raises(ValueError, match='Unsupported aligned projection fields'):
        _validate_selected_columns(
            dataset='bitcoin_block_fee_totals',
            selected_columns=('not_a_column',),
        )
