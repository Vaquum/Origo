from __future__ import annotations

from datetime import date

import origo_control_plane.s34_exchange_sequence_controller as controller
import pytest


def test_exchange_sequence_controller_helper_write_execution_is_disabled() -> None:
    with pytest.raises(
        RuntimeError,
        match='historical helper surface only',
    ):
        controller.run_exchange_sequence_controller_or_raise(
            plan_end_date=date(2026, 3, 26),
            batch_size_days=100,
            concurrency=20,
            projection_mode='deferred',
            runtime_audit_mode='summary',
        )


def test_exchange_sequence_controller_rejects_non_exchange_dataset() -> None:
    with pytest.raises(
        RuntimeError,
        match='supports only exchange_parallel datasets',
    ):
        controller._resolve_exchange_sequence_datasets_or_raise(
            ('okx_spot_trades', 'etf_daily_metrics')
        )


def test_exchange_sequence_controller_rejects_out_of_contract_order() -> None:
    with pytest.raises(
        RuntimeError,
        match='requires datasets in S34 contract order',
    ):
        controller._resolve_exchange_sequence_datasets_or_raise(
            ('bybit_spot_trades', 'okx_spot_trades')
        )
