from __future__ import annotations

from datetime import date
from typing import Any

import origo_control_plane.s34_exchange_sequence_controller as controller
import pytest


def test_exchange_sequence_controller_uses_contract_order_by_default(
    monkeypatch: Any,
) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        controller,
        'run_daily_dataset_tranche_controller_or_raise',
        lambda **kwargs: calls.append(kwargs)
        or {
            'dataset': kwargs['dataset'],
            'controller_stopped_reason': 'no_remaining_work',
            'completed_batch_count': 1,
        },
    )

    result = controller.run_exchange_sequence_controller_or_raise(
        plan_end_date=date(2026, 3, 26),
        batch_size_days=100,
        concurrency=20,
        projection_mode='deferred',
        runtime_audit_mode='summary',
    )

    assert [call['dataset'] for call in calls] == ['okx_spot_trades', 'bybit_spot_trades']
    assert result['datasets'] == ('okx_spot_trades', 'bybit_spot_trades')
    assert result['completed_dataset_count'] == 2


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


def test_exchange_sequence_controller_stops_loudly_on_incomplete_dataset(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        controller,
        'run_daily_dataset_tranche_controller_or_raise',
        lambda **kwargs: {
            'dataset': kwargs['dataset'],
            'controller_stopped_reason': 'max_batches_reached',
            'completed_batch_count': 1,
        },
    )

    with pytest.raises(
        RuntimeError,
        match='requires full dataset completion before advancing',
    ):
        controller.run_exchange_sequence_controller_or_raise(
            plan_end_date=date(2026, 3, 26),
            batch_size_days=100,
            concurrency=20,
            projection_mode='deferred',
            runtime_audit_mode='summary',
        )
