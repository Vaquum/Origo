from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from origo_control_plane.s34_bitcoin_backfill_runner import (
    list_bitcoin_chain_datasets_or_raise,
    plan_next_bitcoin_chain_batch_or_raise,
    run_bitcoin_chain_sequence_controller_or_raise,
    run_bitcoin_mempool_daily_path_or_raise,
)


def test_list_bitcoin_chain_datasets_excludes_mempool_and_preserves_s34_order() -> None:
    assert list_bitcoin_chain_datasets_or_raise() == (
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    )


def test_plan_next_bitcoin_chain_batch_prefers_ambiguous_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner._load_ambiguous_height_range_partition_ids_or_raise',
        lambda **_: ('000000840288-000000840431',),
    )
    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner._load_last_contiguous_terminal_height_or_raise',
        lambda **_: (_ for _ in ()).throw(
            AssertionError('terminal frontier must not be loaded when ambiguity exists')
        ),
    )

    batch = plan_next_bitcoin_chain_batch_or_raise(
        dataset='bitcoin_block_headers',
        plan_end_height=841000,
        batch_size_blocks=144,
        client=cast(Any, object()),
        database='origo',
    )

    assert batch is not None
    assert batch.execution_mode == 'reconcile'
    assert batch.start_height == 840288
    assert batch.end_height == 840431
    assert batch.partition_id == '000000840288-000000840431'


def test_plan_next_bitcoin_chain_batch_uses_contiguous_terminal_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner._load_ambiguous_height_range_partition_ids_or_raise',
        lambda **_: (),
    )
    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner._load_last_contiguous_terminal_height_or_raise',
        lambda **_: 840287,
    )

    batch = plan_next_bitcoin_chain_batch_or_raise(
        dataset='bitcoin_block_headers',
        plan_end_height=840500,
        batch_size_blocks=144,
        client=cast(Any, object()),
        database='origo',
    )

    assert batch is not None
    assert batch.execution_mode == 'backfill'
    assert batch.start_height == 840288
    assert batch.end_height == 840431
    assert batch.partition_id == '000000840288-000000840431'


def test_plan_next_bitcoin_chain_batch_returns_none_when_range_is_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner._load_ambiguous_height_range_partition_ids_or_raise',
        lambda **_: (),
    )
    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner._load_last_contiguous_terminal_height_or_raise',
        lambda **_: 840999,
    )

    batch = plan_next_bitcoin_chain_batch_or_raise(
        dataset='bitcoin_block_headers',
        plan_end_height=840999,
        batch_size_blocks=144,
        client=cast(Any, object()),
        database='origo',
    )

    assert batch is None


def test_plan_next_bitcoin_chain_batch_requires_client_and_database_together() -> None:
    with pytest.raises(
        RuntimeError,
        match='client and database to both be provided or both be None',
    ):
        plan_next_bitcoin_chain_batch_or_raise(
            dataset='bitcoin_block_headers',
            plan_end_height=840999,
            batch_size_blocks=144,
            client=cast(Any, object()),
            database=None,
        )


def test_run_bitcoin_chain_sequence_controller_uses_chain_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def _fake_run_bitcoin_chain_backfill_or_raise(**kwargs: Any) -> dict[str, Any]:
        dataset = cast(str, kwargs['dataset'])
        observed.append(dataset)
        return {
            'dataset': dataset,
            'controller_stopped_reason': 'no_remaining_work',
        }

    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner.run_bitcoin_chain_backfill_or_raise',
        _fake_run_bitcoin_chain_backfill_or_raise,
    )

    result = run_bitcoin_chain_sequence_controller_or_raise(
        plan_end_height=840999,
        batch_size_blocks=144,
        projection_mode='deferred',
        runtime_audit_mode='summary',
    )

    assert observed == [
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    ]
    assert result['controller_stopped_reason'] == 'no_remaining_work'


def test_run_bitcoin_chain_sequence_controller_rejects_partial_batch_limit() -> None:
    with pytest.raises(
        RuntimeError,
        match='max_batches_per_dataset is unsupported',
    ):
        run_bitcoin_chain_sequence_controller_or_raise(
            plan_end_height=840999,
            batch_size_blocks=144,
            projection_mode='deferred',
            runtime_audit_mode='summary',
            max_batches_per_dataset=1,
        )


def test_run_bitcoin_mempool_daily_path_rejects_historical_partition() -> None:
    historical_partition_id = (
        datetime.now(UTC).date() - timedelta(days=1)
    ).isoformat()

    with pytest.raises(
        RuntimeError,
        match='Historical mempool replay is unsupported from first-party Bitcoin Core RPC',
    ):
        run_bitcoin_mempool_daily_path_or_raise(
            projection_mode='deferred',
            runtime_audit_mode='summary',
            requested_partition_id=historical_partition_id,
        )
