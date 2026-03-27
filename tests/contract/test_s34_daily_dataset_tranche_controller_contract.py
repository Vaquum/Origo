from __future__ import annotations

from datetime import date
from typing import Any

import origo_control_plane.s34_daily_dataset_tranche_controller as controller
import pytest


class _FakeClickHouseClient:
    def disconnect(self) -> None:
        return None


def test_plan_next_daily_batch_prefers_reconcile_when_ambiguous_partitions_exist(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        controller,
        '_load_ambiguous_daily_partition_ids_or_raise',
        lambda **_: ('2021-09-01', '2021-09-02', '2021-09-03'),
    )
    monkeypatch.setattr(
        controller,
        'load_last_completed_daily_partition_from_canonical_or_raise',
        lambda **_: (_ for _ in ()).throw(
            AssertionError('resume lookup must not run when ambiguous partitions exist')
        ),
    )

    batch = controller.plan_next_daily_batch_or_raise(
        dataset='okx_spot_trades',
        plan_end_date=date(2021, 9, 10),
        batch_size_days=2,
        client=_FakeClickHouseClient(),
        database='origo',
    )

    assert batch is not None
    assert batch.execution_mode == 'reconcile'
    assert batch.partition_ids == ('2021-09-01', '2021-09-02')
    assert batch.end_date is None
    assert batch.run_id == 's34-tranche-okx_spot_trades-reconcile-2021-09-01-2021-09-02'


def test_plan_next_daily_batch_falls_through_to_backfill_from_terminal_frontier(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        controller,
        '_load_ambiguous_daily_partition_ids_or_raise',
        lambda **_: (),
    )
    monkeypatch.setattr(
        controller,
        'load_last_completed_daily_partition_from_canonical_or_raise',
        lambda **_: '2021-09-02',
    )

    batch = controller.plan_next_daily_batch_or_raise(
        dataset='okx_spot_trades',
        plan_end_date=date(2021, 9, 5),
        batch_size_days=2,
        client=_FakeClickHouseClient(),
        database='origo',
    )

    assert batch is not None
    assert batch.execution_mode == 'backfill'
    assert batch.partition_ids == ('2021-09-03', '2021-09-04')
    assert batch.end_date == date(2021, 9, 4)
    assert batch.run_id == 's34-tranche-okx_spot_trades-backfill-2021-09-03-2021-09-04'


def test_plan_next_daily_batch_rejects_non_exchange_daily_dataset() -> None:
    with pytest.raises(
        RuntimeError,
        match='supports exchange datasets only',
    ):
        controller.plan_next_daily_batch_or_raise(
            dataset='etf_daily_metrics',
            plan_end_date=date(2024, 1, 12),
            batch_size_days=1,
            client=_FakeClickHouseClient(),
            database='origo',
        )


def test_daily_dataset_tranche_controller_chains_batches_until_no_remaining_work(
    monkeypatch: Any,
) -> None:
    planned_batches = iter(
        (
            controller.DailyBatchPlan(
                dataset='okx_spot_trades',
                execution_mode='reconcile',
                partition_ids=('2021-09-01', '2021-09-02'),
                batch_start_partition_id='2021-09-01',
                batch_end_partition_id='2021-09-02',
                end_date=None,
                run_id='run-1',
            ),
            controller.DailyBatchPlan(
                dataset='okx_spot_trades',
                execution_mode='backfill',
                partition_ids=('2021-09-03', '2021-09-04'),
                batch_start_partition_id='2021-09-03',
                batch_end_partition_id='2021-09-04',
                end_date=date(2021, 9, 4),
                run_id='run-2',
            ),
            None,
        )
    )
    backfill_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        controller,
        'plan_next_daily_batch_or_raise',
        lambda **_: next(planned_batches),
    )
    monkeypatch.setattr(
        controller,
        'run_exchange_backfill',
        lambda **kwargs: backfill_calls.append(kwargs)
        or {'run_id': kwargs['run_id'], 'dataset': kwargs['dataset']},
    )

    result = controller.run_daily_dataset_tranche_controller_or_raise(
        dataset='okx_spot_trades',
        plan_end_date=date(2021, 9, 10),
        batch_size_days=2,
        concurrency=20,
        projection_mode='deferred',
        runtime_audit_mode='summary',
    )

    assert result['completed_batch_count'] == 2
    assert result['controller_stopped_reason'] == 'no_remaining_work'
    assert [call['run_id'] for call in backfill_calls] == ['run-1', 'run-2']
    assert backfill_calls[0]['execution_mode'] == 'reconcile'
    assert backfill_calls[0]['partition_ids'] == ['2021-09-01', '2021-09-02']
    assert backfill_calls[1]['execution_mode'] == 'backfill'
    assert backfill_calls[1]['end_date'] == date(2021, 9, 4)
