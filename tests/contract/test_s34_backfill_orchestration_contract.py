from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from origo_control_plane.backfill.s34_contract import get_s34_dataset_contract
from origo_control_plane.backfill.s34_orchestrator import (
    BackfillRunStateStore,
    build_daily_partitions,
    evaluate_numeric_offset_gaps_or_raise,
    load_last_completed_daily_partition_from_canonical_or_raise,
    remaining_daily_partitions_or_raise,
)


def test_build_daily_partitions_is_inclusive() -> None:
    partitions = build_daily_partitions(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 3),
    )
    assert partitions == ('2024-01-01', '2024-01-02', '2024-01-03')


def test_build_daily_partitions_fails_for_reversed_window() -> None:
    with pytest.raises(RuntimeError, match='Daily partition window is invalid'):
        build_daily_partitions(
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 1),
        )


def test_backfill_run_state_store_rejects_regressive_partition_updates(
    tmp_path: Path,
) -> None:
    run_state = BackfillRunStateStore(path=tmp_path / 'run-state.json')
    run_state.mark_completed(
        dataset='binance_spot_trades',
        partition_id='2017-08-19',
        run_id='run-1',
        completed_at_utc=datetime.now(UTC),
    )
    with pytest.raises(RuntimeError, match='Backfill run-state regression'):
        run_state.mark_completed(
            dataset='binance_spot_trades',
            partition_id='2017-08-18',
            run_id='run-2',
            completed_at_utc=datetime.now(UTC),
        )


def test_remaining_daily_partitions_uses_resume_state(tmp_path: Path) -> None:
    run_state = BackfillRunStateStore(path=tmp_path / 'run-state.json')
    contract = get_s34_dataset_contract('binance_spot_trades')

    initial = remaining_daily_partitions_or_raise(
        contract=contract,
        plan_end_date=date(2017, 8, 19),
        last_completed_partition=run_state.last_completed_partition(
            dataset=contract.dataset
        ),
    )
    assert initial == ('2017-08-17', '2017-08-18', '2017-08-19')

    run_state.mark_completed(
        dataset='binance_spot_trades',
        partition_id='2017-08-18',
        run_id='run-1',
        completed_at_utc=datetime.now(UTC),
    )
    resumed = remaining_daily_partitions_or_raise(
        contract=contract,
        plan_end_date=date(2017, 8, 19),
        last_completed_partition=run_state.last_completed_partition(
            dataset=contract.dataset
        ),
    )
    assert resumed == ('2017-08-19',)


def test_evaluate_numeric_offset_gaps_detects_gap_and_duplicates() -> None:
    summary = evaluate_numeric_offset_gaps_or_raise(
        offsets=['100', '101', '103', '103', '105'],
        missing_preview_limit=10,
    )
    assert summary.expected_event_count == 6
    assert summary.observed_event_count == 4
    assert summary.gap_count == 2
    assert summary.duplicate_offset_count == 1
    assert summary.missing_offset_preview == (102, 104)


def test_evaluate_numeric_offset_gaps_rejects_non_numeric_offset() -> None:
    with pytest.raises(RuntimeError, match='offset must be integer text'):
        evaluate_numeric_offset_gaps_or_raise(offsets=['100', 'abc'])


def test_load_last_completed_daily_partition_from_canonical_returns_none() -> None:
    class _FakeClient:
        def execute(self, *_: object, **__: object) -> list[tuple[None]]:
            return [(None,)]

    contract = get_s34_dataset_contract('binance_spot_trades')
    assert (
        load_last_completed_daily_partition_from_canonical_or_raise(
            client=cast(Any, _FakeClient()),
            database='origo',
            contract=contract,
        )
        is None
    )


def test_load_last_completed_daily_partition_from_canonical_rejects_empty() -> None:
    class _FakeClient:
        def execute(self, *_: object, **__: object) -> list[tuple[str]]:
            return [('   ',)]

    contract = get_s34_dataset_contract('binance_spot_trades')
    with pytest.raises(
        RuntimeError,
        match='must not be empty',
    ):
        load_last_completed_daily_partition_from_canonical_or_raise(
            client=cast(Any, _FakeClient()),
            database='origo',
            contract=contract,
        )


def test_load_last_completed_daily_partition_from_canonical_returns_value() -> None:
    class _FakeClient:
        def execute(self, *_: object, **__: object) -> list[tuple[str]]:
            return [('2017-08-18',)]

    contract = get_s34_dataset_contract('binance_spot_trades')
    assert (
        load_last_completed_daily_partition_from_canonical_or_raise(
            client=cast(Any, _FakeClient()),
            database='origo',
            contract=contract,
        )
        == '2017-08-18'
    )


def test_load_last_completed_daily_partition_from_canonical_rejects_before_earliest() -> None:
    class _FakeClient:
        def execute(self, *_: object, **__: object) -> list[tuple[str]]:
            return [('2017-08-16',)]

    contract = get_s34_dataset_contract('binance_spot_trades')
    with pytest.raises(
        RuntimeError,
        match='before dataset earliest partition date',
    ):
        load_last_completed_daily_partition_from_canonical_or_raise(
            client=cast(Any, _FakeClient()),
            database='origo',
            contract=contract,
        )


def test_load_last_completed_daily_partition_from_canonical_uses_max_or_null() -> None:
    captured_query: str | None = None

    class _FakeClient:
        def execute(self, query: str, *_: object, **__: object) -> list[tuple[None]]:
            nonlocal captured_query
            captured_query = query
            return [(None,)]

    contract = get_s34_dataset_contract('okx_spot_trades')
    assert (
        load_last_completed_daily_partition_from_canonical_or_raise(
            client=cast(Any, _FakeClient()),
            database='origo',
            contract=contract,
        )
        is None
    )
    assert captured_query is not None
    assert 'maxOrNull(partition_id)' in captured_query
