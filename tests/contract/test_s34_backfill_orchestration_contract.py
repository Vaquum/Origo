from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest
from origo_control_plane.backfill.s34_contract import get_s34_dataset_contract
from origo_control_plane.backfill.s34_orchestrator import (
    build_daily_partitions,
    evaluate_numeric_offset_gaps_or_raise,
    load_last_completed_daily_partition_from_canonical_or_raise,
    remaining_daily_partitions_or_raise,
)

from origo.events import CanonicalBackfillStateStore, CanonicalStreamKey
from origo.events.backfill_state import PartitionExecutionAssessment
from origo.events.errors import ReconciliationError


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


def test_remaining_daily_partitions_uses_explicit_last_completed_partition() -> None:
    contract = get_s34_dataset_contract('binance_spot_trades')

    initial = remaining_daily_partitions_or_raise(
        contract=contract,
        plan_end_date=date(2017, 8, 19),
        last_completed_partition=None,
    )
    assert initial == ('2017-08-17', '2017-08-18', '2017-08-19')

    resumed = remaining_daily_partitions_or_raise(
        contract=contract,
        plan_end_date=date(2017, 8, 19),
        last_completed_partition='2017-08-18',
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


def test_load_last_completed_daily_partition_from_canonical_delegates_to_state_store(
    monkeypatch: Any,
) -> None:
    captured: dict[str, object] = {}

    def _fake_load(
        self: CanonicalBackfillStateStore,
        *,
        source_id: str,
        stream_id: str,
        earliest_partition_date: date,
    ) -> str | None:
        captured['source_id'] = source_id
        captured['stream_id'] = stream_id
        captured['earliest_partition_date'] = earliest_partition_date
        return '2017-08-18'

    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'load_last_completed_daily_partition_or_raise',
        _fake_load,
    )

    contract = get_s34_dataset_contract('binance_spot_trades')
    value = load_last_completed_daily_partition_from_canonical_or_raise(
        client=cast(Any, object()),
        database='origo',
        contract=contract,
    )

    assert value == '2017-08-18'
    assert captured == {
        'source_id': 'binance',
        'stream_id': 'binance_spot_trades',
        'earliest_partition_date': date(2017, 8, 17),
    }


def test_load_last_completed_daily_partition_from_canonical_surfaces_reconcile_required(
    monkeypatch: Any,
) -> None:
    def _fake_load(
        self: CanonicalBackfillStateStore,
        *,
        source_id: str,
        stream_id: str,
        earliest_partition_date: date,
    ) -> str | None:
        raise ReconciliationError(
            code='RECONCILE_REQUIRED',
            message=(
                f'Dataset resume blocked by canonical rows without terminal proof state '
                f'for {source_id}/{stream_id}/2017-08-18'
            ),
            context={'checked_at_utc': date(2026, 3, 26).isoformat()},
        )

    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'load_last_completed_daily_partition_or_raise',
        _fake_load,
    )

    contract = get_s34_dataset_contract('binance_spot_trades')
    with pytest.raises(ReconciliationError, match='RECONCILE_REQUIRED'):
        load_last_completed_daily_partition_from_canonical_or_raise(
            client=cast(Any, object()),
            database='origo',
            contract=contract,
        )


def test_assert_partition_can_execute_allows_reconcile_for_ambiguous_partition(
    monkeypatch: Any,
) -> None:
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2017-08-17',
    )

    def _fake_assessment(
        self: CanonicalBackfillStateStore,
        *,
        stream_key: CanonicalStreamKey,
    ) -> PartitionExecutionAssessment:
        return PartitionExecutionAssessment(
            stream_key=stream_key,
            latest_proof_state='canonical_written_unproved',
            canonical_row_count=100,
            active_quarantine=False,
        )

    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'assess_partition_execution',
        _fake_assessment,
    )

    state_store = CanonicalBackfillStateStore(
        client=cast(Any, object()),
        database='origo',
    )
    state_store.assert_partition_can_execute_or_raise(
        stream_key=stream_key,
        execution_mode='reconcile',
    )


def test_assert_partition_can_execute_rejects_reconcile_for_clean_partition(
    monkeypatch: Any,
) -> None:
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2017-08-17',
    )

    def _fake_assessment(
        self: CanonicalBackfillStateStore,
        *,
        stream_key: CanonicalStreamKey,
    ) -> PartitionExecutionAssessment:
        return PartitionExecutionAssessment(
            stream_key=stream_key,
            latest_proof_state=None,
            canonical_row_count=0,
            active_quarantine=False,
        )

    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'assess_partition_execution',
        _fake_assessment,
    )

    state_store = CanonicalBackfillStateStore(
        client=cast(Any, object()),
        database='origo',
    )
    with pytest.raises(
        ReconciliationError,
        match='BACKFILL_RECONCILE_NOT_REQUIRED',
    ):
        state_store.assert_partition_can_execute_or_raise(
            stream_key=stream_key,
            execution_mode='reconcile',
        )
