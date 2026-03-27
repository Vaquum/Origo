from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    BitcoinCanonicalEvent,
    build_bitcoin_partition_source_proof_or_raise,
    execute_bitcoin_partition_backfill_or_raise,
)

from origo.events import CanonicalBackfillStateStore, CanonicalStreamKey
from origo.events.backfill_state import PartitionExecutionAssessment
from origo.events.errors import ReconciliationError


def _header_event(*, height: int) -> BitcoinCanonicalEvent:
    second = height - 840000
    return BitcoinCanonicalEvent(
        stream_id='bitcoin_block_headers',
        partition_id='000000840000-000000840001',
        source_offset_or_equivalent=str(height),
        source_event_time_utc=datetime(2024, 4, 20, 0, 0, second, tzinfo=UTC),
        payload={
            'height': height,
            'block_hash': f'{height:064x}',
            'timestamp_ms': 1_713_571_200_000 + (height * 1000),
        },
    )


def test_build_bitcoin_partition_source_proof_orders_numeric_header_offsets() -> None:
    proof = build_bitcoin_partition_source_proof_or_raise(
        stream_id='bitcoin_block_headers',
        partition_id='000000840000-000000840001',
        offset_ordering='numeric',
        source_artifact_identity={
            'source_kind': 'bitcoin_core_rpc_height_range',
            'range_start_height': 840000,
            'range_end_height': 840001,
        },
        events=[_header_event(height=840001), _header_event(height=840000)],
        allow_empty_partition=False,
    )

    assert proof.offset_ordering == 'numeric'
    assert proof.source_row_count == 2
    assert proof.first_offset_or_equivalent == '840000'
    assert proof.last_offset_or_equivalent == '840001'


def test_build_bitcoin_partition_source_proof_allows_empty_mempool_snapshot() -> None:
    proof = build_bitcoin_partition_source_proof_or_raise(
        stream_id='bitcoin_mempool_state',
        partition_id='2024-04-20',
        offset_ordering='lexicographic',
        source_artifact_identity={
            'source_kind': 'bitcoin_core_rpc_mempool_snapshot',
            'snapshot_at_utc': '2024-04-20T00:00:02+00:00',
        },
        events=[],
        allow_empty_partition=True,
    )

    assert proof.stream_key == CanonicalStreamKey(
        source_id='bitcoin_core',
        stream_id='bitcoin_mempool_state',
        partition_id='2024-04-20',
    )
    assert proof.source_row_count == 0
    assert proof.allow_empty_partition is True
    assert proof.first_offset_or_equivalent is None
    assert proof.last_offset_or_equivalent is None


def test_execute_bitcoin_partition_backfill_uses_reconcile_proof_only_fast_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_proof = build_bitcoin_partition_source_proof_or_raise(
        stream_id='bitcoin_block_headers',
        partition_id='000000840000-000000840001',
        offset_ordering='numeric',
        source_artifact_identity={
            'source_kind': 'bitcoin_core_rpc_height_range',
            'range_start_height': 840000,
            'range_end_height': 840001,
        },
        events=[_header_event(height=840000)],
        allow_empty_partition=False,
    )
    captured_states: list[str] = []

    def _assert_partition_can_execute_or_raise(
        self: CanonicalBackfillStateStore,
        *,
        stream_key: CanonicalStreamKey,
        execution_mode: str,
    ) -> None:
        assert stream_key == source_proof.stream_key
        assert execution_mode == 'reconcile'

    def _assess_partition_execution(
        self: CanonicalBackfillStateStore,
        *,
        stream_key: CanonicalStreamKey,
    ) -> PartitionExecutionAssessment:
        assert stream_key == source_proof.stream_key
        return PartitionExecutionAssessment(
            stream_key=stream_key,
            latest_proof_state='canonical_written_unproved',
            canonical_row_count=1,
            active_quarantine=False,
        )

    def _record_source_manifest(
        self: CanonicalBackfillStateStore,
        *,
        source_proof: Any,
        run_id: str,
        manifested_at_utc: datetime,
    ) -> object:
        assert run_id == 'run-reconcile'
        assert manifested_at_utc.tzinfo is not None
        return object()

    def _record_partition_state(
        self: CanonicalBackfillStateStore,
        *,
        source_proof: Any,
        state: str,
        reason: str,
        run_id: str,
        recorded_at_utc: datetime,
        proof_details: dict[str, Any] | None = None,
    ) -> object:
        assert run_id == 'run-reconcile'
        assert recorded_at_utc.tzinfo is not None
        captured_states.append(state)
        return object()

    def _prove_partition_or_quarantine(
        self: CanonicalBackfillStateStore,
        *,
        source_proof: Any,
        run_id: str,
        recorded_at_utc: datetime,
    ) -> object:
        assert run_id == 'run-reconcile'
        assert recorded_at_utc.tzinfo is not None
        return SimpleNamespace(
            state='proved_complete',
            proof_digest_sha256='a' * 64,
        )

    def _write_bitcoin_events_to_canonical(**_: Any) -> dict[str, int]:
        raise AssertionError('reconcile fast path must not write canonical rows')

    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'assert_partition_can_execute_or_raise',
        _assert_partition_can_execute_or_raise,
    )
    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'assess_partition_execution',
        _assess_partition_execution,
    )
    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'record_source_manifest',
        _record_source_manifest,
    )
    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'record_partition_state',
        _record_partition_state,
    )
    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'prove_partition_or_quarantine',
        _prove_partition_or_quarantine,
    )
    monkeypatch.setattr(
        'origo_control_plane.utils.bitcoin_canonical_event_ingest.write_bitcoin_events_to_canonical',
        _write_bitcoin_events_to_canonical,
    )

    summary = execute_bitcoin_partition_backfill_or_raise(
        client=cast(Any, object()),
        database='origo',
        source_proof=source_proof,
        events=[_header_event(height=840000)],
        run_id='run-reconcile',
        ingested_at_utc=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
        execution_mode='reconcile',
    )

    assert captured_states == ['source_manifested', 'canonical_written_unproved']
    assert summary.rows_processed == 1
    assert summary.rows_inserted == 0
    assert summary.rows_duplicate == 1
    assert summary.write_path == 'reconcile_proof_only'
    assert summary.partition_proof_state == 'proved_complete'
    assert summary.partition_proof_digest_sha256 == 'a' * 64


def test_execute_bitcoin_partition_backfill_marks_reconcile_required_before_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_proof = build_bitcoin_partition_source_proof_or_raise(
        stream_id='bitcoin_block_headers',
        partition_id='000000840000-000000840001',
        offset_ordering='numeric',
        source_artifact_identity={
            'source_kind': 'bitcoin_core_rpc_height_range',
            'range_start_height': 840000,
            'range_end_height': 840001,
        },
        events=[_header_event(height=840000)],
        allow_empty_partition=False,
    )
    captured: dict[str, Any] = {}

    def _assert_partition_can_execute_or_raise(
        self: CanonicalBackfillStateStore,
        *,
        stream_key: CanonicalStreamKey,
        execution_mode: str,
    ) -> None:
        raise ReconciliationError(
            code='RECONCILE_REQUIRED',
            message='partition contains canonical rows without terminal proof',
            context={'stream_key': stream_key.partition_id, 'execution_mode': execution_mode},
        )

    def _record_partition_state(
        self: CanonicalBackfillStateStore,
        *,
        source_proof: Any,
        state: str,
        reason: str,
        run_id: str,
        recorded_at_utc: datetime,
        proof_details: dict[str, Any] | None = None,
    ) -> object:
        captured['state'] = state
        captured['reason'] = reason
        captured['run_id'] = run_id
        captured['proof_details'] = proof_details
        return object()

    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'assert_partition_can_execute_or_raise',
        _assert_partition_can_execute_or_raise,
    )
    monkeypatch.setattr(
        CanonicalBackfillStateStore,
        'record_partition_state',
        _record_partition_state,
    )

    with pytest.raises(ReconciliationError, match='RECONCILE_REQUIRED'):
        execute_bitcoin_partition_backfill_or_raise(
            client=cast(Any, object()),
            database='origo',
            source_proof=source_proof,
            events=[_header_event(height=840000)],
            run_id='run-backfill',
            ingested_at_utc=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            execution_mode='backfill',
        )

    assert captured == {
        'state': 'reconcile_required',
        'reason': 'backfill_execution_requires_reconcile',
        'run_id': 'run-backfill',
        'proof_details': {
            'trigger_message': 'partition contains canonical rows without terminal proof'
        },
    }
