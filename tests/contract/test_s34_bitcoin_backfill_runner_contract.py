from __future__ import annotations

from typing import Any, cast
from uuid import UUID

import origo_control_plane.s34_bitcoin_backfill_runner as runner
import pytest
from origo_control_plane.s34_bitcoin_backfill_runner import (
    list_bitcoin_chain_datasets_or_raise,
    plan_next_bitcoin_chain_batch_or_raise,
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


def test_load_ambiguous_height_range_partition_ids_use_authoritative_partition_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_helper(**kwargs: Any) -> tuple[str, ...]:
        captured.update(kwargs)
        return ('000000840288-000000840431',)

    monkeypatch.setattr(
        'origo_control_plane.s34_bitcoin_backfill_runner.load_nonterminal_partition_ids_for_stream_or_raise',
        _fake_helper,
    )

    result = runner._load_ambiguous_height_range_partition_ids_or_raise(
        dataset='bitcoin_block_headers',
        client=cast(Any, object()),
        database='origo',
    )

    assert result == ('000000840288-000000840431',)
    assert captured['database'] == 'origo'
    assert captured['source_id'] == 'bitcoin_core'
    assert captured['stream_id'] == 'bitcoin_block_headers'
    assert captured['terminal_states'] == ('proved_complete', 'empty_proved')
    assert captured['client'] is not None


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


def test_run_bitcoin_chain_sequence_controller_helper_write_execution_is_disabled() -> None:
    with pytest.raises(
        RuntimeError,
        match='historical helper surface only',
    ):
        runner.run_bitcoin_chain_sequence_controller_or_raise(
            plan_end_height=840999,
            batch_size_blocks=144,
            projection_mode='deferred',
            runtime_audit_mode='summary',
        )


def test_run_bitcoin_chain_backfill_helper_write_execution_is_disabled() -> None:
    with pytest.raises(
        RuntimeError,
        match='historical helper surface only',
    ):
        runner.run_bitcoin_chain_backfill_or_raise(
            dataset='bitcoin_block_headers',
            plan_end_height=840999,
            batch_size_blocks=144,
            projection_mode='deferred',
            runtime_audit_mode='summary',
        )


def test_run_bitcoin_mempool_daily_path_helper_write_execution_is_disabled() -> None:
    with pytest.raises(
        RuntimeError,
        match='historical helper surface only',
    ):
        runner.run_bitcoin_mempool_daily_path_or_raise(
            projection_mode='deferred',
            runtime_audit_mode='summary',
        )


def test_create_and_submit_job_run_uses_uuid_dagster_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submitted: dict[str, Any] = {}

    class _FakeDagsterRun:
        def __init__(self, run_id: str) -> None:
            self.run_id = run_id

    class _FakeInstance:
        def get_ref(self) -> str:
            return 'ref'

        def create_run_for_job(self, **kwargs: Any) -> _FakeDagsterRun:
            submitted['create_run_for_job'] = kwargs
            return _FakeDagsterRun(kwargs['run_id'])

        def submit_run(self, run_id: str, request_context: object) -> None:
            submitted['submit_run'] = {
                'run_id': run_id,
                'request_context': request_context,
            }

    handle = runner._DagsterJobHandle(
        workspace_process_context=object(),
        request_context=object(),
        code_location=object(),
        remote_repository=object(),
        remote_job=type(
            '_RemoteJob',
            (),
            {
                'get_remote_origin': staticmethod(lambda: 'remote-origin'),
                'get_python_origin': staticmethod(lambda: 'python-origin'),
            },
        )(),
    )

    monkeypatch.setattr(
        runner,
        'create_execution_plan',
        lambda *args, **kwargs: 'plan',
    )

    dagster_run_id = runner._create_and_submit_job_run_or_raise(
        instance=_FakeInstance(),
        handle=handle,
        job_def=object(),
        control_run_id='s34-bitcoin-test',
        tags={'origo.backfill.control_run_id': 's34-bitcoin-test'},
    )

    assert dagster_run_id == submitted['submit_run']['run_id']
    assert UUID(dagster_run_id)
    assert submitted['create_run_for_job']['tags'] == {
        'origo.backfill.control_run_id': 's34-bitcoin-test'
    }
