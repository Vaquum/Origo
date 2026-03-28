from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import origo_control_plane.s34_etf_backfill_runner as runner
import pytest


def test_load_etf_backfill_summary_requires_clean_terminal_proof_state(
    monkeypatch: Any,
) -> None:
    class _FakeClient:
        def disconnect(self) -> None:
            return None

    monkeypatch.setattr(
        runner,
        '_build_clickhouse_client_or_raise',
        lambda: (_FakeClient(), 'origo'),
    )
    monkeypatch.setattr(
        runner,
        'load_last_completed_daily_partition_from_canonical_or_raise',
        lambda **_: '2026-03-25',
    )
    monkeypatch.setattr(
        runner,
        '_load_ambiguous_daily_partition_ids_or_raise',
        lambda **_: ('2026-03-24',),
    )

    with pytest.raises(
        RuntimeError,
        match='ambiguous partitions remain',
    ):
        runner._load_etf_backfill_summary_or_raise()


def test_run_s34_etf_backfill_submits_job_and_returns_summary(
    monkeypatch: Any,
) -> None:
    submitted: dict[str, Any] = {}

    class _FakeInstance:
        @staticmethod
        def get() -> _FakeInstance:
            return _FakeInstance()

    class _FakeWorkspace:
        def __exit__(self, *_: Any) -> None:
            return None

    handle = runner._DagsterJobHandle(
        workspace_process_context=_FakeWorkspace(),
        request_context=object(),
        code_location=object(),
        remote_repository=object(),
        remote_job=object(),
    )

    monkeypatch.setattr(runner, 'DagsterInstance', _FakeInstance)
    monkeypatch.setattr(
        runner,
        '_load_dagster_job_handle_or_raise',
        lambda **_: handle,
    )
    monkeypatch.setattr(
        runner,
        '_build_clickhouse_client_or_raise',
        lambda: (SimpleNamespace(disconnect=lambda: None), 'origo'),
    )
    monkeypatch.setattr(
        runner,
        '_plan_next_etf_run_or_raise',
        lambda **_: runner._PlannedEtfRun(
            execution_mode='backfill',
            partition_ids=(),
            proof_partition_id=None,
        ),
    )
    monkeypatch.setattr(
        runner,
        '_create_and_submit_etf_run_or_raise',
        lambda **kwargs: (
            submitted.__setitem__('control_run_id', kwargs['control_run_id']),
            submitted.__setitem__('execution_mode', kwargs['execution_mode']),
            '11111111-1111-1111-1111-111111111111',
        )[2],
    )
    monkeypatch.setattr(
        runner,
        '_wait_for_run_success_or_raise',
        lambda **kwargs: runner._CompletedRun(
            run_id=kwargs['run_id'],
            started_at_utc=datetime(2026, 3, 26, 10, 0, tzinfo=UTC),
            finished_at_utc=datetime(2026, 3, 26, 10, 1, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr(
        runner,
        '_load_etf_backfill_summary_or_raise',
        lambda: {
            'dataset': 'etf_daily_metrics',
            'proof_boundary_partition_id': '2026-03-25',
            'terminal_partition_count': 100,
            'ambiguous_partition_count': 0,
        },
    )

    result = runner.run_s34_etf_backfill_or_raise(run_id='s34-etf-test')

    assert submitted['control_run_id'] == 's34-etf-test'
    assert submitted['execution_mode'] == 'backfill'
    assert result['dataset'] == 'etf_daily_metrics'
    assert result['dagster_run_id'] == '11111111-1111-1111-1111-111111111111'
    assert result['proof_summary']['proof_boundary_partition_id'] == '2026-03-25'
    assert result['completed_runs'] == [
        {
            'execution_mode': 'backfill',
            'partition_ids': [],
            'dagster_run_id': '11111111-1111-1111-1111-111111111111',
            'started_at_utc': '2026-03-26T10:00:00+00:00',
            'finished_at_utc': '2026-03-26T10:01:00+00:00',
        }
    ]


def test_build_run_tags_are_deterministic() -> None:
    assert runner._build_run_tags(
        control_run_id='s34-etf-test',
        execution_mode='backfill',
    ) == {
        'origo.backfill.dataset': 'etf_daily_metrics',
        'origo.backfill.control_run_id': 's34-etf-test',
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.execution_mode': 'backfill',
        'origo.backfill.runtime_audit_mode': 'summary',
    }


def test_create_and_submit_etf_run_uses_uuid_dagster_run_id(
    monkeypatch: Any,
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

    monkeypatch.setattr(
        runner,
        'create_execution_plan',
        lambda *args, **kwargs: 'plan',
    )

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

    dagster_run_id = runner._create_and_submit_etf_run_or_raise(
        instance=_FakeInstance(),
        handle=handle,
        control_run_id='s34-etf-test',
        execution_mode='backfill',
    )

    assert dagster_run_id == submitted['submit_run']['run_id']
    assert UUID(dagster_run_id)
    assert submitted['create_run_for_job']['tags'] == {
        'origo.backfill.dataset': 'etf_daily_metrics',
        'origo.backfill.control_run_id': 's34-etf-test',
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.execution_mode': 'backfill',
        'origo.backfill.runtime_audit_mode': 'summary',
    }


def test_build_run_tags_include_partition_filter_when_present() -> None:
    assert runner._build_run_tags(
        control_run_id='s34-etf-test',
        execution_mode='reconcile',
        partition_ids=('2024-01-11',),
    ) == {
        'origo.backfill.dataset': 'etf_daily_metrics',
        'origo.backfill.control_run_id': 's34-etf-test',
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.execution_mode': 'reconcile',
        'origo.backfill.runtime_audit_mode': 'summary',
        'origo.backfill.partition_ids': '2024-01-11',
    }


def test_run_s34_etf_backfill_reconciles_ambiguous_partition_before_backfill(
    monkeypatch: Any,
) -> None:
    submitted: list[dict[str, Any]] = []
    plans = iter(
        [
            runner._PlannedEtfRun(
                execution_mode='reconcile',
                partition_ids=('2024-01-11',),
                proof_partition_id='2024-01-11',
            ),
            runner._PlannedEtfRun(
                execution_mode='backfill',
                partition_ids=(),
                proof_partition_id=None,
            ),
        ]
    )

    class _FakeInstance:
        @staticmethod
        def get() -> _FakeInstance:
            return _FakeInstance()

    class _FakeWorkspace:
        def __exit__(self, *_: Any) -> None:
            return None

    handle = runner._DagsterJobHandle(
        workspace_process_context=_FakeWorkspace(),
        request_context=object(),
        code_location=object(),
        remote_repository=object(),
        remote_job=object(),
    )

    class _FakeClient:
        def disconnect(self) -> None:
            return None

    monkeypatch.setattr(runner, 'DagsterInstance', _FakeInstance)
    monkeypatch.setattr(
        runner,
        '_load_dagster_job_handle_or_raise',
        lambda **_: handle,
    )
    monkeypatch.setattr(
        runner,
        '_build_clickhouse_client_or_raise',
        lambda: (_FakeClient(), 'origo'),
    )
    monkeypatch.setattr(
        runner,
        '_plan_next_etf_run_or_raise',
        lambda **_: next(plans),
    )
    monkeypatch.setattr(
        runner,
        '_create_and_submit_etf_run_or_raise',
        lambda **kwargs: (
            submitted.append(
                {
                    'execution_mode': kwargs['execution_mode'],
                    'partition_ids': kwargs['partition_ids'],
                }
            ),
            str(UUID(int=len(submitted) + 1)),
        )[1],
    )
    monkeypatch.setattr(
        runner,
        '_wait_for_run_success_or_raise',
        lambda **kwargs: runner._CompletedRun(
            run_id=kwargs['run_id'],
            started_at_utc=datetime(2026, 3, 26, 10, 0, tzinfo=UTC),
            finished_at_utc=datetime(2026, 3, 26, 10, 1, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr(
        runner,
        '_load_partition_proof_summary_or_raise',
        lambda **_: {
            'partition_id': '2024-01-11',
            'proof_state': 'proved_complete',
        },
    )
    monkeypatch.setattr(
        runner,
        '_load_etf_backfill_summary_or_raise',
        lambda: {
            'dataset': 'etf_daily_metrics',
            'proof_boundary_partition_id': '2026-03-25',
            'terminal_partition_count': 100,
            'ambiguous_partition_count': 0,
        },
    )

    result = runner.run_s34_etf_backfill_or_raise(run_id='s34-etf-test')

    assert submitted == [
        {
            'execution_mode': 'reconcile',
            'partition_ids': ('2024-01-11',),
        },
        {
            'execution_mode': 'backfill',
            'partition_ids': (),
        },
    ]
    assert result['completed_runs'][0]['execution_mode'] == 'reconcile'
    assert result['completed_runs'][0]['proof_summary'] == {
        'partition_id': '2024-01-11',
        'proof_state': 'proved_complete',
    }
    assert result['completed_runs'][1]['execution_mode'] == 'backfill'
