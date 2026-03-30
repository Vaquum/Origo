from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID

import origo_control_plane.s34_etf_backfill_runner as runner
import pytest
from origo_control_plane.config import resolve_clickhouse_native_settings


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


def test_load_ambiguous_daily_partition_ids_use_authoritative_partition_helper(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_helper(**kwargs: Any) -> tuple[str, ...]:
        captured.update(kwargs)
        return ('2024-01-11',)

    monkeypatch.setattr(
        runner,
        'load_nonterminal_partition_ids_for_stream_or_raise',
        _fake_helper,
    )

    result = runner._load_ambiguous_daily_partition_ids_or_raise(
        client=SimpleNamespace(),
        database='origo',
    )

    assert result == ('2024-01-11',)
    assert captured['database'] == 'origo'
    assert captured['source_id'] == 'etf'
    assert captured['stream_id'] == 'etf_daily_metrics'
    assert captured['terminal_states'] == ('proved_complete', 'empty_proved')
    assert captured['client'] is not None


def test_run_s34_etf_backfill_helper_write_execution_is_disabled() -> None:
    with pytest.raises(
        RuntimeError,
        match='historical helper surface only',
    ):
        runner.run_s34_etf_backfill_or_raise(run_id='s34-etf-test')


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


def test_clickhouse_native_timeout_contract_defaults_and_overrides(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'secret')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.delenv('CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS', raising=False)

    assert resolve_clickhouse_native_settings().send_receive_timeout_seconds == 3600

    monkeypatch.setenv('CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS', '5400')

    assert resolve_clickhouse_native_settings().send_receive_timeout_seconds == 5400


def test_clickhouse_native_timeout_contract_rejects_invalid_override(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'secret')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.setenv('CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS', 'oops')

    with pytest.raises(
        RuntimeError,
        match="CLICKHOUSE_NATIVE_SEND_RECEIVE_TIMEOUT_SECONDS must be an integer",
    ):
        resolve_clickhouse_native_settings()
