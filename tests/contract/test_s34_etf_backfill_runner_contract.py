from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

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
        '_create_and_submit_etf_run_or_raise',
        lambda **kwargs: submitted.setdefault('run_id', kwargs['run_id']) or kwargs['run_id'],
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

    assert submitted['run_id'] == 's34-etf-test'
    assert result['dataset'] == 'etf_daily_metrics'
    assert result['dagster_run_id'] == 's34-etf-test'
    assert result['proof_summary']['proof_boundary_partition_id'] == '2026-03-25'


def test_build_run_tags_are_deterministic() -> None:
    assert runner._build_run_tags(run_id='s34-etf-test') == {
        'origo.backfill.dataset': 'etf_daily_metrics',
        'origo.backfill.control_run_id': 's34-etf-test',
    }
