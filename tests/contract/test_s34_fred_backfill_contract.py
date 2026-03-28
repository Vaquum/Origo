from __future__ import annotations

from typing import Any

import origo_control_plane.jobs.fred_daily_ingest as fred_job
import origo_control_plane.s34_fred_backfill_runner as fred_runner
import pytest
from origo_control_plane.backfill.runtime_contract import BackfillRuntimeContract


class _FakeClickHouseClient:
    pass


def test_fred_job_reconcile_without_explicit_partitions_uses_authoritative_ambiguity(
    monkeypatch: Any,
) -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )
    monkeypatch.setattr(
        fred_job,
        '_load_ambiguous_partition_ids_or_raise',
        lambda **_: ('2026-02-01', '2026-03-01'),
    )

    result = fred_job._resolve_partition_ids_to_process_or_raise(
        client=_FakeClickHouseClient(),
        database='origo',
        runtime_contract=runtime_contract,
        source_partition_rows={
            '2026-02-01': [object()],
            '2026-03-01': [object()],
        },
        explicit_partition_ids=None,
    )

    assert result == ('2026-02-01', '2026-03-01')


def test_fred_job_explicit_partition_ids_must_exist_in_source_history() -> None:
    runtime_contract = BackfillRuntimeContract(
        projection_mode='deferred',
        execution_mode='reconcile',
        runtime_audit_mode='summary',
    )

    with pytest.raises(
        RuntimeError,
        match='Requested FRED partition ids were not present in source history',
    ):
        fred_job._resolve_partition_ids_to_process_or_raise(
            client=_FakeClickHouseClient(),
            database='origo',
            runtime_contract=runtime_contract,
            source_partition_rows={'2026-02-01': [object()]},
            explicit_partition_ids=('2026-02-01', '2026-03-01'),
        )


def test_s34_fred_backfill_runner_prefers_reconcile_when_ambiguity_exists(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        fred_runner,
        '_load_ambiguous_partition_ids_or_raise',
        lambda **_: ('1947-01-01', '1947-02-01'),
    )

    plan = fred_runner._plan_next_fred_run_or_raise(
        client=_FakeClickHouseClient(),
        database='origo',
    )

    assert plan.execution_mode == 'reconcile'
    assert plan.partition_ids == ()
    assert plan.ambiguous_partition_count == 2


def test_s34_fred_backfill_runner_builds_required_run_tags() -> None:
    tags = fred_runner._build_run_tags(
        control_run_id='s34-fred-test',
        execution_mode='reconcile',
        partition_ids=('2026-02-01', '2026-03-01'),
    )

    assert tags == {
        'origo.backfill.dataset': 'fred_series_metrics',
        'origo.backfill.control_run_id': 's34-fred-test',
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.execution_mode': 'reconcile',
        'origo.backfill.runtime_audit_mode': 'summary',
        'origo.backfill.partition_ids': '2026-02-01,2026-03-01',
    }
