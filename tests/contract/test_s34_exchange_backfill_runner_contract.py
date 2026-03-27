from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, cast

import origo_control_plane.s34_exchange_backfill_runner as backfill_runner
import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]


class _FakeClickHouseClient:
    def __init__(self, **_: Any) -> None:
        pass

    def disconnect(self) -> None:
        return None


def test_s34_exchange_backfill_runner_dry_run_uses_contract_planner(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
        str(tmp_path / 'manifest.jsonl'),
    )
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'runtime-audit.jsonl'),
    )
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.setattr(backfill_runner, 'ClickHouseClient', _FakeClickHouseClient)
    monkeypatch.setattr(
        backfill_runner,
        'load_missing_daily_partitions_from_canonical_or_raise',
        lambda **_: ('2017-08-17', '2017-08-18', '2017-08-19'),
    )
    monkeypatch.setattr(
        backfill_runner.CanonicalBackfillStateStore,
        'assert_partition_can_execute_or_raise',
        lambda *args, **kwargs: None,
    )

    result = backfill_runner.run_exchange_backfill(
        dataset='binance_spot_trades',
        end_date=date(2017, 8, 19),
        max_partitions=None,
        run_id='s34-test-dry-run',
        dry_run=True,
        projection_mode='deferred',
        runtime_audit_mode='summary',
        concurrency=10,
    )

    planned = cast(list[str], result['planned_partitions'])
    assert result['dry_run'] is True
    assert planned == ['2017-08-17', '2017-08-18', '2017-08-19']
    assert result['resume_state_source'] == 'canonical_backfill_partition_proofs'
    assert result['last_completed_partition'] is None


def test_s34_exchange_backfill_runner_reconcile_dry_run_requires_explicit_partitions(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
        str(tmp_path / 'manifest.jsonl'),
    )
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'runtime-audit.jsonl'),
    )
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.setattr(backfill_runner, 'ClickHouseClient', _FakeClickHouseClient)
    monkeypatch.setattr(
        backfill_runner,
        'load_missing_daily_partitions_from_canonical_or_raise',
        lambda **_: (_ for _ in ()).throw(AssertionError('missing-partition planner must not run')),
    )
    monkeypatch.setattr(
        backfill_runner.CanonicalBackfillStateStore,
        'assert_partition_can_execute_or_raise',
        lambda *args, **kwargs: None,
    )

    result = backfill_runner.run_exchange_backfill(
        dataset='binance_spot_trades',
        end_date=None,
        max_partitions=None,
        run_id='s34-test-reconcile-dry-run',
        dry_run=True,
        projection_mode='deferred',
        runtime_audit_mode='summary',
        concurrency=10,
        execution_mode='reconcile',
        partition_ids=['2017-08-19', '2017-08-17', '2017-08-17'],
    )

    planned = cast(list[str], result['planned_partitions'])
    assert result['dry_run'] is True
    assert result['execution_mode'] == 'reconcile'
    assert planned == ['2017-08-17', '2017-08-19']
    assert result['partition_ids'] == ['2017-08-17', '2017-08-19']
    assert result['last_completed_partition'] is None
    assert result['end_date'] is None


def test_s34_exchange_backfill_runner_dry_run_plans_missing_gap_behind_frontier(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
        str(tmp_path / 'manifest.jsonl'),
    )
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'runtime-audit.jsonl'),
    )
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.setattr(backfill_runner, 'ClickHouseClient', _FakeClickHouseClient)
    monkeypatch.setattr(
        backfill_runner,
        'load_missing_daily_partitions_from_canonical_or_raise',
        lambda **_: ('2017-08-21', '2017-08-22'),
    )
    monkeypatch.setattr(
        backfill_runner.CanonicalBackfillStateStore,
        'assert_partition_can_execute_or_raise',
        lambda *args, **kwargs: None,
    )

    result = backfill_runner.run_exchange_backfill(
        dataset='binance_spot_trades',
        end_date=date(2026, 3, 12),
        max_partitions=None,
        run_id='s34-test-dry-run-gap',
        dry_run=True,
        projection_mode='deferred',
        runtime_audit_mode='summary',
        concurrency=10,
    )

    assert result['planned_partitions'] == ['2017-08-21', '2017-08-22']
    assert result['last_completed_partition'] == '2017-08-20'


def test_s34_exchange_backfill_runner_reconcile_requires_partition_ids(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
        str(tmp_path / 'manifest.jsonl'),
    )
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'runtime-audit.jsonl'),
    )
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.setattr(backfill_runner, 'ClickHouseClient', _FakeClickHouseClient)

    with pytest.raises(
        RuntimeError,
        match='execution_mode=reconcile requires at least one explicit --partition-id',
    ):
        backfill_runner.run_exchange_backfill(
            dataset='binance_spot_trades',
            end_date=None,
            max_partitions=None,
            run_id='s34-test-reconcile-missing-partition',
            dry_run=True,
            projection_mode='deferred',
            runtime_audit_mode='summary',
            concurrency=10,
            execution_mode='reconcile',
            partition_ids=None,
        )


def test_s34_exchange_backfill_concurrency_env_requires_integer_ge_10(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('ORIGO_S34_BACKFILL_CONCURRENCY', '9')
    with pytest.raises(RuntimeError, match='must be >= 10'):
        backfill_runner._load_env_backfill_concurrency_or_raise()

    monkeypatch.setenv('ORIGO_S34_BACKFILL_CONCURRENCY', '15')
    assert backfill_runner._load_env_backfill_concurrency_or_raise() == 15


def test_s34_exchange_backfill_runner_rejects_okx_concurrency_above_source_safe_limit() -> None:
    with pytest.raises(
        RuntimeError,
        match='Requested backfill concurrency exceeds source-safe partition-run limit',
    ):
        backfill_runner._assert_requested_concurrency_allowed_or_raise(
            dataset='okx_spot_trades',
            concurrency=2,
        )

    backfill_runner._assert_requested_concurrency_allowed_or_raise(
        dataset='okx_spot_trades',
        concurrency=1,
    )
    backfill_runner._assert_requested_concurrency_allowed_or_raise(
        dataset='binance_spot_trades',
        concurrency=20,
    )


def test_dagster_yaml_enforces_okx_dataset_run_queue_limit() -> None:
    dagster_config = yaml.safe_load(
        (_REPO_ROOT / 'control-plane' / 'dagster.yaml').read_text(encoding='utf-8')
    )
    run_queue = cast(dict[str, Any], dagster_config['run_queue'])
    tag_limits = cast(list[dict[str, Any]], run_queue['tag_concurrency_limits'])
    assert {
        'key': 'origo.backfill.dataset',
        'value': 'okx_spot_trades',
        'limit': 1,
    } in tag_limits


def test_s34_exchange_backfill_runner_builds_required_partition_run_tags() -> None:
    tags = backfill_runner._build_partition_run_tags(
        dataset='binance_spot_trades',
        run_id='s34-test-run',
        projection_mode='deferred',
        runtime_audit_mode='summary',
        execution_mode='reconcile',
    )

    assert tags == {
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.runtime_audit_mode': 'summary',
        'origo.backfill.execution_mode': 'reconcile',
        'origo.backfill.dataset': 'binance_spot_trades',
        'origo.backfill.control_run_id': 's34-test-run',
    }
