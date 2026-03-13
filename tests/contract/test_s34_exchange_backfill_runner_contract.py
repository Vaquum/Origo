from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, cast

import origo_control_plane.s34_exchange_backfill_runner as backfill_runner
import pytest


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
    monkeypatch.setenv(
        'ORIGO_STREAM_QUARANTINE_STATE_PATH',
        str(tmp_path / 'stream-quarantine-state.json'),
    )
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    monkeypatch.setattr(backfill_runner, 'ClickHouseClient', _FakeClickHouseClient)
    monkeypatch.setattr(
        backfill_runner,
        'load_last_completed_daily_partition_from_canonical_or_raise',
        lambda **_: None,
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
    assert result['resume_state_source'] == 'canonical_ingest_cursor_state'
    assert result['last_completed_partition'] is None


def test_s34_exchange_backfill_concurrency_env_requires_integer_ge_10(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('ORIGO_S34_BACKFILL_CONCURRENCY', '9')
    with pytest.raises(RuntimeError, match='must be >= 10'):
        backfill_runner._load_env_backfill_concurrency_or_raise()

    monkeypatch.setenv('ORIGO_S34_BACKFILL_CONCURRENCY', '15')
    assert backfill_runner._load_env_backfill_concurrency_or_raise() == 15
