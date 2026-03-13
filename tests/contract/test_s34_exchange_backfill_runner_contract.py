from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, cast

from origo_control_plane.s34_exchange_backfill_runner import run_exchange_backfill


def test_s34_exchange_backfill_runner_dry_run_uses_contract_planner(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        'ORIGO_BACKFILL_RUN_STATE_PATH',
        str(tmp_path / 'run-state.json'),
    )
    monkeypatch.setenv(
        'ORIGO_BACKFILL_MANIFEST_LOG_PATH',
        str(tmp_path / 'manifest.jsonl'),
    )
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'runtime-audit.jsonl'),
    )

    result = run_exchange_backfill(
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
