from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from origo_control_plane.backfill.source_rate_gate import (
    wait_for_source_rate_gate_or_raise,
)


def test_source_rate_gate_requires_dagster_home(monkeypatch: Any) -> None:
    monkeypatch.delenv('DAGSTER_HOME', raising=False)
    with pytest.raises(RuntimeError, match='DAGSTER_HOME must be set and non-empty'):
        wait_for_source_rate_gate_or_raise(
            source_id='okx_download_link_resolution',
            min_interval_seconds=0.75,
        )


def test_source_rate_gate_waits_and_persists_state(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('DAGSTER_HOME', str(tmp_path))
    observed_sleeps: list[float] = []
    observed_times = iter([100.2, 100.75])
    monkeypatch.setattr('origo_control_plane.backfill.source_rate_gate.time.time', lambda: next(observed_times))
    monkeypatch.setattr(
        'origo_control_plane.backfill.source_rate_gate.time.sleep',
        lambda seconds: observed_sleeps.append(seconds),
    )

    gate_dir = tmp_path / 'storage' / 'source_rate_gates'
    gate_dir.mkdir(parents=True, exist_ok=True)
    state_path = gate_dir / 'okx_download_link_resolution.json'
    state_path.write_text(
        json.dumps(
            {
                'source_id': 'okx_download_link_resolution',
                'min_interval_seconds': 0.75,
                'last_granted_at_epoch_seconds': 100.0,
            }
        ),
        encoding='utf-8',
    )

    wait_for_source_rate_gate_or_raise(
        source_id='okx_download_link_resolution',
        min_interval_seconds=0.75,
    )

    assert observed_sleeps == [pytest.approx(0.55)]
    persisted = json.loads(state_path.read_text(encoding='utf-8'))
    assert persisted['last_granted_at_epoch_seconds'] == 100.75
