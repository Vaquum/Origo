from __future__ import annotations

import json
from pathlib import Path

import pytest

import origo.pathing as pathing
from origo.audit.immutable_log import resolve_required_path_env
from origo.events.quarantine import load_stream_quarantine_state_path
from origo.scraper.rights import resolve_scraper_rights


def _set_repo_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    monkeypatch.setattr(pathing, 'monorepo_root', lambda: root)


def test_resolve_required_path_env_uses_monorepo_root_for_relative_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_repo_root(monkeypatch, tmp_path)
    monkeypatch.setenv('ORIGO_TEST_AUDIT_PATH', 'storage/audit/runtime.jsonl')
    assert resolve_required_path_env('ORIGO_TEST_AUDIT_PATH') == (
        tmp_path / 'storage/audit/runtime.jsonl'
    )


def test_stream_quarantine_path_uses_monorepo_root_for_relative_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_repo_root(monkeypatch, tmp_path)
    monkeypatch.setenv(
        'ORIGO_STREAM_QUARANTINE_STATE_PATH',
        'storage/audit/stream-quarantine-state.json',
    )
    assert load_stream_quarantine_state_path() == (
        tmp_path / 'storage/audit/stream-quarantine-state.json'
    )


def test_scraper_rights_matrix_path_uses_monorepo_root_for_relative_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_repo_root(monkeypatch, tmp_path)
    matrix_path = tmp_path / 'contracts/source-rights-matrix.json'
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    matrix_path.write_text(
        json.dumps(
            {
                'sources': {
                    'binance_spot_trades': {
                        'rights_state': 'Hosted Allowed',
                        'source_ids': ['binance'],
                    }
                }
            },
            ensure_ascii=True,
            sort_keys=True,
        ),
        encoding='utf-8',
    )
    monkeypatch.setenv(
        'ORIGO_SOURCE_RIGHTS_MATRIX_PATH',
        'contracts/source-rights-matrix.json',
    )
    decision = resolve_scraper_rights(
        source_key='binance_spot_trades',
        source_id='binance',
    )
    assert decision.rights_state == 'Hosted Allowed'
