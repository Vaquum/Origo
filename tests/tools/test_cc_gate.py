from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('cc_gate', REPO_ROOT / 'tools/cc_gate.py')
assert SPEC is not None and SPEC.loader is not None
CC_GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CC_GATE)


@pytest.mark.parametrize('subject', ['feat: add source', 'fix(query)!: reject invalid input', 'ci: update checks'])
def test_conventional_subject_is_accepted(subject: str) -> None:
    assert CC_GATE.check_cc(subject) is None


@pytest.mark.parametrize('subject', ['', 'Fix: wrong type', 'fix: ', 'update checks'])
def test_invalid_subject_is_rejected(subject: str) -> None:
    assert CC_GATE.check_cc(subject) is not None


def test_cc_gate_checks_nonmerge_commits_without_formatting_issue_titles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CC_GATE, 'list_commits', lambda *_: [
        {'sha': 'a' * 40, 'subject': 'ordinary commit', 'is_merge': False},
        {'sha': 'b' * 40, 'subject': 'Merge main', 'is_merge': True},
    ])
    failures = CC_GATE.gate('ci: update checks', 'base', 'head')
    assert len(failures) == 1
    assert 'aaaaaaaa' in failures[0]
