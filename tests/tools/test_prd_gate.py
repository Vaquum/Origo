from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / 'tools'))
PRD_GATE = importlib.import_module('prd_gate')


@pytest.mark.parametrize('title', ['fix: repair reader', 'docs: explain setup', 'ci: update checks'])
def test_nonfeature_accepts_an_ordinary_open_issue(title: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PRD_GATE, 'fetch_issue', lambda *_: {'state': 'open', 'labels': [], 'is_pull_request': False})
    assert PRD_GATE.gate(title, 'Closes #613', 'Vaquum/Origo') == []


@pytest.mark.parametrize('title', ['feat: add source', 'feat(source): add adapter', 'feat!: replace interface'])
@pytest.mark.parametrize('labels', [[], ['prd']])
def test_feature_requires_prd_label(title: str, labels: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PRD_GATE, 'fetch_issue', lambda *_: {'state': 'open', 'labels': labels, 'is_pull_request': False})
    failures = PRD_GATE.gate(title, 'Closes #613', 'Vaquum/Origo')
    assert bool(failures) == ('prd' not in labels)


@pytest.mark.parametrize('body', ['', 'Closes #613\nFixes #614', 'Closes Vaquum/Origo#613', 'Fixes: https://github.com/Vaquum/Origo/issues/613'])
def test_missing_multiple_or_qualified_closures_fail_before_api(body: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_fetch(*_args: object) -> None:
        pytest.fail('invalid closing references must fail before fetching an issue')
    monkeypatch.setattr(PRD_GATE, 'fetch_issue', unexpected_fetch)
    assert PRD_GATE.gate('fix: repair reader', body, 'Vaquum/Origo')


def test_repeated_reference_to_same_issue_closes_one_issue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PRD_GATE, 'fetch_issue', lambda *_: {'state': 'open', 'labels': [], 'is_pull_request': False})
    assert PRD_GATE.gate('fix: repair reader', 'Closes #613\nFixes: #613', 'Vaquum/Origo') == []


@pytest.mark.parametrize('issue', [None, {'state': 'closed'}, {'state': 'open', 'is_pull_request': True}])
def test_missing_closed_or_pr_targets_fail(issue: dict[str, object] | None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PRD_GATE, 'fetch_issue', lambda *_: issue)
    assert PRD_GATE.gate('fix: repair reader', 'Closes #613', 'Vaquum/Origo')


def test_prd_form_labels_features_without_requiring_implementation_tickets() -> None:
    form = (REPO_ROOT / '.github/ISSUE_TEMPLATE/prd.yml').read_text()
    assert 'labels: [prd]' in form
    assert 'work_slices' not in form
