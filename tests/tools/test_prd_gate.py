from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import textwrap
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


def test_issue_api_setup_error_fails_the_required_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    def failed_fetch(*_args: object) -> None:
        raise SystemExit(2)
    monkeypatch.setattr(PRD_GATE, 'fetch_issue', failed_fetch)
    with pytest.raises(SystemExit) as failure:
        PRD_GATE.gate('fix: repair reader', 'Closes #613', 'Vaquum/Origo')
    assert failure.value.code == 2


def test_issue_event_reruns_a_green_required_check_without_a_separate_gate_probe(tmp_path: Path) -> None:
    # Deliberate GitHub protocol stand-in: the old canonical result is green.
    # The watcher must invalidate it by requesting a canonical rerun even when
    # an independent issue probe would have failed before reaching that request.
    workflow = (REPO_ROOT / '.github/workflows/pr_checks_prd_on_issue.yml').read_text()
    step = workflow.split('      - name: Re-run PRD gate per linked PR', 1)[1]
    script = textwrap.dedent(step.split('        run: |\n', 1)[1])
    script = script.replace('${{ github.repository }}', 'Vaquum/Origo')
    script = script.replace('${{ github.event.issue.number }}', '613')
    (tmp_path / 'linked_prs.json').write_text(json.dumps([{'number': 614, 'headRefOid': 'a' * 40}]))
    fake_gh = tmp_path / 'gh'
    fake_gh.write_text(f'#!{sys.executable}\n' + textwrap.dedent("""
        import json
        import os
        import sys
        from pathlib import Path
        args = sys.argv[1:]
        if args[:2] == ['pr', 'view']:
            if '--jq' in args:
                print('a' * 40)
            else:
                print(json.dumps({'headRefOid': 'a' * 40, 'headRefName': 'branch',
                                  'headRepository': {'nameWithOwner': 'Vaquum/Origo'}}))
        elif args[:1] == ['api'] and '--method' in args:
            assert args[args.index('--method') + 1] == 'POST'
            assert args[-1] == 'repos/Vaquum/Origo/actions/runs/123/rerun'
            Path(os.environ['ORIGO_RERUN_LOG']).write_text(args[-1])
        elif args[:1] == ['api']:
            assert 'actions/workflows/pr_checks_prd.yml/runs?' in args[1]
            print(json.dumps([{'id': 123, 'status': 'completed', 'conclusion': 'success',
                               'event': 'pull_request', 'head_branch': 'branch',
                               'head_repository': {'full_name': 'Vaquum/Origo'}}]))
        else:
            raise SystemExit('Unexpected GitHub protocol operation: ' + repr(args))
        """))
    fake_gh.chmod(0o755)
    rerun_log = tmp_path / 'rerun.txt'
    result = subprocess.run(
        ['bash', '-c', script], cwd=tmp_path, text=True, capture_output=True, check=False,
        env={**os.environ, 'PATH': str(tmp_path) + os.pathsep + os.environ.get('PATH', ''),
             'ORIGO_RERUN_LOG': str(rerun_log)},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert rerun_log.read_text() == 'repos/Vaquum/Origo/actions/runs/123/rerun'
