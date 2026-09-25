from __future__ import annotations

import hashlib
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENTS_FILE = REPO_ROOT / 'AGENTS.md'
RULESET_WORKFLOW = REPO_ROOT / '.github/workflows/pr_checks_ruleset.yml'
EXPECTED_SHA256 = '908930fe6affee9b200b736c684fba6e0a60e3e08b28d4a07e2059524fbf6014'


def test_repo_agents_file_exists_and_has_expected_sha256() -> None:
    assert AGENTS_FILE.exists()
    assert hashlib.sha256(AGENTS_FILE.read_bytes()).hexdigest() == EXPECTED_SHA256


def test_repo_agents_file_contains_zero_bang_authority_and_ten_laws() -> None:
    agents = AGENTS_FILE.read_text(encoding='utf-8')

    assert '# AGENTS.md' in agents
    assert '## The laws' in agents
    assert '**`zero-bang` is the approving authority.**' in agents

    law_numbers = re.findall(r'^\d+\.\s', agents, flags=re.MULTILINE)
    assert law_numbers == [f'{n}. ' for n in range(1, 11)]


def test_pr_checks_ruleset_runs_agents_contract() -> None:
    workflow = RULESET_WORKFLOW.read_text(encoding='utf-8')

    assert 'tests/tools/test_agents_contract.py' in workflow
    assert 'continue-on-error' not in workflow


def test_law_exception_preserves_ten_laws_and_monitor_authority() -> None:
    text = AGENTS_FILE.read_text()
    assert 'The monitor worker is the sole detector and alert sender' in text
    assert 'public read-only summary' in text
    assert 'Do not add another dashboard, alert path' in text
    test_repo_agents_file_contains_zero_bang_authority_and_ten_laws()
    test_repo_agents_file_exists_and_has_expected_sha256()


def test_pull_request_template_forbids_blocking_backfills() -> None:
    template = (REPO_ROOT / '.github/PULL_REQUEST_TEMPLATE.md').read_text(encoding='utf-8')
    assert "- [ ] No backfill in this change blocks another source's fills" in template
