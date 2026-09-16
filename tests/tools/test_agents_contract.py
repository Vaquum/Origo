from __future__ import annotations

import hashlib
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENTS_FILE = REPO_ROOT / 'AGENTS.md'
RULESET_WORKFLOW = REPO_ROOT / '.github/workflows/pr_checks_ruleset.yml'
EXPECTED_SHA256 = 'd54bdd87f27c8a7d3ce8ccfb29b326eb090dc2fa8eed1b662002c5f163027ae1'


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
