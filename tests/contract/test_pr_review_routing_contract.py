import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT = (
    _REPO_ROOT / 'contracts' / 'governance' / 'pr-review-routing.json'
)
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_DOC = _REPO_ROOT / 'docs' / 'Developer' / 'pr-review-routing-contract.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'


def test_pr_review_routing_contract_payload_is_exact() -> None:
    payload = json.loads(_CONTRACT.read_text())

    assert payload == {
        'review_authority': 'zero-bang',
        'forbidden_pr_authors': ['zero-bang'],
        'required_review_request': True,
        'must_wait_for_review_outcome': True,
        'must_resolve_all_conversations_before_rerequest': True,
        'must_rerequest_review_after_conversation_resolution': True,
        'required_approvals': 1,
        'approval_must_come_from': 'zero-bang',
        'merge_requires_final_zero_bang_approval': True,
    }


def test_repo_governance_surfaces_reference_zero_bang_review_routing() -> None:
    assert '## Pull Request Governance' in _AGENTS.read_text()
    assert 'never be used as the git user, PR author, or implementation identity' in _AGENTS.read_text()
    assert '## PR Governance Contract' in _TOP_LEVEL_PLAN.read_text()
    assert '`S34-G5`' in _WORK_PLAN.read_text()
    assert '`S34-11b`' in _WORK_PLAN.read_text()
    assert 'zero-bang' in _DEV_DOC.read_text()
    assert 'resolve every conversation' in _DEV_DOC.read_text()
    assert 'pr-review-routing-contract.md' in _DEV_INDEX.read_text()
