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
    payload = json.loads(_CONTRACT.read_text(encoding='utf-8'))

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
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_doc_text = _DEV_DOC.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')

    assert '## Pull Request Governance' in agents_text
    assert 'never be used as the git user, PR author, or implementation identity' in agents_text
    assert '## PR Governance Contract' in top_level_plan_text
    assert '`S34-G5`' in work_plan_text
    assert '`S34-11b`' in work_plan_text
    assert 'zero-bang' in dev_doc_text
    assert 'resolve every conversation' in dev_doc_text
    assert 'pr-review-routing-contract.md' in dev_index_text
