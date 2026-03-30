import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-admission.json'
_REQUEST_FAMILY_CONTRACT = (
    _REPO_ROOT / 'contracts' / 'governance' / 'request-task-coverage.json'
)
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_DOC = _REPO_ROOT / 'docs' / 'Developer' / 'task-admission-contract.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'


def test_task_admission_contract_payload_is_exact() -> None:
    payload = json.loads(_CONTRACT.read_text(encoding='utf-8'))

    assert payload == {
        'allowed_primary_task_types_exhaustive_for_origo_engineering_work': True,
        'no_catch_all_task_type': True,
        'request_family_match_required_before_primary_task_type_assignment': True,
        'new_uncovered_recurring_work_family_requires_contract_addition_before_implementation': True,
        'unroutable_request_requires_immediate_operator_report': True,
        'mixed_type_request_without_safe_decomposition_requires_immediate_operator_report': True,
        'reference_or_closeout_only_request_must_inherit_governing_primary_task_type_or_stop': True,
        'review_follow_up_request_must_inherit_governing_primary_task_type_or_stop': True,
        'implementation_forbidden_without_routed_task_type': True,
        'implementation_forbidden_without_routed_contract_coverage': True,
        'touched_system_surface_without_domain_contract_requires_immediate_operator_report': True,
        'operator_report_must_precede_runtime_or_repo_mutation': True,
        'no_route_no_work': True,
    }

    request_family_payload = json.loads(
        _REQUEST_FAMILY_CONTRACT.read_text(encoding='utf-8')
    )
    assert (
        request_family_payload['request_family_match_required_before_primary_task_type_assignment']
        is True
    )


def test_repo_governance_surfaces_reference_task_admission_contract() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_doc_text = _DEV_DOC.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')

    assert '## Task Admission' in agents_text
    assert 'contracts/governance/request-task-coverage.json' in agents_text
    assert 'contracts/governance/task-admission.json' in agents_text
    assert 'No task routing, no work.' in agents_text
    assert 'stop immediately and report back to the operator' in agents_text
    assert 'Every request must match exactly one routed request family' in agents_text
    assert 'genuinely new recurring work class' in agents_text
    assert 'If the request touches any system surface without routed shared-domain contract coverage' in agents_text
    assert 'No implementation, runtime mutation, repo mutation, PR work, deploy work, browser work, or data write may proceed before routed contract coverage is established.' in agents_text
    assert 'contracts/governance/request-task-coverage.json' in top_level_plan_text
    assert 'contracts/governance/task-admission.json' in top_level_plan_text
    assert '`S34-G11`' in work_plan_text
    assert '`S34-G12`' in work_plan_text
    assert '`S34-G13`' in work_plan_text
    assert '`S34-11g`' in work_plan_text
    assert '`S34-11h`' in work_plan_text
    assert '`S34-11i`' in work_plan_text
    assert 'Canonical machine contract: `contracts/governance/task-admission.json`' in dev_doc_text
    assert 'contracts/governance/request-task-coverage.json' in dev_doc_text
    assert 'contracts/governance/contract-applicability.json' in dev_doc_text
    assert 'whole-system Origo governance' in dev_doc_text
    assert 'This document is reference-only for governance.' in dev_doc_text
    assert 'contracts/governance/request-task-coverage.json' in dev_index_text
    assert 'contracts/governance/task-admission.json' in dev_index_text
    assert 'docs/Developer/task-admission-contract.md' in dev_index_text
