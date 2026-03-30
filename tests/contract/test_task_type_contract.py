import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_TASK_TYPES_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-types.json'
_TASK_DECOMPOSITION_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-decomposition.json'
_TASK_ADMISSION_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-admission.json'
_REQUEST_TASK_COVERAGE_CONTRACT = (
    _REPO_ROOT / 'contracts' / 'governance' / 'request-task-coverage.json'
)
_CONTRACT_APPLICABILITY_CONTRACT = (
    _REPO_ROOT / 'contracts' / 'governance' / 'contract-applicability.json'
)
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_DOC = _REPO_ROOT / 'docs' / 'Developer' / 'task-type-contract.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'

_ALLOWED_TASK_TYPES = [
    'proof',
    'governance',
    'operator_surface',
    'runtime_env',
    'correctness',
    'performance',
    'capability',
]


def test_task_type_contract_payloads_are_exact() -> None:
    task_types_payload = json.loads(_TASK_TYPES_CONTRACT.read_text(encoding='utf-8'))
    task_decomposition_payload = json.loads(
        _TASK_DECOMPOSITION_CONTRACT.read_text(encoding='utf-8')
    )
    task_admission_payload = json.loads(
        _TASK_ADMISSION_CONTRACT.read_text(encoding='utf-8')
    )
    request_task_coverage_payload = json.loads(
        _REQUEST_TASK_COVERAGE_CONTRACT.read_text(encoding='utf-8')
    )
    contract_applicability_payload = json.loads(
        _CONTRACT_APPLICABILITY_CONTRACT.read_text(encoding='utf-8')
    )

    assert task_types_payload == {
        'allowed_primary_task_types': _ALLOWED_TASK_TYPES,
        'classification_is_by_requested_outcome': True,
        'unknown_root_cause_starts_as': 'proof',
        'task_types': {
            'proof': {
                'intent_code': 'evidence_and_live_validation_only',
                'forbidden_change_classes': [
                    'runtime_behavior_change',
                    'deploy_or_env_contract_change',
                    'source_or_canonical_semantics_change',
                ],
            },
            'governance': {
                'intent_code': 'rules_routing_and_authority_only',
                'forbidden_change_classes': [
                    'runtime_feature_change',
                    'deploy_or_env_bootstrap_change',
                    'performance_implementation_change',
                ],
            },
            'operator_surface': {
                'intent_code': 'authoritative_operator_or_user_visible_surface',
                'forbidden_change_classes': [
                    'runtime_dependency_bootstrap_change',
                    'source_parser_contract_change',
                    'throughput_only_tuning',
                ],
            },
            'runtime_env': {
                'intent_code': 'execution_environment_delivery_ci_and_toolchain_contract',
                'forbidden_change_classes': [
                    'authoritative_operator_or_user_surface_change',
                    'source_or_canonical_correctness_change',
                    'throughput_only_tuning_without_runtime_contract_change',
                ],
            },
            'correctness': {
                'intent_code': 'make_existing_behavior_true_without_new_capability',
                'forbidden_change_classes': [
                    'new_capability_introduction',
                    'operator_surface_only_change',
                    'performance_only_change',
                ],
            },
            'performance': {
                'intent_code': 'improve_cost_latency_or_parallelism_without_semantic_change',
                'forbidden_change_classes': [
                    'correctness_fix_disguised_as_speed_work',
                    'deploy_or_runtime_bootstrap_change',
                    'new_feature_introduction',
                ],
            },
            'capability': {
                'intent_code': 'new_supported_system_ability_or_surface',
                'forbidden_change_classes': [
                    'repair_only_of_existing_behavior',
                    'governance_only_change',
                    'proof_only_status_work',
                ],
            },
        },
    }

    assert task_decomposition_payload == {
        'exactly_one_primary_task_type_per_work_unit': True,
        'implementation_after_proof_requires_new_primary_task_type': True,
        'mixed_type_requests_must_be_decomposed_before_implementation': True,
        'mixed_type_implementation_branch_or_pr_is_invalid': True,
        'branch_pr_commit_scope_must_match_primary_task_type': True,
        'proof_must_not_be_used_as_cover_for_runtime_changes': True,
        'slice_closeout_is_required_but_not_a_primary_task_type': True,
    }

    assert task_admission_payload == {
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

    assert request_task_coverage_payload['scope'] == 'whole_system_and_all_future_development'
    assert request_task_coverage_payload['request_family_catalog_exhaustive_for_origo_engineering_work'] is True
    assert set(request_task_coverage_payload['families']) == {
        'live_runtime_truth_and_behavior_proof',
        'throughput_capacity_and_hot_path_proof',
        'governance_and_review_control_changes',
        'authoritative_operator_and_user_surface_changes',
        'execution_environment_delivery_and_toolchain_contract_changes',
        'existing_system_behavior_correctness_repairs',
        'existing_system_behavior_performance_repairs',
        'new_system_capability_or_supported_surface',
    }

    assert contract_applicability_payload['task_admission_contracts'] == [
        'request-task-coverage.json',
        'task-types.json',
        'task-decomposition.json',
        'task-admission.json',
    ]
    assert contract_applicability_payload[
        'shared_domain_contracts_must_be_loaded_when_activation_domain_matches'
    ] is True
    assert contract_applicability_payload[
        'touched_system_surface_without_shared_domain_contract_requires_immediate_operator_report'
    ] is True
    for contract_name, contract_payload in contract_applicability_payload['shared_domain_contracts'].items():
        assert contract_name.endswith('.json')
        assert set(contract_payload['applicable_task_types']).issubset(set(_ALLOWED_TASK_TYPES))


def test_repo_governance_surfaces_reference_task_type_contracts() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_doc_text = _DEV_DOC.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')

    assert '## Task Admission' in agents_text
    assert 'contracts/governance/task-types.json' in agents_text
    assert 'contracts/governance/task-decomposition.json' in agents_text
    assert 'contracts/governance/request-task-coverage.json' in agents_text
    assert 'contracts/governance/task-admission.json' in agents_text
    assert 'contracts/governance/contract-applicability.json' in agents_text
    assert '## Task-Type Routing' in agents_text
    assert '## Shared-Domain Overlay Routing' in agents_text
    for task_type in _ALLOWED_TASK_TYPES:
        assert f'### `{task_type}`' in agents_text

    assert 'contracts/governance/task-types.json' in top_level_plan_text
    assert 'contracts/governance/task-decomposition.json' in top_level_plan_text
    assert 'contracts/governance/request-task-coverage.json' in top_level_plan_text
    assert 'contracts/governance/task-admission.json' in top_level_plan_text
    assert 'contracts/governance/contract-applicability.json' in top_level_plan_text
    assert '`S34-G9`' in work_plan_text
    assert '`S34-G10`' in work_plan_text
    assert '`S34-G11`' in work_plan_text
    assert '`S34-G12`' in work_plan_text
    assert '`S34-G13`' in work_plan_text
    assert '`S34-11e`' in work_plan_text
    assert '`S34-11f`' in work_plan_text
    assert '`S34-11g`' in work_plan_text
    assert '`S34-11h`' in work_plan_text
    assert '`S34-11i`' in work_plan_text
    assert 'Canonical machine contracts:' in dev_doc_text
    assert 'whole-system Origo governance' in dev_doc_text
    assert 'This document is reference-only for governance.' in dev_doc_text
    assert 'contracts/governance/request-task-coverage.json' in dev_index_text
    assert 'contracts/governance/task-types.json' in dev_index_text
    assert 'contracts/governance/task-decomposition.json' in dev_index_text
    assert 'contracts/governance/task-admission.json' in dev_index_text
    assert 'contracts/governance/contract-applicability.json' in dev_index_text
    assert 'docs/Developer/task-type-contract.md' in dev_index_text
