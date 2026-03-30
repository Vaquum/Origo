import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'request-task-coverage.json'
_TASK_TYPES_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-types.json'
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_DOC = _REPO_ROOT / 'docs' / 'Developer' / 'request-task-coverage-contract.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'


def test_request_task_coverage_contract_payload_is_exact() -> None:
    payload = json.loads(_CONTRACT.read_text(encoding='utf-8'))
    task_types_payload = json.loads(_TASK_TYPES_CONTRACT.read_text(encoding='utf-8'))

    assert payload == {
        'scope': 'whole_system_and_all_future_development',
        'request_family_catalog_exhaustive_for_origo_engineering_work': True,
        'request_family_match_required_before_primary_task_type_assignment': True,
        'unmatched_request_requires_immediate_operator_report': True,
        'standalone_reference_or_closeout_request_requires_inherited_primary_task_type_or_stop': True,
        'new_uncovered_recurring_work_family_requires_contract_addition_before_implementation': True,
        'families': {
            'live_runtime_truth_and_behavior_proof': {
                'primary_task_type': 'proof',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'dagster_dagit',
                    'canonical_event_log',
                    'proof_state',
                    'projections',
                    'query_export_runtime',
                    'historical_interfaces',
                    'source_runtime_behavior',
                    'automation_runtime_state',
                ],
                'covered_outcomes': [
                    'audit_current_live_state',
                    'reproduce_live_behavior_without_runtime_change',
                    'validate_merged_behavior_in_real_runtime',
                    'prove_deterministic_replay_or_parity_without_semantic_change',
                ],
            },
            'throughput_capacity_and_hot_path_proof': {
                'primary_task_type': 'proof',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'daily_file_ingest_hot_path',
                    'source_fetch_pacing',
                    'worker_pools',
                    'query_latency',
                    'projection_build_latency',
                    'runtime_capacity',
                ],
                'covered_outcomes': [
                    'benchmark_hot_path_behavior',
                    'benchmark_worker_or_capacity_shape',
                    'measure_latency_resource_or_parallelism_without_runtime_change',
                ],
            },
            'governance_and_review_control_changes': {
                'primary_task_type': 'governance',
                'required_additional_contracts': ['pr-review-routing.json'],
                'covered_system_surfaces': [
                    'agents_routing',
                    'machine_contracts',
                    'review_process',
                    'merge_controls',
                    'governance_reference_index',
                ],
                'covered_outcomes': [
                    'change_governance_routing',
                    'change_machine_governance_contracts',
                    'change_review_or_merge_rules',
                    'change_fail_closed_admission_or_domain_coverage',
                ],
            },
            'authoritative_operator_and_user_surface_changes': {
                'primary_task_type': 'operator_surface',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'dagit_launchpad',
                    'partition_selection_and_status',
                    'raw_query_api_contract',
                    'raw_export_api_contract',
                    'historical_http_interface',
                    'historical_python_interface',
                    'visible_status_warning_error_surface',
                ],
                'covered_outcomes': [
                    'change_operator_or_user_visible_inputs_outputs_or_defaults',
                    'change_partition_selection_or_status_surface',
                    'change_request_response_shape_without_new_underlying_capability',
                ],
            },
            'execution_environment_delivery_and_toolchain_contract_changes': {
                'primary_task_type': 'runtime_env',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'env_contracts',
                    'deploy_workflows',
                    'container_images',
                    'ci_quality_gates',
                    'runtime_bootstrap',
                    'toolchain_dependencies',
                    'automation_runtime_wiring',
                ],
                'covered_outcomes': [
                    'change_env_or_secret_contract',
                    'change_deploy_ci_or_bootstrap_contract',
                    'change_runtime_or_toolchain_wiring_without_semantic_feature_change',
                ],
            },
            'existing_system_behavior_correctness_repairs': {
                'primary_task_type': 'correctness',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'source_parsers',
                    'canonical_writer',
                    'proof_state_machine',
                    'projections',
                    'query_export_logic',
                    'historical_interfaces',
                    'schedules_and_sensors',
                    'rights_behavior',
                    'migrations_and_sql_logic',
                ],
                'covered_outcomes': [
                    'fix_existing_behavior_without_new_capability',
                    'fix_source_writer_projection_query_or_export_correctness',
                    'fix_existing_runtime_truth_or_parity_behavior',
                ],
            },
            'existing_system_behavior_performance_repairs': {
                'primary_task_type': 'performance',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'daily_file_ingest_hot_path',
                    'projection_build_path',
                    'query_path',
                    'batch_planning',
                    'worker_concurrency',
                    'source_timeouts_and_pacing',
                    'runtime_resource_usage',
                ],
                'covered_outcomes': [
                    'remove_row_by_row_or_n_plus_one_hot_path',
                    'change_concurrency_timeout_or_pacing_without_semantic_change',
                    'improve_latency_throughput_or_resource_usage_of_existing_behavior',
                ],
            },
            'new_system_capability_or_supported_surface': {
                'primary_task_type': 'capability',
                'required_additional_contracts': [],
                'covered_system_surfaces': [
                    'new_source_or_dataset',
                    'new_job_schedule_or_sensor',
                    'new_query_export_or_historical_surface',
                    'new_projection_or_view',
                    'new_operator_surface',
                    'new_tooling_or_runtime_surface',
                ],
                'covered_outcomes': [
                    'add_new_supported_system_ability',
                    'add_new_dataset_source_or_runtime_surface',
                    'add_new_operator_or_user_visible_supported_surface',
                ],
            },
        },
    }

    allowed_task_types = set(task_types_payload['allowed_primary_task_types'])
    for family in payload['families'].values():
        assert family['primary_task_type'] in allowed_task_types


def test_repo_governance_surfaces_reference_request_task_coverage_contract() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_doc_text = _DEV_DOC.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')

    assert 'contracts/governance/request-task-coverage.json' in agents_text
    assert 'Every request must match exactly one routed request family' in agents_text
    assert 'genuinely new recurring work class' in agents_text
    assert 'Follow any family-specific additional contracts' in agents_text
    assert 'whole Origo system and all future development' in agents_text
    assert 'contracts/governance/request-task-coverage.json' in top_level_plan_text
    assert '`S34-G11`' in work_plan_text
    assert '`S34-G12`' in work_plan_text
    assert '`S34-G13`' in work_plan_text
    assert '`S34-11g`' in work_plan_text
    assert '`S34-11h`' in work_plan_text
    assert '`S34-11i`' in work_plan_text
    assert 'Canonical machine contract: `contracts/governance/request-task-coverage.json`' in dev_doc_text
    assert 'contracts/governance/task-types.json' in dev_doc_text
    assert 'contracts/governance/task-admission.json' in dev_doc_text
    assert 'contracts/governance/contract-applicability.json' in dev_doc_text
    assert 'whole-system Origo governance' in dev_doc_text
    assert 'This document is reference-only for governance.' in dev_doc_text
    assert 'contracts/governance/request-task-coverage.json' in dev_index_text
    assert 'docs/Developer/request-task-coverage-contract.md' in dev_index_text
