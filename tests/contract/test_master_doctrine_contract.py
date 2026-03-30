import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'master-doctrine.json'
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'


def test_master_doctrine_contract_payload_is_exact() -> None:
    payload = json.loads(_CONTRACT.read_text(encoding='utf-8'))

    assert payload == {
        'scope': 'whole_system_and_all_future_development',
        'applies_to_every_slice_step_move_task_and_completion_claim': True,
        'prime_doctrine': 'mechanical_enforcement_over_documentation',
        'closed_foundation_set': [
            'store_correctness',
            'current_truth_read_contract',
            'single_write_entrypoint',
            'operator_truth_unification',
            'envelope_finalization',
            'concurrency_control',
            'adapter_boundary_enforcement',
            'rebuild_proof',
            'critical_path_viability',
        ],
        'anything_not_in_closed_foundation_set_is_deferrable_until_forced': True,
        'foundation_expansion_requires_explicit_operator_approval': True,
        'principles': {
            'intentional_simplicity': {
                'rule': 'if_two_implementations_satisfy_the_same_contract_choose_the_one_with_less_mechanism',
                'anti_pattern': 'cleverness_or_extra_mechanism_without_contract_gain',
            },
            'extreme_ergonomics': {
                'rule': 'the_correct_path_must_be_easier_than_the_shortcut_on_authoritative_surfaces',
                'anti_pattern': 'a_shortcut_that_is_easier_than_the_governed_path',
            },
            'zero_ongoing_human_nursing': {
                'rule': 'nothing_ships_that_requires_routine_babysitting_operator_tending_or_hidden_upkeep_burden',
                'anti_pattern': 'a_system_that_only_keeps_working_if_someone_keeps_watching_it',
            },
            'mechanical_enforcement_over_documentation': {
                'rule': 'if_authoritative_surfaces_allow_the_violation_the_contract_is_not_implemented',
                'anti_pattern': 'documentation_without_mechanical_prevention',
            },
            'defer_until_forced_never_defer_foundations': {
                'rule': 'non_foundation_work_waits_for_a_named_problem_and_deadline_while_the_closed_foundation_set_is_never_deferred',
                'anti_pattern': 'speculative_build_out_or_relabeling_non_foundation_work_as_foundational',
            },
            'one_way_per_concern': {
                'rule': 'there_must_be_one_valid_path_per_concern_or_a_migration_path_with_explicit_end_state_and_deadline',
                'anti_pattern': 'parallel_paths_without_a_termination_date',
            },
            'smallest_surface_area': {
                'rule': 'hardcode_invariant_policy_and_keep_only_environment_variance_as_env_configuration',
                'anti_pattern': 'unnecessary_knobs_options_or_abstractions_that_become_shadow_governance',
            },
            'boring_technology': {
                'rule': 'choose_the_most_proven_operable_and_well_understood_tool_that_satisfies_the_contract',
                'anti_pattern': 'technology_novelty_without_contract_necessity',
            },
        },
    }


def test_master_doctrine_is_routed_and_referenced() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')

    assert '## Master Doctrine' in agents_text
    assert 'contracts/governance/master-doctrine.json' in agents_text
    assert 'mechanical_enforcement_over_documentation' in agents_text
    assert 'closed foundation set lives only in `contracts/governance/master-doctrine.json`' in agents_text
    assert 'Canonical machine contract: `contracts/governance/master-doctrine.json`' in top_level_plan_text
    assert '`S34-G14`' in work_plan_text
    assert '`S34-11j`' in work_plan_text
    assert 'contracts/governance/master-doctrine.json' in dev_index_text
