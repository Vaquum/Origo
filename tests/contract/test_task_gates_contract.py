import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_START_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-start-gate.json'
_END_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'task-end-gate.json'
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'


def test_task_gate_contract_payloads_are_exact() -> None:
    start_payload = json.loads(_START_CONTRACT.read_text(encoding='utf-8'))
    end_payload = json.loads(_END_CONTRACT.read_text(encoding='utf-8'))

    assert start_payload == {
        'scope': 'whole_system_and_all_future_development',
        'applies_to_every_task_type': True,
        'must_be_loaded_after_task_routing_and_before_any_repo_runtime_browser_deploy_or_data_mutation': True,
        'fail_closed_on_no_or_unknown': True,
        'failed_gate_requires_immediate_operator_report_before_work': True,
        'questions_order_is_mandatory': True,
        'questions': [
            {
                'id': 'least_mechanism',
                'prompt': 'Am I choosing the implementation with less mechanism among the options that satisfy the same contract?',
            },
            {
                'id': 'correct_path_is_easiest',
                'prompt': 'Will the correct path be easier than any shortcut on the authoritative surfaces touched by this task?',
            },
            {
                'id': 'no_routine_babysitting',
                'prompt': 'Can this ship without routine babysitting operator tending or hidden upkeep burden?',
            },
            {
                'id': 'mechanical_enforcement_present',
                'prompt': 'Will the governing behavior be enforced mechanically on authoritative surfaces rather than merely documented?',
            },
            {
                'id': 'foundation_scope_honest',
                'prompt': 'If I am treating any part of this work as foundational is it actually inside the closed foundation set and if not is the work deferred until forced?',
            },
            {
                'id': 'single_path_or_dated_migration',
                'prompt': 'Will this leave exactly one valid path for the concern or a migration path with explicit end state and deadline?',
            },
            {
                'id': 'surface_area_minimized',
                'prompt': 'Have I avoided unnecessary knobs options and abstractions while keeping legitimate environment variance in env vars only?',
            },
            {
                'id': 'technology_is_boring',
                'prompt': 'Am I using the most proven operable and well understood technology that satisfies the contract?',
            },
        ],
    }

    assert end_payload == {
        'scope': 'whole_system_and_all_future_development',
        'applies_to_every_task_type': True,
        'must_be_loaded_before_any_done_ready_complete_or_mergeable_claim': True,
        'fail_closed_on_no_or_unknown': True,
        'failed_gate_requires_operator_report_instead_of_completion_claim': True,
        'questions_order_is_mandatory': True,
        'questions': [
            {
                'id': 'mechanism_reduced_or_not_added',
                'prompt': 'Did I reduce or avoid mechanism rather than add clever machinery?',
            },
            {
                'id': 'correct_path_now_easiest',
                'prompt': 'Is the correct path now easier than the shortcut on the authoritative surfaces?',
            },
            {
                'id': 'no_new_babysitting_burden',
                'prompt': 'Did I avoid introducing routine babysitting operator tending or hidden upkeep burden?',
            },
            {
                'id': 'mechanically_enforced_on_authoritative_surfaces',
                'prompt': 'Is the governing behavior now enforced mechanically on authoritative surfaces rather than merely explained?',
            },
            {
                'id': 'no_speculative_non_foundation_scope',
                'prompt': 'Did I avoid speculative non-foundation work and keep the implementation inside the forced scope of the task?',
            },
            {
                'id': 'one_path_only',
                'prompt': 'Is there now exactly one valid path for this concern or an explicit migration path with end state and deadline?',
            },
            {
                'id': 'surface_area_actually_small',
                'prompt': 'Did I minimize knobs options and abstractions while keeping legitimate environment variance in env vars only?',
            },
            {
                'id': 'technology_still_boring',
                'prompt': 'Did I keep the implementation on the most proven operable and well understood technology that satisfies the contract?',
            },
            {
                'id': 'claimed_terminal_state_is_real',
                'prompt': 'If I am claiming done ready complete or mergeable is the claimed terminal state actually reached rather than merely prepared?',
            },
            {
                'id': 'no_authoritative_loophole_remaining',
                'prompt': 'Have I closed every remaining authoritative loophole that would let another agent violate this concern without tripping machinery?',
            },
        ],
    }


def test_task_gates_are_routed_and_referenced() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')

    assert '## Universal Start Gate' in agents_text
    assert 'contracts/governance/task-start-gate.json' in agents_text
    assert 'If any start-gate answer is `no` or `unknown`, stop immediately and report back to the operator before doing any work.' in agents_text
    assert '## Universal End Gate' in agents_text
    assert 'contracts/governance/task-end-gate.json' in agents_text
    assert 'If any end-gate answer is `no` or `unknown`, stop immediately and report back to the operator instead of making the claim.' in agents_text
    assert 'A task is not done if the claimed terminal state is only prepared rather than actually reached.' in agents_text
    assert 'Canonical machine contract: `contracts/governance/task-start-gate.json`' in top_level_plan_text
    assert 'Canonical machine contract: `contracts/governance/task-end-gate.json`' in top_level_plan_text
    assert '`S34-G14`' in work_plan_text
    assert '`S34-11j`' in work_plan_text
    assert 'contracts/governance/task-start-gate.json' in dev_index_text
    assert 'contracts/governance/task-end-gate.json' in dev_index_text
