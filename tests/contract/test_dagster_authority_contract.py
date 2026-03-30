import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT = _REPO_ROOT / 'contracts' / 'governance' / 'dagster-authority.json'
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_WORK_PLAN = _REPO_ROOT / 'spec' / '2-itemized-work-plan.md'
_DEV_DOC = _REPO_ROOT / 'docs' / 'Developer' / 'dagster-authority-contract.md'
_RUNTIME_DOC = _REPO_ROOT / 'docs' / 'Developer' / 's34-canonical-backfill-runtime.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'
_HISTORICAL_DAILY_TRANCHE_DOC = _REPO_ROOT / 'docs' / 'Developer' / 's34-daily-tranche-controller.md'
_HISTORICAL_EXCHANGE_SEQUENCE_DOC = _REPO_ROOT / 'docs' / 'Developer' / 's34-exchange-sequence-controller.md'
_HISTORICAL_ETF_RUNNER_DOC = _REPO_ROOT / 'docs' / 'Developer' / 's34-etf-backfill-runner.md'
_HISTORICAL_BITCOIN_RUNNER_DOC = _REPO_ROOT / 'docs' / 'Developer' / 's34-bitcoin-backfill-runner.md'


def test_dagster_authority_contract_payload_is_exact() -> None:
    payload = json.loads(_CONTRACT.read_text(encoding='utf-8'))

    assert payload == {
        'scope': 'whole_system_and_all_future_development',
        'operator_truth_surface': 'dagster_dagit',
        'manual_write_entrypoint': 'dagster_dagit_dashboard_launch',
        'automated_write_entrypoint': 'dagster_native_schedule_or_sensor_visible_in_dagit',
        'all_current_and_future_event_log_writes_must_be_dagster_native_and_dagit_visible': True,
        'all_current_and_future_canonical_backfill_and_reconcile_writes_in_scope': True,
        'dagster_green_requires_proof': True,
        'dagster_red_requires_nonterminal_or_absent_proof': True,
        'dagster_and_proof_state_must_match_per_partition': True,
        'split_brain_is_hard_failure': True,
        'complete_partition_must_not_show_failed_or_missing_after_normal_dashboard_action': True,
        'reconcile_required_must_be_explicit_before_launch': True,
        'runnable_partition_must_be_launchable_from_dagster_without_hidden_tags': True,
        'dagster_native_automation_must_remain_visible_in_dagit': True,
        'helper_paths_may_observe_only': True,
        'helper_paths_must_not_launch_or_write': True,
        'historical_helper_entrypoints_must_fail_closed_for_write_execution': True,
        'post_merge_validation_must_be_live_in_dagit': True,
        'overrides_older_runner_controller_wording': True,
    }


def test_repo_governance_surfaces_reference_dagster_authority_contract() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    work_plan_text = _WORK_PLAN.read_text(encoding='utf-8')
    dev_doc_text = _DEV_DOC.read_text(encoding='utf-8')
    runtime_doc_text = _RUNTIME_DOC.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')
    live_runtime_section = dev_index_text.split('## Governance reference docs', maxsplit=1)[0]

    assert 'contracts/governance/dagster-authority.json' in agents_text
    assert 'contracts/governance/dagster-authority.json' in top_level_plan_text
    assert '`S34-G6`' in work_plan_text
    assert '`S34-G8`' in work_plan_text
    assert '`S34-G10`' in work_plan_text
    assert '`S34-G12`' in work_plan_text
    assert '`S34-11c`' in work_plan_text
    assert '`S34-11d`' in work_plan_text
    assert '`S34-11f`' in work_plan_text
    assert '`S34-11h`' in work_plan_text
    assert 'Canonical machine contract: `contracts/governance/dagster-authority.json`' in dev_doc_text
    assert 'whole-system Origo governance' in dev_doc_text
    assert 'This document is reference-only for governance.' in dev_doc_text
    assert 'Dagster green with missing or non-terminal proof' in runtime_doc_text
    assert 'ClickHouse stores the live proof/progress/quarantine facts' in runtime_doc_text
    assert 'ClickHouse is the only live authority' not in runtime_doc_text
    assert 'helper utility outside Dagster/Dagit that launches runs or mutates canonical state is a contract violation' in runtime_doc_text
    assert 'contracts/governance/dagster-authority.json' in dev_index_text
    assert 'docs/Developer/dagster-authority-contract.md' in dev_index_text
    assert 's34-etf-backfill-runner.md' not in live_runtime_section
    assert 's34-exchange-sequence-controller.md' not in live_runtime_section
    assert 's34-bitcoin-backfill-runner.md' not in live_runtime_section
    assert 'pre_slice_35_allowed_write_entrypoint' not in _CONTRACT.read_text(encoding='utf-8')
    assert 'post_slice_35_allowed_write_entrypoint' not in _CONTRACT.read_text(encoding='utf-8')


def test_historical_runner_docs_are_explicitly_non_authoritative() -> None:
    for path in (
        _HISTORICAL_DAILY_TRANCHE_DOC,
        _HISTORICAL_EXCHANGE_SEQUENCE_DOC,
        _HISTORICAL_ETF_RUNNER_DOC,
        _HISTORICAL_BITCOIN_RUNNER_DOC,
    ):
        text = path.read_text(encoding='utf-8')
        assert 'Historical slice record only. Not a live runtime contract.' in text
        assert 'Records the historical repo-native' in text
        assert 'fails closed for write execution' in text
