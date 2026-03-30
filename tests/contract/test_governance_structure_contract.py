import json
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GOVERNANCE_DIR = _REPO_ROOT / 'contracts' / 'governance'
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'
_DEV_INDEX = _REPO_ROOT / 'docs' / 'Developer' / 'README.md'
_LEGACY_TASK_TYPE_CONTRACT = _GOVERNANCE_DIR / 'task-type-routing.json'

_EXPECTED_CONTRACT_FILES = {
    'contract-applicability.json',
    'dagster-authority.json',
    'documentation-contract.json',
    'event-sourcing.json',
    'execution-doctrine.json',
    'governance-authority.json',
    'historical-surface.json',
    'ingest-throughput.json',
    'ingestion-guarantees.json',
    'integrity-durability.json',
    'master-doctrine.json',
    'pr-review-routing.json',
    'raw-fidelity.json',
    'request-task-coverage.json',
    'rights-secrets.json',
    'runtime-recovery.json',
    'serving-error-safety.json',
    'slice-closeout.json',
    'source-migration.json',
    'source-onboarding.json',
    'static-analysis.json',
    'storage-sql-discipline.json',
    'task-admission.json',
    'task-decomposition.json',
    'task-end-gate.json',
    'task-start-gate.json',
    'task-types.json',
    'work-plan-discipline.json',
}


def _load_contract(name: str) -> dict[str, Any]:
    return json.loads((_GOVERNANCE_DIR / name).read_text(encoding='utf-8'))


def test_governance_contract_directory_is_atomic_and_complete() -> None:
    assert {path.name for path in _GOVERNANCE_DIR.glob('*.json')} == _EXPECTED_CONTRACT_FILES
    assert not _LEGACY_TASK_TYPE_CONTRACT.exists()


def test_governance_authority_contract_payload_is_exact() -> None:
    assert _load_contract('governance-authority.json') == {
        'scope': 'whole_system_and_all_future_development',
        'routing_surface': 'AGENTS.md',
        'canonical_governance_contract_directory': 'contracts/governance',
        'task_type_route_shape': ['task_type', 'routing', 'contract'],
        'universal_contracts_required_for_every_task': [
            'master-doctrine.json',
            'task-start-gate.json',
            'task-end-gate.json',
        ],
        'task_specific_routing_must_live_only_in_agents': True,
        'shared_domain_overlay_routing_must_live_only_in_agents': True,
        'governance_contracts_must_be_atomic': True,
        'spec_role_for_governance': 'descriptive_only',
        'developer_docs_role_for_governance': 'reference_only',
        'historical_slice_docs_role_for_governance': 'non_authoritative',
        'machine_contracts_override_prose': True,
        'task_start_must_begin_from_agents_routing': True,
        'task_end_must_begin_from_agents_routing': True,
        'completion_claim_without_end_gate_is_invalid': True,
        'open_slice_scope_must_not_limit_governance_scope': True,
        'every_touched_system_surface_must_have_routed_contract_coverage': True,
    }


def test_repo_governance_surfaces_reference_machine_contract_structure() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')
    dev_index_text = _DEV_INDEX.read_text(encoding='utf-8')
    applicability_contract = _load_contract('contract-applicability.json')
    shared_domain_contracts = applicability_contract.get('shared_domain_contracts', {})
    universal_contracts = applicability_contract.get('universal_contracts', [])
    task_admission_contracts = applicability_contract.get('task_admission_contracts', [])

    assert 'AGENTS.md is the only routing surface for governance.' in agents_text or '`AGENTS.md` is the only routing surface for governance.' in agents_text
    assert 'These governance routes and contracts apply repo-wide to the whole Origo system and all future development' in agents_text
    assert '## Master Doctrine' in agents_text
    assert '## Universal Start Gate' in agents_text
    assert '## Universal End Gate' in agents_text
    assert '## Shared-Domain Overlay Routing' in agents_text
    assert 'The governance routing in `AGENTS.md` and the machine contracts in `contracts/governance/` apply repo-wide to the whole Origo system and all future development.' in top_level_plan_text

    for contract_name in sorted(_EXPECTED_CONTRACT_FILES):
        contract_path = f'contracts/governance/{contract_name}'
        assert (
            contract_path in agents_text
            or contract_name in shared_domain_contracts
            or contract_name in universal_contracts
            or contract_name in task_admission_contracts
        )
        assert contract_path in dev_index_text

    for contract_path in (
        'contracts/governance/storage-sql-discipline.json',
        'contracts/governance/serving-error-safety.json',
        'contracts/governance/integrity-durability.json',
        'contracts/governance/rights-secrets.json',
        'contracts/governance/runtime-recovery.json',
        'contracts/governance/ingestion-guarantees.json',
        'contracts/governance/raw-fidelity.json',
        'contracts/governance/event-sourcing.json',
        'contracts/governance/historical-surface.json',
        'contracts/governance/source-onboarding.json',
        'contracts/governance/source-migration.json',
    ):
        assert contract_path in top_level_plan_text
