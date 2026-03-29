from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY_WORKFLOW = _REPO_ROOT / '.github' / 'workflows' / 'deploy-on-merge.yml'


def test_deploy_workflow_syncs_required_fred_revision_history_env() -> None:
    workflow_text = _DEPLOY_WORKFLOW.read_text(encoding='utf-8')

    required_fragments = (
        'read_root_env_example_value ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST',
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST must be present and non-empty in .env.example.',
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST in .env.example must be an integer in range 1..2000.',
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST in .env.example must be in range 1..2000.',
        'origo_fred_revision_history_initial_vintage_dates_per_request_b64',
        "ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST_B64='${origo_fred_revision_history_initial_vintage_dates_per_request_b64}'",
        'decode_env_b64_required ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST_B64 ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST',
        'require_env ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST',
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST must be an integer in range 1..2000.',
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST must be in range 1..2000.',
        '$0 !~ /^ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST=/ &&',
        'echo "ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST=${ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST}"',
        'echo "  ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST=${ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST}"',
    )

    for fragment in required_fragments:
        assert fragment in workflow_text
