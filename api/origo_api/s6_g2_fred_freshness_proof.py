from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from .fred_warnings import (
    FREDPublishFreshnessSnapshot,
    build_fred_publish_freshness_warnings,
    build_warnings_from_snapshot,
)


def run_s6_g2_fred_freshness_proof() -> dict[str, object]:
    synthetic_snapshot = FREDPublishFreshnessSnapshot(
        expected_source_ids=frozenset({'fred_fedfunds', 'fred_dgs10'}),
        latest_publish_by_source={
            'fred_fedfunds': datetime(2026, 3, 7, 0, 0, 0, tzinfo=UTC),
        },
    )
    synthetic_warnings = build_warnings_from_snapshot(
        snapshot=synthetic_snapshot,
        stale_max_age_days=2,
        today_utc=date(2026, 3, 7),
    )
    synthetic_codes = [warning.code for warning in synthetic_warnings]
    if 'FRED_SOURCE_PUBLISH_MISSING' not in synthetic_codes:
        raise RuntimeError('S6-G2 proof expected FRED_SOURCE_PUBLISH_MISSING warning')

    live_warnings = build_fred_publish_freshness_warnings(
        auth_token=None,
        stale_max_age_days=1,
    )
    live_codes = [warning.code for warning in live_warnings]
    if 'FRED_SOURCE_PUBLISH_STALE' not in live_codes:
        raise RuntimeError(
            'S6-G2 proof expected FRED_SOURCE_PUBLISH_STALE warning from live snapshot'
        )

    return {
        'proof_scope': 'Slice 6 S6-G2 FRED publish-timestamp freshness checks',
        'synthetic_warning_codes': synthetic_codes,
        'live_warning_codes': live_codes,
    }


def main() -> None:
    payload = run_s6_g2_fred_freshness_proof()
    output_path = Path(
        'spec/slices/slice-6-fred-integration/guardrails-proof-s6-g2-fred-freshness.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
