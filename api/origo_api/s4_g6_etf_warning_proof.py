from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .etf_warnings import ETFQualitySnapshot, build_warnings_from_snapshot

_EXPECTED_SOURCE_IDS: tuple[str, ...] = (
    'etf_ishares_ibit_daily',
    'etf_invesco_btco_daily',
    'etf_bitwise_bitb_daily',
    'etf_ark_arkb_daily',
    'etf_vaneck_hodl_daily',
    'etf_franklin_ezbc_daily',
    'etf_grayscale_gbtc_daily',
    'etf_fidelity_fbtc_daily',
    'etf_coinshares_brrr_daily',
    'etf_hashdex_defi_daily',
)


def _warning_codes(payload: list[Any]) -> list[str]:
    return sorted({warning.code for warning in payload if hasattr(warning, 'code')})


def run_s4_g6_etf_warning_proof() -> dict[str, Any]:
    today = date(2026, 3, 6)

    missing_warnings = build_warnings_from_snapshot(
        snapshot=ETFQualitySnapshot(
            latest_observed_day=None,
            present_source_ids=frozenset(),
            missing_required_metrics_by_source={},
        ),
        stale_max_age_days=1,
        today_utc=today,
    )
    missing_codes = _warning_codes(missing_warnings)

    degraded_warnings = build_warnings_from_snapshot(
        snapshot=ETFQualitySnapshot(
            latest_observed_day=date(2026, 3, 1),
            present_source_ids=frozenset({'etf_ishares_ibit_daily'}),
            missing_required_metrics_by_source={
                'etf_ishares_ibit_daily': ('btc_units', 'holdings_row_count')
            },
        ),
        stale_max_age_days=1,
        today_utc=today,
    )
    degraded_codes = _warning_codes(degraded_warnings)

    clean_warnings = build_warnings_from_snapshot(
        snapshot=ETFQualitySnapshot(
            latest_observed_day=today,
            present_source_ids=frozenset(_EXPECTED_SOURCE_IDS),
            missing_required_metrics_by_source={},
        ),
        stale_max_age_days=1,
        today_utc=today,
    )
    clean_codes = _warning_codes(clean_warnings)

    if missing_codes != ['ETF_DAILY_MISSING_RECORDS']:
        raise RuntimeError(
            'S4-G6 proof expected missing-only warning for empty ETF daily snapshot'
        )

    expected_degraded_codes = [
        'ETF_DAILY_INCOMPLETE_RECORDS',
        'ETF_DAILY_MISSING_RECORDS',
        'ETF_DAILY_STALE_RECORDS',
    ]
    if degraded_codes != expected_degraded_codes:
        raise RuntimeError(
            'S4-G6 proof expected stale+missing+incomplete warnings for degraded ETF snapshot'
        )

    if clean_codes != []:
        raise RuntimeError(
            'S4-G6 proof expected no ETF warnings for complete latest-day snapshot'
        )

    return {
        'proof_scope': 'Slice 4 S4-G6 ETF stale/missing/incomplete warning generation',
        'missing_only_warning_codes': missing_codes,
        'degraded_warning_codes': degraded_codes,
        'clean_warning_codes': clean_codes,
        'degraded_warning_count': len(degraded_warnings),
    }


def main() -> None:
    payload = run_s4_g6_etf_warning_proof()
    output_path = Path('spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g6.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
