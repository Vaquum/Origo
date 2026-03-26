from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .backfill.s34_contract import (
    assert_s34_backfill_contract_consistency_or_raise,
    get_s34_dataset_contract,
    list_s34_dataset_contracts,
)
from .backfill.s34_orchestrator import (
    build_daily_partitions,
    evaluate_numeric_offset_gaps_or_raise,
    remaining_daily_partitions_or_raise,
)


def run_s34_c1_c2_backfill_contract_proof() -> dict[str, Any]:
    assert_s34_backfill_contract_consistency_or_raise()
    contracts = list_s34_dataset_contracts()
    contract_records = [
        {
            'dataset': contract.dataset,
            'source_id': contract.source_id,
            'stream_id': contract.stream_id,
            'execution_rank': contract.execution_rank,
            'phase': contract.phase,
            'partition_scheme': contract.partition_scheme,
            'earliest_partition_date': (
                None
                if contract.earliest_partition_date is None
                else contract.earliest_partition_date.isoformat()
            ),
            'earliest_height': contract.earliest_height,
            'offset_ordering': contract.offset_ordering,
            'aligned_capable': contract.aligned_capable,
        }
        for contract in contracts
    ]

    binance_contract = get_s34_dataset_contract('binance_spot_trades')
    if binance_contract.earliest_partition_date is None:
        raise RuntimeError('S34-C1/C2 proof expected binance earliest_partition_date')

    dry_plan = build_daily_partitions(
        start_date=binance_contract.earliest_partition_date,
        end_date=date(2017, 8, 19),
    )
    initial_remaining = remaining_daily_partitions_or_raise(
        contract=binance_contract,
        plan_end_date=date(2017, 8, 19),
        last_completed_partition=None,
    )
    resumed_remaining = remaining_daily_partitions_or_raise(
        contract=binance_contract,
        plan_end_date=date(2017, 8, 19),
        last_completed_partition='2017-08-17',
    )

    contiguous_gap_summary = evaluate_numeric_offset_gaps_or_raise(
        offsets=['100', '101', '102', '103'],
    )
    gap_detected_summary = evaluate_numeric_offset_gaps_or_raise(
        offsets=['100', '101', '103', '103', '105'],
    )
    if gap_detected_summary.gap_count <= 0:
        raise RuntimeError('S34-C1/C2 proof expected gap_count > 0 for injected gap')

    return {
        'proof_scope': (
            'Slice 34 S34-C1/S34-C2 backfill contract and proof-driven '
            'resume/orchestration controls'
        ),
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'dataset_contracts': contract_records,
        'binance_sample_plan': {
            'daily_partitions': list(dry_plan),
            'initial_remaining': list(initial_remaining),
            'resumed_remaining_after_2017-08-17': list(resumed_remaining),
            'resume_state_source': 'canonical_backfill_partition_proofs',
        },
        'gap_checks': {
            'contiguous': {
                'expected_event_count': contiguous_gap_summary.expected_event_count,
                'observed_event_count': contiguous_gap_summary.observed_event_count,
                'gap_count': contiguous_gap_summary.gap_count,
                'duplicate_offset_count': contiguous_gap_summary.duplicate_offset_count,
                'missing_offset_preview': list(
                    contiguous_gap_summary.missing_offset_preview
                ),
            },
            'gap_injected': {
                'expected_event_count': gap_detected_summary.expected_event_count,
                'observed_event_count': gap_detected_summary.observed_event_count,
                'gap_count': gap_detected_summary.gap_count,
                'duplicate_offset_count': gap_detected_summary.duplicate_offset_count,
                'missing_offset_preview': list(
                    gap_detected_summary.missing_offset_preview
                ),
            },
        },
    }


def main() -> None:
    result = run_s34_c1_c2_backfill_contract_proof()
    output_path = Path(
        'spec/slices/slice-34-full-canonical-backfill/'
        'capability-proof-s34-c1-c2-contract-orchestration.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
