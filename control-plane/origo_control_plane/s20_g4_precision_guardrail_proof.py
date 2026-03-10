from __future__ import annotations

import json
from pathlib import Path

from origo_control_plane.s20_p6_raw_fidelity_precision_proof import run_s20_p6_proof

_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def main() -> None:
    p6_payload = run_s20_p6_proof()
    payload = {
        'proof_scope': (
            'Slice 20 S20-G4 raw-fidelity and precision guardrail verification '
            'for Bitcoin canonical ingest path'
        ),
        'write_summary': p6_payload['write_summary'],
        'invalid_precision_guardrail_errors': p6_payload[
            'invalid_precision_guardrail_errors'
        ],
        'raw_fidelity_and_precision_verified': p6_payload[
            'raw_fidelity_and_precision_verified'
        ],
    }
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-g4-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
