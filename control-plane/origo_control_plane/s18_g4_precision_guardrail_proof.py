from __future__ import annotations

import json
from pathlib import Path

from .s18_p6_raw_fidelity_precision_proof import run_s18_p6_proof

_SLICE_DIR = Path('spec/slices/slice-18-okx-event-sourcing-port')


def main() -> None:
    p6_payload = run_s18_p6_proof()
    payload = {
        'proof_scope': (
            'Slice 18 S18-G4 raw-fidelity and precision guardrail verification '
            'for OKX canonical ingest path'
        ),
        'write_summary': p6_payload['write_summary'],
        'invalid_precision_guardrail_error': p6_payload[
            'invalid_precision_guardrail_error'
        ],
        'raw_fidelity_and_precision_verified': p6_payload[
            'raw_fidelity_and_precision_verified'
        ],
    }
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s18-g4-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
