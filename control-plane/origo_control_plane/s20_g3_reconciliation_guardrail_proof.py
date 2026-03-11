from __future__ import annotations

import json
from pathlib import Path

from origo_control_plane.s20_p5_no_miss_completeness_proof import run_s20_p5_proof

_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def main() -> None:
    p5_payload = run_s20_p5_proof()
    payload = {
        'proof_scope': (
            'Slice 20 S20-G3 reconciliation/quarantine guardrail verification '
            'for Bitcoin canonical ingest (fail-loud gap detection and quarantine block)'
        ),
        'stream_results': p5_payload['stream_results'],
        'guardrail_verified': p5_payload['no_miss_guardrail_verified'],
    }
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s20-g3-reconciliation-quarantine.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
