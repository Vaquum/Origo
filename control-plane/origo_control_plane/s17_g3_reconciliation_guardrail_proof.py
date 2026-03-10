from __future__ import annotations

import json
from pathlib import Path

from .s17_p5_no_miss_completeness_proof import run_s17_p5_proof

_SLICE_DIR = Path('spec/slices/slice-17-fred-event-sourcing-port')


def main() -> None:
    p5_payload = run_s17_p5_proof()
    payload = {
        'proof_scope': (
            'Slice 17 S17-G3 reconciliation/quarantine guardrail verification '
            'for FRED canonical ingest (fail-loud gap detection + quarantine block)'
        ),
        'stream_key': p5_payload['stream_key'],
        'missing_offsets': p5_payload['missing_offsets'],
        'checkpoint_status': p5_payload['checkpoint_status'],
        'duplicate_checkpoint_status': p5_payload['duplicate_checkpoint_status'],
        'post_quarantine_block_error_code': p5_payload[
            'post_quarantine_block_error_code'
        ],
        'guardrail_verified': p5_payload['no_miss_guardrail_verified'],
    }
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s17-g3-reconciliation-quarantine.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
