from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from origo.events import (
    canonical_source_precision_registry_contract,
    canonicalize_payload_json_with_precision,
)


def _assert_no_float_values(value: Any) -> None:
    if isinstance(value, dict):
        for child_value in cast(dict[str, Any], value).values():
            _assert_no_float_values(child_value)
        return
    if isinstance(value, list):
        for item in cast(list[Any], value):
            _assert_no_float_values(item)
        return
    if isinstance(value, float):
        raise RuntimeError('S14-C8 proof found float value in canonical payload JSON')


def run_s14_c8_proof() -> dict[str, Any]:
    contract = canonical_source_precision_registry_contract()
    payload_json = canonicalize_payload_json_with_precision(
        source_id='binance',
        stream_id='spot_trades',
        payload_raw=(
            b'{"qty":"0.01000000","price":"41000.12345678",'
            b'"quote_qty":"410.00123456","trade_id":"1"}'
        ),
        payload_encoding='utf-8',
    )
    parsed_payload = json.loads(payload_json)
    _assert_no_float_values(parsed_payload)

    missing_rule_error: str | None = None
    try:
        canonicalize_payload_json_with_precision(
            source_id='binance',
            stream_id='spot_trades',
            payload_raw=b'{"trade_id":"1","unexpected_numeric":2}',
            payload_encoding='utf-8',
        )
    except RuntimeError as exc:
        missing_rule_error = str(exc)

    if missing_rule_error is None:
        raise RuntimeError(
            'S14-C8 proof expected missing precision rule failure for unexpected numeric field'
        )

    return {
        'proof_scope': 'Slice 14 S14-C8 source precision registry and no-float canonical typing',
        'registry_version': contract['registry_version'],
        'registry_entries': [
            {
                'source_id': entry['source_id'],
                'stream_id': entry['stream_id'],
                'numeric_fields': [
                    {
                        'field_path': field['field_path'],
                        'numeric_kind': field['numeric_kind'],
                        'scale': field['scale'],
                    }
                    for field in entry['numeric_fields']
                ],
            }
            for entry in contract['entries']
        ],
        'normalized_payload': parsed_payload,
        'missing_rule_error': missing_rule_error,
    }


def main() -> None:
    payload = run_s14_c8_proof()
    repo_root = Path.cwd().resolve()
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c8-precision-registry.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
