from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from origo.events.precision import (
    assert_payload_json_has_no_float_values,
    canonical_source_precision_registry_contract,
    canonicalize_payload_json_with_precision,
)


def run_s14_g4_proof() -> dict[str, Any]:
    contract = canonical_source_precision_registry_contract()
    decimal_rules_missing_scale: list[dict[str, Any]] = []
    for entry in contract['entries']:
        for rule in entry['numeric_fields']:
            if rule['numeric_kind'] == 'decimal' and rule['scale'] is None:
                decimal_rules_missing_scale.append(
                    {
                        'source_id': entry['source_id'],
                        'stream_id': entry['stream_id'],
                        'field_path': rule['field_path'],
                    }
                )
    if decimal_rules_missing_scale != []:
        raise RuntimeError(
            'S14-G4 proof expected explicit scale for every decimal precision rule, '
            f'found missing scale entries={decimal_rules_missing_scale}'
        )

    payload_json = canonicalize_payload_json_with_precision(
        source_id='binance',
        stream_id='spot_trades',
        payload_raw=(
            b'{"qty":"0.01000000","price":"43500.12345678",'
            b'"quote_qty":"435.00123457","trade_id":"6001"}'
        ),
        payload_encoding='utf-8',
    )
    assert_payload_json_has_no_float_values(
        source_id='binance',
        stream_id='spot_trades',
        payload_json=payload_json,
    )
    parsed = json.loads(payload_json)
    if not isinstance(parsed, dict):
        raise RuntimeError('S14-G4 proof expected canonical payload to decode to object')
    parsed_dict = cast(dict[str, Any], parsed)
    if not isinstance(parsed_dict.get('trade_id'), int):
        raise RuntimeError('S14-G4 proof expected trade_id to be canonical int')
    for decimal_field in ('price', 'qty', 'quote_qty'):
        decimal_value = parsed_dict.get(decimal_field)
        if not isinstance(decimal_value, str):
            raise RuntimeError(
                f'S14-G4 proof expected {decimal_field} to be canonical decimal string'
            )
        fraction = decimal_value.split('.')[-1]
        if len(fraction) != 8:
            raise RuntimeError(
                f'S14-G4 proof expected {decimal_field} scale=8, got value={decimal_value}'
            )

    float_guard_error: str | None = None
    try:
        assert_payload_json_has_no_float_values(
            source_id='binance',
            stream_id='spot_trades',
            payload_json='{"trade_id":1,"price":43500.1}',
        )
    except RuntimeError as exc:
        float_guard_error = str(exc)
    if float_guard_error is None:
        raise RuntimeError('S14-G4 proof expected float guardrail to fail loudly')

    return {
        'proof_scope': (
            'Slice 14 S14-G4 canonical precision guardrails '
            '(no float storage, explicit decimal scale metadata)'
        ),
        'registry_version': contract['registry_version'],
        'registry_entry_count': len(contract['entries']),
        'decimal_rules_missing_scale': decimal_rules_missing_scale,
        'canonical_payload_json': payload_json,
        'float_guard_error': float_guard_error,
        'precision_guardrail_verified': True,
    }


def main() -> None:
    payload = run_s14_g4_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'guardrails-proof-s14-g4-precision.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
