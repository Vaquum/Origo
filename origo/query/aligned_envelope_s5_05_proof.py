from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from api.origo_api.schemas import RawQueryResponse
from origo.data._internal.generic_endpoints import query_aligned_wide_rows_envelope
from origo.query.aligned_core import AlignedDataset


@dataclass(frozen=True)
class _AlignedEnvelopeCase:
    name: str
    dataset: AlignedDataset
    fields: tuple[str, ...]
    time_range: tuple[str, str]


_CASES: tuple[_AlignedEnvelopeCase, ...] = (
    _AlignedEnvelopeCase(
        name='binance_spot_aligned_envelope',
        dataset='binance_spot_trades',
        fields=('aligned_at_utc', 'open_price', 'close_price', 'trade_count'),
        time_range=('2017-08-17T12:00:00Z', '2017-08-17T13:00:00Z'),
    ),
    _AlignedEnvelopeCase(
        name='etf_forward_fill_aligned_envelope',
        dataset='etf_daily_metrics',
        fields=(
            'source_id',
            'metric_name',
            'valid_from_utc',
            'valid_to_utc_exclusive',
            'metric_value_string',
        ),
        time_range=('2026-03-05T12:00:00Z', '2026-03-07T12:00:00Z'),
    ),
)


def _stable_hash(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _json_ready_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized_row: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                normalized_row[key] = value.isoformat()
            else:
                normalized_row[key] = value
        normalized.append(normalized_row)
    return normalized


def _run_case(case: _AlignedEnvelopeCase) -> dict[str, Any]:
    envelope = query_aligned_wide_rows_envelope(
        dataset=case.dataset,
        select_cols=list(case.fields),
        time_range=case.time_range,
        auth_token=None,
    )
    envelope['warnings'] = []

    if envelope.get('mode') != 'aligned_1s':
        raise RuntimeError(
            f'S5-C5 proof expected mode=aligned_1s in envelope for case={case.name}'
        )
    if envelope.get('source') != case.dataset:
        raise RuntimeError(
            f'S5-C5 proof expected source={case.dataset} for case={case.name}'
        )

    row_count = envelope.get('row_count')
    if not isinstance(row_count, int) or row_count <= 0:
        raise RuntimeError(f'S5-C5 proof expected non-empty row_count for case={case.name}')

    schema_payload_obj = envelope.get('schema')
    if not isinstance(schema_payload_obj, list):
        raise RuntimeError(f'S5-C5 proof expected non-empty schema for case={case.name}')
    schema_payload = cast(list[Any], schema_payload_obj)
    if len(schema_payload) == 0:
        raise RuntimeError(f'S5-C5 proof expected non-empty schema for case={case.name}')
    typed_schema_payload: list[dict[str, Any]] = []
    for item in schema_payload:
        if not isinstance(item, dict):
            raise RuntimeError(
                f'S5-C5 proof expected schema items to be objects for case={case.name}'
            )
        typed_schema_payload.append(cast(dict[str, Any], item))

    schema_names: list[str] = []
    for item in typed_schema_payload:
        name = item.get('name')
        if not isinstance(name, str) or name.strip() == '':
            raise RuntimeError(
                f'S5-C5 proof expected schema item name to be non-empty for case={case.name}'
            )
        schema_names.append(name)
    if schema_names != list(case.fields):
        raise RuntimeError(
            f'S5-C5 proof expected schema names={list(case.fields)}, got={schema_names}'
        )

    rows_payload_obj = envelope.get('rows')
    if not isinstance(rows_payload_obj, list):
        raise RuntimeError(f'S5-C5 proof expected rows list for case={case.name}')
    rows_payload = cast(list[Any], rows_payload_obj)
    typed_rows_payload: list[dict[str, Any]] = []
    for row in rows_payload:
        if not isinstance(row, dict):
            raise RuntimeError(
                f'S5-C5 proof expected each row to be an object for case={case.name}'
            )
        typed_rows_payload.append(cast(dict[str, Any], row))
    if len(typed_rows_payload) != row_count:
        raise RuntimeError(
            f'S5-C5 proof expected row_count to match rows length for case={case.name}'
        )

    response = RawQueryResponse.model_validate(envelope)
    if response.mode != 'aligned_1s':
        raise RuntimeError(
            f'S5-C5 proof expected API response mode=aligned_1s for case={case.name}'
        )

    rows = _json_ready_rows(response.rows)
    return {
        'case': case.name,
        'dataset': case.dataset,
        'window': {
            'time_range': [case.time_range[0], case.time_range[1]],
        },
        'row_count': response.row_count,
        'schema': [item.model_dump() for item in response.schema_items],
        'rows_hash_sha256': _stable_hash(rows),
        'first_row': rows[0],
        'last_row': rows[-1],
    }


def run_s5_05_proof() -> dict[str, Any]:
    case_summaries = [_run_case(case) for case in _CASES]
    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 S5-C5 aligned response envelope and schema metadata',
        'cases': case_summaries,
    }


def main() -> None:
    result = run_s5_05_proof()
    output_path = Path(
        'spec/slices/slice-5-raw-query-aligned-1s/capability-proof-s5-c5-aligned-envelope.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
