from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from origo_control_plane.s14_c9_spot_trades_pilot_cutover import run_s14_c9_proof


def _require_dict(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be object')
    return cast(dict[str, Any], value)


def _require_list(value: Any, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be list')
    return cast(list[Any], value)


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def _require_str(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f'{label} must be str, got {type(value).__name__}')
    return value


def _summarize_and_assert_acceptance(run_payload: dict[str, Any]) -> dict[str, Any]:
    legacy_seed_rows = _require_int(
        run_payload.get('legacy_seed_rows'),
        label='legacy_seed_rows',
    )
    ingest_runs = _require_list(run_payload.get('ingest_runs'), label='ingest_runs')
    if len(ingest_runs) != 2:
        raise RuntimeError(
            f'S14-P6 proof expected two ingest runs, got {len(ingest_runs)}'
        )
    ingest_run_1 = _require_dict(ingest_runs[0], label='ingest_runs[0]')
    ingest_run_2 = _require_dict(ingest_runs[1], label='ingest_runs[1]')

    inserted_1 = _require_int(ingest_run_1.get('inserted'), label='ingest_run_1.inserted')
    duplicate_2 = _require_int(
        ingest_run_2.get('duplicate'),
        label='ingest_run_2.duplicate',
    )
    if inserted_1 != legacy_seed_rows:
        raise RuntimeError(
            'S14-P6 proof expected ingest run #1 to insert all pilot rows, '
            f'expected={legacy_seed_rows} got={inserted_1}'
        )
    if duplicate_2 != legacy_seed_rows:
        raise RuntimeError(
            'S14-P6 proof expected ingest run #2 replay to classify all rows as duplicate, '
            f'expected={legacy_seed_rows} got={duplicate_2}'
        )

    parity = _require_dict(run_payload.get('parity'), label='parity')
    native_parity = _require_dict(parity.get('native'), label='parity.native')
    aligned_parity = _require_dict(parity.get('aligned_1s'), label='parity.aligned_1s')

    native_row_count = _require_int(native_parity.get('row_count'), label='native.row_count')
    aligned_row_count = _require_int(
        aligned_parity.get('row_count'),
        label='aligned_1s.row_count',
    )
    if native_row_count <= 0:
        raise RuntimeError('S14-P6 proof expected native parity row_count > 0')
    if aligned_row_count <= 0:
        raise RuntimeError('S14-P6 proof expected aligned_1s parity row_count > 0')

    return {
        'legacy_seed_rows': legacy_seed_rows,
        'native_row_count': native_row_count,
        'aligned_1s_row_count': aligned_row_count,
        'native_baseline_hash_sha256': _require_str(
            native_parity.get('baseline_hash_sha256'),
            label='native.baseline_hash_sha256',
        ),
        'native_pilot_hash_sha256': _require_str(
            native_parity.get('pilot_hash_sha256'),
            label='native.pilot_hash_sha256',
        ),
        'aligned_1s_baseline_hash_sha256': _require_str(
            aligned_parity.get('baseline_hash_sha256'),
            label='aligned_1s.baseline_hash_sha256',
        ),
        'aligned_1s_pilot_hash_sha256': _require_str(
            aligned_parity.get('pilot_hash_sha256'),
            label='aligned_1s.pilot_hash_sha256',
        ),
    }


def run_s14_p6_proof() -> dict[str, Any]:
    run_1_full = run_s14_c9_proof()
    run_2_full = run_s14_c9_proof()

    run_1 = _summarize_and_assert_acceptance(run_1_full)
    run_2 = _summarize_and_assert_acceptance(run_2_full)

    if run_1 != run_2:
        raise RuntimeError(
            'S14-P6 proof failed: pilot cutover parity summary differs across repeated runs'
        )

    return {
        'proof_scope': (
            'Slice 14 S14-P6 pilot cutover acceptance and parity '
            '(native + aligned_1s) on fixed windows'
        ),
        'run_1': run_1,
        'run_2': run_2,
        'deterministic_match': True,
    }


def main() -> None:
    payload = run_s14_p6_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'proof-s14-p6-pilot-cutover-parity.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
