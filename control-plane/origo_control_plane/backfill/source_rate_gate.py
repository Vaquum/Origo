from __future__ import annotations

import fcntl
import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import cast

_DAGSTER_HOME_ENV = 'DAGSTER_HOME'


def _load_gate_root_dir_or_raise() -> Path:
    dagster_home = os.environ.get(_DAGSTER_HOME_ENV)
    if dagster_home is None or dagster_home.strip() == '':
        raise RuntimeError(f'{_DAGSTER_HOME_ENV} must be set and non-empty')
    dagster_home_path = Path(dagster_home)
    if not dagster_home_path.is_absolute():
        raise RuntimeError(
            f'{_DAGSTER_HOME_ENV} must be an absolute path, got {dagster_home!r}'
        )
    gate_root = dagster_home_path / 'storage' / 'source_rate_gates'
    gate_root.mkdir(parents=True, exist_ok=True)
    return gate_root


def _load_last_granted_epoch_seconds_or_raise(
    *,
    state_path: Path,
    source_id: str,
) -> float | None:
    if not state_path.exists():
        return None
    try:
        state_payload = json.loads(state_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'Source rate gate state is invalid JSON: {state_path}') from exc
    if not isinstance(state_payload, dict):
        raise RuntimeError(f'Source rate gate state must be JSON object: {state_path}')
    typed_payload = cast(dict[str, object], state_payload)
    raw_source_id = typed_payload.get('source_id')
    if raw_source_id != source_id:
        raise RuntimeError(
            'Source rate gate state source_id mismatch: '
            f'expected={source_id!r} got={raw_source_id!r} path={state_path}'
        )
    raw_last_granted = typed_payload.get('last_granted_at_epoch_seconds')
    if not isinstance(raw_last_granted, (int, float)):
        raise RuntimeError(
            'Source rate gate state must contain numeric last_granted_at_epoch_seconds: '
            f'path={state_path}'
        )
    return float(raw_last_granted)


def _write_last_granted_epoch_seconds_or_raise(
    *,
    state_path: Path,
    source_id: str,
    min_interval_seconds: float,
    granted_at_epoch_seconds: float,
) -> None:
    payload = {
        'source_id': source_id,
        'min_interval_seconds': min_interval_seconds,
        'last_granted_at_epoch_seconds': granted_at_epoch_seconds,
    }
    temp_path = state_path.with_suffix('.tmp')
    temp_path.write_text(
        json.dumps(payload, sort_keys=True, indent=2),
        encoding='utf-8',
    )
    temp_path.replace(state_path)


def wait_for_source_rate_gate_or_raise(
    *,
    source_id: str,
    min_interval_seconds: float,
    log_fn: Callable[[str], object] | None = None,
) -> None:
    if source_id.strip() == '':
        raise RuntimeError('source_id must be non-empty')
    if min_interval_seconds <= 0:
        raise RuntimeError(
            f'min_interval_seconds must be > 0, got {min_interval_seconds}'
        )
    gate_root = _load_gate_root_dir_or_raise()
    lock_path = gate_root / f'{source_id}.lock'
    state_path = gate_root / f'{source_id}.json'
    with lock_path.open('a+', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        last_granted_at = _load_last_granted_epoch_seconds_or_raise(
            state_path=state_path,
            source_id=source_id,
        )
        now = time.time()
        wait_seconds = 0.0
        if last_granted_at is not None:
            elapsed = now - last_granted_at
            wait_seconds = max(0.0, min_interval_seconds - elapsed)
        if wait_seconds > 0:
            if log_fn is not None:
                log_fn(
                    'Waiting for source rate gate: '
                    f'source_id={source_id} wait_seconds={wait_seconds:.3f} '
                    f'min_interval_seconds={min_interval_seconds:.3f}'
                )
            time.sleep(wait_seconds)
            now = time.time()
        _write_last_granted_epoch_seconds_or_raise(
            state_path=state_path,
            source_id=source_id,
            min_interval_seconds=min_interval_seconds,
            granted_at_epoch_seconds=now,
        )
