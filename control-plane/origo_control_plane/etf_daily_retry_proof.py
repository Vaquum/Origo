from __future__ import annotations

import importlib
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

_BASE_ENV: dict[str, str] = {
    'CLICKHOUSE_HOST': 'localhost',
    'CLICKHOUSE_PORT': '9000',
    'CLICKHOUSE_HTTP_PORT': '8123',
    'CLICKHOUSE_USER': 'default',
    'CLICKHOUSE_PASSWORD': 'origo',
    'CLICKHOUSE_DATABASE': 'origo',
    'ORIGO_ETF_DAILY_SCHEDULE_CRON': '0 2 * * *',
    'ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE': 'UTC',
}
_RETRY_MAX_ATTEMPTS_ENV = 'ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS'
_RETRY_INTERVAL_ENV = 'ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS'
_MODULE_NAME = 'origo_control_plane.definitions'
_SENSOR_NAME = 'origo_etf_daily_retry_sensor'
_RETRY_ATTEMPT_TAG = 'origo.etf.retry_attempt'
_RETRY_ORIGIN_TAG = 'origo.etf.retry_origin_run_id'
_RETRY_PARENT_TAG = 'origo.etf.retry_parent_run_id'
_RETRY_WINDOW_END_TAG = 'origo.etf.retry_window_end_utc'


def _set_env(updates: dict[str, str]) -> dict[str, str | None]:
    original: dict[str, str | None] = {}
    for key, value in updates.items():
        original[key] = os.environ.get(key)
        os.environ[key] = value
    return original


def _restore_env(original: dict[str, str | None]) -> None:
    for key, value in original.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _clear_module_cache(module_name: str) -> None:
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(f'{module_name}.'):
            del sys.modules[name]


def _load_definitions_module() -> ModuleType:
    if _MODULE_NAME in sys.modules:
        return importlib.reload(sys.modules[_MODULE_NAME])
    return importlib.import_module(_MODULE_NAME)


def run_s4_g4_retry_proof() -> dict[str, Any]:
    env_original = _set_env(_BASE_ENV)
    missing_retry_max_error: str | None = None
    sensor_min_interval_seconds: int | None = None
    within_window_run_key: str | None = None
    within_window_retry_attempt_tag: str | None = None
    within_window_origin_tag: str | None = None
    within_window_parent_tag: str | None = None
    outside_window_skip_message: str | None = None
    max_attempts_skip_message: str | None = None
    chained_retry_run_key: str | None = None

    try:
        os.environ[_RETRY_INTERVAL_ENV] = '60'
        os.environ.pop(_RETRY_MAX_ATTEMPTS_ENV, None)
        _clear_module_cache(_MODULE_NAME)
        try:
            _load_definitions_module()
        except RuntimeError as exc:
            missing_retry_max_error = str(exc)

        os.environ[_RETRY_MAX_ATTEMPTS_ENV] = '2'
        os.environ[_RETRY_INTERVAL_ENV] = '60'
        _clear_module_cache(_MODULE_NAME)
        module = _load_definitions_module()
        defs = getattr(module, 'defs')
        sensor_def = defs.get_sensor_def(_SENSOR_NAME)
        sensor_min_interval_seconds = int(sensor_def.minimum_interval_seconds)

        helper = getattr(module, '_build_etf_daily_retry_response')

        now_utc = datetime(2026, 3, 6, 12, 0, 0, tzinfo=UTC)

        within_window_result = helper(
            failed_run_id='run-1',
            failed_run_tags={},
            failed_run_config={},
            failure_created_at_utc=now_utc - timedelta(hours=1),
            now_utc=now_utc,
        )
        within_window_run_key = str(within_window_result.run_key)
        within_window_tags = dict(within_window_result.tags)
        within_window_retry_attempt_tag = within_window_tags.get(_RETRY_ATTEMPT_TAG)
        within_window_origin_tag = within_window_tags.get(_RETRY_ORIGIN_TAG)
        within_window_parent_tag = within_window_tags.get(_RETRY_PARENT_TAG)
        if _RETRY_WINDOW_END_TAG not in within_window_tags:
            raise RuntimeError('S4-G4 proof expected retry window end tag in RunRequest')

        outside_window_result = helper(
            failed_run_id='run-2',
            failed_run_tags={},
            failed_run_config={},
            failure_created_at_utc=now_utc - timedelta(hours=25),
            now_utc=now_utc,
        )
        outside_window_skip_message = str(outside_window_result.skip_message)

        max_attempts_result = helper(
            failed_run_id='run-3',
            failed_run_tags={_RETRY_ATTEMPT_TAG: '2'},
            failed_run_config={},
            failure_created_at_utc=now_utc - timedelta(hours=1),
            now_utc=now_utc,
        )
        max_attempts_skip_message = str(max_attempts_result.skip_message)

        chained_retry_result = helper(
            failed_run_id='run-4',
            failed_run_tags={
                _RETRY_ATTEMPT_TAG: '1',
                _RETRY_ORIGIN_TAG: 'origin-1',
            },
            failed_run_config={},
            failure_created_at_utc=now_utc - timedelta(hours=2),
            now_utc=now_utc,
        )
        chained_retry_run_key = str(chained_retry_result.run_key)
    finally:
        _restore_env(env_original)

    if missing_retry_max_error != f'{_RETRY_MAX_ATTEMPTS_ENV} must be set and non-empty':
        raise RuntimeError(
            'S4-G4 proof expected strict missing-env failure for '
            f'{_RETRY_MAX_ATTEMPTS_ENV}, got: {missing_retry_max_error}'
        )
    if sensor_min_interval_seconds != 60:
        raise RuntimeError(
            'S4-G4 proof expected sensor interval 60 seconds, '
            f'got {sensor_min_interval_seconds}'
        )
    if within_window_run_key != 'origo-etf-retry-run-1-1':
        raise RuntimeError(
            'S4-G4 proof expected within-window retry run key '
            f'origo-etf-retry-run-1-1, got {within_window_run_key}'
        )
    if within_window_retry_attempt_tag != '1':
        raise RuntimeError(
            'S4-G4 proof expected retry attempt tag 1, '
            f'got {within_window_retry_attempt_tag}'
        )
    if within_window_origin_tag != 'run-1':
        raise RuntimeError(
            'S4-G4 proof expected retry origin tag run-1, '
            f'got {within_window_origin_tag}'
        )
    if within_window_parent_tag != 'run-1':
        raise RuntimeError(
            'S4-G4 proof expected retry parent tag run-1, '
            f'got {within_window_parent_tag}'
        )
    if 'outside retry window' not in outside_window_skip_message:
        raise RuntimeError(
            'S4-G4 proof expected outside-window skip reason, '
            f'got {outside_window_skip_message}'
        )
    if 'max retry attempts reached' not in max_attempts_skip_message:
        raise RuntimeError(
            'S4-G4 proof expected max-attempts skip reason, '
            f'got {max_attempts_skip_message}'
        )
    if chained_retry_run_key != 'origo-etf-retry-origin-1-2':
        raise RuntimeError(
            'S4-G4 proof expected chained retry run key '
            f'origo-etf-retry-origin-1-2, got {chained_retry_run_key}'
        )

    return {
        'proof_scope': 'Slice 4 S4-G4 ETF retry window behavior',
        'missing_retry_max_env_error': missing_retry_max_error,
        'sensor_name': _SENSOR_NAME,
        'sensor_min_interval_seconds': sensor_min_interval_seconds,
        'within_window_retry_run_key': within_window_run_key,
        'within_window_retry_attempt_tag': within_window_retry_attempt_tag,
        'within_window_retry_origin_tag': within_window_origin_tag,
        'within_window_retry_parent_tag': within_window_parent_tag,
        'outside_window_skip_message': outside_window_skip_message,
        'max_attempts_skip_message': max_attempts_skip_message,
        'chained_retry_run_key': chained_retry_run_key,
    }


def main() -> None:
    payload = run_s4_g4_retry_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = repo_root / 'spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g4.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
