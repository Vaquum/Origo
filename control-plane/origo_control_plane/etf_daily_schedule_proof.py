from __future__ import annotations

import importlib
import json
import os
import sys
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
}
_SCHEDULE_ENV_CRON = 'ORIGO_ETF_DAILY_SCHEDULE_CRON'
_SCHEDULE_ENV_TIMEZONE = 'ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE'
_SCHEDULE_NAME = 'origo_etf_daily_ingest_schedule'
_JOB_NAME = 'origo_etf_daily_ingest_job'
_MODULE_NAME = 'origo_control_plane.definitions'


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


def run_s4_g3_schedule_proof() -> dict[str, Any]:
    env_original = _set_env(_BASE_ENV)
    schedule_env_original = _set_env({_SCHEDULE_ENV_TIMEZONE: 'UTC'})
    missing_cron_error: str | None = None
    cron_value = '17 6 * * *'
    timezone_value = 'UTC'
    schedule_def: Any = None
    job_def: Any = None
    try:
        os.environ.pop(_SCHEDULE_ENV_CRON, None)
        _clear_module_cache(_MODULE_NAME)
        try:
            _load_definitions_module()
        except RuntimeError as exc:
            missing_cron_error = str(exc)

        os.environ[_SCHEDULE_ENV_CRON] = cron_value
        os.environ[_SCHEDULE_ENV_TIMEZONE] = timezone_value
        _clear_module_cache(_MODULE_NAME)
        module = _load_definitions_module()
        defs = getattr(module, 'defs')

        schedule_def = defs.get_schedule_def(_SCHEDULE_NAME)
        job_def = defs.get_job_def(_JOB_NAME)
    finally:
        _restore_env(schedule_env_original)
        _restore_env(env_original)

    if missing_cron_error != f'{_SCHEDULE_ENV_CRON} must be set and non-empty':
        raise RuntimeError(
            'S4-G3 proof expected strict missing-env failure for '
            f'{_SCHEDULE_ENV_CRON}, got: {missing_cron_error}'
        )

    if schedule_def.job_name != _JOB_NAME:
        raise RuntimeError(
            f'S4-G3 proof expected schedule job_name={_JOB_NAME}, '
            f'got {schedule_def.job_name}'
        )
    if schedule_def.cron_schedule != cron_value:
        raise RuntimeError(
            f'S4-G3 proof expected cron={cron_value}, got {schedule_def.cron_schedule}'
        )
    if schedule_def.execution_timezone != timezone_value:
        raise RuntimeError(
            'S4-G3 proof expected timezone='
            f'{timezone_value}, got {schedule_def.execution_timezone}'
        )
    if job_def.name != _JOB_NAME:
        raise RuntimeError(
            f'S4-G3 proof expected job name={_JOB_NAME}, got {job_def.name}'
        )

    return {
        'proof_scope': 'Slice 4 S4-G3 ETF daily schedule wiring',
        'missing_cron_env_error': missing_cron_error,
        'schedule_name': schedule_def.name,
        'job_name': schedule_def.job_name,
        'cron_schedule': schedule_def.cron_schedule,
        'execution_timezone': schedule_def.execution_timezone,
    }


def main() -> None:
    payload = run_s4_g3_schedule_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root / 'spec/slices/slice-4-etf-use-case/guardrails-proof-s4-g3.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
