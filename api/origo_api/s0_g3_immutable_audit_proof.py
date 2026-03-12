from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast


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


def _json_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    ).encode('utf-8')
    digest = hashlib.sha256()
    digest.update(canonical)
    return digest.hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    lines = [line for line in path.read_text(encoding='utf-8').splitlines() if line != '']
    parsed: list[dict[str, Any]] = []
    for line in lines:
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise RuntimeError('S0-G3 proof expected JSON object line in audit log')
        normalized: dict[str, Any] = {}
        for key, value in cast(dict[Any, Any], raw).items():
            if not isinstance(key, str):
                raise RuntimeError('S0-G3 proof expected string keys in audit log JSON')
            normalized[key] = value
        parsed.append(normalized)
    return parsed


def _read_state_retention_days(path: Path) -> int:
    raw = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise RuntimeError('S0-G3 proof expected JSON object in state file')
    payload = cast(dict[str, Any], raw)
    retention_days = payload.get('retention_days')
    if not isinstance(retention_days, int):
        raise RuntimeError('S0-G3 proof expected retention_days int in state file')
    return retention_days


def _load_scraper_audit_module() -> Any:
    module_path = Path(__file__).resolve().parents[2] / 'origo' / 'scraper' / 'audit.py'
    module_spec = importlib.util.spec_from_file_location(
        'origo_scraper_audit_proof_module',
        module_path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError('S0-G3 proof failed to build scraper audit module spec')
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def run_s0_g3_immutable_audit_proof() -> dict[str, Any]:
    with TemporaryDirectory(prefix='origo-s0-g3-proof-') as temp_dir:
        temp_root = Path(temp_dir)
        export_log_path = temp_root / 'audit' / 'export-events.jsonl'
        fred_log_path = temp_root / 'audit' / 'fred-alert-events.jsonl'
        scraper_log_path = temp_root / 'audit' / 'scraper-events.jsonl'
        export_hash_1: str | None = None
        export_hash_2: str | None = None
        fred_hash: str | None = None
        scraper_hash: str | None = None
        tamper_error: str | None = None
        invalid_retention_error: str | None = None

        env_updates = {
            'ORIGO_EXPORT_AUDIT_LOG_PATH': str(export_log_path),
            'ORIGO_FRED_ALERT_AUDIT_LOG_PATH': str(fred_log_path),
            'ORIGO_SCRAPER_AUDIT_LOG_PATH': str(scraper_log_path),
            'ORIGO_AUDIT_LOG_RETENTION_DAYS': '365',
        }
        original_env = _set_env(env_updates)
        try:
            export_module = importlib.import_module('api.origo_api.export_audit')
            fred_module = importlib.import_module('api.origo_api.fred_alert_audit')
            scraper_module = _load_scraper_audit_module()

            setattr(export_module, '_audit_log_singleton', None)
            setattr(fred_module, '_fred_alert_audit_singleton', None)
            setattr(scraper_module, '_audit_log_singleton', None)

            export_log = export_module.get_export_audit_log()
            fred_log = fred_module.get_fred_alert_audit_log()
            scraper_log = scraper_module.get_scraper_audit_log()

            export_hash_1 = export_log.append_event(
                event_type='export_submit_accepted',
                export_id='proof-export-1',
                payload={
                    'dataset': 'binance_spot_trades',
                    'format': 'parquet',
                },
            )
            export_hash_2 = export_log.append_event(
                event_type='export_status_polled',
                export_id='proof-export-1',
                payload={
                    'status': 'QUEUED',
                },
            )
            fred_hash = fred_log.append_event(
                event_type='fred_query_warning',
                payload={
                    'warning_codes': ['FRED_SOURCE_PUBLISH_STALE'],
                    'warning_count': 1,
                },
            )
            scraper_hash = scraper_log.append_event(
                event_type='scraper_run_started',
                run_id='proof-run-1',
                source_id=None,
                payload={'adapter_name': 'proof-adapter'},
            )

            export_events = _read_jsonl(export_log_path)
            if len(export_events) != 2:
                raise RuntimeError('S0-G3 proof expected 2 export audit events')
            fred_events = _read_jsonl(fred_log_path)
            if len(fred_events) != 1:
                raise RuntimeError('S0-G3 proof expected 1 FRED audit event')
            scraper_events = _read_jsonl(scraper_log_path)
            if len(scraper_events) != 1:
                raise RuntimeError('S0-G3 proof expected 1 scraper audit event')

            export_state_path = Path(f'{export_log_path}.state.json')
            fred_state_path = Path(f'{fred_log_path}.state.json')
            scraper_state_path = Path(f'{scraper_log_path}.state.json')
            for state_path in [export_state_path, fred_state_path, scraper_state_path]:
                if not state_path.exists():
                    raise RuntimeError(
                        f'S0-G3 proof expected state file to exist: {state_path}'
                    )

            export_retention_days = _read_state_retention_days(export_state_path)
            fred_retention_days = _read_state_retention_days(fred_state_path)
            scraper_retention_days = _read_state_retention_days(scraper_state_path)
            if export_retention_days != 365:
                raise RuntimeError('S0-G3 proof expected export retention_days=365')
            if fred_retention_days != 365:
                raise RuntimeError('S0-G3 proof expected FRED retention_days=365')
            if scraper_retention_days != 365:
                raise RuntimeError('S0-G3 proof expected scraper retention_days=365')

            second_event = dict(export_events[1])
            second_event['sequence'] = 1
            second_event['previous_event_hash'] = None
            second_event.pop('event_hash', None)
            second_event['event_hash'] = _json_hash(second_event)
            export_log_path.write_text(
                json.dumps(second_event, sort_keys=True, separators=(',', ':')) + '\n',
                encoding='utf-8',
            )

            setattr(export_module, '_audit_log_singleton', None)
            try:
                export_module.get_export_audit_log().append_event(
                    event_type='export_status_polled',
                    export_id='proof-export-1',
                    payload={'status': 'STARTED'},
                )
            except RuntimeError as exc:
                tamper_error = str(exc)
            if tamper_error is None:
                raise RuntimeError(
                    'S0-G3 proof expected fail-loud tamper detection on truncated log'
                )

            os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'] = '364'
            setattr(export_module, '_audit_log_singleton', None)
            try:
                export_module.get_export_audit_log()
            except RuntimeError as exc:
                invalid_retention_error = str(exc)
            if invalid_retention_error is None:
                raise RuntimeError(
                    'S0-G3 proof expected fail-loud error for retention_days < 365'
                )
        finally:
            _restore_env(original_env)

    if export_hash_1 is None or export_hash_2 is None:
        raise RuntimeError('S0-G3 proof expected export hashes to be populated')
    if fred_hash is None:
        raise RuntimeError('S0-G3 proof expected FRED hash to be populated')
    if scraper_hash is None:
        raise RuntimeError('S0-G3 proof expected scraper hash to be populated')

    return {
        'proof_scope': 'Slice 0 S0-G3 immutable audit-log sink with 1-year retention',
        'retention_days': 365,
        'export': {
            'event_count': 2,
            'state_retention_days': 365,
            'event_hashes': [export_hash_1, export_hash_2],
        },
        'fred_alerts': {
            'event_count': 1,
            'state_retention_days': 365,
            'event_hash': fred_hash,
        },
        'scraper': {
            'event_count': 1,
            'state_retention_days': 365,
            'event_hash': scraper_hash,
        },
        'tamper_detection_error': tamper_error,
        'invalid_retention_error': invalid_retention_error,
    }


def main() -> None:
    payload = run_s0_g3_immutable_audit_proof()
    output_path = Path(
        'spec/slices/slice-0-bootstrap/guardrails-proof-s0-g3-immutable-audit.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
