from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from origo.audit.immutable_log import (
    ImmutableAuditLog,
    load_audit_log_retention_days,
)


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
    output: list[dict[str, Any]] = []
    for line in lines:
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise RuntimeError('Test expected JSON object line')
        normalized: dict[str, Any] = {}
        for key, value in cast(dict[Any, Any], raw).items():
            if not isinstance(key, str):
                raise RuntimeError('Test expected string keys')
            normalized[key] = value
        output.append(normalized)
    return output


def test_immutable_audit_log_appends_hash_chain_and_state(tmp_path: Path) -> None:
    log_path = tmp_path / 'audit' / 'events.jsonl'
    sink = ImmutableAuditLog(path=log_path, sink_name='test_sink', retention_days=365)

    hash_1 = sink.append_event(
        event_type='event_one',
        payload={'value': 1},
        attributes={'run_id': 'run-1', 'source_id': None},
    )
    hash_2 = sink.append_event(
        event_type='event_two',
        payload={'value': 2},
        attributes={'run_id': 'run-1', 'source_id': 'source-a'},
    )

    events = _read_jsonl(log_path)
    assert len(events) == 2
    assert events[0]['sequence'] == 1
    assert events[0]['event_hash'] == hash_1
    assert events[1]['sequence'] == 2
    assert events[1]['previous_event_hash'] == hash_1
    assert events[1]['event_hash'] == hash_2

    state_path = Path(f'{log_path}.state.json')
    state_payload = json.loads(state_path.read_text(encoding='utf-8'))
    assert state_payload['retention_days'] == 365
    assert state_payload['max_sequence'] == 2
    assert state_payload['max_event_hash'] == hash_2


def test_immutable_audit_log_rejects_retention_below_one_year(tmp_path: Path) -> None:
    log_path = tmp_path / 'audit' / 'events.jsonl'
    with pytest.raises(RuntimeError, match='retention_days must be >= 365'):
        ImmutableAuditLog(path=log_path, sink_name='test_sink', retention_days=364)


def test_immutable_audit_log_detects_state_backed_tamper(tmp_path: Path) -> None:
    log_path = tmp_path / 'audit' / 'events.jsonl'
    sink = ImmutableAuditLog(path=log_path, sink_name='test_sink', retention_days=365)
    sink.append_event(
        event_type='event_one',
        payload={'value': 1},
        attributes={'run_id': 'run-1'},
    )
    sink.append_event(
        event_type='event_two',
        payload={'value': 2},
        attributes={'run_id': 'run-1'},
    )

    events = _read_jsonl(log_path)
    tampered_event = dict(events[1])
    tampered_event['sequence'] = 1
    tampered_event['previous_event_hash'] = None
    tampered_event.pop('event_hash', None)
    tampered_event['event_hash'] = _json_hash(tampered_event)
    log_path.write_text(
        json.dumps(tampered_event, sort_keys=True, separators=(',', ':')) + '\n',
        encoding='utf-8',
    )

    sink_after_tamper = ImmutableAuditLog(
        path=log_path,
        sink_name='test_sink',
        retention_days=365,
    )
    with pytest.raises(RuntimeError, match='audit mutation detected'):
        sink_after_tamper.append_event(
            event_type='event_three',
            payload={'value': 3},
            attributes={'run_id': 'run-1'},
        )


def test_load_audit_retention_days_enforces_minimum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '364')
    with pytest.raises(
        RuntimeError,
        match='ORIGO_AUDIT_LOG_RETENTION_DAYS must be >= 365',
    ):
        load_audit_log_retention_days()
