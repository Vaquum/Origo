from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from origo.events.ingest_state import CanonicalStreamKey


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    lines = [line for line in path.read_text(encoding='utf-8').splitlines() if line != '']
    output: list[dict[str, Any]] = []
    for line in lines:
        parsed = json.loads(line)
        if not isinstance(parsed, dict):
            raise RuntimeError('Expected runtime audit line to be JSON object')
        output.append(cast(dict[str, Any], parsed))
    return output


def test_canonical_runtime_audit_log_writes_ingest_and_projector_events(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    audit_log_path = tmp_path / 'audit' / 'canonical-runtime-events.jsonl'
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv('ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH', str(audit_log_path))

    module = importlib.import_module('origo.events.runtime_audit')
    setattr(module, '_runtime_audit_singleton', None)

    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='btcusdt',
    )
    audit_log = module.get_canonical_runtime_audit_log()
    scoped_audit_log_path = audit_log.resolve_stream_log_path(stream_key=stream_key)
    audit_log.append_ingest_events(
        stream_key=stream_key,
        events=[
            module.CanonicalRuntimeIngestEvent(
                event_type='canonical_ingest_inserted',
                source_offset_or_equivalent='1001',
                event_id='00000000-0000-0000-0000-000000000001',
                payload_sha256_raw='a' * 64,
                status='inserted',
                run_id='run-1',
            )
        ],
    )
    audit_log.append_projector_checkpoint_event(
        event_type='projector_checkpointed',
        projector_id='projector-v1',
        stream_key=stream_key,
        run_id='run-1',
        checkpoint_revision=1,
        last_event_id='00000000-0000-0000-0000-000000000001',
        last_source_offset_or_equivalent='1001',
        status='checkpointed',
        state={'projected_rows': 1},
    )

    events = _read_jsonl(scoped_audit_log_path)
    assert len(events) == 2
    assert events[0]['event_type'] == 'canonical_ingest_inserted'
    assert events[1]['event_type'] == 'projector_checkpointed'


def test_canonical_runtime_audit_log_writes_batched_ingest_events(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    audit_log_path = tmp_path / 'audit' / 'canonical-runtime-batch.jsonl'
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv('ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH', str(audit_log_path))

    module = importlib.import_module('origo.events.runtime_audit')
    setattr(module, '_runtime_audit_singleton', None)

    audit_log = module.get_canonical_runtime_audit_log()
    binance_stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2020-01-01',
    )
    okx_stream_key = CanonicalStreamKey(
        source_id='okx',
        stream_id='okx_spot_trades',
        partition_id='2020-01-01',
    )
    audit_log.append_ingest_events(
        events=[
            {
                'event_type': 'canonical_ingest_inserted',
                'source_id': 'binance',
                'stream_id': 'binance_spot_trades',
                'partition_id': '2020-01-01',
                'source_offset_or_equivalent': '1',
                'event_id': '00000000-0000-0000-0000-000000000001',
                'payload_sha256_raw': 'a' * 64,
                'status': 'inserted',
                'run_id': 'run-1',
            },
            {
                'event_type': 'canonical_ingest_duplicate',
                'source_id': 'okx',
                'stream_id': 'okx_spot_trades',
                'partition_id': '2020-01-01',
                'source_offset_or_equivalent': '2',
                'event_id': '00000000-0000-0000-0000-000000000002',
                'payload_sha256_raw': 'b' * 64,
                'status': 'duplicate',
                'run_id': 'run-1',
            },
        ]
    )

    binance_events = _read_jsonl(audit_log.resolve_stream_log_path(stream_key=binance_stream_key))
    okx_events = _read_jsonl(audit_log.resolve_stream_log_path(stream_key=okx_stream_key))
    assert len(binance_events) == 1
    assert len(okx_events) == 1
    assert binance_events[0]['sequence'] == 1
    assert okx_events[0]['sequence'] == 1
    assert binance_events[0]['event_type'] == 'canonical_ingest_inserted'
    assert okx_events[0]['event_type'] == 'canonical_ingest_duplicate'


def test_canonical_runtime_audit_log_writes_ingest_batch_summary_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    audit_log_path = tmp_path / 'audit' / 'canonical-runtime-summary.jsonl'
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv('ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH', str(audit_log_path))

    module = importlib.import_module('origo.events.runtime_audit')
    setattr(module, '_runtime_audit_singleton', None)

    stream_key = CanonicalStreamKey(
        source_id='okx',
        stream_id='okx_spot_trades',
        partition_id='2021-05-19',
    )
    audit_log = module.get_canonical_runtime_audit_log()
    scoped_audit_log_path = audit_log.resolve_stream_log_path(stream_key=stream_key)
    audit_log.append_ingest_batch_event(
        stream_key=stream_key,
        event_type='canonical_ingest_batch',
        run_id='run-summary',
        batch_event_count=3,
        inserted_count=2,
        duplicate_count=1,
        first_source_offset_or_equivalent='10',
        last_source_offset_or_equivalent='12',
        first_event_id='00000000-0000-0000-0000-000000000010',
        last_event_id='00000000-0000-0000-0000-000000000012',
    )

    events = _read_jsonl(scoped_audit_log_path)
    assert len(events) == 1
    assert events[0]['event_type'] == 'canonical_ingest_batch'
    payload = events[0]['payload']
    assert payload['batch_event_count'] == 3
    assert payload['inserted_count'] == 2
    assert payload['duplicate_count'] == 1
