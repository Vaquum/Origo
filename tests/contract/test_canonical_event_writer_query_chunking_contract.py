from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from origo.events.quarantine import StreamQuarantineRegistry
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter


class _RecordingClickHouseClient:
    def __init__(self) -> None:
        self.select_identity_batch_sizes: list[int] = []
        self.insert_calls = 0

    def execute(
        self, query: str, params: dict[str, Any] | list[tuple[object, ...]]
    ) -> list[tuple[object, ...]]:
        normalized = query.strip().upper()
        if normalized.startswith('SELECT'):
            if not isinstance(params, dict):
                raise RuntimeError('SELECT params must be dict')
            identity_keys = params.get('identity_keys')
            if not isinstance(identity_keys, list):
                raise RuntimeError('identity_keys must be list')
            self.select_identity_batch_sizes.append(len(identity_keys))
            return []
        if normalized.startswith('INSERT'):
            if not isinstance(params, list):
                raise RuntimeError('INSERT params must be list')
            self.insert_calls += 1
            return []
        raise RuntimeError('Unexpected query shape in test recording client')


def _event_input(*, offset: int) -> CanonicalEventWriteInput:
    trade_id_text = str(offset)
    payload_raw = (
        '{"is_best_match":true,"is_buyer_maker":false,'
        f'"price":"41000.12345678","qty":"0.01000000","quote_qty":"410.00123456","trade_id":"{trade_id_text}"'
        '}'
    ).encode()
    return CanonicalEventWriteInput(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2021-05-19',
        source_offset_or_equivalent=trade_id_text,
        source_event_time_utc=datetime(2021, 5, 19, 0, 0, 1, tzinfo=UTC),
        ingested_at_utc=datetime(2026, 3, 12, 0, 0, 1, tzinfo=UTC),
        payload_content_type='application/json',
        payload_encoding='utf-8',
        payload_raw=payload_raw,
        run_id='run-query-chunking',
    )


def test_canonical_event_writer_chunks_identity_lookup_query_size(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv(
        'ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH',
        str(tmp_path / 'canonical-runtime-audit.jsonl'),
    )
    # Ensure test-local singleton wiring after env mutation.
    import origo.events.runtime_audit as runtime_audit

    runtime_audit._runtime_audit_singleton = None

    client = _RecordingClickHouseClient()
    writer = CanonicalEventWriter(
        client=client,  # type: ignore[arg-type]
        database='origo',
        quarantine_registry=StreamQuarantineRegistry(
            path=tmp_path / 'stream-quarantine-state.json'
        ),
    )

    total_events = 3_105
    event_inputs = [_event_input(offset=i) for i in range(1, total_events + 1)]
    results = writer.write_events(event_inputs)

    assert len(results) == total_events
    assert all(result.status == 'inserted' for result in results)
    assert client.insert_calls == 1
    assert client.select_identity_batch_sizes != []
    assert max(client.select_identity_batch_sizes) <= 1_500
    assert sum(client.select_identity_batch_sizes) == total_events


def test_canonical_event_writer_summary_audit_mode_writes_batch_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    audit_log_path = tmp_path / 'canonical-runtime-audit-summary.jsonl'
    monkeypatch.setenv('ORIGO_AUDIT_LOG_RETENTION_DAYS', '365')
    monkeypatch.setenv('ORIGO_CANONICAL_RUNTIME_AUDIT_LOG_PATH', str(audit_log_path))
    monkeypatch.setenv('ORIGO_CANONICAL_RUNTIME_AUDIT_MODE', 'summary')
    # Ensure test-local singleton wiring after env mutation.
    import origo.events.runtime_audit as runtime_audit

    runtime_audit._runtime_audit_singleton = None

    client = _RecordingClickHouseClient()
    writer = CanonicalEventWriter(
        client=client,  # type: ignore[arg-type]
        database='origo',
        quarantine_registry=StreamQuarantineRegistry(
            path=tmp_path / 'stream-quarantine-state.json'
        ),
    )

    event_inputs = [_event_input(offset=i) for i in range(1, 11)]
    results = writer.write_events(event_inputs)

    assert len(results) == 10
    lines = [line for line in audit_log_path.read_text(encoding='utf-8').splitlines() if line != '']
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed['event_type'] == 'canonical_ingest_batch'
    assert parsed['payload']['batch_event_count'] == 10
    assert parsed['payload']['inserted_count'] == 10
    assert parsed['payload']['duplicate_count'] == 0
