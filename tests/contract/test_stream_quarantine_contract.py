from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from origo.events.errors import StreamQuarantineError
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.quarantine import (
    StreamQuarantineRegistry,
    load_stream_quarantine_state_path,
)


def test_load_stream_quarantine_state_path_requires_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('ORIGO_STREAM_QUARANTINE_STATE_PATH', raising=False)
    with pytest.raises(StreamQuarantineError, match='ORIGO_STREAM_QUARANTINE_STATE_PATH'):
        load_stream_quarantine_state_path()


def test_stream_quarantine_registry_persists_and_blocks(tmp_path: Path) -> None:
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='btcusdt',
    )
    registry = StreamQuarantineRegistry(path=tmp_path / 'stream-quarantine-state.json')
    state = registry.quarantine(
        stream_key=stream_key,
        run_id='test-run',
        reason='gap_detected_from_completeness_reconciliation',
        gap_details={'missing_offsets': ['1003']},
        quarantined_at_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    assert state.stream_key == stream_key
    assert state.gap_details['missing_offsets'] == ['1003']
    with pytest.raises(StreamQuarantineError, match='Stream is quarantined'):
        registry.assert_not_quarantined(stream_key=stream_key)
