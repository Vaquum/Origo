from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from origo.events.errors import ProjectorRuntimeError, ReconciliationError
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import (
    CanonicalProjectorRuntime,
    ProjectorCheckpointState,
    ProjectorEvent,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATIONS_DIR = _REPO_ROOT / 'control-plane' / 'migrations' / 'sql'


class _FakeClickHouseClient:
    def execute(self, _query: str, _params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
        return []


class _RaisingClickHouseClient:
    def execute(self, _query: str, _params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
        raise RuntimeError('EXECUTE_CALLED')


def _extract_create_table_columns(sql: str) -> list[str]:
    columns: list[str] = []
    in_columns = False
    for raw_line in sql.splitlines():
        line = raw_line.strip()
        if line == '':
            continue
        if line.startswith('CREATE TABLE'):
            in_columns = True
            continue
        if not in_columns:
            continue
        if line.startswith(')'):
            break
        columns.append(line.split()[0].rstrip(','))
    return columns


def test_canonical_projector_migration_files_exist() -> None:
    assert (_MIGRATIONS_DIR / '0022__create_canonical_projector_checkpoints.sql').exists()
    assert (_MIGRATIONS_DIR / '0023__create_canonical_projector_watermarks.sql').exists()


def test_canonical_projector_checkpoint_sql_columns_match_contract() -> None:
    sql = (_MIGRATIONS_DIR / '0022__create_canonical_projector_checkpoints.sql').read_text(
        encoding='utf-8'
    )
    assert _extract_create_table_columns(sql) == [
        'projector_id',
        'source_id',
        'stream_id',
        'partition_id',
        'checkpoint_revision',
        'last_event_id',
        'last_source_offset_or_equivalent',
        'last_source_event_time_utc',
        'last_ingested_at_utc',
        'run_id',
        'state_json',
        'checkpointed_at_utc',
    ]


def test_canonical_projector_watermark_sql_columns_match_contract() -> None:
    sql = (_MIGRATIONS_DIR / '0023__create_canonical_projector_watermarks.sql').read_text(
        encoding='utf-8'
    )
    assert _extract_create_table_columns(sql) == [
        'projector_id',
        'source_id',
        'stream_id',
        'partition_id',
        'watermark_revision',
        'watermark_event_id',
        'watermark_source_offset_or_equivalent',
        'watermark_source_event_time_utc',
        'watermark_ingested_at_utc',
        'run_id',
        'recorded_at_utc',
    ]


def test_projector_runtime_requires_positive_batch_size() -> None:
    with pytest.raises(ProjectorRuntimeError, match='batch_size must be > 0') as exc_info:
        CanonicalProjectorRuntime(
            client=_FakeClickHouseClient(),
            database='origo',
            projector_id='projector',
            stream_key=CanonicalStreamKey(
                source_id='binance',
                stream_id='binance_spot_trades',
                partition_id='btcusdt',
            ),
            batch_size=0,
        )
    assert exc_info.value.code == 'PROJECTOR_INVALID_BATCH_SIZE'


def test_projector_runtime_start_stop_cycle_with_empty_source() -> None:
    runtime = CanonicalProjectorRuntime(
        client=_FakeClickHouseClient(),
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        ),
        batch_size=10,
    )
    start_state = runtime.start()
    assert start_state.resumed is False
    assert runtime.fetch_next_batch() == []
    runtime.stop()


def test_projector_runtime_requires_terminal_partition_proof_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_missing_proof(self: object, *, stream_key: CanonicalStreamKey) -> None:
        raise ReconciliationError(
            code='BACKFILL_PARTITION_PROOF_MISSING',
            message=(
                'Projection requires terminal partition proof state '
                f'for {stream_key.source_id}/{stream_key.stream_id}/{stream_key.partition_id}'
            ),
        )

    monkeypatch.setattr(
        'origo.events.projector.CanonicalBackfillStateStore.'
        'assert_partition_terminally_proved_or_raise',
        _raise_missing_proof,
    )

    runtime = CanonicalProjectorRuntime(
        client=_RaisingClickHouseClient(),
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='2024-01-01',
        ),
        batch_size=10,
        require_terminal_partition_proof=True,
    )

    with pytest.raises(
        ProjectorRuntimeError,
        match='requires terminal partition proof state',
    ) as exc_info:
        runtime.start()
    assert exc_info.value.code == 'BACKFILL_PARTITION_PROOF_MISSING'


def test_projector_runtime_checks_terminal_partition_proof_before_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, CanonicalStreamKey] = {}

    def _record_proof_check(
        self: object,
        *,
        stream_key: CanonicalStreamKey,
    ) -> None:
        observed['stream_key'] = stream_key

    monkeypatch.setattr(
        'origo.events.projector.CanonicalBackfillStateStore.'
        'assert_partition_terminally_proved_or_raise',
        _record_proof_check,
    )

    runtime = CanonicalProjectorRuntime(
        client=_FakeClickHouseClient(),
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='2024-01-01',
        ),
        batch_size=10,
        require_terminal_partition_proof=True,
    )

    start_state = runtime.start()
    assert start_state.resumed is False
    assert observed['stream_key'] == CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2024-01-01',
    )
    runtime.stop()


def test_projector_runtime_rejects_invalid_fetch_order() -> None:
    with pytest.raises(ProjectorRuntimeError, match='fetch_order must be one of') as exc_info:
        CanonicalProjectorRuntime(
            client=_FakeClickHouseClient(),
            database='origo',
            projector_id='projector',
            stream_key=CanonicalStreamKey(
                source_id='binance',
                stream_id='binance_spot_trades',
                partition_id='btcusdt',
            ),
            batch_size=10,
            fetch_order='invalid',  # type: ignore[arg-type]
        )
    assert exc_info.value.code == 'PROJECTOR_INVALID_FETCH_ORDER'


def test_projector_runtime_stop_before_start_fails() -> None:
    runtime = CanonicalProjectorRuntime(
        client=_FakeClickHouseClient(),
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        ),
        batch_size=10,
    )
    with pytest.raises(ProjectorRuntimeError, match='Projector runtime is not started') as exc_info:
        runtime.stop()
    assert exc_info.value.code == 'PROJECTOR_NOT_STARTED'


def _checkpoint(
    *,
    revision: int,
    event_id: UUID,
    source_offset: str,
    ingested_at_utc: datetime,
) -> ProjectorCheckpointState:
    return ProjectorCheckpointState(
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        ),
        checkpoint_revision=revision,
        last_event_id=event_id,
        last_source_offset_or_equivalent=source_offset,
        last_source_event_time_utc=ingested_at_utc,
        last_ingested_at_utc=ingested_at_utc,
        run_id='run-previous',
        state_json='{}',
        checkpointed_at_utc=ingested_at_utc,
    )


def _event(
    *,
    event_id: UUID,
    source_offset: str,
    ingested_at_utc: datetime,
) -> ProjectorEvent:
    return ProjectorEvent(
        event_id=event_id,
        source_offset_or_equivalent=source_offset,
        source_event_time_utc=ingested_at_utc,
        ingested_at_utc=ingested_at_utc,
        payload_json='{}',
        payload_sha256_raw='sha',
    )


def test_commit_checkpoint_ingested_order_rejects_non_advancing_event_id() -> None:
    runtime = CanonicalProjectorRuntime(
        client=_FakeClickHouseClient(),
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        ),
        batch_size=10,
    )
    runtime.start()
    checkpoint_time = datetime(2026, 3, 12, 18, 0, 0, tzinfo=UTC)
    latest_checkpoint = _checkpoint(
        revision=3,
        event_id=UUID('00000000-0000-0000-0000-000000000010'),
        source_offset='100',
        ingested_at_utc=checkpoint_time,
    )
    runtime.fetch_latest_checkpoint = lambda: latest_checkpoint
    next_event = _event(
        event_id=UUID('00000000-0000-0000-0000-00000000000f'),
        source_offset='101',
        ingested_at_utc=checkpoint_time,
    )

    with pytest.raises(ProjectorRuntimeError, match='event order did not advance') as exc_info:
        runtime.commit_checkpoint(
            last_event=next_event,
            run_id='run-1',
            checkpointed_at_utc=checkpoint_time,
        )
    assert exc_info.value.code == 'PROJECTOR_EVENT_ORDER_NOT_ADVANCED'


def test_commit_checkpoint_source_offset_order_allows_equal_offset_with_event_tiebreak() -> None:
    runtime = CanonicalProjectorRuntime(
        client=_FakeClickHouseClient(),
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        ),
        batch_size=10,
        fetch_order='source_offset_numeric',
    )
    runtime._assert_numeric_source_offsets_or_raise = lambda: None
    runtime.start()
    runtime._client = _RaisingClickHouseClient()
    checkpoint_time = datetime(2026, 3, 12, 18, 0, 0, tzinfo=UTC)
    latest_checkpoint = _checkpoint(
        revision=7,
        event_id=UUID('00000000-0000-0000-0000-000000000010'),
        source_offset='500',
        ingested_at_utc=checkpoint_time,
    )
    runtime.fetch_latest_checkpoint = lambda: latest_checkpoint
    next_event = _event(
        event_id=UUID('00000000-0000-0000-0000-000000000011'),
        source_offset='500',
        ingested_at_utc=checkpoint_time,
    )

    with pytest.raises(RuntimeError, match='EXECUTE_CALLED'):
        runtime.commit_checkpoint(
            last_event=next_event,
            run_id='run-2',
            checkpointed_at_utc=checkpoint_time,
        )
