from __future__ import annotations

from pathlib import Path

import pytest

from origo.events.errors import ProjectorRuntimeError
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATIONS_DIR = _REPO_ROOT / 'control-plane' / 'migrations' / 'sql'


class _FakeClickHouseClient:
    def execute(self, _query: str, _params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
        return []


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
