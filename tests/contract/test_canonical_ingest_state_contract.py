from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from origo.events.ingest_state import (
    CanonicalIngestStateStore,
    CanonicalStreamKey,
    CompletenessCheckpointInput,
    CursorAdvanceInput,
    _compare_offsets,
    completeness_checkpoint_id,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATIONS_DIR = _REPO_ROOT / 'control-plane' / 'migrations' / 'sql'


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


def test_canonical_ingest_state_migration_files_exist() -> None:
    assert (_MIGRATIONS_DIR / '0019__create_canonical_ingest_cursor_state.sql').exists()
    assert (_MIGRATIONS_DIR / '0020__create_canonical_ingest_cursor_ledger.sql').exists()
    assert (
        _MIGRATIONS_DIR / '0021__create_canonical_source_completeness_checkpoints.sql'
    ).exists()


def test_canonical_ingest_cursor_state_sql_columns_match_contract() -> None:
    sql = (_MIGRATIONS_DIR / '0019__create_canonical_ingest_cursor_state.sql').read_text(
        encoding='utf-8'
    )
    assert _extract_create_table_columns(sql) == [
        'source_id',
        'stream_id',
        'partition_id',
        'cursor_revision',
        'last_source_offset_or_equivalent',
        'last_event_id',
        'last_source_event_time_utc',
        'last_ingested_at_utc',
        'offset_ordering',
        'updated_by_run_id',
        'updated_at_utc',
    ]


def test_canonical_ingest_cursor_ledger_sql_columns_match_contract() -> None:
    sql = (
        _MIGRATIONS_DIR / '0020__create_canonical_ingest_cursor_ledger.sql'
    ).read_text(encoding='utf-8')
    assert _extract_create_table_columns(sql) == [
        'source_id',
        'stream_id',
        'partition_id',
        'ledger_revision',
        'previous_source_offset_or_equivalent',
        'next_source_offset_or_equivalent',
        'event_id',
        'offset_ordering',
        'change_reason',
        'run_id',
        'recorded_at_utc',
    ]


def test_canonical_completeness_checkpoint_sql_columns_match_contract() -> None:
    sql = (
        _MIGRATIONS_DIR
        / '0021__create_canonical_source_completeness_checkpoints.sql'
    ).read_text(encoding='utf-8')
    assert _extract_create_table_columns(sql) == [
        'source_id',
        'stream_id',
        'partition_id',
        'checkpoint_revision',
        'checkpoint_id',
        'offset_ordering',
        'check_scope_start_offset',
        'check_scope_end_offset',
        'last_checked_source_offset_or_equivalent',
        'expected_event_count',
        'observed_event_count',
        'gap_count',
        'status',
        'gap_details_json',
        'checked_by_run_id',
        'checked_at_utc',
    ]


def test_completeness_checkpoint_id_is_deterministic() -> None:
    checkpoint_id_1 = completeness_checkpoint_id(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='btcusdt',
        last_checked_source_offset_or_equivalent='1700000000010',
        expected_event_count=100,
        observed_event_count=100,
        gap_count=0,
        status='ok',
        gap_details_json='{}',
    )
    checkpoint_id_2 = completeness_checkpoint_id(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='btcusdt',
        last_checked_source_offset_or_equivalent='1700000000010',
        expected_event_count=100,
        observed_event_count=100,
        gap_count=0,
        status='ok',
        gap_details_json='{}',
    )
    assert isinstance(checkpoint_id_1, UUID)
    assert checkpoint_id_1 == checkpoint_id_2


def test_compare_offsets_numeric_mode_enforces_monotonicity() -> None:
    assert _compare_offsets('1', '2', offset_ordering='numeric') == 1
    assert _compare_offsets('2', '2', offset_ordering='numeric') == 0
    assert _compare_offsets('3', '2', offset_ordering='numeric') == -1


def test_compare_offsets_numeric_mode_rejects_non_integer_offsets() -> None:
    with pytest.raises(RuntimeError, match='Numeric offset ordering requires integer'):
        _compare_offsets('abc', '2', offset_ordering='numeric')


def test_compare_offsets_lexicographic_mode() -> None:
    assert _compare_offsets('a10', 'a11', offset_ordering='lexicographic') == 1
    assert _compare_offsets('a11', 'a11', offset_ordering='lexicographic') == 0
    assert _compare_offsets('a12', 'a11', offset_ordering='lexicographic') == -1


def test_compare_offsets_opaque_mode() -> None:
    assert _compare_offsets('x', 'y', offset_ordering='opaque') == 1
    assert _compare_offsets('x', 'x', offset_ordering='opaque') == 0


def test_canonical_ingest_state_store_requires_non_empty_database() -> None:
    with pytest.raises(RuntimeError, match='database must be set and non-empty'):
        CanonicalIngestStateStore(client=object(), database='')  # type: ignore[arg-type]


def test_cursor_advance_input_contract_smoke() -> None:
    stream_key = CanonicalStreamKey(
        source_id='binance', stream_id='binance_spot_trades', partition_id='btcusdt'
    )
    cursor_input = CursorAdvanceInput(
        stream_key=stream_key,
        next_source_offset_or_equivalent='100',
        event_id=UUID('5a9d8d73-46bb-48d2-98e8-9832f5a15d11'),
        offset_ordering='numeric',
        run_id='test-run',
        change_reason='ingest_batch_append',
        ingested_at_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
        source_event_time_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        updated_at_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
    )
    checkpoint = CompletenessCheckpointInput(
        stream_key=stream_key,
        offset_ordering='numeric',
        check_scope_start_offset='1',
        check_scope_end_offset='100',
        last_checked_source_offset_or_equivalent='100',
        expected_event_count=100,
        observed_event_count=100,
        gap_count=0,
        status='ok',
        gap_details={},
        checked_by_run_id='test-run',
        checked_at_utc=datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC),
    )
    assert cursor_input.stream_key == stream_key
    assert checkpoint.stream_key == stream_key
