from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from origo.events.aligned_projector import (
    AlignedProjectionPolicyState,
    CanonicalAligned1sProjector,
    CanonicalAlignedPolicyStore,
    aligned_projection_policy_id,
)
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import ProjectorEvent

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATIONS_DIR = _REPO_ROOT / 'control-plane' / 'migrations' / 'sql'


class _FakeClickHouseClient:
    def execute(
        self, _query: str, _params: dict[str, object] | None = None
    ) -> list[tuple[object, ...]]:
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


def test_canonical_aligned_projector_migration_files_exist() -> None:
    assert (_MIGRATIONS_DIR / '0024__create_canonical_aligned_1s_aggregates.sql').exists()
    assert (
        _MIGRATIONS_DIR / '0025__create_canonical_aligned_projection_policies.sql'
    ).exists()


def test_canonical_aligned_aggregate_sql_columns_match_contract() -> None:
    sql = (_MIGRATIONS_DIR / '0024__create_canonical_aligned_1s_aggregates.sql').read_text(
        encoding='utf-8'
    )
    assert _extract_create_table_columns(sql) == [
        'view_id',
        'view_version',
        'source_id',
        'stream_id',
        'partition_id',
        'aligned_at_utc',
        'bucket_event_count',
        'first_event_id',
        'last_event_id',
        'first_source_offset_or_equivalent',
        'last_source_offset_or_equivalent',
        'latest_source_event_time_utc',
        'latest_ingested_at_utc',
        'payload_rows_json',
        'bucket_sha256',
        'projector_id',
        'projected_at_utc',
    ]


def test_canonical_aligned_policy_sql_columns_match_contract() -> None:
    sql = (
        _MIGRATIONS_DIR / '0025__create_canonical_aligned_projection_policies.sql'
    ).read_text(encoding='utf-8')
    assert _extract_create_table_columns(sql) == [
        'view_id',
        'view_version',
        'source_id',
        'stream_id',
        'partition_id',
        'policy_revision',
        'policy_id',
        'bucket_size_seconds',
        'tier_policy',
        'retention_hot_days',
        'retention_warm_days',
        'recorded_by_run_id',
        'recorded_at_utc',
    ]


def test_aligned_projection_policy_id_is_deterministic() -> None:
    policy_id_1 = aligned_projection_policy_id(
        view_id='aligned_1s_raw',
        view_version=1,
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
        bucket_size_seconds=1,
        tier_policy='hot_1s_warm_1m',
        retention_hot_days=7,
        retention_warm_days=90,
    )
    policy_id_2 = aligned_projection_policy_id(
        view_id='aligned_1s_raw',
        view_version=1,
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
        bucket_size_seconds=1,
        tier_policy='hot_1s_warm_1m',
        retention_hot_days=7,
        retention_warm_days=90,
    )
    assert policy_id_1 == policy_id_2


def test_project_batch_is_deterministic() -> None:
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
    )
    policy_state = AlignedProjectionPolicyState(
        view_id='aligned_1s_raw',
        view_version=1,
        stream_key=stream_key,
        policy_revision=1,
        policy_id=UUID('31d1fd96-cf8f-4f9f-bf8a-8863f96557e7'),
        bucket_size_seconds=1,
        tier_policy='hot_1s_warm_1m',
        retention_hot_days=7,
        retention_warm_days=90,
        recorded_by_run_id='test-run',
        recorded_at_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    events = [
        ProjectorEvent(
            event_id=UUID('de389bce-ecb4-4055-a137-f03ce13a63ef'),
            source_offset_or_equivalent='2',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            ingested_at_utc=datetime(2024, 1, 1, 0, 0, 1, 900000, tzinfo=UTC),
            payload_json='{"trade_id":"2"}',
            payload_sha256_raw='2' * 64,
        ),
        ProjectorEvent(
            event_id=UUID('ce695f0d-dd9c-4a37-9c40-b7cc72d9384d'),
            source_offset_or_equivalent='1',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            ingested_at_utc=datetime(2024, 1, 1, 0, 0, 1, 100000, tzinfo=UTC),
            payload_json='{"trade_id":"1"}',
            payload_sha256_raw='1' * 64,
        ),
        ProjectorEvent(
            event_id=UUID('89fb8709-fdb0-4ba2-b3ef-0f06f3382de5'),
            source_offset_or_equivalent='3',
            source_event_time_utc=datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
            ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2, 100000, tzinfo=UTC),
            payload_json='{"trade_id":"3"}',
            payload_sha256_raw='3' * 64,
        ),
    ]
    projector = CanonicalAligned1sProjector(
        client=_FakeClickHouseClient(),
        database='origo',
    )
    rows_1 = projector.project_batch(
        events=events,
        policy=policy_state,
        projector_id='projector-v1',
        projected_at_utc=datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC),
    )
    rows_2 = projector.project_batch(
        events=events,
        policy=policy_state,
        projector_id='projector-v1',
        projected_at_utc=datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC),
    )
    assert len(rows_1) == 2
    assert [row.bucket_event_count for row in rows_1] == [2, 1]
    assert [row.aligned_at_utc for row in rows_1] == [
        datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),
    ]
    assert [row.bucket_sha256 for row in rows_1] == [row.bucket_sha256 for row in rows_2]
    assert [row.payload_rows_json for row in rows_1] == [
        row.payload_rows_json for row in rows_2
    ]


def test_policy_store_requires_non_empty_database() -> None:
    with pytest.raises(RuntimeError, match='database must be set and non-empty'):
        CanonicalAlignedPolicyStore(
            client=_FakeClickHouseClient(),
            database='',
        )
