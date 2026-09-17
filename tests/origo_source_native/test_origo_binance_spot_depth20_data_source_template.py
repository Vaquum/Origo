from __future__ import annotations

from datetime import datetime, timezone

import pytest
from dagster import (
    DagsterInstance,
    DefaultScheduleStatus,
    RunRequest,
    build_schedule_context,
    materialize,
)

from .helpers import ORIGO_DATABASE

DEPTH20_FIRST_PARTITION_KEY = '2026-05-14T10:28:00+0000'
DEPTH20_SCHEDULE_PARTITION_KEY = '2026-05-14T10:31:00+0000'
DEPTH20_EXPECTED_COLUMNS = [
    'datetime',
    'source_timestamp_ms',
    'book_mid_price',
    'book_spread_bps',
    'book_bid_depth_20_notional',
    'book_ask_depth_20_notional',
    'book_imbalance_20',
]
DEPTH20_TEST_LEVELS_SQL = '[' + ','.join(f'({level}.0,{level}.0)' for level in range(1, 21)) + ']'


class _FakeDepthSettings:
    database = ORIGO_DATABASE


class _FakeDepthClient:
    def disconnect(self) -> None:
        self.disconnected = True


def _utc(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def _patch_depth_clickhouse(origo_definitions_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        origo_definitions_module,
        'get_depth_clickhouse_settings',
        lambda: _FakeDepthSettings(),
    )
    monkeypatch.setattr(
        origo_definitions_module,
        'make_depth_clickhouse_client',
        lambda settings: _FakeDepthClient(),
    )


def _table_metadata(query_origo, table_name: str) -> tuple[str, str, str]:
    rows = query_origo(
        f"""
        SELECT engine, partition_key, sorting_key
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name = '{table_name}'
        """
    )

    assert len(rows) == 1
    engine, partition_key, sorting_key = rows[0]
    return str(engine), str(partition_key), str(sorting_key)


def test_binance_spot_depth20_snapshots_table_name_contract(
    origo_assets: dict[str, object],
) -> None:
    assert origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME'] == 'binance_spot_depth20_snapshots'


def test_binance_spot_depth20_1m_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['DEPTH20_1M_TABLE_NAME'] == 'binance_spot_depth20_1m'


def test_binance_spot_depth20_source_native_schema_matches_history_payload(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth20_snapshots_table_origo'],
        ]
    )
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == [
        'datetime',
        'source_timestamp_ms',
        'last_update_id',
        'bids',
        'asks',
    ]
    assert [type_name for _, type_name, *_ in rows] == [
        'DateTime64(3)',
        'UInt64',
        'UInt64',
        'Array(Tuple(Float64, Float64))',
        'Array(Tuple(Float64, Float64))',
    ]
    assert _table_metadata(query_origo, origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']) == (
        'ReplacingMergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_depth20_1m_schema_contains_bookkeeping_and_five_scalar_book_columns(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth20_1m_table_origo'],
        ]
    )
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DEPTH20_1M_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == DEPTH20_EXPECTED_COLUMNS
    assert [type_name for _, type_name, *_ in rows] == ['DateTime', 'UInt64', *(['Float64'] * 5)]
    assert _table_metadata(query_origo, origo_assets['DEPTH20_1M_TABLE_NAME']) == (
        'ReplacingMergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_depth20_table_creation_jobs_are_registered(
    origo_definitions_module,
) -> None:
    snapshots_job_def = origo_definitions_module.defs.get_job_def(
        'create_binance_spot_depth20_snapshots_table_origo_job'
    )
    projection_job_def = origo_definitions_module.defs.get_job_def(
        'create_binance_spot_depth20_1m_table_origo_job'
    )

    assert set(snapshots_job_def.graph.node_dict.keys()) == {
        'create_binance_spot_depth20_snapshots_table_origo'
    }
    assert set(projection_job_def.graph.node_dict.keys()) == {
        'create_binance_spot_depth20_1m_table_origo'
    }


def test_binance_spot_depth20_data_source_job_and_schedule_are_registered(
    origo_definitions_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    schedule_def = repository_def.get_schedule_def('binance_spot_depth20_1m_schedule')
    job_def = origo_definitions_module.defs.get_job_def(
        'refresh_binance_spot_depth20_data_source_job'
    )

    def fake_reconciliation_run_requests(context, spec) -> list[RunRequest]:
        return [
            RunRequest(
                partition_key=DEPTH20_SCHEDULE_PARTITION_KEY,
                run_key=f'binance_spot_depth20:source:{DEPTH20_SCHEDULE_PARTITION_KEY}:20260514T103200Z',
            )
        ]

    monkeypatch.setattr(
        origo_definitions_module,
        '_depth_reconciliation_run_requests',
        fake_reconciliation_run_requests,
    )
    context = build_schedule_context(
        scheduled_execution_time=datetime(2026, 5, 14, 10, 32, tzinfo=timezone.utc),
        repository_def=repository_def,
    )
    tick = schedule_def.evaluate_tick(context)

    assert schedule_def.cron_schedule == '* * * * *'
    assert schedule_def.execution_timezone == 'UTC'
    assert schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert set(job_def.graph.node_dict.keys()) >= {
        'sync_binance_spot_depth20_snapshots_to_origo',
        'refresh_binance_spot_depth20_1m_origo',
    }
    assert job_def.partitions_def is not None
    assert job_def.partitions_def.get_first_partition_key() == DEPTH20_FIRST_PARTITION_KEY
    assert len(tick.run_requests) == 1
    assert tick.run_requests[0].partition_key == DEPTH20_SCHEDULE_PARTITION_KEY
    assert (
        tick.run_requests[0].run_key
        == f'binance_spot_depth20:source:{DEPTH20_SCHEDULE_PARTITION_KEY}:20260514T103200Z'
    )
    assert tick.run_requests[0].run_config == {}


def test_binance_spot_depth20_schedule_requests_source_available_incomplete_lookback_minutes(
    origo_definitions_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    schedule_def = repository_def.get_schedule_def('binance_spot_depth20_1m_schedule')
    latest_minute = _utc(2026, 5, 14, 10, 34)
    status_by_minute = {
        _utc(2026, 5, 14, 10, 31): origo_definitions_module.DepthLiveStoreStatus(
            60, 1, True, latest_minute
        ),
        _utc(2026, 5, 14, 10, 32): origo_definitions_module.DepthLiveStoreStatus(
            0, 0, False, latest_minute
        ),
        _utc(2026, 5, 14, 10, 33): origo_definitions_module.DepthLiveStoreStatus(
            0, 0, False, latest_minute
        ),
        _utc(2026, 5, 14, 10, 34): origo_definitions_module.DepthLiveStoreStatus(
            0, 0, False, latest_minute
        ),
    }

    def fake_store_status(client, database, spec, minute_start):
        return status_by_minute[minute_start]

    def fake_source_has_rows(spec, minute_start: datetime) -> bool:
        return minute_start in {_utc(2026, 5, 14, 10, 33), _utc(2026, 5, 14, 10, 34)}

    _patch_depth_clickhouse(origo_definitions_module, monkeypatch)
    monkeypatch.setattr(origo_definitions_module, 'DEPTH_SOURCE_LOOKBACK_MINUTES', 4)
    monkeypatch.setattr(origo_definitions_module, '_depth_store_status', fake_store_status)
    monkeypatch.setattr(origo_definitions_module, '_depth_source_has_rows', fake_source_has_rows)

    tick = schedule_def.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=_utc(2026, 5, 14, 10, 35),
            repository_def=repository_def,
        )
    )

    assert [request.partition_key for request in tick.run_requests] == [
        '2026-05-14T10:33:00+0000',
        '2026-05-14T10:34:00+0000',
    ]
    assert [request.run_key for request in tick.run_requests] == [
        'binance_spot_depth20:source:2026-05-14T10:33:00+0000:20260514T103500Z',
        'binance_spot_depth20:source:2026-05-14T10:34:00+0000:20260514T103500Z',
    ]


def test_binance_spot_depth20_schedule_skips_completed_raw_projection_arrow_minutes(
    origo_definitions_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    schedule_def = repository_def.get_schedule_def('binance_spot_depth20_1m_schedule')
    latest_minute = _utc(2026, 5, 14, 10, 34)

    def fake_store_status(client, database, spec, minute_start):
        return origo_definitions_module.DepthLiveStoreStatus(60, 1, True, latest_minute)

    def fail_if_source_checked(spec, minute_start: datetime) -> bool:
        raise AssertionError('completed raw minutes must not query source history')

    _patch_depth_clickhouse(origo_definitions_module, monkeypatch)
    monkeypatch.setattr(origo_definitions_module, 'DEPTH_SOURCE_LOOKBACK_MINUTES', 4)
    monkeypatch.setattr(origo_definitions_module, '_depth_store_status', fake_store_status)
    monkeypatch.setattr(origo_definitions_module, '_depth_source_has_rows', fail_if_source_checked)

    tick = schedule_def.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=_utc(2026, 5, 14, 10, 35),
            repository_def=repository_def,
        )
    )

    assert tick.run_requests == []
    assert tick.skip_message == 'Depth20: no source-available raw depth gaps in lookback.'


def test_depth_source_has_rows_returns_false_on_request_exception(
    origo_definitions_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_get(*args: object, **kwargs: object) -> object:
        raise origo_definitions_module.requests.RequestException('source unavailable')

    monkeypatch.setenv('BINANCE_SPOT_DEPTH20_BASE_URL', 'https://source.example')
    monkeypatch.setenv('BINANCE_SPOT_DEPTH20_AUTH_TOKEN', 'test-token')
    monkeypatch.setattr(origo_definitions_module.requests, 'get', fail_get)

    assert (
        origo_definitions_module._depth_source_has_rows(
            origo_definitions_module.DEPTH20_LIVE_RECONCILIATION_SPEC,
            _utc(2026, 5, 14, 10, 34),
        )
        is False
    )


def test_binance_spot_depth20_projection_repair_schedule_requests_raw_available_gaps(
    origo_definitions_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    schedule_def = repository_def.get_schedule_def(
        'binance_spot_depth20_projection_repair_schedule'
    )
    latest_minute = _utc(2026, 5, 14, 10, 34)
    projection_gap = _utc(2026, 5, 14, 10, 33)

    def fake_store_status(client, database, spec, minute_start):
        projection_rows = 0 if minute_start == projection_gap else 1
        return origo_definitions_module.DepthLiveStoreStatus(
            60, projection_rows, True, latest_minute
        )

    _patch_depth_clickhouse(origo_definitions_module, monkeypatch)
    monkeypatch.setattr(origo_definitions_module, 'DEPTH_SOURCE_LOOKBACK_MINUTES', 4)
    monkeypatch.setattr(origo_definitions_module, '_depth_store_status', fake_store_status)

    tick = schedule_def.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=_utc(2026, 5, 14, 10, 35),
            repository_def=repository_def,
        )
    )

    assert [request.partition_key for request in tick.run_requests] == ['2026-05-14T10:33:00+0000']
    assert tick.run_requests[0].run_key == (
        'binance_spot_depth20:projection:2026-05-14T10:33:00+0000:20260514T103500Z'
    )


def test_binance_spot_depth20_projection_repair_job_is_projection_only(
    origo_definitions_module,
) -> None:
    repair_job = origo_definitions_module.defs.get_job_def(
        'repair_binance_spot_depth20_projection_job'
    )

    assert set(repair_job.graph.node_dict.keys()) == {'refresh_binance_spot_depth20_1m_origo'}


def test_binance_spot_depth20_arrow_repair_schedule_requests_raw_available_gaps(
    origo_definitions_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    schedule_def = repository_def.get_schedule_def('binance_spot_depth20_arrow_repair_schedule')
    latest_minute = _utc(2026, 5, 14, 10, 32)
    arrow_gap = _utc(2026, 5, 14, 10, 33)

    def fake_store_status(client, database, spec, minute_start):
        return origo_definitions_module.DepthLiveStoreStatus(
            60,
            1,
            minute_start != arrow_gap,
            latest_minute,
        )

    _patch_depth_clickhouse(origo_definitions_module, monkeypatch)
    monkeypatch.setattr(origo_definitions_module, 'DEPTH_SOURCE_LOOKBACK_MINUTES', 4)
    monkeypatch.setattr(origo_definitions_module, '_depth_store_status', fake_store_status)

    tick = schedule_def.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=_utc(2026, 5, 14, 10, 35),
            repository_def=repository_def,
        )
    )

    assert [request.partition_key for request in tick.run_requests] == [
        'depth20_snapshots',
        'depth20_snapshots',
    ]
    assert [request.run_key for request in tick.run_requests] == [
        'binance_spot_depth20:arrow:2026-05-14T10:33:00+0000:20260514T103500Z',
        'binance_spot_depth20:arrow:2026-05-14T10:34:00+0000:20260514T103500Z',
    ]
    assert [
        request.run_config['ops']['build_depth_snapshot_store_arrow']['config'][
            'source_partition_key'
        ]
        for request in tick.run_requests
    ] == ['2026-05-14T10:33:00+0000', '2026-05-14T10:34:00+0000']


def test_binance_spot_depth20_backfill_job_is_manual_data_source_only(
    origo_definitions_module,
) -> None:
    backfill_job = origo_definitions_module.defs.get_job_def(
        'backfill_binance_spot_depth20_data_source_job'
    )
    node_names = set(backfill_job.graph.node_dict.keys())

    assert node_names == {
        'sync_binance_spot_depth20_snapshots_to_origo',
        'refresh_binance_spot_depth20_1m_origo',
    }
    assert backfill_job.partitions_def is not None
    assert backfill_job.partitions_def.get_partition_keys(
        current_time=datetime(2026, 5, 14, 10, 31, tzinfo=timezone.utc)
    ) == [
        '2026-05-14T10:28:00+0000',
        '2026-05-14T10:29:00+0000',
        '2026-05-14T10:30:00+0000',
    ]


def test_binance_spot_depth20_reconcile_job_reports_existing_table_minutes(
    origo_definitions_module,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    reconcile_job = origo_definitions_module.defs.get_job_def(
        'reconcile_binance_spot_depth20_partition_state_origo_job'
    )
    node_names = set(reconcile_job.graph.node_dict.keys())
    instance = DagsterInstance.ephemeral()
    partition_key = '2026-05-14T10:28:00+0000'

    setup_result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth20_snapshots_table_origo'],
            origo_assets['create_binance_spot_depth20_1m_table_origo'],
        ],
        instance=instance,
    )
    assert setup_result.success

    query_origo(
        f"""
        INSERT INTO {ORIGO_DATABASE}.{origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']}
        (
            datetime,
            source_timestamp_ms,
            last_update_id,
            bids,
            asks
        ) VALUES (
            toDateTime64('2026-05-14 10:28:00.000', 3),
            1,
            1,
            {DEPTH20_TEST_LEVELS_SQL},
            {DEPTH20_TEST_LEVELS_SQL}
        )
        """
    )
    query_origo(
        f"""
        INSERT INTO {ORIGO_DATABASE}.{origo_assets['DEPTH20_1M_TABLE_NAME']}
        (
            datetime,
            source_timestamp_ms,
            book_mid_price,
            book_spread_bps,
            book_bid_depth_20_notional,
            book_ask_depth_20_notional,
            book_imbalance_20
        ) VALUES (
            toDateTime('2026-05-14 10:28:00'),
            1,
            1.0,
            1.0,
            1.0,
            1.0,
            0.0
        )
        """
    )

    reconcile_result = materialize(
        [origo_assets['reconcile_binance_spot_depth20_partition_state_origo']],
        instance=instance,
    )

    assert node_names == {'reconcile_binance_spot_depth20_partition_state_origo'}
    assert reconcile_job.partitions_def is None
    assert reconcile_result.success
    assert instance.get_materialized_partitions(
        origo_assets['sync_binance_spot_depth20_snapshots_to_origo'].key
    ) == {partition_key}
    assert instance.get_materialized_partitions(
        origo_assets['refresh_binance_spot_depth20_1m_origo'].key
    ) == {partition_key}


@pytest.fixture(autouse=True)
def _schedule_run_storage(monkeypatch):
    from functools import partial

    with DagsterInstance.local_temp() as instance:
        monkeypatch.setattr(__name__ + '.build_schedule_context', partial(build_schedule_context, instance=instance))
        yield
