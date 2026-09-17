from __future__ import annotations

from datetime import datetime, timezone

from dagster import (
    DagsterInstance,
    materialize,
)

from .helpers import ORIGO_DATABASE

DEPTH20_FIRST_PARTITION_KEY = '2026-05-14T10:28:00+0000'
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


def _utc(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


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


def test_binance_spot_depth20_data_source_job_is_registered_without_a_schedule(
    origo_definitions_module,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    job_def = origo_definitions_module.defs.get_job_def(
        'refresh_binance_spot_depth20_data_source_job'
    )

    # The depth worker (origo.workers.depth) owns the minute path; the job stays for operators.
    assert not repository_def.has_schedule_def('binance_spot_depth20_1m_schedule')
    assert not repository_def.has_schedule_def('binance_spot_depth20_projection_repair_schedule')
    assert not repository_def.has_schedule_def('binance_spot_depth20_arrow_repair_schedule')
    assert set(job_def.graph.node_dict.keys()) >= {
        'sync_binance_spot_depth20_snapshots_to_origo',
        'refresh_binance_spot_depth20_1m_origo',
    }
    assert job_def.partitions_def is not None
    assert job_def.partitions_def.get_first_partition_key() == DEPTH20_FIRST_PARTITION_KEY
def test_binance_spot_depth20_projection_repair_job_is_projection_only(
    origo_definitions_module,
) -> None:
    repair_job = origo_definitions_module.defs.get_job_def(
        'repair_binance_spot_depth20_projection_job'
    )

    assert set(repair_job.graph.node_dict.keys()) == {'refresh_binance_spot_depth20_1m_origo'}


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
