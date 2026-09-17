from __future__ import annotations

import json
from datetime import datetime, timezone
from math import isclose
from typing import cast

from dagster import (
    DagsterInstance,
    materialize,
)

from .helpers import BINANCE_FIXTURE_ROOT, ORIGO_DATABASE

DEPTH200_FIRST_PARTITION_KEY = '2026-06-14T12:56:00+0000'
DEPTH200_EXPECTED_COLUMNS = [
    'datetime',
    'source_timestamp_ms',
    'book_mid_price',
    'book_spread_bps',
    'book_bid_depth_200_notional',
    'book_ask_depth_200_notional',
    'book_imbalance_200',
]
DEPTH200_FIXTURE_PATH = BINANCE_FIXTURE_ROOT / 'spot/depth200/history'
DEPTH200_TEST_LEVELS_SQL = '[' + ','.join(f'({level}.0,{level}.0)' for level in range(1, 201)) + ']'


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


def _fixture_records() -> list[dict[str, object]]:
    return [
        cast(dict[str, object], json.loads(line))
        for line in DEPTH200_FIXTURE_PATH.read_text().splitlines()
        if line.strip()
    ]


def _depth(record: dict[str, object]) -> dict[str, object]:
    return cast(dict[str, object], record['d'])


def _timestamp_ms(record: dict[str, object]) -> int:
    return int(record['t'])


def _levels(record: dict[str, object], side: str) -> list[list[str]]:
    return cast(list[list[str]], _depth(record)[side])


def _projection_values(record: dict[str, object]) -> tuple[float, float, float, float, float]:
    bids = [(float(price), float(quantity)) for price, quantity in _levels(record, 'bids')]
    asks = [(float(price), float(quantity)) for price, quantity in _levels(record, 'asks')]
    bid_depth = sum(price * quantity for price, quantity in bids)
    ask_depth = sum(price * quantity for price, quantity in asks)
    mid_price = (bids[0][0] + asks[0][0]) / 2
    spread_bps = ((asks[0][0] - bids[0][0]) / mid_price) * 10000
    imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
    return mid_price, spread_bps, bid_depth, ask_depth, imbalance


def test_binance_spot_depth200_snapshots_table_name_contract(
    origo_assets: dict[str, object],
) -> None:
    assert origo_assets['DEPTH200_SNAPSHOTS_TABLE_NAME'] == 'binance_spot_depth200_snapshots'


def test_binance_spot_depth200_1m_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['DEPTH200_1M_TABLE_NAME'] == 'binance_spot_depth200_1m'


def test_binance_spot_depth200_source_native_schema_matches_history_payload(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth200_snapshots_table_origo'],
        ]
    )
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DEPTH200_SNAPSHOTS_TABLE_NAME']}
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
    assert _table_metadata(query_origo, origo_assets['DEPTH200_SNAPSHOTS_TABLE_NAME']) == (
        'ReplacingMergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_depth200_sync_inserts_real_history_payload(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    records = _fixture_records()
    first_record = records[0]
    timestamps = [_timestamp_ms(record) for record in records]
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth200_snapshots_table_origo'],
            origo_assets['sync_binance_spot_depth200_snapshots_to_origo'],
        ],
        partition_key=DEPTH200_FIRST_PARTITION_KEY,
    )
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            count(),
            min(source_timestamp_ms),
            max(source_timestamp_ms),
            min(length(bids)),
            max(length(bids)),
            min(length(asks)),
            max(length(asks))
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH200_SNAPSHOTS_TABLE_NAME']} FINAL
        """
    )
    first_rows = query_origo(
        f"""
        SELECT source_timestamp_ms, last_update_id, length(bids), length(asks)
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH200_SNAPSHOTS_TABLE_NAME']} FINAL
        ORDER BY source_timestamp_ms
        LIMIT 1
        """
    )

    assert rows == [(len(records), min(timestamps), max(timestamps), 200, 200, 200, 200)]
    assert first_rows == [
        (
            _timestamp_ms(first_record),
            int(_depth(first_record)['lastUpdateId']),
            200,
            200,
        )
    ]


def test_binance_spot_depth200_1m_schema_contains_bookkeeping_and_five_scalar_book_columns(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth200_1m_table_origo'],
        ]
    )
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DEPTH200_1M_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == DEPTH200_EXPECTED_COLUMNS
    assert [type_name for _, type_name, *_ in rows] == ['DateTime', 'UInt64', *(['Float64'] * 5)]
    assert _table_metadata(query_origo, origo_assets['DEPTH200_1M_TABLE_NAME']) == (
        'ReplacingMergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_depth200_refresh_projects_latest_synced_snapshot(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    records = _fixture_records()
    latest_record = max(records, key=_timestamp_ms)
    expected_projection = _projection_values(latest_record)
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth200_snapshots_table_origo'],
            origo_assets['create_binance_spot_depth200_1m_table_origo'],
            origo_assets['sync_binance_spot_depth200_snapshots_to_origo'],
            origo_assets['refresh_binance_spot_depth200_1m_origo'],
        ],
        partition_key=DEPTH200_FIRST_PARTITION_KEY,
    )
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            toString(datetime),
            source_timestamp_ms,
            book_mid_price,
            book_spread_bps,
            book_bid_depth_200_notional,
            book_ask_depth_200_notional,
            book_imbalance_200
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH200_1M_TABLE_NAME']} FINAL
        """
    )

    assert len(rows) == 1
    row = rows[0]
    assert row[0] == '2026-06-14 12:56:00'
    assert row[1] == _timestamp_ms(latest_record)
    for actual, expected in zip(row[2:], expected_projection, strict=True):
        assert isclose(float(actual), expected, rel_tol=1e-12, abs_tol=1e-9)


def test_binance_spot_depth200_table_creation_jobs_are_registered(
    origo_definitions_module,
) -> None:
    snapshots_job_def = origo_definitions_module.defs.get_job_def(
        'create_binance_spot_depth200_snapshots_table_origo_job'
    )
    projection_job_def = origo_definitions_module.defs.get_job_def(
        'create_binance_spot_depth200_1m_table_origo_job'
    )

    assert set(snapshots_job_def.graph.node_dict.keys()) == {
        'create_binance_spot_depth200_snapshots_table_origo'
    }
    assert set(projection_job_def.graph.node_dict.keys()) == {
        'create_binance_spot_depth200_1m_table_origo'
    }


def test_binance_spot_depth200_data_source_job_is_registered_without_a_schedule(
    origo_definitions_module,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    job_def = origo_definitions_module.defs.get_job_def(
        'refresh_binance_spot_depth200_data_source_job'
    )

    # The depth worker (origo.workers.depth) owns the minute path; the job stays for operators.
    assert not repository_def.has_schedule_def('binance_spot_depth200_1m_schedule')
    assert not repository_def.has_schedule_def('binance_spot_depth200_projection_repair_schedule')
    assert not repository_def.has_schedule_def('binance_spot_depth200_arrow_repair_schedule')
    assert set(job_def.graph.node_dict.keys()) >= {
        'sync_binance_spot_depth200_snapshots_to_origo',
        'refresh_binance_spot_depth200_1m_origo',
    }
    assert job_def.partitions_def is not None
    assert job_def.partitions_def.get_first_partition_key() == DEPTH200_FIRST_PARTITION_KEY
def test_binance_spot_depth200_projection_repair_job_is_projection_only(
    origo_definitions_module,
) -> None:
    repair_job = origo_definitions_module.defs.get_job_def(
        'repair_binance_spot_depth200_projection_job'
    )

    assert set(repair_job.graph.node_dict.keys()) == {'refresh_binance_spot_depth200_1m_origo'}


def test_binance_spot_depth200_backfill_job_is_manual_data_source_only(
    origo_definitions_module,
) -> None:
    backfill_job = origo_definitions_module.defs.get_job_def(
        'backfill_binance_spot_depth200_data_source_job'
    )
    node_names = set(backfill_job.graph.node_dict.keys())

    assert node_names == {
        'sync_binance_spot_depth200_snapshots_to_origo',
        'refresh_binance_spot_depth200_1m_origo',
    }
    assert backfill_job.partitions_def is not None
    assert backfill_job.partitions_def.get_partition_keys(
        current_time=datetime(2026, 6, 14, 12, 59, tzinfo=timezone.utc)
    ) == [
        '2026-06-14T12:56:00+0000',
        '2026-06-14T12:57:00+0000',
        '2026-06-14T12:58:00+0000',
    ]


def test_binance_spot_depth200_reconcile_job_reports_existing_table_minutes(
    origo_definitions_module,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    reconcile_job = origo_definitions_module.defs.get_job_def(
        'reconcile_binance_spot_depth200_partition_state_origo_job'
    )
    node_names = set(reconcile_job.graph.node_dict.keys())
    instance = DagsterInstance.ephemeral()
    partition_key = '2026-06-14T12:56:00+0000'

    setup_result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth200_snapshots_table_origo'],
            origo_assets['create_binance_spot_depth200_1m_table_origo'],
        ],
        instance=instance,
    )
    assert setup_result.success

    query_origo(
        f"""
        INSERT INTO {ORIGO_DATABASE}.{origo_assets['DEPTH200_SNAPSHOTS_TABLE_NAME']}
        (
            datetime,
            source_timestamp_ms,
            last_update_id,
            bids,
            asks
        ) VALUES (
            toDateTime64('2026-06-14 12:56:00.000', 3),
            1,
            1,
            {DEPTH200_TEST_LEVELS_SQL},
            {DEPTH200_TEST_LEVELS_SQL}
        )
        """
    )
    query_origo(
        f"""
        INSERT INTO {ORIGO_DATABASE}.{origo_assets['DEPTH200_1M_TABLE_NAME']}
        (
            datetime,
            source_timestamp_ms,
            book_mid_price,
            book_spread_bps,
            book_bid_depth_200_notional,
            book_ask_depth_200_notional,
            book_imbalance_200
        ) VALUES (
            toDateTime('2026-06-14 12:56:00'),
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
        [origo_assets['reconcile_binance_spot_depth200_partition_state_origo']],
        instance=instance,
    )

    assert node_names == {'reconcile_binance_spot_depth200_partition_state_origo'}
    assert reconcile_job.partitions_def is None
    assert reconcile_result.success
    assert instance.get_materialized_partitions(
        origo_assets['sync_binance_spot_depth200_snapshots_to_origo'].key
    ) == {partition_key}
    assert instance.get_materialized_partitions(
        origo_assets['refresh_binance_spot_depth200_1m_origo'].key
    ) == {partition_key}
