from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from dagster import (
    DagsterInstance,
    materialize,
)

from origo.assets.create_binance_spot_depth20_snapshots_table_origo import (
    get_clickhouse_settings,
    make_clickhouse_client,
)
from origo.assets.refresh_binance_spot_depth20_1m_origo import (
    refresh_minute,
)

from .helpers import ORIGO_DATABASE

DEPTH20_FIRST_PARTITION_KEY = '2026-05-14T10:28:00+0000'
# The first two production snapshots of each minute, 100 ms apart; provenance.json binds them.
DEPTH20_FIXTURES = Path(__file__).parent / 'fixtures' / 'depth_arrow_retention' / 'depth20_snapshots'
Snapshot = tuple[datetime, int, int, list[tuple[float, float]], list[tuple[float, float]]]
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
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def _authentic_snapshots(minute: datetime) -> list[Snapshot]:
    frame = pl.read_ipc(DEPTH20_FIXTURES / f'{minute:%Y%m%dT%H%M%SZ}.arrow', memory_map=False)
    return [
        (
            datetime(1970, 1, 1) + timedelta(milliseconds=row['source_timestamp_ms']),
            row['source_timestamp_ms'],
            row['last_update_id'],
            [(level['price'], level['qty']) for level in row['bids']],
            [(level['price'], level['qty']) for level in row['asks']],
        )
        for row in frame.to_dicts()
    ]


def _retain(snapshots: list[Snapshot]) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.binance_spot_depth20_snapshots '
            '(datetime, source_timestamp_ms, last_update_id, bids, asks) VALUES',
            snapshots,
        )
    finally:
        client.disconnect()


def _refresh(minutes: list[datetime]) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        for minute in minutes:
            assert refresh_minute(client, ORIGO_DATABASE, minute) == 1
    finally:
        client.disconnect()


def _book_features(snapshot: Snapshot) -> tuple[float, float, float, float, float]:
    bids, asks = snapshot[3], snapshot[4]
    mid = (bids[0][0] + asks[0][0]) / 2
    bid_notional = sum(price * quantity for price, quantity in bids)
    ask_notional = sum(price * quantity for price, quantity in asks)
    return (
        mid,
        (asks[0][0] - bids[0][0]) / mid * 10000,
        bid_notional,
        ask_notional,
        (bid_notional - ask_notional) / (bid_notional + ask_notional),
    )


def _create_depth20_tables(origo_assets: dict[str, object], instance: DagsterInstance) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth20_snapshots_table_origo'],
            origo_assets['create_binance_spot_depth20_1m_table_origo'],
        ],
        instance=instance,
    )
    assert result.success


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










def test_binance_spot_depth20_minute_takes_its_latest_snapshot(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    minute = _utc(2026, 9, 15, 8, 0)
    earlier, latest = _authentic_snapshots(minute)
    assert earlier[1] < latest[1]
    assert _book_features(earlier) != _book_features(latest)
    _create_depth20_tables(origo_assets, DagsterInstance.ephemeral())
    _retain([earlier, latest])

    _refresh([minute])

    rows = query_origo(
        f"""
        SELECT source_timestamp_ms, book_mid_price, book_spread_bps,
               book_bid_depth_20_notional, book_ask_depth_20_notional, book_imbalance_20
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH20_1M_TABLE_NAME']} FINAL
        """
    )
    assert len(rows) == 1
    assert rows[0][0] == latest[1]
    assert rows[0][1:] == pytest.approx(_book_features(latest))
