"""Perp aggregate latest time-bar formula: table names over the generic bar math."""

from datetime import datetime

from origo.assets.create_origo_database import ClickHouseClientProtocol

from .generic_bars import insert_time_minute_rows

LATEST_KLINES_TABLE_NAME = 'binance_perp_aggtrades_klines_latest'
LATEST_RAW_TABLE_NAME = 'binance_perp_aggtrades_latest'


def _insert_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    insert_time_minute_rows(
        client,
        database,
        minute_start,
        klines_table=LATEST_KLINES_TABLE_NAME,
        raw_table=LATEST_RAW_TABLE_NAME,
        id_column='agg_trade_id',
    )
