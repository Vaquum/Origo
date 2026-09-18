"""Perp latest dollar-bar formula: table names over the generic bar math."""

from datetime import datetime

from origo.assets.create_origo_database import ClickHouseClientProtocol

from .generic_bars import insert_dollar_minute_rows
from .perp_dollar_klines import DOLLAR_KLINE_SIZE

LATEST_DOLLAR_KLINES_TABLE_NAME = 'binance_futures_dollar_klines_latest'
LATEST_RAW_TABLE_NAME = 'binance_futures_trades_latest'


def _insert_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    insert_dollar_minute_rows(
        client,
        database,
        minute_start,
        klines_table=LATEST_DOLLAR_KLINES_TABLE_NAME,
        raw_table=LATEST_RAW_TABLE_NAME,
        kline_size=DOLLAR_KLINE_SIZE,
    )
