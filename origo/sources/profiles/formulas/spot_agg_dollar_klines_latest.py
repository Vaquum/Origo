"""Spot aggregate latest dollar-bar formula: table names over the generic bar math."""

from datetime import datetime

from origo.assets.create_origo_database import ClickHouseClientProtocol

from .generic_bars import insert_dollar_minute_rows
from .spot_agg_dollar_klines import DOLLAR_KLINE_SIZE

LATEST_DOLLAR_KLINES_TABLE_NAME = 'binance_spot_aggtrades_dollar_klines_latest'
LATEST_RAW_TABLE_NAME = 'binance_spot_aggtrades_latest'


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
        id_column='agg_trade_id',
        kline_size=DOLLAR_KLINE_SIZE,
        quote_expr='(price * quantity)',
    )
