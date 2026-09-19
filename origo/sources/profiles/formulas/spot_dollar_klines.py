"""Spot dollar-bar formula: table names over the generic bar math."""

from origo.assets.create_origo_database import ClickHouseClientProtocol

from .generic_bars import insert_dollar_partition_rows

DOLLAR_KLINES_TABLE_NAME = 'binance_spot_dollar_klines'
RAW_TABLE_NAME = 'binance_daily_spot_trades'

DOLLAR_KLINE_SIZE = 1_000_000.0


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    insert_dollar_partition_rows(
        client,
        database,
        partition_date,
        klines_table=DOLLAR_KLINES_TABLE_NAME,
        raw_table=RAW_TABLE_NAME,
        kline_size=DOLLAR_KLINE_SIZE,
    )
