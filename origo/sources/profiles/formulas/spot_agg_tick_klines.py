"""Spot aggregate tick-bar formula: table names over the generic bar math.

Ticks count aggregate events, not underlying trades.
"""

from origo.assets.create_origo_database import ClickHouseClientProtocol

from .generic_bars import insert_tick_partition_rows

RAW_TABLE_NAME = 'binance_daily_spot_aggtrades'
TICK_KLINES_TABLE_NAME = 'binance_spot_aggtrades_tick_klines'

TICK_KLINE_SIZE = 1_000


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    insert_tick_partition_rows(
        client,
        database,
        partition_date,
        klines_table=TICK_KLINES_TABLE_NAME,
        raw_table=RAW_TABLE_NAME,
        id_column='agg_trade_id',
        kline_size=TICK_KLINE_SIZE,
    )
