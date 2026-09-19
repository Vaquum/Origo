"""Perp aggregate volume-bar formula: table names over the generic bar math."""

from origo.assets.create_origo_database import ClickHouseClientProtocol

from .generic_bars import insert_volume_partition_rows

RAW_TABLE_NAME = 'binance_daily_perp_aggtrades'
VOLUME_KLINES_TABLE_NAME = 'binance_perp_aggtrades_volume_klines'

VOLUME_KLINE_SIZE = 100.0


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    insert_volume_partition_rows(
        client,
        database,
        partition_date,
        klines_table=VOLUME_KLINES_TABLE_NAME,
        raw_table=RAW_TABLE_NAME,
        id_column='agg_trade_id',
        kline_size=VOLUME_KLINE_SIZE,
    )
