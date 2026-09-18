"""Spot aggregate time-bar formula: table names over the generic bar math."""

from clickhouse_driver import Client as ClickhouseClient

from .generic_bars import insert_time_partition_rows

KLINES_TABLE_NAME = 'binance_spot_aggtrades_klines'
RAW_TABLE_NAME = 'binance_daily_spot_aggtrades'


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    insert_time_partition_rows(
        client,
        database,
        partition_date,
        klines_table=KLINES_TABLE_NAME,
        raw_table=RAW_TABLE_NAME,
        id_column='agg_trade_id',
    )
