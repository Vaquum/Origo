"""Spot aligned-table formula: table names over the generic bar math."""

from clickhouse_driver import Client as ClickhouseClient

from .generic_bars import insert_aligned_partition_rows

ALIGNED_TABLE_NAME = 'aligned_1m_exchange'
KLINES_TABLE_NAME = 'binance_spot_klines'

BINANCE_SPOT_DATASET_SOURCE = 'binance_spot'


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    insert_aligned_partition_rows(
        client,
        database,
        partition_date,
        aligned_table=ALIGNED_TABLE_NAME,
        klines_table=KLINES_TABLE_NAME,
        dataset_source=BINANCE_SPOT_DATASET_SOURCE,
    )
