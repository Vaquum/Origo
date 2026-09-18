"""Ported from the spot formula; the SQL and arithmetic are unchanged.

The revisioned math is intentionally unrounded (full precision in the database;
the file exports round at publication). The retired futures pipeline rounded
several measures inside the database; that rounding retired with it.
"""

from clickhouse_driver import Client as ClickhouseClient

ALIGNED_TABLE_NAME = 'aligned_1m_exchange'
KLINES_TABLE_NAME = 'binance_futures_klines'

BINANCE_PERP_DATASET_SOURCE = 'binance_perp'


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{ALIGNED_TABLE_NAME}
        SELECT
            '{BINANCE_PERP_DATASET_SOURCE}' AS dataset_source,
            datetime,
            open,
            high,
            low,
            close,
            mean,
            std,
            median,
            iqr,
            volume,
            maker_ratio,
            no_of_trades,
            open_liquidity,
            high_liquidity,
            low_liquidity,
            close_liquidity,
            liquidity_sum,
            maker_volume,
            maker_liquidity
        FROM {database}.{KLINES_TABLE_NAME}
        WHERE toDate(datetime) = toDate('{partition_date}')
        ORDER BY datetime
        """
    )
