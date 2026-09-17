"""Relocated verbatim from origo/assets/refresh_aligned_1m_exchange_from_binance_spot_origo.py; the SQL and arithmetic are unchanged."""

from clickhouse_driver import Client as ClickhouseClient

ALIGNED_TABLE_NAME = 'aligned_1m_exchange'
KLINES_TABLE_NAME = 'binance_spot_klines'

BINANCE_SPOT_DATASET_SOURCE = 'binance_spot'


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{ALIGNED_TABLE_NAME}
        SELECT
            '{BINANCE_SPOT_DATASET_SOURCE}' AS dataset_source,
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
