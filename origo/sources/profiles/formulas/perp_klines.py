"""Ported from the spot formula; the SQL and arithmetic are unchanged.

The revisioned math is intentionally unrounded (full precision in the database;
the file exports round at publication). The retired futures pipeline rounded
several measures inside the database; that rounding retired with it.
"""

from clickhouse_driver import Client as ClickhouseClient

KLINES_TABLE_NAME = 'binance_futures_klines'
RAW_TABLE_NAME = 'binance_daily_futures_trades'


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{KLINES_TABLE_NAME}
        SELECT
            kline_datetime AS datetime,
            argMin(price, trade_id) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, trade_id) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
            sumKahan(quantity) AS volume,
            avg(is_buyer_maker) AS maker_ratio,
            count() AS no_of_trades,
            argMin(price * quantity, trade_id) AS open_liquidity,
            max(price * quantity) AS high_liquidity,
            min(price * quantity) AS low_liquidity,
            argMax(price * quantity, trade_id) AS close_liquidity,
            sum(price * quantity) AS liquidity_sum,
            sumKahan(is_buyer_maker * quantity) AS maker_volume,
            sum(is_buyer_maker * price * quantity) AS maker_liquidity
        FROM (
            SELECT
                *,
                toDateTime(60 * intDiv(toUnixTimestamp(datetime), 60)) AS kline_datetime
            FROM {database}.{RAW_TABLE_NAME}
            WHERE toDate(datetime) = toDate('{partition_date}')
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )
