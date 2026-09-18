"""Ported from the spot formula; the SQL and arithmetic are unchanged.

The revisioned math is intentionally unrounded (full precision in the database;
the file exports round at publication). The retired futures pipeline rounded
several measures inside the database; that rounding retired with it.
"""

from origo.assets.create_origo_database import ClickHouseClientProtocol

RAW_TABLE_NAME = 'binance_daily_futures_trades'
VOLUME_KLINES_TABLE_NAME = 'binance_futures_volume_klines'

VOLUME_KLINE_SIZE = 100.0


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{VOLUME_KLINES_TABLE_NAME}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            volume_bar_id,
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
                toUInt64(floor(running_volume_before / {VOLUME_KLINE_SIZE})) AS volume_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum(quantity) OVER (
                            ORDER BY datetime, trade_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - quantity,
                        0.0
                    ) AS running_volume_before
                FROM {database}.{RAW_TABLE_NAME}
                WHERE toDate(datetime) = toDate('{partition_date}')
            )
        )
        GROUP BY volume_bar_id
        ORDER BY volume_bar_id
        """
    )
