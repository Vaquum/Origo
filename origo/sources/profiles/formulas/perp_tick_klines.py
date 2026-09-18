"""Ported from the spot formula; the SQL and arithmetic are unchanged.

Revisioned math keeps full precision; the retired futures pipeline's in-database rounding retired with it.
"""

from origo.assets.create_origo_database import ClickHouseClientProtocol

RAW_TABLE_NAME = 'binance_daily_futures_trades'
TICK_KLINES_TABLE_NAME = 'binance_futures_tick_klines'

TICK_KLINE_SIZE = 1_000


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{TICK_KLINES_TABLE_NAME}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            tick_bar_id,
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
                toUInt64(intDiv(running_trade_count_before, {TICK_KLINE_SIZE})) AS tick_bar_id
            FROM (
                SELECT
                    *,
                    row_number() OVER (ORDER BY datetime, trade_id) - 1 AS running_trade_count_before
                FROM {database}.{RAW_TABLE_NAME}
                WHERE toDate(datetime) = toDate('{partition_date}')
            )
        )
        GROUP BY tick_bar_id
        ORDER BY tick_bar_id
        """
    )
