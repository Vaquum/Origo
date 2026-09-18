"""Ported from the spot formula; the SQL and arithmetic are unchanged.

Revisioned math keeps full precision; the retired futures pipeline's in-database rounding retired with it.
"""

from datetime import datetime, timedelta
from origo.assets.create_origo_database import ClickHouseClientProtocol
from .perp_dollar_klines import DOLLAR_KLINE_SIZE

LATEST_DOLLAR_KLINES_TABLE_NAME = 'binance_futures_dollar_klines_latest'
LATEST_RAW_TABLE_NAME = 'binance_futures_trades_latest'


def _minute_bounds(minute_start: datetime) -> tuple[str, str]:
    minute_end = minute_start + timedelta(minutes=1)
    return (
        minute_start.strftime('%Y-%m-%d %H:%M:%S'),
        minute_end.strftime('%Y-%m-%d %H:%M:%S'),
    )


def _insert_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        INSERT INTO {database}.{LATEST_DOLLAR_KLINES_TABLE_NAME}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            dollar_bar_id,
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
                toUInt64(floor(running_quote_before / {DOLLAR_KLINE_SIZE})) AS dollar_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum(quote_quantity) OVER (
                            ORDER BY datetime, trade_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - quote_quantity,
                        0.0
                    ) AS running_quote_before
                FROM {database}.{LATEST_RAW_TABLE_NAME}
                WHERE datetime >= toDateTime64('{start_datetime}', 3)
                  AND datetime < toDateTime64('{end_datetime}', 3)
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
    )
