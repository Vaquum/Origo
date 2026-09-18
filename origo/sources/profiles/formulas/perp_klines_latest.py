"""Ported from the spot formula; the SQL and arithmetic are unchanged.

Revisioned math keeps full precision; the retired futures pipeline's in-database rounding retired with it.
"""

from datetime import datetime, timedelta
from origo.assets.create_origo_database import ClickHouseClientProtocol

LATEST_KLINES_TABLE_NAME = 'binance_futures_klines_latest'
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
        INSERT INTO {database}.{LATEST_KLINES_TABLE_NAME}
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
            FROM {database}.{LATEST_RAW_TABLE_NAME}
            WHERE datetime >= toDateTime64('{start_datetime}', 3)
              AND datetime < toDateTime64('{end_datetime}', 3)
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )
