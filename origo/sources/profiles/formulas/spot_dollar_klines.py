"""Relocated verbatim from origo/assets/refresh_binance_spot_dollar_klines_origo.py; the SQL and arithmetic are unchanged."""

from datetime import datetime, timedelta
from origo.assets.create_origo_database import ClickHouseClientProtocol

DOLLAR_KLINES_TABLE_NAME = 'binance_spot_dollar_klines'
RAW_TABLE_NAME = 'binance_daily_spot_trades'

DOLLAR_KLINE_SIZE = 1_000_000.0


def _partition_datetime_bounds(partition_date: str) -> tuple[str, str]:
    start_datetime = datetime.strptime(partition_date, '%Y-%m-%d')
    end_datetime = start_datetime + timedelta(days=1)
    return (
        start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
    )


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    start_datetime, end_datetime = _partition_datetime_bounds(partition_date)
    client.execute(
        f"""
        INSERT INTO {database}.{DOLLAR_KLINES_TABLE_NAME}
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
                FROM {database}.{RAW_TABLE_NAME}
                WHERE datetime >= toDateTime64('{start_datetime}', 6)
                  AND datetime < toDateTime64('{end_datetime}', 6)
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
    )
