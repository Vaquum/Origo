"""Registered base-cell contributions for the market state cube (PRD-0022)."""

from datetime import UTC, datetime

from ..columnar import QUERY_MEMORY_BYTES
from ..contracts import BuildContext, Column, ComponentSpec, SourceError

CUBE_START = datetime(2021, 1, 1, tzinfo=UTC)
BASE_TIME_US = 56_250_000
BASE_PRICE_USDT = 125

_COLUMNS = (
    Column('time_index', 'UInt64'),
    Column('price_index', 'UInt64'),
    Column('first_trade_at', 'DateTime64(6)'),
    Column('volume', 'Float64'),
    Column('trade_count', 'UInt32'),
    Column('taker_buy_volume', 'Float64'),
    Column('taker_buy_trade_count', 'UInt32'),
)


def build_market_state(context: BuildContext) -> None:
    """Aggregate one validated private raw partition, retaining global cell keys."""
    suffix = '_latest' if context.partition.provisional else ''
    raw = context.table('raw' + suffix)
    target = context.table('market_state' + suffix)
    params = {'cube_start': CUBE_START.strftime('%Y-%m-%d %H:%M:%S')}
    cutoff = "toDateTime64(%(cube_start)s, 6, 'UTC')"
    settings = {'max_threads': 1, 'max_memory_usage': QUERY_MEMORY_BYTES}
    context.client.execute(
        f"""INSERT INTO {target}
        SELECT
            accurateCast(intDiv(toUnixTimestamp64Micro(datetime)
                - toUnixTimestamp64Micro({cutoff}), {BASE_TIME_US}), 'UInt64') AS time_index,
            accurateCast(floor(price / {BASE_PRICE_USDT}), 'UInt64') AS price_index,
            min(toDateTime64(datetime, 6, 'UTC')) AS first_trade_at,
            sumKahan(quote_quantity) AS volume,
            accurateCast(count(), 'UInt32') AS trade_count,
            sumKahanIf(quote_quantity, is_buyer_maker = 0) AS taker_buy_volume,
            accurateCast(countIf(is_buyer_maker = 0), 'UInt32') AS taker_buy_trade_count
        FROM (
            SELECT datetime, trade_id, price, quote_quantity, is_buyer_maker
            FROM {raw} WHERE datetime >= {cutoff}
            ORDER BY datetime, trade_id
        )
        GROUP BY time_index, price_index""",
        params,
        settings=settings,
    )
    raw_counts = context.client.execute(
        f'SELECT count(), countIf(is_buyer_maker = 0) FROM {raw} WHERE datetime >= {cutoff}',
        params,
        settings=settings,
    )
    cell_counts = context.client.execute(
        f'SELECT sum(toUInt64(trade_count)), sum(toUInt64(taker_buy_trade_count)) FROM {target}',
        settings=settings,
    )
    if cell_counts != raw_counts:
        raise SourceError(
            'COMPONENT_CONTENT_INVALID',
            'Market state contributions do not account for every eligible raw trade and taker buy.',
        )


MARKET_STATE_COMPONENTS: tuple[ComponentSpec, ...] = (
    ComponentSpec(
        'market_state',
        _COLUMNS,
        ('time_index', 'price_index'),
        'first_trade_at',
        build_market_state,
        start_at=CUBE_START,
        activation_group='market_state',
    ),
    ComponentSpec(
        'market_state_latest',
        _COLUMNS,
        ('time_index', 'price_index'),
        'first_trade_at',
        build_market_state,
        provisional=True,
        current_target='market_state',
        start_at=CUBE_START,
        activation_group='market_state',
    ),
)
