"""Registered base-cell contributions for the market state cube (PRD-0022) and its detail
(PRD-0023)."""

from datetime import UTC, datetime, timedelta

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


_DETAIL_COLUMNS = (
    Column('time_index', 'UInt64'),
    Column('price_index', 'UInt64'),
    Column('first_event_at', 'DateTime64(6)'),
    Column('trade_count', 'UInt32'),
    Column('base_volume', 'UInt64'),
    Column('path_length', 'UInt64'),
    Column('dwell', 'UInt32'),
    Column('high', 'Float64'),
    Column('low', 'Float64'),
    Column('first_trade_at', 'DateTime64(6)'),
    Column('first_trade_id', 'UInt64'),
    Column('first_price', 'Float64'),
    Column('last_trade_at', 'DateTime64(6)'),
    Column('last_trade_id', 'UInt64'),
    Column('last_price', 'Float64'),
)
_ROW_CENTS = BASE_PRICE_USDT * 100


def build_market_state_detail(context: BuildContext) -> None:
    """Trade-by-trade path, dwell, base volume and trade prices per base cell of one partition.

    Units are satoshis, cents and microseconds. Each move between consecutive trades belongs
    to the later trade's column and is split across the half-open rows it passes. Each price
    holds until the next trade; the last holds until the partition end, and the first also
    covers the partition start before it. A trade's cell takes its own share of both; the
    moves and holds that leave it are expanded into the other cells afterwards.
    """
    suffix = '_latest' if context.partition.provisional else ''
    raw = context.table('raw' + suffix)
    target = context.table('market_state_detail' + suffix)
    stamp = '%Y-%m-%d %H:%M:%S.%f'
    params = {
        'cube_start': CUBE_START.strftime('%Y-%m-%d %H:%M:%S'),
        'start': context.partition.start.strftime(stamp),
        'end': context.partition.end.strftime(stamp),
    }
    cutoff = "toDateTime64(%(cube_start)s, 6, 'UTC')"
    settings = {'max_threads': 1, 'max_memory_usage': QUERY_MEMORY_BYTES}
    prices = '0., 0., toUInt64(0), tuple(0., toInt64(0)), toUInt64(0), tuple(0., toInt64(0))'
    context.client.execute(
        f"""INSERT INTO {target}
        WITH toUnixTimestamp64Micro({cutoff}) AS t0, toInt64({BASE_TIME_US}) AS col,
            toInt64({_ROW_CENTS}) AS rowc,
            toUnixTimestamp64Micro(toDateTime64(%(start)s, 6, 'UTC')) AS p_start,
            toUnixTimestamp64Micro(toDateTime64(%(end)s, 6, 'UTC')) AS p_end
        SELECT
            accurateCast(piece.1, 'UInt64') AS time_index,
            accurateCast(piece.2, 'UInt64') AS price_index,
            fromUnixTimestamp64Micro(min(piece.3), 'UTC') AS first_event_at,
            accurateCast(sum(piece.4), 'UInt32') AS trade_count,
            sum(piece.5) AS base_volume,
            accurateCast(sum(piece.6), 'UInt64') AS path_length,
            accurateCast(sum(piece.7), 'UInt32') AS dwell,
            maxIf(piece.8, piece.4 > 0) AS high,
            minIf(piece.9, piece.4 > 0) AS low,
            fromUnixTimestamp64Micro(if(sum(piece.4) > 0,
                argMinIf(piece.11.2, piece.10, piece.4 > 0), min(piece.3)), 'UTC')
                AS first_trade_at,
            minIf(piece.10, piece.4 > 0) AS first_trade_id,
            argMinIf(piece.11.1, piece.10, piece.4 > 0) AS first_price,
            fromUnixTimestamp64Micro(if(sum(piece.4) > 0,
                argMaxIf(piece.13.2, piece.12, piece.4 > 0), min(piece.3)), 'UTC')
                AS last_trade_at,
            maxIf(piece.12, piece.4 > 0) AS last_trade_id,
            argMaxIf(piece.13.1, piece.12, piece.4 > 0) AS last_price
        FROM (
            SELECT arrayConcat(
                [tuple(oc, orow, ev, n, sats, path, dwell, high, low,
                    first_id, first, last_id, last)],
                arrayFlatten(arrayMap(m -> arrayMap(r -> tuple(oc, r, m.3, toUInt64(0), toUInt64(0),
                        least(m.2, (r + 1) * rowc) - greatest(m.1, r * rowc), toInt64(0), {prices}),
                    arrayFilter(r -> r != orow
                        AND least(m.2, (r + 1) * rowc) - greatest(m.1, r * rowc) > 0,
                        range(intDiv(m.1, rowc), intDiv(m.2, rowc) + 1))), moves)),
                arrayFlatten(arrayMap(d -> arrayMap(cc -> tuple(cc, orow,
                        greatest(d.1, t0 + cc * col), toUInt64(0), toUInt64(0), toInt64(0),
                        least(d.2, t0 + (cc + 1) * col) - greatest(d.1, t0 + cc * col), {prices}),
                    arrayFilter(cc -> cc != oc,
                        range(intDiv(d.1 - t0, col), intDiv(d.2 - 1 - t0, col) + 1))), stays))
            ) AS pieces
            FROM (
                SELECT oc, orow, min(greatest(d_start, t0 + oc * col)) AS ev, count() AS n,
                    sum(sats) AS sats, sum(own_path) AS path, sum(own_dwell) AS dwell,
                    max(price) AS high, min(price) AS low,
                    min(id) AS first_id, argMin(tuple(price, us), id) AS first,
                    max(id) AS last_id, argMax(tuple(price, us), id) AS last,
                    groupArrayIf(tuple(lo, hi, us),
                        c_prev != -1 AND intDiv(lo, rowc) != intDiv(hi, rowc)) AS moves,
                    groupArrayIf(tuple(d_start, d_end),
                        d_start < t0 + oc * col OR d_end > t0 + (oc + 1) * col) AS stays
                FROM (
                    SELECT us, id, price, sats, c_prev, oc, orow, lo, hi, d_start, d_end,
                        if(c_prev = -1, toInt64(0), greatest(toInt64(0),
                            least(hi, (orow + 1) * rowc) - greatest(lo, orow * rowc))) AS own_path,
                        least(d_end, t0 + (oc + 1) * col)
                            - greatest(d_start, t0 + oc * col) AS own_dwell
                    FROM (
                        SELECT us, id, price, sats, c_prev,
                            intDiv(us - t0, col) AS oc,
                            toInt64(floor(price / {BASE_PRICE_USDT})) AS orow,
                            if(c_prev = -1, p_start, us) AS d_start,
                            if(us_next = -1, p_end, us_next) AS d_end,
                            least(c_prev, c) AS lo, greatest(c_prev, c) AS hi
                        FROM (
                            SELECT toUnixTimestamp64Micro(datetime) AS us, trade_id AS id, price,
                                toInt64(round(price * 100)) AS c,
                                toUInt64(round(quantity * 100000000)) AS sats,
                                lagInFrame(toInt64(round(price * 100)), 1, toInt64(-1))
                                    OVER w AS c_prev,
                                leadInFrame(toUnixTimestamp64Micro(datetime), 1, toInt64(-1))
                                    OVER w AS us_next
                            FROM {raw} WHERE datetime >= {cutoff}
                            WINDOW w AS (ORDER BY datetime ASC, trade_id ASC
                                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
                        )
                    )
                )
                GROUP BY oc, orow
            )
        )
        ARRAY JOIN pieces AS piece
        GROUP BY time_index, price_index""",
        params,
        settings=settings,
    )
    [(count, sats, path, unconverted, disordered)] = context.client.execute(
        f"""SELECT count(), sum(sats), sumIf(abs(c - c_prev), c_prev != -1),
            countIf(abs(price * 100 - c) > 0.01 OR abs(quantity * 100000000 - sats) > 0.01
                OR toInt64(floor(price / {BASE_PRICE_USDT})) != intDiv(c, {_ROW_CENTS})),
            countIf(c_prev != -1 AND id_prev >= trade_id)
        FROM (
            SELECT trade_id, price, quantity, toInt64(round(price * 100)) AS c,
                toUInt64(round(quantity * 100000000)) AS sats,
                lagInFrame(toInt64(round(price * 100)), 1, toInt64(-1)) OVER w AS c_prev,
                lagInFrame(trade_id) OVER w AS id_prev
            FROM {raw} WHERE datetime >= {cutoff}
            WINDOW w AS (ORDER BY datetime ASC, trade_id ASC
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
        )""",
        params,
        settings=settings,
    )
    if int(str(unconverted)):
        raise SourceError(
            'COMPONENT_CONTENT_INVALID',
            'Market state detail needs whole-cent prices and whole-satoshi quantities.',
        )
    if int(str(disordered)):
        raise SourceError(
            'COMPONENT_CONTENT_INVALID',
            'Market state detail needs trade IDs that increase with trade time.',
        )
    cells = context.client.execute(
        f'SELECT sum(toUInt64(trade_count)), sum(base_volume), sum(path_length) FROM {target}',
        settings=settings,
    )
    columns = {
        int(str(column)): int(str(dwell))
        for column, dwell in context.client.execute(
            f'SELECT time_index, sum(toUInt64(dwell)) FROM {target} GROUP BY time_index',
            settings=settings,
        )
    }
    start = (context.partition.start - CUBE_START) // timedelta(microseconds=1)
    end = (context.partition.end - CUBE_START) // timedelta(microseconds=1)
    held = {
        column: min(end, (column + 1) * BASE_TIME_US) - max(start, column * BASE_TIME_US)
        for column in range(start // BASE_TIME_US, (end - 1) // BASE_TIME_US + 1)
    }
    if cells != [(count, sats, path)] or columns != (held if int(str(count)) else {}):
        raise SourceError(
            'COMPONENT_CONTENT_INVALID',
            'Market state detail does not account for every eligible trade, satoshi, move '
            'and microsecond of its partition.',
        )


MARKET_STATE_DETAIL_COMPONENTS: tuple[ComponentSpec, ...] = (
    ComponentSpec(
        'market_state_detail',
        _DETAIL_COLUMNS,
        ('time_index', 'price_index'),
        'first_event_at',
        build_market_state_detail,
        start_at=CUBE_START,
        activation_group='market_state_detail',
    ),
    ComponentSpec(
        'market_state_detail_latest',
        _DETAIL_COLUMNS,
        ('time_index', 'price_index'),
        'first_event_at',
        build_market_state_detail,
        provisional=True,
        current_target='market_state_detail',
        start_at=CUBE_START,
        activation_group='market_state_detail',
    ),
)
