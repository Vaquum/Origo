from __future__ import annotations

import importlib
import re
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from decimal import Decimal
from typing import cast

from ..contracts import BuildContext, Client, Column, ComponentSpec, Row

_MEASURES = tuple(
    Column(name, 'UInt64' if name == 'no_of_trades' else 'Float64')
    for name in (
        'open high low close mean std median iqr volume maker_ratio no_of_trades '
        'open_liquidity high_liquidity low_liquidity close_liquidity liquidity_sum '
        'maker_volume maker_liquidity'
    ).split()
)
_RAW = (
    Column('trade_id', 'UInt64'),
    Column('price', 'Float64'),
    Column('quantity', 'Float64'),
    Column('quote_quantity', 'Float64'),
    Column('timestamp', 'UInt64'),
    Column('is_buyer_maker', 'UInt8'),
    Column('is_best_match', 'UInt8'),
    Column('datetime', 'DateTime64(6)'),
)
_NAMES = {
    'binance_daily_spot_trades': 'raw',
    'binance_spot_klines': 'time',
    'binance_spot_dollar_klines': 'dollar',
    'binance_spot_volume_klines': 'volume',
    'binance_spot_tick_klines': 'tick',
    'binance_spot_dollar_imbalance_klines': 'imbalance',
    'aligned_1m_exchange': 'aligned',
    'binance_spot_trades_latest': 'raw_latest',
    'binance_spot_klines_latest': 'time_latest',
    'binance_spot_dollar_klines_latest': 'dollar_latest',
}
_PATTERN = re.compile(r'\b(' + '|'.join(sorted(_NAMES, key=len, reverse=True)) + r')\b')


class _ProjectionClient:
    def __init__(self, client: Client) -> None:
        self.client = client

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        return self.client.execute(
            _PATTERN.sub(lambda match: _NAMES[match[0]], query), params, settings
        )

    def disconnect(self) -> None:
        raise RuntimeError('A projection cannot disconnect its build-owned connection.')


def _raw(context: BuildContext) -> None:
    if context.revision.insert_bulk is not None and not context.partition.provisional:
        context.revision.insert_bulk(context.table('raw'))
        return
    provisional = context.partition.provisional
    table = context.table('raw_latest' if provisional else 'raw')

    def rows() -> Iterator[Row]:
        for row in context.revision.rows():
            values = tuple(float(value) if isinstance(value, Decimal) else value for value in row)
            yield (context.partition.start, *values) if provisional else values

    context.client.execute(f'INSERT INTO {table} VALUES', rows())


def _daily(module: str) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        legacy = importlib.import_module(f'origo.assets.{module}')
        calculate = cast(Callable[[Client, str, str], None], legacy._insert_partition_rows)
        calculate(
            _ProjectionClient(context.client),
            context.database,
            context.partition.start.date().isoformat(),
        )

    return build


def _minute(module: str) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        legacy = importlib.import_module(f'origo.assets.{module}')
        calculate = cast(Callable[[Client, str, datetime], None], legacy._insert_minute_rows)
        calculate(_ProjectionClient(context.client), context.database, context.partition.start)

    return build


def _imbalance(context: BuildContext) -> None:
    from ..columnar import arrow_client

    legacy = importlib.import_module(
        'origo.assets.refresh_binance_spot_dollar_imbalance_klines_origo'
    )
    with arrow_client() as client:
        values = client.query_arrow(
            f'SELECT * FROM {context.table("raw")} ORDER BY datetime, trade_id'
        )
        if not values.num_rows:
            raise RuntimeError('A canonical imbalance component requires non-empty raw input.')
        from ..arrow_types import ArrowTable

        calculate = cast(Callable[[ArrowTable], ArrowTable], legacy._kline_rows)
        bars = calculate(values)
        client.insert_arrow(context.table('imbalance'), bars)


def _bar_columns(kind: str, *, imbalance: bool = False) -> tuple[Column, ...]:
    columns = (
        Column('start_datetime', 'DateTime'),
        Column('end_datetime', 'DateTime'),
        Column(f'{kind}_bar_id', 'UInt64'),
        *_MEASURES,
    )
    if imbalance:
        columns += tuple(
            Column(name, 'Float64')
            for name in ('taker_buy_liquidity', 'taker_sell_liquidity', 'dollar_imbalance')
        )
    return columns


SPOT_COMPONENTS = (
    ComponentSpec('raw', _RAW, ('datetime', 'trade_id'), 'datetime', _raw),
    ComponentSpec(
        'time',
        (Column('datetime', 'DateTime'), *_MEASURES),
        ('datetime',),
        'datetime',
        _daily('refresh_binance_spot_klines_origo'),
    ),
    ComponentSpec(
        'dollar',
        _bar_columns('dollar'),
        ('dollar_bar_id',),
        'start_datetime',
        _daily('refresh_binance_spot_dollar_klines_origo'),
    ),
    ComponentSpec(
        'volume',
        _bar_columns('volume'),
        ('volume_bar_id',),
        'start_datetime',
        _daily('refresh_binance_spot_volume_klines_origo'),
    ),
    ComponentSpec(
        'tick',
        _bar_columns('tick'),
        ('tick_bar_id',),
        'start_datetime',
        _daily('refresh_binance_spot_tick_klines_origo'),
    ),
    ComponentSpec(
        'imbalance',
        _bar_columns('dollar_imbalance', imbalance=True),
        ('dollar_imbalance_bar_id',),
        'start_datetime',
        _imbalance,
    ),
    ComponentSpec(
        'aligned',
        (
            Column('dataset_source', 'LowCardinality(String)'),
            Column('datetime', 'DateTime'),
            *_MEASURES,
        ),
        ('dataset_source', 'datetime'),
        'datetime',
        _daily('refresh_aligned_1m_exchange_from_binance_spot_origo'),
    ),
    ComponentSpec(
        'raw_latest',
        (Column('minute_start', 'DateTime'), *_RAW[:-1], Column('datetime', 'DateTime64(3)')),
        ('datetime', 'trade_id'),
        'datetime',
        _raw,
        provisional=True,
        current_target='raw',
    ),
    ComponentSpec(
        'time_latest',
        (Column('datetime', 'DateTime'), *_MEASURES),
        ('datetime',),
        'datetime',
        _minute('refresh_binance_spot_klines_latest_origo'),
        provisional=True,
        current_target='time',
    ),
    ComponentSpec(
        'dollar_latest',
        _bar_columns('dollar'),
        ('dollar_bar_id',),
        'start_datetime',
        _minute('refresh_binance_spot_dollar_klines_latest_origo'),
        provisional=True,
        current_target='dollar',
    ),
)
