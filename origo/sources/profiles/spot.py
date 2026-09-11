from __future__ import annotations

import importlib
import re
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from decimal import Decimal
from typing import Protocol, cast

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


class _ArrowTable(Protocol):
    def to_pylist(self) -> list[dict[str, object]]: ...


class _ArrowFactory(Protocol):
    def table(self, data: Mapping[str, list[object]]) -> _ArrowTable: ...


def _imbalance(context: BuildContext) -> None:
    values = context.client.execute(
        f'SELECT * FROM {context.table("raw")} ORDER BY datetime, trade_id'
    )
    if not values:
        raise RuntimeError('A canonical imbalance component requires non-empty raw input.')
    columns = {column.name: [row[index] for row in values] for index, column in enumerate(_RAW)}
    arrow = cast(_ArrowFactory, importlib.import_module('pyarrow'))
    legacy = importlib.import_module(
        'origo.assets.refresh_binance_spot_dollar_imbalance_klines_origo'
    )
    calculate = cast(Callable[[_ArrowTable], _ArrowTable], legacy._kline_rows)
    bars = calculate(arrow.table(columns)).to_pylist()
    schema = _bar_columns('dollar_imbalance', imbalance=True)
    context.client.execute(
        f'INSERT INTO {context.table("imbalance")} VALUES',
        [tuple(bar[column.name] for column in schema) for bar in bars],
    )


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
