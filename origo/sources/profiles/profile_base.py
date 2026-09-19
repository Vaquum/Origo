"""Shared bar-profile factory (PRD-0013 row 4).

Spot and perp declare their profile parameters on a ProfileDeclaration; the
base owns the measure columns, the projection rewrite client, the raw loader,
the daily/minute/imbalance builders, and the ten-component assembly. Legacy
aliases and retired tables/rows stay per-source: only the shared builders move.
"""

from __future__ import annotations

import importlib
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import cast

from ..contracts import BuildContext, Client, Column, ComponentSpec, Row
from .legacy_order import OrderedRawClient

_MEASURES = tuple(
    Column(name, 'UInt64' if name == 'no_of_trades' else 'Float64')
    for name in (
        'open high low close mean std median iqr volume maker_ratio no_of_trades '
        'open_liquidity high_liquidity low_liquidity close_liquidity liquidity_sum '
        'maker_volume maker_liquidity'
    ).split()
)


@dataclass(frozen=True)
class ProfileDeclaration:
    """Per-source profile parameters; everything else is shared machinery."""

    raw_columns: tuple[Column, ...]
    rewrite_names: dict[str, str]
    formula_prefix: str
    imbalance_module: str
    id_column: str = 'trade_id'


class _ProjectionClient:
    def __init__(self, client: Client, names: Mapping[str, str]) -> None:
        self.client = client
        self.names = names
        self.pattern = re.compile(r'\b(' + '|'.join(sorted(names, key=len, reverse=True)) + r')\b')

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        return self.client.execute(
            self.pattern.sub(lambda match: self.names[match[0]], query), params, settings
        )

    def disconnect(self) -> None:
        raise RuntimeError('A projection cannot disconnect its build-owned connection.')


def _raw(context: BuildContext) -> None:
    if context.revision.insert_bulk is not None and not context.partition.provisional:
        context.revision.insert_bulk(context.table('raw'))
        # Only this private day's stage: stable blocks preserve exact float reductions.
        context.client.execute(f'OPTIMIZE TABLE {context.table("raw")} FINAL')
        return
    provisional = context.partition.provisional
    table = context.table('raw_latest' if provisional else 'raw')

    def rows() -> Iterator[Row]:
        for row in context.revision.rows():
            values = tuple(float(value) if isinstance(value, Decimal) else value for value in row)
            yield (context.partition.start, *values) if provisional else values

    context.client.execute(f'INSERT INTO {table} VALUES', rows())


def _daily(
    module: str, names: Mapping[str, str], id_column: str
) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        formulas = importlib.import_module(f'origo.sources.profiles.formulas.{module}')
        calculate = cast(Callable[[Client, str, str], None], formulas._insert_partition_rows)
        calculate(
            _ProjectionClient(
                OrderedRawClient(context.client, context.table('raw'), id_column), names
            ),
            context.database,
            context.partition.start.date().isoformat(),
        )

    return build


def _minute(module: str, names: Mapping[str, str]) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        formulas = importlib.import_module(f'origo.sources.profiles.formulas.{module}')
        calculate = cast(Callable[[Client, str, datetime], None], formulas._insert_minute_rows)
        calculate(
            _ProjectionClient(context.client, names), context.database, context.partition.start
        )

    return build


def _imbalance(module: str, id_column: str) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        from ..columnar import arrow_client

        formulas = importlib.import_module(f'origo.sources.profiles.formulas.{module}')
        with arrow_client() as client:
            values = client.query_arrow(
                f'SELECT * FROM {context.table("raw")} ORDER BY datetime, {id_column}'
            )
            if not values.num_rows:
                raise RuntimeError('A canonical imbalance component requires non-empty raw input.')
            from ..arrow_types import ArrowTable

            calculate = cast(Callable[[ArrowTable], ArrowTable], formulas._kline_rows)
            bars = calculate(values)
            client.insert_arrow(context.table('imbalance'), bars)

    return build


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


def build_components(decl: ProfileDeclaration) -> tuple[ComponentSpec, ...]:
    """Assemble the ten component specs from one source declaration."""
    prefix = decl.formula_prefix
    names = decl.rewrite_names
    raw_key = ('datetime', decl.id_column)
    return (
        ComponentSpec('raw', decl.raw_columns, raw_key, 'datetime', _raw),
        ComponentSpec(
            'time',
            (Column('datetime', 'DateTime'), *_MEASURES),
            ('datetime',),
            'datetime',
            _daily(f'{prefix}_klines', names, decl.id_column),
        ),
        ComponentSpec(
            'dollar',
            _bar_columns('dollar'),
            ('dollar_bar_id',),
            'start_datetime',
            _daily(f'{prefix}_dollar_klines', names, decl.id_column),
        ),
        ComponentSpec(
            'volume',
            _bar_columns('volume'),
            ('volume_bar_id',),
            'start_datetime',
            _daily(f'{prefix}_volume_klines', names, decl.id_column),
        ),
        ComponentSpec(
            'tick',
            _bar_columns('tick'),
            ('tick_bar_id',),
            'start_datetime',
            _daily(f'{prefix}_tick_klines', names, decl.id_column),
        ),
        ComponentSpec(
            'imbalance',
            _bar_columns('dollar_imbalance', imbalance=True),
            ('dollar_imbalance_bar_id',),
            'start_datetime',
            _imbalance(decl.imbalance_module, decl.id_column),
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
            _daily(f'{prefix}_aligned', names, decl.id_column),
        ),
        ComponentSpec(
            'raw_latest',
            (
                Column('minute_start', 'DateTime'),
                *decl.raw_columns[:-1],
                Column('datetime', 'DateTime64(3)'),
            ),
            raw_key,
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
            _minute(f'{prefix}_klines_latest', names),
            provisional=True,
            current_target='time',
        ),
        ComponentSpec(
            'dollar_latest',
            _bar_columns('dollar'),
            ('dollar_bar_id',),
            'start_datetime',
            _minute(f'{prefix}_dollar_klines_latest', names),
            provisional=True,
            current_target='dollar',
        ),
    )
