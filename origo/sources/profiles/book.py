from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date

from ..contracts import BuildContext, Column, ComponentSpec, Row

BOOK_FIRST_DAY = date(2025, 6, 28)
BOOK_FIRST_HOUR = {'spot': 7, 'perp': 6}
BOOK_COMPONENT_KEYS = ('depth20', 'depth200', 'depth20_1m', 'depth200_1m')
BOOK_SAMPLE_INTERVAL_MS = {20: 100, 200: 1000}
# A full depth-200 page measured 67 MiB peak query memory on original recorded states.
BOOK_HASH_CHUNK_ROWS = 2048


def _snapshots(depth: int) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        suffix = '_latest' if context.partition.provisional else ''
        table = context.table(f'depth{depth}{suffix}')

        def rows() -> Iterator[Row]:
            for row in context.revision.rows():
                if row[0] == depth:
                    yield row[1:]

        context.client.execute(
            f'INSERT INTO {table} VALUES',
            rows(),
            settings={'insert_block_size': BOOK_HASH_CHUNK_ROWS},
        )

    return build


def _minute(depth: int) -> Callable[[BuildContext], None]:
    def build(context: BuildContext) -> None:
        suffix = '_latest' if context.partition.provisional else ''
        context.client.execute(f"""INSERT INTO {context.table(f'depth{depth}_1m{suffix}')}
            WITH chosen AS (
                SELECT toStartOfMinute(datetime) AS minute,
                       argMax(tuple(source_timestamp_ms, bids, asks), tuple(datetime, source_timestamp_ms, last_update_id)) AS sample
                FROM {context.table(f'depth{depth}{suffix}')} GROUP BY minute
            ), levels AS (
                SELECT minute, sample.1 AS source_timestamp_ms, sample.2 AS bids, sample.3 AS asks FROM chosen
            ), measures AS (
                SELECT minute, source_timestamp_ms,
                       (bids[1].1 + asks[1].1) / 2 AS mid,
                       (asks[1].1 - bids[1].1) / mid * 10000 AS spread,
                       arraySum(arrayMap(level -> level.1 * level.2, bids)) AS bid_notional,
                       arraySum(arrayMap(level -> level.1 * level.2, asks)) AS ask_notional
                FROM levels
            ) SELECT minute, source_timestamp_ms, mid, spread, bid_notional, ask_notional,
                     (bid_notional - ask_notional) / (bid_notional + ask_notional) FROM measures""")

    return build


def book_components() -> tuple[ComponentSpec, ...]:
    components: list[ComponentSpec] = []
    for provisional in (False, True):
        suffix = '_latest' if provisional else ''
        for depth in (20, 200):
            components.append(
                ComponentSpec(
                    f'depth{depth}{suffix}',
                    (
                        Column('datetime', 'DateTime64(3)'),
                        Column('source_timestamp_ms', 'UInt64'),
                        Column('last_update_id', 'UInt64'),
                        Column('bids', 'Array(Tuple(Float64, Float64))'),
                        Column('asks', 'Array(Tuple(Float64, Float64))'),
                    ),
                    ('datetime',),
                    'datetime',
                    _snapshots(depth),
                    provisional=provisional,
                    current_target=f'depth{depth}' if provisional else None,
                    hash_chunk_rows=BOOK_HASH_CHUNK_ROWS,
                )
            )
        for depth in (20, 200):
            components.append(
                ComponentSpec(
                    f'depth{depth}_1m{suffix}',
                    (
                        Column('datetime', 'DateTime'),
                        Column('source_timestamp_ms', 'UInt64'),
                        Column('book_mid_price', 'Float64'),
                        Column('book_spread_bps', 'Float64'),
                        Column(f'book_bid_depth_{depth}_notional', 'Float64'),
                        Column(f'book_ask_depth_{depth}_notional', 'Float64'),
                        Column(f'book_imbalance_{depth}', 'Float64'),
                    ),
                    ('datetime',),
                    'datetime',
                    _minute(depth),
                    provisional=provisional,
                    current_target=f'depth{depth}_1m' if provisional else None,
                )
            )
    return tuple(components)
