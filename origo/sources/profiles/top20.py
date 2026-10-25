from __future__ import annotations

from ..contracts import BuildContext, Column, ComponentSpec


def _snapshots(context: BuildContext) -> None:
    suffix = '_latest' if context.partition.provisional else ''
    context.client.execute(
        f'INSERT INTO {context.table("depth20" + suffix)} VALUES',
        context.revision.rows(),
        settings={'insert_block_size': 2048},
    )


def top20_components() -> tuple[ComponentSpec, ...]:
    components: list[ComponentSpec] = []
    for provisional in (False, True):
        suffix = '_latest' if provisional else ''
        components.append(
            ComponentSpec(
                'depth20' + suffix,
                (
                    Column('datetime', 'DateTime'),
                    Column('exchange_time_us', 'UInt64'),
                    Column('observed_at_us', 'UInt64'),
                    Column('last_update_id', 'UInt64'),
                    Column('collector', 'String'),
                    Column('session', 'String'),
                    Column('sequence', 'UInt64'),
                    Column('transaction_time_us', 'UInt64'),
                    Column('first_update_id', 'UInt64'),
                    Column('previous_update_id', 'UInt64'),
                    Column('bids', 'Array(Tuple(Float64, Float64))'),
                    Column('asks', 'Array(Tuple(Float64, Float64))'),
                    Column('original_snapshot', 'String'),
                ),
                ('datetime',),
                'datetime',
                _snapshots,
                provisional=provisional,
                current_target='depth20' if provisional else None,
                hash_chunk_rows=2048,
            )
        )
        components.append(
            ComponentSpec(
                'depth20_1m' + suffix,
                (
                    Column('datetime', 'DateTime'),
                    Column('source_timestamp_ms', 'UInt64'),
                    Column('book_mid_price', 'Float64'),
                    Column('book_spread_bps', 'Float64'),
                    Column('book_bid_depth_20_notional', 'Float64'),
                    Column('book_ask_depth_20_notional', 'Float64'),
                    Column('book_imbalance_20', 'Float64'),
                ),
                ('datetime',),
                'datetime',
                _metrics,
                provisional=provisional,
                current_target='depth20_1m' if provisional else None,
            )
        )
    return tuple(components)


def _metrics(context: BuildContext) -> None:
    suffix = '_latest' if context.partition.provisional else ''
    context.client.execute(f"""INSERT INTO {context.table('depth20_1m' + suffix)}
        WITH chosen AS (
            SELECT toStartOfMinute(datetime) AS minute,
                   argMax(tuple(exchange_time_us, bids, asks), tuple(datetime, exchange_time_us, last_update_id)) AS sample
            FROM {context.table('depth20' + suffix)} GROUP BY minute
        ), measures AS (
            SELECT minute, intDiv(sample.1, 1000) AS source_timestamp_ms,
                   (sample.2[1].1 + sample.3[1].1) / 2 AS mid,
                   (sample.3[1].1 - sample.2[1].1) / mid * 10000 AS spread,
                   arraySum(arrayMap(level -> level.1 * level.2, sample.2)) AS bid_notional,
                   arraySum(arrayMap(level -> level.1 * level.2, sample.3)) AS ask_notional
            FROM chosen
        ) SELECT minute, source_timestamp_ms, mid, spread, bid_notional, ask_notional,
                 (bid_notional - ask_notional) / (bid_notional + ask_notional) FROM measures""")
