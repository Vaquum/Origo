"""Shared acceptance cases for the revisioned-source backfill suites.

Each source declares one :class:`SourceCase`: its spec key, its fixture
root, the twelve public products the playbook contract requires, and the
carve-outs under which its archive rows must equal its REST rows.
Per-source suites import their case and the shared comparison helper
instead of mirroring the comparison inline.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

Row = Sequence[Any]


@dataclass(frozen=True)
class SourceCase:
    spec_key: str
    fixtures_root: Path
    inventory: tuple[str, ...]
    time_carveout: str
    quote_carveout: str
    row_layout: str


SPOT_CASE: Final = SourceCase(
    spec_key='binance_spot_trades',
    fixtures_root=Path('tests/fixtures/binance/spot'),
    inventory=(
        'time_1m',
        'time_15m',
        'time_30m',
        'time_1h',
        'time_2h',
        'time_4h',
        'dollar_1M',
        'dollar_15M',
        'dollar_30M',
        'dollar_60M',
        'dollar_120M',
        'dollar_240M',
    ),
    time_carveout='microseconds_to_milliseconds',
    quote_carveout='none',
    row_layout='trades',
)

PERP_CASE: Final = SourceCase(
    spec_key='binance_perp_trades',
    fixtures_root=Path('tests/fixtures/binance/futures'),
    inventory=(
        'perp_time_1m',
        'perp_time_15m',
        'perp_time_30m',
        'perp_time_1h',
        'perp_time_2h',
        'perp_time_4h',
        'perp_dollar_1M',
        'perp_dollar_15M',
        'perp_dollar_30M',
        'perp_dollar_60M',
        'perp_dollar_120M',
        'perp_dollar_240M',
    ),
    time_carveout='none',
    quote_carveout='recompute_price_times_quantity',
    row_layout='trades',
)

SPOT_AGG_CASE: Final = SourceCase(
    spec_key='binance_spot_aggtrades',
    fixtures_root=Path('tests/fixtures/binance/spot'),
    inventory=(
        'spot_agg_time_1m',
        'spot_agg_time_15m',
        'spot_agg_time_30m',
        'spot_agg_time_1h',
        'spot_agg_time_2h',
        'spot_agg_time_4h',
        'spot_agg_dollar_1M',
        'spot_agg_dollar_15M',
        'spot_agg_dollar_30M',
        'spot_agg_dollar_60M',
        'spot_agg_dollar_120M',
        'spot_agg_dollar_240M',
    ),
    time_carveout='microseconds_to_milliseconds',
    quote_carveout='absent',
    row_layout='aggregates',
)

PERP_AGG_CASE: Final = SourceCase(
    spec_key='binance_perp_aggtrades',
    fixtures_root=Path('tests/fixtures/binance/futures'),
    inventory=(
        'perp_agg_time_1m',
        'perp_agg_time_15m',
        'perp_agg_time_30m',
        'perp_agg_time_1h',
        'perp_agg_time_2h',
        'perp_agg_time_4h',
        'perp_agg_dollar_1M',
        'perp_agg_dollar_15M',
        'perp_agg_dollar_30M',
        'perp_agg_dollar_60M',
        'perp_agg_dollar_120M',
        'perp_agg_dollar_240M',
    ),
    time_carveout='none',
    quote_carveout='absent',
    row_layout='aggregates',
)

SOURCE_CASES: Final[tuple[SourceCase, ...]] = (SPOT_CASE, PERP_CASE, SPOT_AGG_CASE, PERP_AGG_CASE)


def assert_archive_rest_equal(
    archive_rows: Sequence[Row],
    rest_rows: Sequence[Row],
    *,
    case: SourceCase,
) -> None:
    """Assert REST rows equal archive rows under the case's carve-outs.

    Trades rows are ``(trade_id, price, quantity, quote_quantity,
    timestamp, *flags)``; aggregate rows are ``(agg_trade_id, price,
    quantity, first_trade_id, last_trade_id, timestamp, *flags)`` with no
    quote column. Identity, price, quantity, and flags compare exactly; the
    timestamp and quote columns compare through the case's declared
    carve-outs.
    """
    if case.row_layout == 'aggregates':
        ts_index, flags = 5, slice(6, 8)
    elif case.row_layout == 'trades':
        ts_index, flags = 4, slice(5, 7)
    else:
        raise AssertionError(f'unknown row layout: {case.row_layout!r}')
    expected = {row[0]: row for row in archive_rows}
    assert len(expected) == len(rest_rows)
    for row in rest_rows:
        official = expected[row[0]]
        assert row[:3] == official[:3]
        assert row[flags] == official[flags]
        if case.time_carveout == 'microseconds_to_milliseconds':
            assert row[ts_index] == official[ts_index] // 1000
        elif case.time_carveout == 'none':
            assert row[ts_index] == official[ts_index]
        else:
            raise AssertionError(f'unknown time carve-out: {case.time_carveout!r}')
        if case.quote_carveout == 'recompute_price_times_quantity':
            recomputed = row[1] * row[2]
            assert row[3] == recomputed
            assert official[3] == recomputed
        elif case.quote_carveout == 'none':
            assert row[3] == official[3]
        elif case.quote_carveout == 'absent':
            assert row[3:5] == official[3:5]
        else:
            raise AssertionError(f'unknown quote carve-out: {case.quote_carveout!r}')
