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
)

SOURCE_CASES: Final[tuple[SourceCase, ...]] = (SPOT_CASE, PERP_CASE)


def assert_archive_rest_equal(
    archive_rows: Sequence[Row],
    rest_rows: Sequence[Row],
    *,
    case: SourceCase,
) -> None:
    """Assert REST rows equal archive rows under the case's carve-outs.

    Row layout on both sides is ``(trade_id, price, quantity,
    quote_quantity, timestamp, *flags)``. Identity, price, quantity, and
    flags compare exactly; the timestamp and quote-quantity columns compare
    through the case's declared carve-outs.
    """
    expected = {row[0]: row for row in archive_rows}
    assert len(expected) == len(rest_rows)
    for row in rest_rows:
        official = expected[row[0]]
        assert row[:3] == official[:3]
        assert row[5:7] == official[5:7]
        if case.time_carveout == 'microseconds_to_milliseconds':
            assert row[4] == official[4] // 1000
        elif case.time_carveout == 'none':
            assert row[4] == official[4]
        else:
            raise AssertionError(f'unknown time carve-out: {case.time_carveout!r}')
        if case.quote_carveout == 'recompute_price_times_quantity':
            recomputed = row[1] * row[2]
            assert row[3] == recomputed
            assert official[3] == recomputed
        elif case.quote_carveout == 'none':
            assert row[3] == official[3]
        else:
            raise AssertionError(f'unknown quote carve-out: {case.quote_carveout!r}')
