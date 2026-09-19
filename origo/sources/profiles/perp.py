"""Perp bar profile: declaration over the shared profile factory."""

from __future__ import annotations

from ..contracts import Column
from .profile_base import ProfileDeclaration, build_components

_RAW = (
    Column('trade_id', 'UInt64'),
    Column('price', 'Float64'),
    Column('quantity', 'Float64'),
    Column('quote_quantity', 'Float64'),
    Column('timestamp', 'UInt64'),
    Column('is_buyer_maker', 'UInt8'),
    Column('datetime', 'DateTime64(6)'),
)
# Rewrite keys for the ported formulas. The retired futures tables are dropped by the
# source spec; these names survive only inside the formula SQL that is rewritten here.
_NAMES = {
    'binance_daily_futures_trades': 'raw',
    'binance_futures_klines': 'time',
    'binance_futures_dollar_klines': 'dollar',
    'binance_futures_volume_klines': 'volume',
    'binance_futures_tick_klines': 'tick',
    'binance_futures_dollar_imbalance_klines': 'imbalance',
    'aligned_1m_exchange': 'aligned',
    'binance_futures_trades_latest': 'raw_latest',
    'binance_futures_klines_latest': 'time_latest',
    'binance_futures_dollar_klines_latest': 'dollar_latest',
}
# Legacy tables without a successor and without surviving readers, so no aliases.
PERP_RETIRED_TABLES = (
    'binance_daily_futures_trades',
    'binance_daily_futures_trades_ingestion',
    'binance_futures_klines',
    'aligned_1m_exchange',
)

_DECL = ProfileDeclaration(
    raw_columns=_RAW,
    rewrite_names=_NAMES,
    formula_prefix='perp',
    imbalance_module='perp_dollar_imbalance_klines',
)

PERP_COMPONENTS = build_components(_DECL)
