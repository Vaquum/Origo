"""Spot aggregate bar profile: declaration over the shared profile factory."""

from __future__ import annotations

from ..contracts import Column
from .profile_base import ProfileDeclaration, build_components

_RAW = (
    Column('agg_trade_id', 'UInt64'),
    Column('price', 'Float64'),
    Column('quantity', 'Float64'),
    Column('first_trade_id', 'Int64'),
    Column('last_trade_id', 'Int64'),
    Column('timestamp', 'UInt64'),
    Column('is_buyer_maker', 'UInt8'),
    Column('is_best_match', 'UInt8'),
    Column('datetime', 'DateTime64(6)'),
)
# Real table names rewrite to build-key tables; the shared aligned table keeps
# its name with an aggregate dataset source.
_NAMES = {
    'binance_daily_spot_aggtrades': 'raw',
    'binance_spot_aggtrades_klines': 'time',
    'binance_spot_aggtrades_dollar_klines': 'dollar',
    'binance_spot_aggtrades_volume_klines': 'volume',
    'binance_spot_aggtrades_tick_klines': 'tick',
    'binance_spot_aggtrades_dollar_imbalance_klines': 'imbalance',
    'aligned_1m_exchange': 'aligned',
    'binance_spot_aggtrades_latest': 'raw_latest',
    'binance_spot_aggtrades_klines_latest': 'time_latest',
    'binance_spot_aggtrades_dollar_klines_latest': 'dollar_latest',
}

_DECL = ProfileDeclaration(
    raw_columns=_RAW,
    rewrite_names=_NAMES,
    formula_prefix='spot_agg',
    imbalance_module='spot_agg_dollar_imbalance_klines',
    id_column='agg_trade_id',
)

SPOT_AGG_COMPONENTS = build_components(_DECL)
