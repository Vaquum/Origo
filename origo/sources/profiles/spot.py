"""Spot bar profile: declaration over the shared profile factory."""

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
# The legacy spot table names, served as views over the components that replaced them;
# aligned_1m_exchange stays a table because the futures pipeline still writes it.
SPOT_ALIASES = tuple((name, key) for name, key in _NAMES.items() if name != 'aligned_1m_exchange')
# The rows the retired spot refresh wrote into the futures pipeline's aligned table; the live
# spot aligned rows are the source's own component.
SPOT_RETIRED_ROWS = (('aligned_1m_exchange', "dataset_source = 'binance_spot'"),)
# Legacy tables without a successor: ingestion ledgers, watermarks and the per-interval cuts.
SPOT_RETIRED_TABLES = (
    'binance_daily_spot_trades_ingestion',
    'binance_spot_trades_latest_ingestion',
    'binance_spot_latest_watermarks',
    *(f'binance_spot_{label}_klines_latest' for label in ('15m', '30m', '1h', '2h', '4h')),
    *(
        f'binance_spot_{label}_dollar_klines_latest'
        for label in ('15M', '30M', '60M', '120M', '240M')
    ),
)

_DECL = ProfileDeclaration(
    raw_columns=_RAW,
    rewrite_names=_NAMES,
    formula_prefix='spot',
    imbalance_module='spot_dollar_imbalance_klines',
)

SPOT_COMPONENTS = build_components(_DECL)
