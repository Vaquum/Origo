"""Perp aggregate publication series. Names and paths carry the perp_agg prefix so the aggregate mirror never collides with the trades sources'."""

from .spot_series import MountKlineSpec

EXPORT_START_YEAR = 2020


EXPORT_START_MONTH = 1


SPECS: tuple[MountKlineSpec, ...] = (
    MountKlineSpec('perp_agg_time_1m', 'time', 1, 'perp_agg/time/1m'),
    MountKlineSpec('perp_agg_time_15m', 'time', 15, 'perp_agg/time/15m'),
    MountKlineSpec('perp_agg_time_30m', 'time', 30, 'perp_agg/time/30m'),
    MountKlineSpec('perp_agg_time_1h', 'time', 60, 'perp_agg/time/1h'),
    MountKlineSpec('perp_agg_time_2h', 'time', 120, 'perp_agg/time/2h'),
    MountKlineSpec('perp_agg_time_4h', 'time', 240, 'perp_agg/time/4h'),
    MountKlineSpec('perp_agg_dollar_1M', 'dollar', 1, 'perp_agg/dollar/1M'),
    MountKlineSpec('perp_agg_dollar_15M', 'dollar', 15, 'perp_agg/dollar/15M'),
    MountKlineSpec('perp_agg_dollar_30M', 'dollar', 30, 'perp_agg/dollar/30M'),
    MountKlineSpec('perp_agg_dollar_60M', 'dollar', 60, 'perp_agg/dollar/60M'),
    MountKlineSpec('perp_agg_dollar_120M', 'dollar', 120, 'perp_agg/dollar/120M'),
    MountKlineSpec('perp_agg_dollar_240M', 'dollar', 240, 'perp_agg/dollar/240M'),
)
