"""Spot aggregate publication series. Names and paths carry the spot_agg prefix so the aggregate mirror never collides with the trades sources'."""

from .spot_series import MountKlineSpec

EXPORT_START_YEAR = 2020


EXPORT_START_MONTH = 1


SPECS: tuple[MountKlineSpec, ...] = (
    MountKlineSpec('spot_agg_time_1m', 'time', 1, 'spot_agg/time/1m'),
    MountKlineSpec('spot_agg_time_15m', 'time', 15, 'spot_agg/time/15m'),
    MountKlineSpec('spot_agg_time_30m', 'time', 30, 'spot_agg/time/30m'),
    MountKlineSpec('spot_agg_time_1h', 'time', 60, 'spot_agg/time/1h'),
    MountKlineSpec('spot_agg_time_2h', 'time', 120, 'spot_agg/time/2h'),
    MountKlineSpec('spot_agg_time_4h', 'time', 240, 'spot_agg/time/4h'),
    MountKlineSpec('spot_agg_dollar_1M', 'dollar', 1, 'spot_agg/dollar/1M'),
    MountKlineSpec('spot_agg_dollar_15M', 'dollar', 15, 'spot_agg/dollar/15M'),
    MountKlineSpec('spot_agg_dollar_30M', 'dollar', 30, 'spot_agg/dollar/30M'),
    MountKlineSpec('spot_agg_dollar_60M', 'dollar', 60, 'spot_agg/dollar/60M'),
    MountKlineSpec('spot_agg_dollar_120M', 'dollar', 120, 'spot_agg/dollar/120M'),
    MountKlineSpec('spot_agg_dollar_240M', 'dollar', 240, 'spot_agg/dollar/240M'),
)
