"""Perp publication series. Names and paths carry the perp prefix so the perp mirror never collides with spot's."""

from .spot_series import MountKlineSpec

EXPORT_START_YEAR = 2020


EXPORT_START_MONTH = 1


SPECS: tuple[MountKlineSpec, ...] = (
    MountKlineSpec('perp_time_1m', 'time', 1, 'perp/time/1m'),
    MountKlineSpec('perp_time_15m', 'time', 15, 'perp/time/15m'),
    MountKlineSpec('perp_time_30m', 'time', 30, 'perp/time/30m'),
    MountKlineSpec('perp_time_1h', 'time', 60, 'perp/time/1h'),
    MountKlineSpec('perp_time_2h', 'time', 120, 'perp/time/2h'),
    MountKlineSpec('perp_time_4h', 'time', 240, 'perp/time/4h'),
    MountKlineSpec('perp_dollar_1M', 'dollar', 1, 'perp/dollar/1M'),
    MountKlineSpec('perp_dollar_15M', 'dollar', 15, 'perp/dollar/15M'),
    MountKlineSpec('perp_dollar_30M', 'dollar', 30, 'perp/dollar/30M'),
    MountKlineSpec('perp_dollar_60M', 'dollar', 60, 'perp/dollar/60M'),
    MountKlineSpec('perp_dollar_120M', 'dollar', 120, 'perp/dollar/120M'),
    MountKlineSpec('perp_dollar_240M', 'dollar', 240, 'perp/dollar/240M'),
)
