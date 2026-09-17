"""Relocated verbatim from origo/assets/publish_binance_spot_klines_to_mount.py; behaviour unchanged."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

DEFAULT_MOUNT_DIR = '/opt/parquet'


EXPORT_START_YEAR = 2020


EXPORT_START_MONTH = 1


@dataclass(frozen=True)
class MountKlineSpec:
    name: str
    family: Literal['time', 'dollar']
    size: int  # interval minutes (time) | dollar ratio (dollar)
    sub_path: str  # e.g. "time/1m" or "dollar/1M"


SPECS: tuple[MountKlineSpec, ...] = (
    MountKlineSpec('time_1m', 'time', 1, 'time/1m'),
    MountKlineSpec('time_15m', 'time', 15, 'time/15m'),
    MountKlineSpec('time_30m', 'time', 30, 'time/30m'),
    MountKlineSpec('time_1h', 'time', 60, 'time/1h'),
    MountKlineSpec('time_2h', 'time', 120, 'time/2h'),
    MountKlineSpec('time_4h', 'time', 240, 'time/4h'),
    MountKlineSpec('dollar_1M', 'dollar', 1, 'dollar/1M'),
    MountKlineSpec('dollar_15M', 'dollar', 15, 'dollar/15M'),
    MountKlineSpec('dollar_30M', 'dollar', 30, 'dollar/30M'),
    MountKlineSpec('dollar_60M', 'dollar', 60, 'dollar/60M'),
    MountKlineSpec('dollar_120M', 'dollar', 120, 'dollar/120M'),
    MountKlineSpec('dollar_240M', 'dollar', 240, 'dollar/240M'),
)


def _mount_dir() -> Path:
    return Path(os.environ.get('LOCAL_PARQUET_DIR', DEFAULT_MOUNT_DIR))


def month_path(sub_path: str, year: int, month: int) -> Path:
    return _mount_dir() / sub_path / f'{year:04d}' / f'{month:02d}.parquet'
