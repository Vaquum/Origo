from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ['HistoricalData', 'compute_data_bars']

if TYPE_CHECKING:
    from origo.data.historical_data import HistoricalData as HistoricalData
    from origo.data.utils.compute_data_bars import (
        compute_data_bars as compute_data_bars,
    )


def __getattr__(name: str) -> Any:
    if name == 'HistoricalData':
        from origo.data.historical_data import HistoricalData

        return HistoricalData
    if name == 'compute_data_bars':
        from origo.data.utils.compute_data_bars import compute_data_bars

        return compute_data_bars
    raise AttributeError(f"module 'origo' has no attribute '{name}'")
