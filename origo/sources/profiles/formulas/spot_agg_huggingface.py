"""Spot aggregate Hugging Face snapshots. The rollup math is shared with spot; only the dataset cards name the aggregate channel."""

from .huggingface_dollar import (
    get_binance_spot_dollar_klines as get_spot_agg_dollar_klines,
)
from .huggingface_time import (
    get_binance_spot_klines_from_1m_projection as get_spot_agg_klines_from_1m_projection,
)


def build_time_dataset_card(
    *,
    export_end_date: str,
    row_count: int,
    file_name: str,
    cadence_label: str,
    resolution_label: str,
) -> str:
    return f"""# BTCUSDT {cadence_label} spot aggregate klines

This dataset is exported daily from `origo.binance_spot_aggtrades_time_current` at {resolution_label} resolution.

Latest snapshot:

- file: `{file_name}`
- start date: `2020-01-01`
- rows: `{row_count}`
- end date: `{export_end_date}`
- columns: `datetime`, `open`, `high`, `low`, `close`, `mean`, `std`, `volume`, `maker_ratio`, `no_of_trades`, `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`, `liquidity_sum`, `maker_volume`, `maker_liquidity`

Notes:

- Source market: Binance spot BTCUSDT aggregate trades
- Source table: `origo.binance_spot_aggtrades_time_current`
- `median` and `iqr` are intentionally omitted from the exported Parquet snapshot
- Timestamps are UTC
"""


def build_dollar_dataset_card(
    *,
    export_end_date: str,
    row_count: int,
    file_name: str,
    size_label: str,
    resolution_label: str,
    database_name: str,
) -> str:
    return f"""# BTCUSDT {size_label} dollar spot aggregate klines

This dataset is exported daily from `{database_name}.binance_spot_aggtrades_dollar_current` using {resolution_label} dollar bars derived from the 1M Origo dollar-kline foundation.

Latest snapshot:

- file: `{file_name}`
- start date: `2020-01-01`
- rows: `{row_count}`
- end date: `{export_end_date}`
- columns: `start_datetime`, `end_datetime`, `dollar_bar_id`, `open`, `high`, `low`, `close`, `mean`, `std`, `volume`, `maker_ratio`, `no_of_trades`, `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`, `liquidity_sum`, `maker_volume`, `maker_liquidity`

Notes:

- Source market: Binance spot BTCUSDT aggregate trades
- Source table: `{database_name}.binance_spot_aggtrades_dollar_current`
- `median` and `iqr` are intentionally omitted from the exported Parquet snapshot
- Timestamps are UTC
"""
