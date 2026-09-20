"""Public spot publications rendered from a pinned source state.

Declaration over consumer_base: the twelve-series dataset map, the unscoped legacy
series specs, and the LIVE consumer tuple.
"""

from __future__ import annotations

from typing import cast

from huggingface_hub import HfApi

from ..contracts import ConsumerRenderer, ConsumerSpec, Snapshot, SnapshotReader
from .consumer_base import ConsumerDeclaration
from .consumer_base import huggingface as _render_huggingface
from .consumer_base import mount as _render_mount
from .formulas import huggingface_dollar as dollar_snapshot
from .formulas import huggingface_time as time_snapshot
from .formulas.spot_series import EXPORT_START_MONTH, EXPORT_START_YEAR, SPECS

EXPORT_START_DATE = f'{EXPORT_START_YEAR:04d}-{EXPORT_START_MONTH:02d}-01'


# The public Hugging Face datasets, exactly as the retired per-series publishers named them.
HUGGINGFACE_DATASETS: dict[str, tuple[str, str | None, str, str]] = {
    'time_1m': (
        'vaquum/binance_btcusdt_1m_klines',
        'HUGGINGFACE_DATASET_REPO_ID',
        'btcusdt_1m_kline_20200101_to_',
        '1-minute',
    ),
    'time_15m': (
        'vaquum/binance_btcusdt_15m_klines',
        None,
        'btcusdt_15m_kline_20200101_to_',
        '15-minute',
    ),
    'time_30m': (
        'vaquum/binance_btcusdt_30m_klines',
        None,
        'btcusdt_30m_kline_20200101_to_',
        '30-minute',
    ),
    'time_1h': (
        'vaquum/binance_btcusdt_1h_klines',
        None,
        'btcusdt_1h_kline_20200101_to_',
        '1-hour',
    ),
    'time_2h': (
        'vaquum/binance_btcusdt_2h_klines',
        None,
        'btcusdt_2h_kline_20200101_to_',
        '2-hour',
    ),
    'time_4h': (
        'vaquum/binance_btcusdt_4h_klines',
        None,
        'btcusdt_4h_kline_20200101_to_',
        '4-hour',
    ),
    'dollar_1M': (
        'vaquum/binance_btcusdt_1M_dollar_klines',
        None,
        'btcusdt_1M_dollar_kline_20200101_to_',
        '1M-dollar',
    ),
    'dollar_15M': (
        'vaquum/binance_btcusdt_15M_dollar_klines',
        None,
        'btcusdt_15M_dollar_kline_20200101_to_',
        '15M-dollar',
    ),
    'dollar_30M': (
        'vaquum/binance_btcusdt_30M_dollar_klines',
        None,
        'btcusdt_30M_dollar_kline_20200101_to_',
        '30M-dollar',
    ),
    'dollar_60M': (
        'vaquum/binance_btcusdt_60M_dollar_klines',
        None,
        'btcusdt_60M_dollar_kline_20200101_to_',
        '60M-dollar',
    ),
    'dollar_120M': (
        'vaquum/binance_btcusdt_120M_dollar_klines',
        None,
        'btcusdt_120M_dollar_kline_20200101_to_',
        '120M-dollar',
    ),
    'dollar_240M': (
        'vaquum/binance_btcusdt_240M_dollar_klines',
        None,
        'btcusdt_240M_dollar_kline_20200101_to_',
        '240M-dollar',
    ),
}

_DECL = ConsumerDeclaration(
    specs=SPECS,
    export_start_date=EXPORT_START_DATE,
    datasets=HUGGINGFACE_DATASETS,
    scope_staging_to_source=False,
    renderer_label='spot',
    label_of=lambda name: name.split('_', 1)[1],
    commit_infix='',
)


def _mount(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    allow_full: bool = False,
) -> None:
    _render_mount(reader, snapshot, destination, decl=_DECL, allow_full=allow_full)


def _huggingface(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    allow_full: bool = False,
) -> None:
    _ = allow_full  # Snapshot renders always run whole; no worker cap applies.
    # Snapshot callables resolve through their modules at call time (patch seams).
    _render_huggingface(
        reader,
        snapshot,
        destination,
        decl=_DECL,
        hf_api=HfApi,
        time_klines=time_snapshot.get_binance_spot_klines_from_1m_projection,
        dollar_klines=dollar_snapshot.get_binance_spot_dollar_klines,
        time_card=time_snapshot.build_dataset_card,
        dollar_card=dollar_snapshot.build_dataset_card,
    )


Renderer = ConsumerRenderer

SPOT_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount), public=True),
    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),
)
