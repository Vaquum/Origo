"""Public perp publications rendered from a pinned source state.

Declaration over consumer_base: the twelve-series dataset map, the perp-scoped
series specs, and the CANARY consumer tuple (local mount plus upload-less shadow).
Series paths carry the perp prefix so the perp mirror never collides with spot's.
"""

from __future__ import annotations

from typing import cast

from huggingface_hub import HfApi

from ..contracts import ConsumerRenderer, ConsumerSpec, Snapshot, SnapshotReader
from .consumer_base import ConsumerDeclaration
from .consumer_base import huggingface as _render_huggingface
from .consumer_base import mount as _render_mount
from .formulas import perp_huggingface as perp_snapshot
from .formulas.perp_series import EXPORT_START_MONTH, EXPORT_START_YEAR, SPECS

EXPORT_START_DATE = f'{EXPORT_START_YEAR:04d}-{EXPORT_START_MONTH:02d}-01'


# The public Hugging Face datasets, exactly as the retired per-series publishers named them.
HUGGINGFACE_DATASETS: dict[str, tuple[str, str | None, str, str]] = {
    'perp_time_1m': (
        'vaquum/binance_btcusdt_perp_1m_klines',
        'HUGGINGFACE_PERP_DATASET_REPO_ID',
        'btcusdt_perp_1m_kline_20200101_to_',
        '1-minute',
    ),
    'perp_time_15m': (
        'vaquum/binance_btcusdt_perp_15m_klines',
        None,
        'btcusdt_perp_15m_kline_20200101_to_',
        '15-minute',
    ),
    'perp_time_30m': (
        'vaquum/binance_btcusdt_perp_30m_klines',
        None,
        'btcusdt_perp_30m_kline_20200101_to_',
        '30-minute',
    ),
    'perp_time_1h': (
        'vaquum/binance_btcusdt_perp_1h_klines',
        None,
        'btcusdt_perp_1h_kline_20200101_to_',
        '1-hour',
    ),
    'perp_time_2h': (
        'vaquum/binance_btcusdt_perp_2h_klines',
        None,
        'btcusdt_perp_2h_kline_20200101_to_',
        '2-hour',
    ),
    'perp_time_4h': (
        'vaquum/binance_btcusdt_perp_4h_klines',
        None,
        'btcusdt_perp_4h_kline_20200101_to_',
        '4-hour',
    ),
    'perp_dollar_1M': (
        'vaquum/binance_btcusdt_perp_1M_klines',
        None,
        'btcusdt_perp_1M_kline_20200101_to_',
        '1M-dollar',
    ),
    'perp_dollar_15M': (
        'vaquum/binance_btcusdt_perp_15M_klines',
        None,
        'btcusdt_perp_15M_kline_20200101_to_',
        '15M-dollar',
    ),
    'perp_dollar_30M': (
        'vaquum/binance_btcusdt_perp_30M_klines',
        None,
        'btcusdt_perp_30M_kline_20200101_to_',
        '30M-dollar',
    ),
    'perp_dollar_60M': (
        'vaquum/binance_btcusdt_perp_60M_klines',
        None,
        'btcusdt_perp_60M_kline_20200101_to_',
        '60M-dollar',
    ),
    'perp_dollar_120M': (
        'vaquum/binance_btcusdt_perp_120M_klines',
        None,
        'btcusdt_perp_120M_kline_20200101_to_',
        '120M-dollar',
    ),
    'perp_dollar_240M': (
        'vaquum/binance_btcusdt_perp_240M_klines',
        None,
        'btcusdt_perp_240M_kline_20200101_to_',
        '240M-dollar',
    ),
}

_DECL = ConsumerDeclaration(
    specs=SPECS,
    export_start_date=EXPORT_START_DATE,
    datasets=HUGGINGFACE_DATASETS,
    scope_staging_to_source=True,
    renderer_label='perp',
    label_of=lambda name: name.split('_')[2],
    commit_infix='perp ',
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
        time_klines=perp_snapshot.get_perp_klines_from_1m_projection,
        dollar_klines=perp_snapshot.get_perp_dollar_klines,
        time_card=perp_snapshot.build_time_dataset_card,
        dollar_card=perp_snapshot.build_dollar_dataset_card,
    )


Renderer = ConsumerRenderer

PERP_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount), public=True),
    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),
)
