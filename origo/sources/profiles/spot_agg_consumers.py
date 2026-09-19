"""Public spot aggregate publications rendered from a pinned source state.

Declaration over consumer_base: the twelve-series dataset map, the
spot_agg-scoped series specs, and the LIVE consumer tuple (local mount plus
uploading huggingface). Series paths carry the spot_agg prefix so the aggregate
mirror never collides with the trades sources'.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from huggingface_hub import HfApi

from ..contracts import ConsumerSpec, Snapshot, SnapshotReader
from .consumer_base import ConsumerDeclaration
from .consumer_base import huggingface as _render_huggingface
from .consumer_base import mount as _render_mount
from .formulas import spot_agg_huggingface as agg_snapshot
from .formulas.spot_agg_series import EXPORT_START_MONTH, EXPORT_START_YEAR, SPECS

EXPORT_START_DATE = f'{EXPORT_START_YEAR:04d}-{EXPORT_START_MONTH:02d}-01'


HUGGINGFACE_DATASETS: dict[str, tuple[str, str | None, str, str]] = {
    'spot_agg_time_1m': (
        'vaquum/binance_btcusdt_spot_agg_1m_klines',
        'HUGGINGFACE_SPOT_AGG_DATASET_REPO_ID',
        'btcusdt_spot_agg_1m_kline_20200101_to_',
        '1-minute',
    ),
    'spot_agg_time_15m': (
        'vaquum/binance_btcusdt_spot_agg_15m_klines',
        None,
        'btcusdt_spot_agg_15m_kline_20200101_to_',
        '15-minute',
    ),
    'spot_agg_time_30m': (
        'vaquum/binance_btcusdt_spot_agg_30m_klines',
        None,
        'btcusdt_spot_agg_30m_kline_20200101_to_',
        '30-minute',
    ),
    'spot_agg_time_1h': (
        'vaquum/binance_btcusdt_spot_agg_1h_klines',
        None,
        'btcusdt_spot_agg_1h_kline_20200101_to_',
        '1-hour',
    ),
    'spot_agg_time_2h': (
        'vaquum/binance_btcusdt_spot_agg_2h_klines',
        None,
        'btcusdt_spot_agg_2h_kline_20200101_to_',
        '2-hour',
    ),
    'spot_agg_time_4h': (
        'vaquum/binance_btcusdt_spot_agg_4h_klines',
        None,
        'btcusdt_spot_agg_4h_kline_20200101_to_',
        '4-hour',
    ),
    'spot_agg_dollar_1M': (
        'vaquum/binance_btcusdt_spot_agg_1M_klines',
        None,
        'btcusdt_spot_agg_1M_kline_20200101_to_',
        '1M-dollar',
    ),
    'spot_agg_dollar_15M': (
        'vaquum/binance_btcusdt_spot_agg_15M_klines',
        None,
        'btcusdt_spot_agg_15M_kline_20200101_to_',
        '15M-dollar',
    ),
    'spot_agg_dollar_30M': (
        'vaquum/binance_btcusdt_spot_agg_30M_klines',
        None,
        'btcusdt_spot_agg_30M_kline_20200101_to_',
        '30M-dollar',
    ),
    'spot_agg_dollar_60M': (
        'vaquum/binance_btcusdt_spot_agg_60M_klines',
        None,
        'btcusdt_spot_agg_60M_kline_20200101_to_',
        '60M-dollar',
    ),
    'spot_agg_dollar_120M': (
        'vaquum/binance_btcusdt_spot_agg_120M_klines',
        None,
        'btcusdt_spot_agg_120M_kline_20200101_to_',
        '120M-dollar',
    ),
    'spot_agg_dollar_240M': (
        'vaquum/binance_btcusdt_spot_agg_240M_klines',
        None,
        'btcusdt_spot_agg_240M_kline_20200101_to_',
        '240M-dollar',
    ),
}

_DECL = ConsumerDeclaration(
    specs=SPECS,
    export_start_date=EXPORT_START_DATE,
    datasets=HUGGINGFACE_DATASETS,
    scope_staging_to_source=True,
    renderer_label='spot aggregate',
    label_of=lambda name: name.split('_')[3],
    commit_infix='spot aggregate ',
    id_column='agg_trade_id',
    quote_expr='(price * quantity)',
)


def _mount(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
    _render_mount(reader, snapshot, destination, decl=_DECL)


def _huggingface(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
    # Snapshot callables resolve through their modules at call time (patch seams).
    _render_huggingface(
        reader,
        snapshot,
        destination,
        decl=_DECL,
        hf_api=HfApi,
        time_klines=agg_snapshot.get_spot_agg_klines_from_1m_projection,
        dollar_klines=agg_snapshot.get_spot_agg_dollar_klines,
        time_card=agg_snapshot.build_time_dataset_card,
        dollar_card=agg_snapshot.build_dollar_dataset_card,
    )


Renderer = Callable[[SnapshotReader, Snapshot, str], None]

SPOT_AGG_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount), public=True),
    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),
)
