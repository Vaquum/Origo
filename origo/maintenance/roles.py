"""Explicit disposal policy. Unknown and mixed jobs keep their provenance."""

from typing import Literal

from dagster import DagsterRun

PROJECTION_JOBS = frozenset(
    {
        'maintain_operational_metadata_job',
        'build_bar_store_arrow_job',
        'build_depth_snapshot_store_arrow_job',
        'publish_binance_spot_klines_to_mount_job',
        'backfill_binance_spot_klines_to_mount_job',
        'publish_btc_briefing_feed_job',
        'publish_btc_briefing_history_job',
        'backfill_binance_spot_dollar_klines_origo_job',
        'repair_binance_spot_depth20_projection_job',
        'repair_binance_spot_depth200_projection_job',
        'publish_binance_spot_klines_to_huggingface_job',
        'publish_binance_spot_15m_klines_to_huggingface_job',
        'publish_binance_spot_30m_klines_to_huggingface_job',
        'publish_binance_spot_1h_klines_to_huggingface_job',
        'publish_binance_spot_2h_klines_to_huggingface_job',
        'publish_binance_spot_4h_klines_to_huggingface_job',
        'publish_binance_spot_1M_dollar_klines_to_huggingface_job',
        'publish_binance_spot_15M_dollar_klines_to_huggingface_job',
        'publish_binance_spot_30M_dollar_klines_to_huggingface_job',
        'publish_binance_spot_60M_dollar_klines_to_huggingface_job',
        'publish_binance_spot_120M_dollar_klines_to_huggingface_job',
        'publish_binance_spot_240M_dollar_klines_to_huggingface_job',
    }
)
SOURCE_JOBS = frozenset(
    {
        'refresh_binance_spot_data_source_job',
        'refresh_binance_futures_data_source_job',
        'refresh_binance_spot_latest_data_source_job',
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'backfill_binance_spot_trades_origo_job',
        'backfill_binance_spot_depth20_data_source_job',
        'backfill_binance_spot_depth200_data_source_job',
        'insert_daily_binance_spot_trades_to_origo_job',
        'insert_daily_binance_futures_trades_to_origo_job',
        'insert_monthly_trades_to_tdw_job',
        'insert_monthly_futures_trades_to_tdw_job',
        'insert_monthly_agg_trades_to_tdw_job',
        'insert_monthly_futures_agg_trades_to_tdw_job',
    }
)


def run_role(run: DagsterRun) -> Literal['source', 'projection', 'unknown']:
    if run.tags.get('origo_source_key') or run.job_name in SOURCE_JOBS:
        return 'source'
    if run.job_name in PROJECTION_JOBS:
        return 'projection'
    return 'unknown'
