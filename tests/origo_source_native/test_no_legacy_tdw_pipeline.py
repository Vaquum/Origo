from __future__ import annotations

import importlib.util

REMOVED_ASSET_MODULES = (
    'cleanup_binance_daily_trades',
    'create_binance_agg_trades_table',
    'create_binance_daily_trades_table',
    'create_binance_futures_trades_table',
    'create_binance_trades_complete_view',
    'create_binance_trades_daily_summary',
    'create_binance_trades_day_of_month_summary',
    'create_binance_trades_hour_of_day_summary',
    'create_binance_trades_hourly_summary',
    'create_binance_trades_month_of_year_summary',
    'create_binance_trades_monthly_summary',
    'create_binance_trades_table',
    'create_binance_trades_week_of_year_summary',
    'create_tdw_database',
    'create_tdw_database_v2',
    'daily_trades_to_tdw',
    'monthly_agg_trades_to_tdw',
    'monthly_futures_agg_trades_to_tdw',
    'monthly_futures_trades_to_tdw',
    'monthly_trades_to_tdw',
    'publish_binance_spot_klines_to_huggingface',
    'publish_binance_spot_15m_klines_to_huggingface',
    'publish_binance_spot_30m_klines_to_huggingface',
    'publish_binance_spot_1h_klines_to_huggingface',
    'publish_binance_spot_2h_klines_to_huggingface',
    'publish_binance_spot_4h_klines_to_huggingface',
    'publish_binance_spot_1M_dollar_klines_to_huggingface',
    'publish_binance_spot_15M_dollar_klines_to_huggingface',
    'publish_binance_spot_30M_dollar_klines_to_huggingface',
    'publish_binance_spot_60M_dollar_klines_to_huggingface',
    'publish_binance_spot_120M_dollar_klines_to_huggingface',
    'publish_binance_spot_240M_dollar_klines_to_huggingface',
    'daily_trades_to_origo',
    'create_binance_trades_table_origo',
    'create_binance_spot_klines_table_origo',
    'create_binance_spot_dollar_klines_table_origo',
    'create_binance_spot_volume_klines_table_origo',
    'create_binance_spot_tick_klines_table_origo',
    'create_binance_spot_dollar_imbalance_klines_table_origo',
    'create_binance_spot_latest_tables_origo',
    'refresh_binance_spot_klines_origo',
    'refresh_binance_spot_dollar_klines_origo',
    'refresh_binance_spot_volume_klines_origo',
    'refresh_binance_spot_tick_klines_origo',
    'refresh_binance_spot_dollar_imbalance_klines_origo',
    'refresh_aligned_1m_exchange_from_binance_spot_origo',
    'sync_binance_spot_trades_latest_origo',
    'refresh_binance_spot_klines_latest_origo',
    'refresh_binance_spot_dollar_klines_latest_origo',
    'refresh_binance_spot_latest_cuts_origo',
    'cleanup_binance_spot_latest_origo',
    'publish_binance_spot_klines_to_mount',
    'build_bar_store_arrow',
    'create_binance_futures_trades_table_origo',
    'create_binance_futures_klines_table_origo',
    'daily_futures_trades_to_origo',
    'refresh_binance_futures_klines_origo',
    'create_aligned_1m_exchange_table_origo',
    'refresh_aligned_1m_exchange_from_binance_futures_origo',
)

REMOVED_UTIL_MODULES = (
    'asset_insert_to_tdw',
    'get_tdw_monthly_table_config',
    'binance_spot_latest',
    'publish_binance_spot_kline_snapshot_to_huggingface',
    'publish_binance_spot_dollar_kline_snapshot_to_huggingface',
    'binance_file_to_polars',
    'check_if_has_header',
    'atomic_day_write',
    'daily_gap_repair',
)

LEGACY_SUMMARY_NAMES = (
    'create_binance_trades_daily_summary',
    'create_binance_trades_day_of_month_summary',
    'create_binance_trades_hour_of_day_summary',
    'create_binance_trades_hourly_summary',
    'create_binance_trades_month_of_year_summary',
    'create_binance_trades_monthly_summary',
    'create_binance_trades_week_of_year_summary',
)

FORBIDDEN_NAME_TOKENS = ('tdw', 'binance_trades_complete')


def test_definitions_exposes_no_tdw_assets(origo_definitions_module: object) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    asset_keys: set[str] = set()
    for assets_def in defs.assets:
        asset_keys.update(key.to_user_string() for key in assets_def.keys)

    offending = {
        key for key in asset_keys if any(token in key for token in FORBIDDEN_NAME_TOKENS)
    }
    assert offending == set()
    assert asset_keys.isdisjoint(LEGACY_SUMMARY_NAMES)


def test_definitions_exposes_no_tdw_jobs_or_schedules(
    origo_definitions_module: object,
) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    job_names = {job.name for job in defs.jobs}
    schedule_names = {schedule.name for schedule in defs.schedules}
    all_names = job_names | schedule_names

    offending = {
        name for name in all_names if any(token in name for token in FORBIDDEN_NAME_TOKENS)
    }
    assert offending == set()
    assert all_names.isdisjoint(f'{name}_job' for name in LEGACY_SUMMARY_NAMES)


def test_tdw_asset_modules_are_absent() -> None:
    present = [
        name
        for name in REMOVED_ASSET_MODULES
        if importlib.util.find_spec(f'origo.assets.{name}') is not None
    ]
    assert present == []

    present_utils = [
        name
        for name in REMOVED_UTIL_MODULES
        if importlib.util.find_spec(f'origo.utils.{name}') is not None
    ]
    assert present_utils == []
    assert importlib.util.find_spec('origo.query.get_binance_spot_klines') is None


def test_legacy_package_is_absent() -> None:
    assert importlib.util.find_spec('tdw_control_plane') is None


def test_legacy_futures_jobs_and_schedules_are_absent(origo_definitions_module: object) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    names = {job.name for job in defs.jobs} | {schedule.name for schedule in defs.schedules}
    assert names.isdisjoint(
        {
            'refresh_binance_futures_data_source_job',
            'daily_binance_futures_pipeline_schedule',
            'binance_futures_daily_gap_repair_schedule',
            'create_binance_daily_futures_trades_table_origo_job',
            'create_binance_futures_klines_table_origo_job',
            'create_aligned_1m_exchange_table_origo_job',
        }
    )
