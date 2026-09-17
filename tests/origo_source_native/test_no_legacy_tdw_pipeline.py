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
)

REMOVED_UTIL_MODULES = (
    'asset_insert_to_tdw',
    'get_tdw_monthly_table_config',
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
