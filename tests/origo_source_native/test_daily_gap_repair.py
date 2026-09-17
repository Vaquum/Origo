from __future__ import annotations

from datetime import date
from typing import Any

from dagster import DefaultScheduleStatus, SkipReason

from origo.utils.daily_gap_repair import gap_repair_run_requests, repair_window


def _futures_client_and_spec(origo_definitions_module: Any) -> tuple[Any, str, Any]:
    settings = origo_definitions_module.get_origo_clickhouse_settings()
    client = origo_definitions_module.make_origo_clickhouse_client(settings)
    return client, settings.database, origo_definitions_module.FUTURES_DAILY_GAP_REPAIR_SPEC


def test_missing_day_with_archive_is_requested_once_per_day(
    materialize_binance_futures_raw_assets: Any,
    origo_definitions_module: Any,
) -> None:
    materialize_binance_futures_raw_assets(partition_key='2019-09-08')
    client, database, spec = _futures_client_and_spec(origo_definitions_module)

    try:
        result = gap_repair_run_requests(client, database, spec, date(2019, 9, 11), frozenset())
    finally:
        client.disconnect()

    assert isinstance(result, list)
    assert [request.partition_key for request in result] == ['2019-09-09']
    assert result[0].run_key == 'daily_gap_repair:futures:2019-09-09:2019-09-11'


def test_day_with_in_progress_run_is_not_repaired(
    materialize_binance_futures_raw_assets: Any,
    origo_definitions_module: Any,
) -> None:
    materialize_binance_futures_raw_assets(partition_key='2019-09-08')
    client, database, spec = _futures_client_and_spec(origo_definitions_module)

    try:
        result = gap_repair_run_requests(
            client, database, spec, date(2019, 9, 11), frozenset({date(2019, 9, 9)})
        )
    finally:
        client.disconnect()

    assert isinstance(result, SkipReason)


def test_loaded_window_skips(
    materialize_binance_futures_raw_assets: Any,
    origo_definitions_module: Any,
) -> None:
    materialize_binance_futures_raw_assets(partition_key='2019-09-08')
    client, database, spec = _futures_client_and_spec(origo_definitions_module)

    try:
        result = gap_repair_run_requests(client, database, spec, date(2019, 9, 10), frozenset())
    finally:
        client.disconnect()

    assert isinstance(result, SkipReason)


def test_repair_window_leaves_yesterday_to_the_daily_tick() -> None:
    window = repair_window(date(2024, 1, 4), date(2017, 8, 17))
    assert window is not None
    start, end = window
    assert end == date(2024, 1, 2)
    assert start == date(2023, 12, 21)
    assert repair_window(date(2017, 8, 18), date(2017, 8, 17)) is None


def test_definitions_wires_gap_repair_schedules(
    origo_definitions_module: Any,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    for name, job_name, market in (
        (
            'binance_futures_daily_gap_repair_schedule',
            'refresh_binance_futures_data_source_job',
            'futures',
        ),
    ):
        schedule_def = repository_def.get_schedule_def(name)
        assert schedule_def.cron_schedule == '30 * * * *'
        assert schedule_def.default_status is DefaultScheduleStatus.RUNNING
        assert schedule_def.job_name == job_name
        spec = getattr(
            origo_definitions_module, f'{market.upper()}_DAILY_GAP_REPAIR_SPEC'
        )
        assert spec.market == market

