from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster import DefaultScheduleStatus, build_schedule_context

from .helpers import (
    BINANCE_FUTURES_DATASET_SOURCE,
    ORIGO_DATABASE,
    load_expected_futures_ledger_payload,
    load_expected_futures_trade_count,
    load_expected_futures_trade_rows,
)

TDW_CONTRACT_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / 'fixtures' / 'origo' / 'binance_futures_1m_contract.json'
)
TDW_KLINE_COLUMN_TYPES = [
    'DateTime',
    *(['Float64'] * 10),
    'UInt64',
    *(['Float64'] * 7),
]
TDW_ALIGNED_COLUMN_TYPES = ['LowCardinality(String)', *TDW_KLINE_COLUMN_TYPES]


def _table_metadata(query_origo, table_name: str) -> tuple[str, str, str]:
    rows = query_origo(
        f"""
        SELECT engine, partition_key, sorting_key
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name = '{table_name}'
        """
    )

    assert len(rows) == 1
    engine, partition_key, sorting_key = rows[0]
    return str(engine), str(partition_key), str(sorting_key)


def _load_tdw_contract_fixture() -> dict[str, Any]:
    return json.loads(TDW_CONTRACT_FIXTURE_PATH.read_text())


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return value


def _rows_to_dicts(columns: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {column: _normalize_value(value) for column, value in zip(columns, row, strict=True)}
        for row in rows
    ]


def _evaluate_schedule(
    origo_definitions_module,
    schedule_name: str,
    scheduled_time: datetime,
) -> object:
    repository_def = origo_definitions_module.defs.get_repository_def()
    context = build_schedule_context(
        scheduled_execution_time=scheduled_time,
        repository_def=repository_def,
    )
    schedule_def = repository_def.get_schedule_def(schedule_name)
    result = schedule_def.evaluate_tick(context)
    return result.run_requests


def test_binance_daily_futures_trades_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['FUTURES_RAW_TABLE_NAME'] == 'binance_daily_futures_trades'
    assert origo_assets['FUTURES_LEDGER_TABLE_NAME'] == 'binance_daily_futures_trades_ingestion'


def test_binance_futures_klines_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['FUTURES_KLINES_TABLE_NAME'] == 'binance_futures_klines'


def test_daily_binance_futures_pipeline_schedule_targets_binance_futures_data_source_job(
    origo_definitions_module,
) -> None:
    schedule_def = origo_definitions_module.daily_binance_futures_pipeline_schedule
    resolved_schedule_def = origo_definitions_module.defs.get_repository_def().get_schedule_def(
        'daily_binance_futures_pipeline_schedule'
    )
    job_def = origo_definitions_module.defs.get_job_def('refresh_binance_futures_data_source_job')
    node_names = set(job_def.graph.node_dict.keys())

    assert not hasattr(origo_definitions_module, 'daily_futures_pipeline_schedule')
    assert schedule_def.job.name == 'refresh_binance_futures_data_source_job'
    assert resolved_schedule_def.cron_schedule == '0 10 * * *'
    assert resolved_schedule_def.execution_timezone == 'UTC'
    assert resolved_schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert node_names >= {
        'insert_daily_binance_futures_trades_to_origo',
        'refresh_binance_futures_klines_origo',
        'refresh_aligned_1m_exchange_from_binance_futures_origo',
    }


def test_daily_binance_futures_pipeline_schedule_requests_latest_daily_partition(
    origo_definitions_module,
    materialize_binance_futures_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = _evaluate_schedule(
        origo_definitions_module,
        'daily_binance_futures_pipeline_schedule',
        datetime(2019, 9, 9, 10, tzinfo=timezone.utc),
    )

    assert isinstance(result, list)
    assert [request.partition_key for request in result] == ['2019-09-08']
    assert [request.run_key for request in result] == ['2019-09-08']

    materialize_result = materialize_binance_futures_data_source_assets(
        partition_key=result[0].partition_key
    )
    assert materialize_result.success

    rows = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{BINANCE_FUTURES_DATASET_SOURCE}'
          AND toDate(datetime) = toDate('{result[0].partition_key}')
        """
    )
    assert rows[0][0] > 0


def test_daily_binance_futures_ingest_retries_hourly_for_late_binance_archives(
    origo_assets: dict[str, object],
) -> None:
    retry_policy = origo_assets['insert_daily_binance_futures_trades_to_origo'].op.retry_policy

    assert retry_policy is not None
    assert retry_policy.max_retries == 23
    assert retry_policy.delay == 3600


def test_binance_source_template_schedules_are_registered_in_defs(
    origo_definitions_module,
) -> None:
    schedule_names = {schedule.name for schedule in origo_definitions_module.defs.schedules}

    assert 'daily_binance_futures_pipeline_schedule' in schedule_names
    assert 'daily_pipeline_schedule' not in schedule_names
    assert 'daily_spot_pipeline_schedule' not in schedule_names
    assert 'daily_futures_pipeline_schedule' not in schedule_names


def test_insert_daily_binance_futures_trades_to_origo_accepts_headerless_fixture_day(
    materialize_binance_futures_raw_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_futures_raw_assets(partition_key='2019-09-08')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            futures_trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            datetime
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2019-09-08')
        ORDER BY datetime, futures_trade_id
        """
    )

    assert rows == load_expected_futures_trade_rows('2019-09-08')


def test_insert_daily_binance_futures_trades_to_origo_accepts_headered_fixture_day(
    materialize_binance_futures_raw_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_futures_raw_assets(partition_key='2024-04-20')
    assert result.success

    count_rows = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2024-04-20')
        """
    )
    first_rows = query_origo(
        f"""
        SELECT
            futures_trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            datetime
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2024-04-20')
        ORDER BY datetime, futures_trade_id
        LIMIT 3
        """
    )

    assert count_rows == [(load_expected_futures_trade_count('2024-04-20'),)]
    assert first_rows == load_expected_futures_trade_rows('2024-04-20', limit=3)


def test_binance_daily_futures_trades_ingestion_ledger_matches_fixture_day(
    materialize_binance_futures_raw_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    expected = load_expected_futures_ledger_payload('2019-09-08')

    result = materialize_binance_futures_raw_assets(partition_key='2019-09-08')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            toString(source_date),
            source_file,
            dagster_partition_key,
            zip_checksum,
            csv_checksum,
            source_row_count,
            inserted_row_count,
            status
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_LEDGER_TABLE_NAME']}
        WHERE source_date = toDate('2019-09-08')
        """
    )

    assert rows == [
        (
            expected['source_date'],
            expected['source_file'],
            expected['source_date'],
            expected['zip_checksum'],
            expected['csv_checksum'],
            expected['source_row_count'],
            expected['source_row_count'],
            'success',
        )
    ]


def test_binance_futures_klines_schema_matches_tdw_contract_fixture(
    materialize_binance_futures_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()

    result = materialize_binance_futures_data_source_assets(partition_key=fixture['fixture_day'])
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['FUTURES_KLINES_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == fixture['columns']
    assert [type_name for _, type_name, *_ in rows] == TDW_KLINE_COLUMN_TYPES
    assert _table_metadata(query_origo, origo_assets['FUTURES_KLINES_TABLE_NAME']) == (
        'MergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_futures_klines_rows_match_tdw_contract_fixture_for_fixture_day(
    materialize_binance_futures_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()
    fixture_day = fixture['fixture_day']
    columns = fixture['columns']

    result = materialize_binance_futures_data_source_assets(partition_key=fixture_day)
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_KLINES_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('{fixture_day}')
        ORDER BY datetime
        """
    )

    assert _rows_to_dicts(columns, rows) == fixture['binance_futures_klines_rows']


def test_aligned_1m_exchange_rows_match_binance_futures_contract_fixture_plus_dataset_source(
    materialize_binance_futures_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()
    fixture_day = fixture['fixture_day']
    columns = ['dataset_source', *fixture['columns']]

    result = materialize_binance_futures_data_source_assets(partition_key=fixture_day)
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_FUTURES_DATASET_SOURCE"]}'
          AND toDate(datetime) = toDate('{fixture_day}')
        ORDER BY datetime
        """
    )

    assert _rows_to_dicts(columns, rows) == fixture['aligned_1m_exchange_rows']


def test_same_partition_rerun_is_idempotent_across_futures_raw_single_source_and_aligned(
    materialize_binance_futures_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    kline_columns = _load_tdw_contract_fixture()['columns']
    aligned_columns = ['dataset_source', *kline_columns]

    first = materialize_binance_futures_data_source_assets(partition_key='2019-09-08')
    first_raw_count = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2019-09-08')
        """
    )
    first_kline_rows = query_origo(
        f"""
        SELECT
            {', '.join(kline_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_KLINES_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2019-09-08')
        ORDER BY datetime
        """
    )
    first_aligned_rows = query_origo(
        f"""
        SELECT
            {', '.join(aligned_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_FUTURES_DATASET_SOURCE"]}'
          AND toDate(datetime) = toDate('2019-09-08')
        ORDER BY datetime
        """
    )

    second = materialize_binance_futures_data_source_assets(partition_key='2019-09-08')
    second_raw_count = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2019-09-08')
        """
    )
    second_kline_rows = query_origo(
        f"""
        SELECT
            {', '.join(kline_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['FUTURES_KLINES_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2019-09-08')
        ORDER BY datetime
        """
    )
    second_aligned_rows = query_origo(
        f"""
        SELECT
            {', '.join(aligned_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_FUTURES_DATASET_SOURCE"]}'
          AND toDate(datetime) = toDate('2019-09-08')
        ORDER BY datetime
        """
    )

    assert first.success
    assert second.success
    assert first_raw_count == second_raw_count == [(load_expected_futures_trade_count('2019-09-08'),)]
    assert first_kline_rows == second_kline_rows
    assert first_aligned_rows == second_aligned_rows


def test_aligned_1m_exchange_holds_the_futures_dataset_source(
    materialize_binance_futures_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_futures_data_source_assets(partition_key='2019-09-08')

    rows = query_origo(
        f"""
        SELECT dataset_source, count()
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        GROUP BY dataset_source
        ORDER BY dataset_source
        """
    )

    assert result.success
    assert rows == [
        (BINANCE_FUTURES_DATASET_SOURCE, len(_load_tdw_contract_fixture()['aligned_1m_exchange_rows'])),
    ]
