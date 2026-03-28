# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from dagster import Definitions

from origo.pathing import resolve_repo_relative_path

from .assets.bitcoin_block_fee_totals_to_origo import (
    insert_bitcoin_block_fee_totals_to_origo,
)
from .assets.bitcoin_block_headers_to_origo import (
    insert_bitcoin_block_headers_to_origo,
)
from .assets.bitcoin_block_subsidy_schedule_to_origo import (
    insert_bitcoin_block_subsidy_schedule_to_origo,
)
from .assets.bitcoin_block_transactions_to_origo import (
    insert_bitcoin_block_transactions_to_origo,
)
from .assets.bitcoin_circulating_supply_to_origo import (
    insert_bitcoin_circulating_supply_to_origo,
)
from .assets.bitcoin_mempool_state_to_origo import (
    insert_bitcoin_mempool_state_to_origo,
)
from .assets.bitcoin_network_hashrate_estimate_to_origo import (
    insert_bitcoin_network_hashrate_estimate_to_origo,
)
from .assets.daily_bybit_spot_trades_to_origo import (
    insert_daily_bybit_spot_trades_to_origo,
)
from .assets.daily_okx_spot_trades_to_origo import (
    insert_daily_okx_spot_trades_to_origo,
)
from .assets.daily_trades_to_origo import insert_daily_binance_trades_to_origo
from .assets.monthly_trades_to_origo import insert_monthly_binance_trades_to_origo
from .backfill.runtime_contract import default_exchange_runtime_tags
from .jobs.etf_daily_ingest import (
    origo_etf_daily_backfill_job,
    origo_etf_daily_ingest_job,
)
from .jobs.raw_export_native import origo_raw_export_native_job

define_asset_job: Any = getattr(dg, 'define_asset_job')
schedule: Any = getattr(dg, 'schedule')
run_failure_sensor: Any = getattr(dg, 'run_failure_sensor')
RunRequest: Any = getattr(dg, 'RunRequest')
RunsFilter: Any = getattr(dg, 'RunsFilter')
SkipReason: Any = getattr(dg, 'SkipReason')

# Data Insertion Jobs

insert_monthly_binance_trades_job = define_asset_job(
    name='insert_monthly_trades_to_origo_job',
    selection=['insert_monthly_binance_trades_to_origo'],
)

insert_daily_binance_trades_job = define_asset_job(
    name='insert_daily_trades_to_origo_job',
    selection=['insert_daily_binance_trades_to_origo'],
)

insert_daily_okx_spot_trades_job = define_asset_job(
    name='insert_daily_okx_spot_trades_to_origo_job',
    selection=['insert_daily_okx_spot_trades_to_origo'],
)

insert_daily_bybit_spot_trades_job = define_asset_job(
    name='insert_daily_bybit_spot_trades_to_origo_job',
    selection=['insert_daily_bybit_spot_trades_to_origo'],
)

insert_bitcoin_block_headers_job = define_asset_job(
    name='insert_bitcoin_block_headers_to_origo_job',
    selection=['insert_bitcoin_block_headers_to_origo'],
)

insert_bitcoin_block_fee_totals_job = define_asset_job(
    name='insert_bitcoin_block_fee_totals_to_origo_job',
    selection=['insert_bitcoin_block_fee_totals_to_origo'],
)

insert_bitcoin_block_subsidy_schedule_job = define_asset_job(
    name='insert_bitcoin_block_subsidy_schedule_to_origo_job',
    selection=['insert_bitcoin_block_subsidy_schedule_to_origo'],
)

insert_bitcoin_network_hashrate_estimate_job = define_asset_job(
    name='insert_bitcoin_network_hashrate_estimate_to_origo_job',
    selection=['insert_bitcoin_network_hashrate_estimate_to_origo'],
)

insert_bitcoin_circulating_supply_job = define_asset_job(
    name='insert_bitcoin_circulating_supply_to_origo_job',
    selection=['insert_bitcoin_circulating_supply_to_origo'],
)

insert_bitcoin_block_transactions_job = define_asset_job(
    name='insert_bitcoin_block_transactions_to_origo_job',
    selection=['insert_bitcoin_block_transactions_to_origo'],
)

insert_bitcoin_mempool_state_job = define_asset_job(
    name='insert_bitcoin_mempool_state_to_origo_job',
    selection=['insert_bitcoin_mempool_state_to_origo'],
)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _require_int_env(name: str) -> int:
    raw = _require_env(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got '{raw}'") from exc


def _require_positive_int_env(name: str) -> int:
    value = _require_int_env(name)
    if value <= 0:
        raise RuntimeError(f'{name} must be > 0')
    return value


_ORIGO_ETF_DAILY_SCHEDULE_CRON = _require_env('ORIGO_ETF_DAILY_SCHEDULE_CRON')
_ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE = _require_env(
    'ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE'
)
_ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS = _require_positive_int_env(
    'ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS'
)
_ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS = _require_positive_int_env(
    'ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS'
)
_ORIGO_ETF_ANOMALY_LOG_PATH = _require_env('ORIGO_ETF_ANOMALY_LOG_PATH')
_ORIGO_ETF_DISCORD_WEBHOOK_URL = _require_env('ORIGO_ETF_DISCORD_WEBHOOK_URL')
_ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS = _require_positive_int_env(
    'ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS'
)
_ORIGO_ETF_DISCORD_TIMEOUT_SECONDS = _require_positive_int_env(
    'ORIGO_ETF_DISCORD_TIMEOUT_SECONDS'
)
_ORIGO_ETF_DAILY_RETRY_WINDOW_HOURS = 24
_ORIGO_ETF_DAILY_INGEST_JOB_NAME = 'origo_etf_daily_ingest_job'
_ORIGO_ETF_DAILY_RETRY_ATTEMPT_TAG = 'origo.etf.retry_attempt'
_ORIGO_ETF_DAILY_RETRY_ORIGIN_RUN_ID_TAG = 'origo.etf.retry_origin_run_id'
_ORIGO_ETF_DAILY_RETRY_PARENT_RUN_ID_TAG = 'origo.etf.retry_parent_run_id'
_ORIGO_ETF_DAILY_RETRY_WINDOW_END_TAG = 'origo.etf.retry_window_end_utc'
_ORIGO_ETF_INGEST_ANOMALY_CODE = 'ETF_DAILY_INGEST_RUN_FAILURE'


@schedule(
    job=insert_daily_binance_trades_job,
    cron_schedule='0 1 * * *',
    execution_timezone='UTC',
)
def daily_pipeline_schedule() -> Any:
    return RunRequest(tags=default_exchange_runtime_tags())


@schedule(
    job=origo_etf_daily_ingest_job,
    cron_schedule=_ORIGO_ETF_DAILY_SCHEDULE_CRON,
    execution_timezone=_ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE,
)
def origo_etf_daily_ingest_schedule() -> dict[str, str]:
    return {}


def _parse_retry_attempt_tag(*, value: str | None) -> int:
    if value is None:
        return 0
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError(
            f'{_ORIGO_ETF_DAILY_RETRY_ATTEMPT_TAG} must be an integer when set'
        ) from exc
    if parsed < 0:
        raise RuntimeError(f'{_ORIGO_ETF_DAILY_RETRY_ATTEMPT_TAG} must be >= 0')
    return parsed


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _resolve_path(path_value: str) -> Path:
    return resolve_repo_relative_path(path_value)


def _append_etf_anomaly_log(*, event_payload: Mapping[str, Any]) -> None:
    log_path = _resolve_path(_ORIGO_ETF_ANOMALY_LOG_PATH)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf-8') as handle:
        handle.write(
            json.dumps(dict(event_payload), ensure_ascii=True, sort_keys=True) + '\n'
        )


def _post_etf_discord_alert(*, content: str) -> int:
    response = requests.post(
        _ORIGO_ETF_DISCORD_WEBHOOK_URL,
        json={'content': content},
        timeout=_ORIGO_ETF_DISCORD_TIMEOUT_SECONDS,
    )
    if response.status_code < 200 or response.status_code >= 300:
        raise RuntimeError(
            'ETF Discord webhook request failed '
            f'(status_code={response.status_code}, body={response.text})'
        )
    return int(response.status_code)


def _emit_etf_ingest_anomaly_alert(
    *, anomaly_code: str, run_id: str, details: Mapping[str, Any]
) -> dict[str, Any]:
    emitted_at_utc = datetime.now(UTC).isoformat()
    event_payload: dict[str, Any] = {
        'event_type': 'etf_ingest_anomaly',
        'anomaly_code': anomaly_code,
        'run_id': run_id,
        'emitted_at_utc': emitted_at_utc,
        'details': dict(details),
    }
    _append_etf_anomaly_log(event_payload=event_payload)
    _post_etf_discord_alert(
        content=(
            f'[{anomaly_code}] ETF ingest anomaly run_id={run_id}\n'
            f'```json\n{json.dumps(dict(details), ensure_ascii=True, sort_keys=True)}\n```'
        )
    )
    return event_payload


def _build_etf_daily_retry_response(
    *,
    failed_run_id: str,
    failed_run_tags: Mapping[str, str],
    failed_run_config: Mapping[str, Any],
    failure_created_at_utc: datetime,
    now_utc: datetime,
) -> Any:
    created_at_utc = _to_utc(failure_created_at_utc)
    now_utc_normalized = _to_utc(now_utc)
    retry_window_end = created_at_utc + timedelta(hours=_ORIGO_ETF_DAILY_RETRY_WINDOW_HOURS)
    if now_utc_normalized > retry_window_end:
        return SkipReason(
            'ETF retry skipped: outside retry window '
            f'(created_at_utc={created_at_utc.isoformat()}, '
            f'retry_window_end_utc={retry_window_end.isoformat()}, '
            f'now_utc={now_utc_normalized.isoformat()})'
        )

    current_retry_attempt = _parse_retry_attempt_tag(
        value=failed_run_tags.get(_ORIGO_ETF_DAILY_RETRY_ATTEMPT_TAG)
    )
    if current_retry_attempt >= _ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS:
        return SkipReason(
            'ETF retry skipped: max retry attempts reached '
            f'(current_retry_attempt={current_retry_attempt}, '
            f'max_retry_attempts={_ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS})'
        )

    next_retry_attempt = current_retry_attempt + 1
    origin_run_id = failed_run_tags.get(
        _ORIGO_ETF_DAILY_RETRY_ORIGIN_RUN_ID_TAG,
        failed_run_id,
    )
    retry_tags = dict(failed_run_tags)
    retry_tags[_ORIGO_ETF_DAILY_RETRY_ATTEMPT_TAG] = str(next_retry_attempt)
    retry_tags[_ORIGO_ETF_DAILY_RETRY_ORIGIN_RUN_ID_TAG] = origin_run_id
    retry_tags[_ORIGO_ETF_DAILY_RETRY_PARENT_RUN_ID_TAG] = failed_run_id
    retry_tags[_ORIGO_ETF_DAILY_RETRY_WINDOW_END_TAG] = retry_window_end.isoformat()

    return RunRequest(
        run_key=f'origo-etf-retry-{origin_run_id}-{next_retry_attempt}',
        job_name=_ORIGO_ETF_DAILY_INGEST_JOB_NAME,
        run_config=dict(failed_run_config),
        tags=retry_tags,
    )


@run_failure_sensor(
    name='origo_etf_daily_retry_sensor',
    minimum_interval_seconds=_ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS,
)
def origo_etf_daily_retry_sensor(context: Any) -> Any:
    dagster_run = context.dagster_run
    if dagster_run.job_name != _ORIGO_ETF_DAILY_INGEST_JOB_NAME:
        return SkipReason(
            'ETF retry skipped: run job is not ETF daily ingest '
            f'(job_name={dagster_run.job_name})'
        )

    run_records = context.instance.get_run_records(
        filters=RunsFilter(run_ids=[dagster_run.run_id]),
        limit=1,
    )
    if len(run_records) != 1:
        raise RuntimeError(
            'Expected exactly one run record for retry sensor, '
            f'run_id={dagster_run.run_id}, got={len(run_records)}'
        )

    run_record = run_records[0]
    return _build_etf_daily_retry_response(
        failed_run_id=dagster_run.run_id,
        failed_run_tags=dagster_run.tags,
        failed_run_config=dagster_run.run_config,
        failure_created_at_utc=run_record.create_timestamp,
        now_utc=datetime.now(UTC),
    )


@run_failure_sensor(
    name='origo_etf_ingest_anomaly_alert_sensor',
    minimum_interval_seconds=_ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS,
)
def origo_etf_ingest_anomaly_alert_sensor(context: Any) -> Any:
    dagster_run = context.dagster_run
    if dagster_run.job_name != _ORIGO_ETF_DAILY_INGEST_JOB_NAME:
        return SkipReason(
            'ETF anomaly alert skipped: run job is not ETF daily ingest '
            f'(job_name={dagster_run.job_name})'
        )

    retry_attempt = dagster_run.tags.get(_ORIGO_ETF_DAILY_RETRY_ATTEMPT_TAG, '0')
    _emit_etf_ingest_anomaly_alert(
        anomaly_code=_ORIGO_ETF_INGEST_ANOMALY_CODE,
        run_id=dagster_run.run_id,
        details={
            'job_name': dagster_run.job_name,
            'retry_attempt': retry_attempt,
        },
    )
    return SkipReason(
        'ETF anomaly alert emitted '
        f'(anomaly_code={_ORIGO_ETF_INGEST_ANOMALY_CODE}, run_id={dagster_run.run_id})'
    )


defs = Definitions(
    assets=[
        insert_monthly_binance_trades_to_origo,
        insert_daily_binance_trades_to_origo,
        insert_daily_okx_spot_trades_to_origo,
        insert_daily_bybit_spot_trades_to_origo,
        insert_bitcoin_block_headers_to_origo,
        insert_bitcoin_block_fee_totals_to_origo,
        insert_bitcoin_block_subsidy_schedule_to_origo,
        insert_bitcoin_network_hashrate_estimate_to_origo,
        insert_bitcoin_circulating_supply_to_origo,
        insert_bitcoin_block_transactions_to_origo,
        insert_bitcoin_mempool_state_to_origo,
    ],
    schedules=[daily_pipeline_schedule, origo_etf_daily_ingest_schedule],
    sensors=[origo_etf_daily_retry_sensor, origo_etf_ingest_anomaly_alert_sensor],
    jobs=[
        insert_monthly_binance_trades_job,
        insert_daily_binance_trades_job,
        insert_daily_okx_spot_trades_job,
        insert_daily_bybit_spot_trades_job,
        insert_bitcoin_block_headers_job,
        insert_bitcoin_block_fee_totals_job,
        insert_bitcoin_block_subsidy_schedule_job,
        insert_bitcoin_network_hashrate_estimate_job,
        insert_bitcoin_circulating_supply_job,
        insert_bitcoin_block_transactions_job,
        insert_bitcoin_mempool_state_job,
        origo_etf_daily_backfill_job,
        origo_etf_daily_ingest_job,
        origo_raw_export_native_job,
    ],
)
