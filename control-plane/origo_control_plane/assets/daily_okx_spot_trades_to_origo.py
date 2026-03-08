import base64
import csv
import hashlib
import json
import sys
import zipfile
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any, cast

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database
CLICKHOUSE_TABLE = 'okx_spot_trades'

_OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT = (
    'https://www.okx.com/priapi/v5/broker/public/trade-data/download-link'
)
_OKX_SPOT_INSTRUMENT_ID = 'BTC-USDT'
_OKX_TRADE_HISTORY_MODULE = '1'
_OKX_SOURCE_DAY_UTC_OFFSET_HOURS = 8
_REQUEST_TIMEOUT_SECONDS = 120
_EXPECTED_CSV_HEADER = (
    'instrument_name',
    'trade_id',
    'side',
    'price',
    'size',
    'created_time',
)

daily_partitions = DailyPartitionsDefinition(start_date='2021-09-01')


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return value


def _okx_source_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day - timedelta(hours=_OKX_SOURCE_DAY_UTC_OFFSET_HOURS)
    end_utc_exclusive = start_utc + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _resolve_okx_daily_file_url(*, date_str: str) -> tuple[str, str]:
    begin_ms, _ = _okx_source_day_window_utc_ms(date_str)
    payload = {
        'module': _OKX_TRADE_HISTORY_MODULE,
        'instType': 'SPOT',
        'instQueryParam': {'instIdList': [_OKX_SPOT_INSTRUMENT_ID]},
        'dateQuery': {
            'dateAggrType': 'daily',
            'begin': str(begin_ms),
            'end': str(begin_ms),
        },
    }

    response = requests.post(
        _OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT,
        json=payload,
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    body = _expect_dict(response.json(), 'OKX download-link response')
    code = body.get('code')
    if code != '0':
        raise RuntimeError(f'OKX download-link API returned non-zero code: {code}')

    raw_data = _expect_dict(body.get('data'), 'OKX download-link response.data')

    raw_details = _expect_list(raw_data.get('details'), 'OKX download-link response.details')
    if len(raw_details) != 1:
        raise RuntimeError(
            'OKX download-link response data.details must contain exactly one item '
            f'for date={date_str}, got={raw_details}'
        )
    detail = _expect_dict(raw_details[0], 'OKX download-link response.details[0]')

    raw_group_details = _expect_list(
        detail.get('groupDetails'),
        'OKX download-link response.details[0].groupDetails',
    )
    if len(raw_group_details) != 1:
        raise RuntimeError(
            'OKX download-link response detail.groupDetails must contain exactly one '
            f'item for date={date_str}, got={raw_group_details}'
        )
    group_detail = _expect_dict(
        raw_group_details[0],
        'OKX download-link response.details[0].groupDetails[0]',
    )

    filename = _expect_non_empty_str(
        group_detail.get('filename'),
        'OKX download-link response filename',
    )
    url = _expect_non_empty_str(
        group_detail.get('url'),
        'OKX download-link response url',
    )

    expected_filename = f'{_OKX_SPOT_INSTRUMENT_ID}-trades-{date_str}.zip'
    if filename != expected_filename:
        raise RuntimeError(
            'OKX download-link filename mismatch for requested day: '
            f'expected={expected_filename} got={filename}'
        )

    return filename, url


def _verify_source_content_md5_or_raise(*, payload: bytes, content_md5_b64: str) -> None:
    digest_md5 = hashlib.md5(payload).digest()
    actual_b64 = base64.b64encode(digest_md5).decode('ascii')
    if actual_b64 != content_md5_b64:
        raise RuntimeError(
            'OKX source content-md5 mismatch: '
            f'expected={content_md5_b64} actual={actual_b64}'
        )


def _parse_okx_csv_rows_or_raise(
    *, csv_payload: bytes
) -> list[tuple[str, int, str, float, float, float, int, datetime]]:
    text = csv_payload.decode('utf-8')
    reader = csv.reader(text.splitlines())

    header = next(reader, None)
    if header is None:
        raise RuntimeError('OKX CSV payload is empty')
    if tuple(header) != _EXPECTED_CSV_HEADER:
        raise RuntimeError(
            'OKX CSV header mismatch: '
            f'expected={_EXPECTED_CSV_HEADER} got={tuple(header)}'
        )

    rows: list[tuple[str, int, str, float, float, float, int, datetime]] = []
    for row_index, row in enumerate(reader, start=2):
        if len(row) != 6:
            raise RuntimeError(
                f'OKX CSV row has unexpected column count at line={row_index}: '
                f'expected=6 got={len(row)}'
            )

        instrument_name = row[0]
        if instrument_name != _OKX_SPOT_INSTRUMENT_ID:
            raise RuntimeError(
                f'OKX CSV instrument_name mismatch at line={row_index}: '
                f'expected={_OKX_SPOT_INSTRUMENT_ID} got={instrument_name}'
            )

        trade_id = int(row[1])
        side = row[2]
        if side not in {'buy', 'sell'}:
            raise RuntimeError(
                f'OKX CSV side must be buy/sell at line={row_index}, got={side}'
            )

        price = float(row[3])
        size = float(row[4])
        timestamp = int(row[5])
        if len(str(timestamp)) != 13:
            raise RuntimeError(
                f'OKX CSV timestamp must be 13-digit ms at line={row_index}, '
                f'got={timestamp}'
            )

        dt = datetime.fromtimestamp(timestamp / 1000.0, tz=UTC)
        quote_quantity = price * size

        rows.append(
            (
                instrument_name,
                trade_id,
                side,
                price,
                size,
                quote_quantity,
                timestamp,
                dt,
            )
        )

    return rows


@asset(
    partitions_def=daily_partitions,
    group_name='okx_data',
    description='Downloads, validates, extracts, and loads OKX BTC spot trades daily data into ClickHouse',
)
def insert_daily_okx_spot_trades_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    partition_date_str = context.asset_partition_key_for_output()
    date_str = partition_date_str
    day_start_ts_utc_ms, day_end_ts_utc_ms = _okx_source_day_window_utc_ms(date_str)

    filename, file_url = _resolve_okx_daily_file_url(date_str=date_str)
    context.log.info(
        f'Processing selected partition: {partition_date_str}, resolved file: {filename}'
    )
    context.log.info(f'Downloading OKX trade data from {file_url}')

    file_response = requests.get(file_url, timeout=_REQUEST_TIMEOUT_SECONDS)
    file_response.raise_for_status()
    zip_data = file_response.content

    source_content_md5 = file_response.headers.get('Content-MD5')
    if source_content_md5 is None or source_content_md5.strip() == '':
        raise RuntimeError('OKX source response is missing Content-MD5 header')

    _verify_source_content_md5_or_raise(
        payload=zip_data,
        content_md5_b64=source_content_md5,
    )

    zip_sha256 = hashlib.sha256(zip_data).hexdigest()

    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        csv_names = [name for name in zip_ref.namelist() if name.lower().endswith('.csv')]
        if len(csv_names) != 1:
            raise RuntimeError(
                'OKX daily zip must contain exactly one CSV file, '
                f'got={csv_names}'
            )
        csv_name = csv_names[0]
        with zip_ref.open(csv_name) as csv_file:
            csv_payload = csv_file.read()

    csv_sha256 = hashlib.sha256(csv_payload).hexdigest()
    parsed_rows = _parse_okx_csv_rows_or_raise(csv_payload=csv_payload)

    integrity_report = run_exchange_integrity_suite_rows(
        dataset='okx_spot_trades',
        rows=parsed_rows,
    )
    context.log.info(
        f'Exchange integrity suite passed: {integrity_report.to_dict()}'
    )

    del csv_payload
    del zip_data

    client: ClickhouseClient | None = None
    try:
        context.log.info(
            f'Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
        )
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            compression=True,
            send_receive_timeout=900,
        )

        check_result = client.execute(f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE timestamp >= {day_start_ts_utc_ms}
              AND timestamp < {day_end_ts_utc_ms}
        """)
        existing_count = check_result[0][0]
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing OKX records for {date_str}. Deleting before reinserting.'
            )
            client.execute(f"""
                ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
                DELETE WHERE timestamp >= {day_start_ts_utc_ms}
                       AND timestamp < {day_end_ts_utc_ms}
            """)

        client.execute(
            f"""
            INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            (
                instrument_name,
                trade_id,
                side,
                price,
                size,
                quote_quantity,
                timestamp,
                datetime
            ) SETTINGS async_insert=1, wait_for_async_insert=1
            VALUES
            """,
            parsed_rows,
            settings={'max_execution_time': 900},
        )

        verify_result = client.execute(f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE timestamp >= {day_start_ts_utc_ms}
              AND timestamp < {day_end_ts_utc_ms}
        """)
        inserted_count = verify_result[0][0]

        stats_result = client.execute(f"""
            SELECT
                min(trade_id),
                max(trade_id),
                avg(price),
                count(distinct trade_id) % 1000
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE timestamp >= {day_start_ts_utc_ms}
              AND timestamp < {day_end_ts_utc_ms}
        """)

        if inserted_count != len(parsed_rows):
            raise RuntimeError(
                'OKX row count mismatch after insertion: '
                f'expected={len(parsed_rows)} actual={inserted_count}'
            )

        data_verification = {
            'min_trade_id': stats_result[0][0],
            'max_trade_id': stats_result[0][1],
            'avg_price': stats_result[0][2],
            'id_uniqueness_check': stats_result[0][3],
        }

        result_data: dict[str, Any] = {
            'date': date_str,
            'source_filename': filename,
            'source_url': file_url,
            'rows_inserted': inserted_count,
            'zip_sha256': zip_sha256,
            'csv_sha256': csv_sha256,
            'source_content_md5': source_content_md5,
            'data_verification': data_verification,
        }

        context.log.info('Successfully processed OKX daily file: ' + json.dumps(result_data))
        return result_data
    finally:
        if client:
            try:
                client.disconnect()
            except Exception as exc:
                active_exception = sys.exc_info()[1]
                if active_exception is not None:
                    active_exception.add_note(
                        f'ClickHouse disconnect failed during cleanup: {exc}'
                    )
                    context.log.warning(
                        f'Failed to disconnect ClickHouse client cleanly: {exc}'
                    )
                else:
                    raise RuntimeError(
                        f'Failed to disconnect ClickHouse client cleanly: {exc}'
                    ) from exc
