import csv
import gzip
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

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
CLICKHOUSE_TABLE = 'bybit_spot_trades'

_BYBIT_BASE_URL = 'https://public.bybit.com/trading/BTCUSDT/'
_BYBIT_SYMBOL = 'BTCUSDT'
_REQUEST_TIMEOUT_SECONDS = 120
_EXPECTED_CSV_HEADER = (
    'timestamp',
    'symbol',
    'side',
    'size',
    'price',
    'tickDirection',
    'trdMatchID',
    'grossValue',
    'homeNotional',
    'foreignNotional',
)

daily_partitions = DailyPartitionsDefinition(start_date='2020-03-25')


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _resolve_bybit_daily_file_url(*, date_str: str) -> tuple[str, str]:
    filename = f'{_BYBIT_SYMBOL}{date_str}.csv.gz'
    return filename, _BYBIT_BASE_URL + filename


def _parse_bybit_timestamp_ms_or_raise(*, raw_value: str, row_index: int) -> int:
    try:
        timestamp_seconds = Decimal(raw_value)
    except InvalidOperation as exc:
        raise RuntimeError(
            f'Bybit CSV timestamp is invalid at line={row_index}: {raw_value}'
        ) from exc
    timestamp_ms = int(
        (timestamp_seconds * Decimal(1000)).to_integral_value(
            rounding=ROUND_HALF_UP
        )
    )
    return timestamp_ms


def _parse_bybit_csv_rows_or_raise(
    *,
    csv_payload: bytes,
    date_str: str,
    day_start_ts_utc_ms: int,
    day_end_ts_utc_ms: int,
) -> list[
    tuple[
        str,
        int,
        str,
        str,
        float,
        float,
        float,
        int,
        datetime,
        str,
        float,
        float,
        float,
    ]
]:
    text = csv_payload.decode('utf-8')
    reader = csv.reader(text.splitlines())

    header = next(reader, None)
    if header is None:
        raise RuntimeError('Bybit CSV payload is empty')
    if tuple(header) != _EXPECTED_CSV_HEADER:
        raise RuntimeError(
            'Bybit CSV header mismatch: '
            f'expected={_EXPECTED_CSV_HEADER} got={tuple(header)}'
        )

    rows: list[
        tuple[
            str,
            int,
            str,
            str,
            float,
            float,
            float,
            int,
            datetime,
            str,
            float,
            float,
            float,
        ]
    ] = []
    for row_index, row in enumerate(reader, start=2):
        if len(row) != 10:
            raise RuntimeError(
                f'Bybit CSV row has unexpected column count at line={row_index}: '
                f'expected=10 got={len(row)}'
            )

        timestamp_ms = _parse_bybit_timestamp_ms_or_raise(
            raw_value=row[0],
            row_index=row_index,
        )
        if timestamp_ms < day_start_ts_utc_ms or timestamp_ms >= day_end_ts_utc_ms:
            raise RuntimeError(
                'Bybit CSV timestamp is outside requested UTC day window '
                f'at line={row_index}, date={date_str}, '
                f'day_start_ts_utc_ms={day_start_ts_utc_ms}, '
                f'day_end_ts_utc_ms={day_end_ts_utc_ms}, '
                f'timestamp_ms={timestamp_ms}'
            )

        symbol = row[1]
        if symbol != _BYBIT_SYMBOL:
            raise RuntimeError(
                f'Bybit CSV symbol mismatch at line={row_index}: '
                f'expected={_BYBIT_SYMBOL} got={symbol}'
            )

        side = row[2].strip().lower()
        if side not in {'buy', 'sell'}:
            raise RuntimeError(
                f'Bybit CSV side must be Buy/Sell at line={row_index}, got={row[2]}'
            )

        size = float(row[3])
        price = float(row[4])
        tick_direction = row[5].strip()
        if tick_direction == '':
            raise RuntimeError(
                f'Bybit CSV tickDirection must be non-empty at line={row_index}'
            )
        trd_match_id = row[6].strip()
        if trd_match_id == '':
            raise RuntimeError(
                f'Bybit CSV trdMatchID must be non-empty at line={row_index}'
            )
        gross_value = float(row[7])
        home_notional = float(row[8])
        foreign_notional = float(row[9])
        quote_quantity = foreign_notional
        dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC)
        trade_id = row_index - 1

        rows.append(
            (
                symbol,
                trade_id,
                trd_match_id,
                side,
                price,
                size,
                quote_quantity,
                timestamp_ms,
                dt,
                tick_direction,
                gross_value,
                home_notional,
                foreign_notional,
            )
        )

    return rows


@asset(
    partitions_def=daily_partitions,
    group_name='bybit_data',
    description='Downloads, validates, extracts, and loads Bybit BTC spot trades daily data into ClickHouse',
)
def insert_daily_bybit_spot_trades_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    partition_date_str = context.asset_partition_key_for_output()
    date_str = partition_date_str
    day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(date_str)

    filename, file_url = _resolve_bybit_daily_file_url(date_str=date_str)
    context.log.info(
        f'Processing selected partition: {partition_date_str}, resolved file: {filename}'
    )
    context.log.info(f'Downloading Bybit trade data from {file_url}')

    file_response = requests.get(file_url, timeout=_REQUEST_TIMEOUT_SECONDS)
    file_response.raise_for_status()
    gzip_payload = file_response.content
    if len(gzip_payload) == 0:
        raise RuntimeError('Bybit source file payload is empty')

    source_etag = file_response.headers.get('ETag')
    if source_etag is None or source_etag.strip() == '':
        raise RuntimeError('Bybit source response is missing ETag header')

    gzip_sha256 = hashlib.sha256(gzip_payload).hexdigest()
    try:
        csv_payload = gzip.decompress(gzip_payload)
    except OSError as exc:
        raise RuntimeError(f'Bybit gzip decompression failed for {filename}') from exc
    csv_sha256 = hashlib.sha256(csv_payload).hexdigest()

    parsed_rows = _parse_bybit_csv_rows_or_raise(
        csv_payload=csv_payload,
        date_str=date_str,
        day_start_ts_utc_ms=day_start_ts_utc_ms,
        day_end_ts_utc_ms=day_end_ts_utc_ms,
    )

    integrity_report = run_exchange_integrity_suite_rows(
        dataset='bybit_spot_trades',
        rows=parsed_rows,
    )
    context.log.info(
        f'Exchange integrity suite passed: {integrity_report.to_dict()}'
    )

    del csv_payload
    del gzip_payload

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
                f'Found {existing_count} existing Bybit records for {date_str}. Deleting before reinserting.'
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
                symbol,
                trade_id,
                trd_match_id,
                side,
                price,
                size,
                quote_quantity,
                timestamp,
                datetime,
                tick_direction,
                gross_value,
                home_notional,
                foreign_notional
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
                'Bybit row count mismatch after insertion: '
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
            'partition_day': date_str,
            'source_filename': filename,
            'source_url': file_url,
            'rows_inserted': inserted_count,
            'gzip_sha256': gzip_sha256,
            'csv_sha256': csv_sha256,
            'source_etag': source_etag,
            'data_verification': data_verification,
        }

        context.log.info(
            'Successfully processed Bybit daily file: ' + json.dumps(result_data)
        )
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
