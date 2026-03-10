import gzip
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.bybit_aligned_projector import (
    project_bybit_spot_trades_aligned,
)
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)
from origo_control_plane.utils.bybit_native_projector import (
    project_bybit_spot_trades_native,
)
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database

_BYBIT_BASE_URL = 'https://public.bybit.com/trading/BTCUSDT/'
_BYBIT_SYMBOL = 'BTCUSDT'
_REQUEST_TIMEOUT_SECONDS = 120

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


@asset(
    partitions_def=daily_partitions,
    group_name='bybit_data',
    description='Downloads, validates, and writes Bybit BTC spot trades into canonical event log',
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

    events = parse_bybit_spot_trade_csv(
        csv_content=csv_payload,
        date_str=date_str,
        day_start_ts_utc_ms=day_start_ts_utc_ms,
        day_end_ts_utc_ms=day_end_ts_utc_ms,
    )
    integrity_report = run_exchange_integrity_suite_rows(
        dataset='bybit_spot_trades',
        rows=[event.to_integrity_tuple() for event in events],
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

        write_summary = write_bybit_spot_trades_to_canonical(
            client=client,
            database=CLICKHOUSE_DATABASE,
            events=events,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
        )
        rows_processed = int(write_summary['rows_processed'])
        rows_inserted = int(write_summary['rows_inserted'])
        rows_duplicate = int(write_summary['rows_duplicate'])
        if rows_processed != len(events):
            raise RuntimeError(
                'Canonical writer summary mismatch: '
                f'rows_processed={rows_processed} expected={len(events)}'
            )
        if rows_inserted + rows_duplicate != rows_processed:
            raise RuntimeError(
                'Canonical writer summary mismatch: '
                f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
                f'rows_processed={rows_processed}'
            )

        projected_at_utc = datetime.now(UTC)
        partition_ids = {event.partition_id for event in events}
        native_projection_summary = project_bybit_spot_trades_native(
            client=client,
            database=CLICKHOUSE_DATABASE,
            partition_ids=partition_ids,
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        )
        aligned_projection_summary = project_bybit_spot_trades_aligned(
            client=client,
            database=CLICKHOUSE_DATABASE,
            partition_ids=partition_ids,
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        )

        result_data: dict[str, Any] = {
            'date': date_str,
            'source_filename': filename,
            'source_url': file_url,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'source_partition_span': {
                'first_day': min(partition_ids),
                'last_day': max(partition_ids),
            },
            'gzip_sha256': gzip_sha256,
            'csv_sha256': csv_sha256,
            'source_etag': source_etag,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary.to_dict(),
            'aligned_projection_summary': aligned_projection_summary.to_dict(),
        }
        context.log.info(
            'Successfully processed Bybit daily file: ' + json.dumps(result_data)
        )
        return result_data
    finally:
        if client is not None:
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
