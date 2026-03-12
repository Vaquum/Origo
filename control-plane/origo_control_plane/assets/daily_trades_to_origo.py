import hashlib
import sys
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from origo_control_plane.config import require_env, resolve_clickhouse_native_settings
from origo_control_plane.utils.binance_aligned_projector import (
    project_binance_spot_trades_aligned,
)
from origo_control_plane.utils.binance_canonical_event_ingest import (
    parse_binance_spot_trade_csv,
    write_binance_spot_trades_to_canonical,
)
from origo_control_plane.utils.binance_native_projector import (
    project_binance_spot_trades_native,
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

daily_partitions = DailyPartitionsDefinition(start_date='2017-08-17')
_BACKFILL_PROJECTION_MODE_ENV = 'ORIGO_BACKFILL_PROJECTION_MODE'


def _load_backfill_projection_mode_or_raise() -> str:
    raw_mode = require_env(_BACKFILL_PROJECTION_MODE_ENV)
    normalized = raw_mode.strip().lower()
    if normalized not in {'inline', 'deferred'}:
        raise RuntimeError(
            f'{_BACKFILL_PROJECTION_MODE_ENV} must be one of [inline, deferred], '
            f'got={raw_mode!r}'
        )
    return normalized


@asset(
    partitions_def=daily_partitions,
    group_name='binance_data',
    description='Downloads, validates, and writes Binance BTC spot trades into canonical event log',
)
def insert_daily_binance_trades_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    partition_date_str = context.asset_partition_key_for_output()
    day_file_name = f'BTCUSDT-trades-{partition_date_str}.zip'
    context.log.info(
        f'Processing selected partition: {partition_date_str}, file: {day_file_name}'
    )
    return _process_day(context, day_file_name, partition_date_str)


def _process_day(
    context: AssetExecutionContext,
    day_file_name: str,
    partition_date_str: str,
) -> dict[str, Any]:
    projection_mode = _load_backfill_projection_mode_or_raise()
    base_url = 'https://data.binance.vision/data/spot/daily/trades/BTCUSDT/'
    file_url = base_url + day_file_name
    checksum_url = file_url + '.CHECKSUM'

    context.log.info(f'Downloading checksum from {checksum_url}')
    checksum_response = requests.get(checksum_url, timeout=60)
    checksum_response.raise_for_status()

    expected_checksum = checksum_response.text.split()[0].strip()
    context.log.info(f'Expected checksum: {expected_checksum}')

    context.log.info(f'Downloading trade data from {file_url}')
    response = requests.get(file_url, timeout=60)
    response.raise_for_status()
    zip_data = response.content
    context.log.info(f'Downloaded {len(zip_data) / 1024 / 1024:.2f} MB of data')

    actual_checksum = hashlib.sha256(zip_data).hexdigest()
    context.log.info(f'Actual checksum: {actual_checksum}')
    if actual_checksum != expected_checksum:
        raise ValueError(
            f'Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}'
        )

    context.log.info('Extracting CSV from zip file')
    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        context.log.info(f'Found CSV file: {csv_filename}')
        with zip_ref.open(csv_filename) as csv_file:
            csv_content = csv_file.read()

    csv_checksum = hashlib.sha256(csv_content).hexdigest()
    context.log.info(f'CSV checksum: {csv_checksum}')

    events = parse_binance_spot_trade_csv(csv_content)
    context.log.info(f'Parsed {len(events)} rows from CSV')

    integrity_rows = [event.to_integrity_tuple() for event in events]
    integrity_report = run_exchange_integrity_suite_rows(
        dataset='binance_spot_trades',
        rows=integrity_rows,
    )
    context.log.info(
        f'Exchange integrity suite passed: {integrity_report.to_dict()}'
    )

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

        write_summary = write_binance_spot_trades_to_canonical(
            client=client,
            database=CLICKHOUSE_DATABASE,
            events=events,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
        )
        context.log.info(f'Canonical write summary: {write_summary}')

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

        if projection_mode == 'inline':
            native_projection_summary_dict = project_binance_spot_trades_native(
                client=client,
                database=CLICKHOUSE_DATABASE,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=projected_at_utc,
            ).to_dict()
            aligned_projection_summary_dict = project_binance_spot_trades_aligned(
                client=client,
                database=CLICKHOUSE_DATABASE,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=projected_at_utc,
            ).to_dict()
        else:
            native_projection_summary_dict = {
                'partitions_processed': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }
            aligned_projection_summary_dict = {
                'partitions_processed': 0,
                'policies_recorded': 0,
                'policies_duplicate': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }

        result_data: dict[str, Any] = {
            'date': day_file_name,
            'partition_date': partition_date_str,
            'projection_mode': projection_mode,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'zip_checksum': actual_checksum,
            'csv_checksum': csv_checksum,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary_dict,
            'aligned_projection_summary': aligned_projection_summary_dict,
        }

        context.log.info(f'Successfully processed {day_file_name}')
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
