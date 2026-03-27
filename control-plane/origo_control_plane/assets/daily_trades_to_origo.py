import hashlib
import sys
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

import polars as pl
import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from origo.events import CanonicalBackfillStateStore
from origo.events.errors import ReconciliationError
from origo_control_plane.backfill import (
    apply_runtime_audit_mode_or_raise,
    load_backfill_runtime_contract_or_raise,
)
from origo_control_plane.backfill.runtime_contract import FastInsertMode
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.binance_aligned_projector import (
    project_binance_spot_trades_aligned,
)
from origo_control_plane.utils.binance_canonical_event_ingest import (
    build_binance_partition_source_proof,
    create_staged_binance_spot_trade_csv_or_raise,
    drop_staged_binance_spot_trade_csv_or_raise,
    parse_binance_spot_trade_csv_frame,
    write_binance_spot_trade_frame_to_canonical,
    write_staged_binance_spot_trade_csv_to_canonical,
)
from origo_control_plane.utils.binance_native_projector import (
    project_binance_spot_trades_native,
)
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_frame,
)

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database

daily_partitions = DailyPartitionsDefinition(start_date='2017-08-17')


def _resolve_fast_insert_mode_or_raise(
    *,
    latest_proof_state: str | None,
    canonical_row_count: int,
    active_quarantine: bool,
    partition_id: str,
    projection_mode: str,
    execution_mode: str,
) -> FastInsertMode:
    if projection_mode != 'deferred' or execution_mode != 'backfill':
        return 'writer'
    if latest_proof_state is not None:
        raise RuntimeError(
            'Fast canonical insert requires partition with no prior proof state, '
            f'found state={latest_proof_state} for partition_id={partition_id}'
        )
    if canonical_row_count != 0:
        raise RuntimeError(
            'Fast canonical insert requires empty canonical partition, '
            f'found canonical_row_count={canonical_row_count} for partition_id={partition_id}'
        )
    if active_quarantine:
        raise RuntimeError(
            'Fast canonical insert requires non-quarantined partition, '
            f'found active quarantine for partition_id={partition_id}'
        )
    return 'assume_new_partition'


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
    runtime_contract = load_backfill_runtime_contract_or_raise(context)
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    projection_mode = runtime_contract.projection_mode
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

    events_frame = parse_binance_spot_trade_csv_frame(
        csv_content,
        partition_id=partition_date_str,
    )
    context.log.info(f'Parsed {events_frame.height} rows from CSV')

    integrity_report = run_exchange_integrity_suite_frame(
        dataset='binance_spot_trades',
        frame=events_frame.select(
            pl.col('trade_id').alias('trade_id'),
            pl.col('price_text').alias('price'),
            pl.col('quantity_text').alias('quantity'),
            pl.col('quote_quantity_text').alias('quote_quantity'),
            pl.col('timestamp_ms').alias('timestamp'),
            pl.col('is_buyer_maker').alias('is_buyer_maker'),
            pl.col('is_best_match').alias('is_best_match'),
        ),
    )
    context.log.info(
        f'Exchange integrity suite passed: {integrity_report.to_dict()}'
    )

    client: ClickhouseClient | None = None
    stage_table: str | None = None
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
        state_store = CanonicalBackfillStateStore(
            client=client,
            database=CLICKHOUSE_DATABASE,
        )
        stage_table = create_staged_binance_spot_trade_csv_or_raise(
            client=client,
            database=CLICKHOUSE_DATABASE,
            frame=events_frame,
        )
        source_proof = build_binance_partition_source_proof(
            client=client,
            database=CLICKHOUSE_DATABASE,
            stage_table=stage_table,
            partition_id=partition_date_str,
            file_url=file_url,
            checksum_url=checksum_url,
            zip_sha256=actual_checksum,
            csv_sha256=csv_checksum,
            csv_filename=csv_filename,
        )
        try:
            state_store.assert_partition_can_execute_or_raise(
                stream_key=source_proof.stream_key,
                execution_mode=runtime_contract.execution_mode,
            )
        except ReconciliationError as exc:
            if (
                runtime_contract.execution_mode == 'backfill'
                and exc.code == 'RECONCILE_REQUIRED'
            ):
                state_store.record_partition_state(
                    source_proof=source_proof,
                    state='reconcile_required',
                    reason='backfill_execution_requires_reconcile',
                    run_id=context.run_id,
                    recorded_at_utc=datetime.now(UTC),
                    proof_details={'trigger_message': exc.message},
                )
            raise
        execution_assessment = state_store.assess_partition_execution(
            stream_key=source_proof.stream_key
        )
        prove_existing_canonical_without_write = (
            runtime_contract.execution_mode == 'reconcile'
            and execution_assessment.canonical_row_count > 0
        )
        source_manifested_at_utc = datetime.now(UTC)
        state_store.record_source_manifest(
            source_proof=source_proof,
            run_id=context.run_id,
            manifested_at_utc=source_manifested_at_utc,
        )
        state_store.record_partition_state(
            source_proof=source_proof,
            state='source_manifested',
            reason='source_manifest_recorded',
            run_id=context.run_id,
            recorded_at_utc=source_manifested_at_utc,
        )

        if prove_existing_canonical_without_write:
            write_path = 'reconcile_proof_only'
            write_summary = {
                'rows_processed': source_proof.source_row_count,
                'rows_inserted': 0,
                'rows_duplicate': source_proof.source_row_count,
            }
            context.log.info(
                'Reconcile detected existing canonical rows; proving partition '
                'directly without duplicate-writer replay'
            )
            proof_reason = 'reconcile_existing_canonical_rows_detected'
        else:
            fast_insert_mode = _resolve_fast_insert_mode_or_raise(
                latest_proof_state=execution_assessment.latest_proof_state,
                canonical_row_count=execution_assessment.canonical_row_count,
                active_quarantine=execution_assessment.active_quarantine,
                partition_id=partition_date_str,
                projection_mode=runtime_contract.projection_mode,
                execution_mode=runtime_contract.execution_mode,
            )
            if fast_insert_mode == 'assume_new_partition':
                write_summary = write_staged_binance_spot_trade_csv_to_canonical(
                    client=client,
                    database=CLICKHOUSE_DATABASE,
                    stage_table=stage_table,
                    partition_id=partition_date_str,
                    row_count=source_proof.source_row_count,
                    run_id=context.run_id,
                    ingested_at_utc=datetime.now(UTC),
                )
                write_path = 'staged_fast_insert'
            else:
                write_summary = write_binance_spot_trade_frame_to_canonical(
                    client=client,
                    database=CLICKHOUSE_DATABASE,
                    partition_id=partition_date_str,
                    frame=events_frame,
                    run_id=context.run_id,
                    ingested_at_utc=datetime.now(UTC),
                    fast_insert_mode=fast_insert_mode,
                )
                write_path = 'writer'
            context.log.info(f'Canonical write summary: {write_summary}')
            proof_reason = 'canonical_write_completed'
        rows_processed = int(write_summary['rows_processed'])
        rows_inserted = int(write_summary['rows_inserted'])
        rows_duplicate = int(write_summary['rows_duplicate'])
        if rows_processed != events_frame.height:
            raise RuntimeError(
                'Canonical writer summary mismatch: '
                f'rows_processed={rows_processed} expected={events_frame.height}'
            )
        if rows_inserted + rows_duplicate != rows_processed:
            raise RuntimeError(
                'Canonical writer summary mismatch: '
                f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
                f'rows_processed={rows_processed}'
            )
        proof_recorded_at_utc = datetime.now(UTC)
        state_store.record_partition_state(
            source_proof=source_proof,
            state='canonical_written_unproved',
            reason=proof_reason,
            run_id=context.run_id,
            recorded_at_utc=proof_recorded_at_utc,
        )
        partition_proof = state_store.prove_partition_or_quarantine(
            source_proof=source_proof,
            run_id=context.run_id,
            recorded_at_utc=proof_recorded_at_utc,
        )

        projected_at_utc = datetime.now(UTC)
        partition_ids = {partition_date_str}
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
            'write_path': write_path,
            'rows_processed': rows_processed,
            'rows_inserted': rows_inserted,
            'rows_duplicate': rows_duplicate,
            'zip_checksum': actual_checksum,
            'csv_checksum': csv_checksum,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary_dict,
            'aligned_projection_summary': aligned_projection_summary_dict,
            'partition_proof_state': partition_proof.state,
            'partition_proof_digest_sha256': partition_proof.proof_digest_sha256,
        }

        context.log.info(f'Successfully processed {day_file_name}')
        return result_data
    finally:
        if client is not None and stage_table is not None:
            try:
                drop_staged_binance_spot_trade_csv_or_raise(
                    client=client,
                    database=CLICKHOUSE_DATABASE,
                    stage_table=stage_table,
                )
            except Exception as exc:
                active_exception = sys.exc_info()[1]
                if active_exception is not None:
                    active_exception.add_note(
                        f'ClickHouse stage cleanup failed during cleanup: {exc}'
                    )
                    context.log.warning(
                        f'Failed to drop Binance stage table cleanly: {exc}'
                    )
                else:
                    raise RuntimeError(
                        f'Failed to drop Binance stage table cleanly: {exc}'
                    ) from exc
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
