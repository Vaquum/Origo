import gzip
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

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
from origo_control_plane.utils.bybit_aligned_projector import (
    project_bybit_spot_trades_aligned,
)
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    build_bybit_partition_source_proof,
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)
from origo_control_plane.utils.bybit_native_projector import (
    project_bybit_spot_trades_native,
)
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)
from origo_control_plane.utils.exchange_source_contracts import (
    EXCHANGE_SOURCE_REQUEST_TIMEOUT_SECONDS,
    resolve_bybit_daily_file_url,
)

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database

daily_partitions = DailyPartitionsDefinition(start_date='2020-03-25')


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


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


@asset(
    partitions_def=daily_partitions,
    group_name='bybit_data',
    description='Downloads, validates, and writes Bybit BTC spot trades into canonical event log',
)
def insert_daily_bybit_spot_trades_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    runtime_contract = load_backfill_runtime_contract_or_raise(context)
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    projection_mode = runtime_contract.projection_mode
    partition_date_str = context.asset_partition_key_for_output()
    date_str = partition_date_str
    day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(date_str)

    filename, file_url = resolve_bybit_daily_file_url(date_str=date_str)
    context.log.info(
        f'Processing selected partition: {partition_date_str}, resolved file: {filename}'
    )
    context.log.info(f'Downloading Bybit trade data from {file_url}')

    file_response = requests.get(
        file_url,
        timeout=EXCHANGE_SOURCE_REQUEST_TIMEOUT_SECONDS,
    )
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
        state_store = CanonicalBackfillStateStore(
            client=client,
            database=CLICKHOUSE_DATABASE,
        )
        source_proof = build_bybit_partition_source_proof(
            partition_id=partition_date_str,
            events=events,
            source_file_url=file_url,
            source_filename=filename,
            gzip_sha256=gzip_sha256,
            csv_sha256=csv_sha256,
            source_etag=source_etag,
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
            write_path = 'reconcile_proof_only'
        else:
            fast_insert_mode = _resolve_fast_insert_mode_or_raise(
                latest_proof_state=execution_assessment.latest_proof_state,
                canonical_row_count=execution_assessment.canonical_row_count,
                active_quarantine=execution_assessment.active_quarantine,
                partition_id=partition_date_str,
                projection_mode=runtime_contract.projection_mode,
                execution_mode=runtime_contract.execution_mode,
            )
            write_summary = write_bybit_spot_trades_to_canonical(
                client=client,
                database=CLICKHOUSE_DATABASE,
                events=events,
                run_id=context.run_id,
                ingested_at_utc=datetime.now(UTC),
                fast_insert_mode=fast_insert_mode,
            )
            proof_reason = 'canonical_write_completed'
            write_path = 'writer'
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
        partition_ids = {event.partition_id for event in events}
        if projection_mode == 'inline':
            native_projection_summary_dict = project_bybit_spot_trades_native(
                client=client,
                database=CLICKHOUSE_DATABASE,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=projected_at_utc,
            ).to_dict()
            aligned_projection_summary_dict = project_bybit_spot_trades_aligned(
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
            'date': date_str,
            'source_filename': filename,
            'source_url': file_url,
            'projection_mode': projection_mode,
            'write_path': write_path,
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
            'native_projection_summary': native_projection_summary_dict,
            'aligned_projection_summary': aligned_projection_summary_dict,
            'partition_proof_state': partition_proof.state,
            'partition_proof_digest_sha256': partition_proof.proof_digest_sha256,
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
