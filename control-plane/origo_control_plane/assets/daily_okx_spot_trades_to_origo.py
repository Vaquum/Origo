import base64
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

from origo.events import CanonicalBackfillStateStore
from origo.events.errors import ReconciliationError
from origo_control_plane.backfill import (
    apply_runtime_audit_mode_or_raise,
    load_backfill_runtime_contract_or_raise,
)
from origo_control_plane.backfill.runtime_contract import FastInsertMode
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.exchange_integrity import (
    run_exchange_integrity_suite_rows,
)
from origo_control_plane.utils.okx_aligned_projector import (
    project_okx_spot_trades_aligned,
)
from origo_control_plane.utils.okx_canonical_event_ingest import (
    build_okx_partition_source_proof,
    parse_okx_spot_trade_csv,
    write_okx_spot_trades_to_canonical,
)
from origo_control_plane.utils.okx_native_projector import (
    project_okx_spot_trades_native,
)

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database

_OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT = (
    'https://www.okx.com/priapi/v5/broker/public/trade-data/download-link'
)
_OKX_SPOT_INSTRUMENT_ID = 'BTC-USDT'
_OKX_TRADE_HISTORY_MODULE = '1'
_OKX_SOURCE_DAY_UTC_OFFSET_HOURS = 8
_REQUEST_TIMEOUT_SECONDS = 120

daily_partitions = DailyPartitionsDefinition(start_date='2021-09-01')


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


@asset(
    partitions_def=daily_partitions,
    group_name='okx_data',
    description='Downloads, validates, and writes OKX BTC spot trades into canonical event log',
)
def insert_daily_okx_spot_trades_to_origo(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    runtime_contract = load_backfill_runtime_contract_or_raise(context)
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    projection_mode = runtime_contract.projection_mode
    partition_date_str = context.asset_partition_key_for_output()
    date_str = partition_date_str
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

    events = parse_okx_spot_trade_csv(csv_payload)
    integrity_report = run_exchange_integrity_suite_rows(
        dataset='okx_spot_trades',
        rows=[event.to_integrity_tuple() for event in events],
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
        state_store = CanonicalBackfillStateStore(
            client=client,
            database=CLICKHOUSE_DATABASE,
        )
        source_proof = build_okx_partition_source_proof(
            canonical_partition_id=partition_date_str,
            events=events,
            source_file_url=file_url,
            source_filename=filename,
            zip_sha256=zip_sha256,
            csv_sha256=csv_sha256,
            content_md5_b64=source_content_md5,
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
            write_summary = write_okx_spot_trades_to_canonical(
                client=client,
                database=CLICKHOUSE_DATABASE,
                events=events,
                run_id=context.run_id,
                ingested_at_utc=datetime.now(UTC),
                canonical_partition_id=partition_date_str,
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
        source_partition_ids = {event.partition_id for event in events}
        partition_ids = {partition_date_str}
        if projection_mode == 'inline':
            native_projection_summary_dict = project_okx_spot_trades_native(
                client=client,
                database=CLICKHOUSE_DATABASE,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=projected_at_utc,
            ).to_dict()
            aligned_projection_summary_dict = project_okx_spot_trades_aligned(
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
                'first_day': min(source_partition_ids),
                'last_day': max(source_partition_ids),
            },
            'zip_sha256': zip_sha256,
            'csv_sha256': csv_sha256,
            'source_content_md5': source_content_md5,
            'integrity_report': integrity_report.to_dict(),
            'native_projection_summary': native_projection_summary_dict,
            'aligned_projection_summary': aligned_projection_summary_dict,
            'partition_proof_state': partition_proof.state,
            'partition_proof_digest_sha256': partition_proof.proof_digest_sha256,
        }
        context.log.info('Successfully processed OKX daily file: ' + json.dumps(result_data))
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
