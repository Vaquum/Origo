from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
from clickhouse_connect.driver.exceptions import DatabaseError
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from origo.data._internal.generic_endpoints import (
    HISTORICAL_SOURCE_TO_DATASET,
    query_aligned_wide_rows_envelope,
    query_bitcoin_dataset_data,
    query_etf_daily_metrics_data,
    query_fred_series_metrics_data,
    query_native_wide_rows_envelope,
    query_spot_klines_data,
    query_spot_trades_data,
)
from origo.query.aligned_core import AlignedDataset

from .dagster_graphql import (
    DagsterGraphQLError,
    DagsterRunNotFoundError,
    get_run_snapshot,
    launch_export_run,
)
from .etf_warnings import build_etf_daily_quality_warnings
from .export_audit import get_export_audit_log
from .export_tags import read_export_tags
from .fred_alert_audit import emit_fred_warning_alerts_and_audit
from .fred_warnings import build_fred_publish_freshness_warnings
from .rights import RightsGateError, resolve_export_rights, resolve_query_rights
from .schemas import (
    ExportFormat,
    ExportStatus,
    HistoricalBitcoinDatasetRequest,
    HistoricalETFDailyMetricsRequest,
    HistoricalFREDSeriesMetricsRequest,
    HistoricalSpotKlinesRequest,
    HistoricalSpotTradesRequest,
    RawExportArtifact,
    RawExportMode,
    RawExportRequest,
    RawExportStatusResponse,
    RawExportSubmitResponse,
    RawQueryDataset,
    RawQueryFreshness,
    RawQueryMode,
    RawQueryRequest,
    RawQueryResponse,
    RawQueryWarning,
    RightsState,
)

app = FastAPI(title='Origo Raw API', version='0.1.23')
_ALIGNED_QUERY_DATASETS: frozenset[AlignedDataset] = frozenset(
    {
        'binance_spot_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }
)

ORIGO_INTERNAL_API_KEY = os.environ.get('ORIGO_INTERNAL_API_KEY')
if ORIGO_INTERNAL_API_KEY is None or ORIGO_INTERNAL_API_KEY.strip() == '':
    raise RuntimeError('ORIGO_INTERNAL_API_KEY must be set and non-empty')

try:
    QUERY_MAX_CONCURRENCY = int(os.environ['ORIGO_QUERY_MAX_CONCURRENCY'])
except KeyError as exc:
    raise RuntimeError('ORIGO_QUERY_MAX_CONCURRENCY must be set and non-empty') from exc
except ValueError as exc:
    raise RuntimeError('ORIGO_QUERY_MAX_CONCURRENCY must be an integer') from exc

try:
    QUERY_MAX_QUEUE = int(os.environ['ORIGO_QUERY_MAX_QUEUE'])
except KeyError as exc:
    raise RuntimeError('ORIGO_QUERY_MAX_QUEUE must be set and non-empty') from exc
except ValueError as exc:
    raise RuntimeError('ORIGO_QUERY_MAX_QUEUE must be an integer') from exc

try:
    ALIGNED_QUERY_MAX_CONCURRENCY = int(
        os.environ['ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY']
    )
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError(
        'ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY must be an integer'
    ) from exc

try:
    ALIGNED_QUERY_MAX_QUEUE = int(os.environ['ORIGO_ALIGNED_QUERY_MAX_QUEUE'])
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_ALIGNED_QUERY_MAX_QUEUE must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError('ORIGO_ALIGNED_QUERY_MAX_QUEUE must be an integer') from exc

try:
    EXPORT_MAX_CONCURRENCY = int(os.environ['ORIGO_EXPORT_MAX_CONCURRENCY'])
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_EXPORT_MAX_CONCURRENCY must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError('ORIGO_EXPORT_MAX_CONCURRENCY must be an integer') from exc

try:
    EXPORT_MAX_QUEUE = int(os.environ['ORIGO_EXPORT_MAX_QUEUE'])
except KeyError as exc:
    raise RuntimeError('ORIGO_EXPORT_MAX_QUEUE must be set and non-empty') from exc
except ValueError as exc:
    raise RuntimeError('ORIGO_EXPORT_MAX_QUEUE must be an integer') from exc

ORIGO_SOURCE_RIGHTS_MATRIX_PATH = os.environ.get('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')
if (
    ORIGO_SOURCE_RIGHTS_MATRIX_PATH is None
    or ORIGO_SOURCE_RIGHTS_MATRIX_PATH.strip() == ''
):
    raise RuntimeError('ORIGO_SOURCE_RIGHTS_MATRIX_PATH must be set and non-empty')

ORIGO_EXPORT_AUDIT_LOG_PATH = os.environ.get('ORIGO_EXPORT_AUDIT_LOG_PATH')
if ORIGO_EXPORT_AUDIT_LOG_PATH is None or ORIGO_EXPORT_AUDIT_LOG_PATH.strip() == '':
    raise RuntimeError('ORIGO_EXPORT_AUDIT_LOG_PATH must be set and non-empty')

try:
    AUDIT_LOG_RETENTION_DAYS = int(os.environ['ORIGO_AUDIT_LOG_RETENTION_DAYS'])
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_AUDIT_LOG_RETENTION_DAYS must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError(
        'ORIGO_AUDIT_LOG_RETENTION_DAYS must be an integer'
    ) from exc

try:
    ETF_DAILY_STALE_MAX_AGE_DAYS = int(os.environ['ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS'])
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError(
        'ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS must be an integer'
    ) from exc

try:
    FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS = int(
        os.environ['ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS']
    )
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError(
        'ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS must be an integer'
    ) from exc

try:
    ALIGNED_FRESHNESS_MAX_AGE_SECONDS = int(
        os.environ['ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS']
    )
except KeyError as exc:
    raise RuntimeError(
        'ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS must be set and non-empty'
    ) from exc
except ValueError as exc:
    raise RuntimeError(
        'ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS must be an integer'
    ) from exc

if QUERY_MAX_CONCURRENCY <= 0:
    raise RuntimeError('ORIGO_QUERY_MAX_CONCURRENCY must be > 0')
if QUERY_MAX_QUEUE < 0:
    raise RuntimeError('ORIGO_QUERY_MAX_QUEUE must be >= 0')
if ALIGNED_QUERY_MAX_CONCURRENCY <= 0:
    raise RuntimeError('ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY must be > 0')
if ALIGNED_QUERY_MAX_QUEUE < 0:
    raise RuntimeError('ORIGO_ALIGNED_QUERY_MAX_QUEUE must be >= 0')
if EXPORT_MAX_CONCURRENCY <= 0:
    raise RuntimeError('ORIGO_EXPORT_MAX_CONCURRENCY must be > 0')
if EXPORT_MAX_QUEUE < 0:
    raise RuntimeError('ORIGO_EXPORT_MAX_QUEUE must be >= 0')
if AUDIT_LOG_RETENTION_DAYS < 365:
    raise RuntimeError('ORIGO_AUDIT_LOG_RETENTION_DAYS must be >= 365')
if ETF_DAILY_STALE_MAX_AGE_DAYS <= 0:
    raise RuntimeError('ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS must be > 0')
if FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS <= 0:
    raise RuntimeError('ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS must be > 0')
if ALIGNED_FRESHNESS_MAX_AGE_SECONDS <= 0:
    raise RuntimeError('ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS must be > 0')

_query_semaphore = asyncio.Semaphore(QUERY_MAX_CONCURRENCY)
_queued_requests = 0
_queue_lock = asyncio.Lock()

_aligned_query_semaphore = asyncio.Semaphore(ALIGNED_QUERY_MAX_CONCURRENCY)
_queued_aligned_requests = 0
_queue_aligned_lock = asyncio.Lock()

_export_dispatch_semaphore = asyncio.Semaphore(EXPORT_MAX_CONCURRENCY)
_pending_export_dispatch_requests = 0
_pending_export_dispatch_lock = asyncio.Lock()
_export_status_lock = asyncio.Lock()
_last_export_status: dict[str, ExportStatus] = {}


@dataclass(frozen=True)
class _ExportFailure:
    error_code: str
    error_message: str


def _require_internal_api_key(x_api_key: str = Header(..., alias='X-API-Key')) -> None:
    if x_api_key != ORIGO_INTERNAL_API_KEY:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'AUTH_INVALID_API_KEY',
                'message': 'Invalid API key',
            },
        )


def _build_warnings(request: RawQueryRequest) -> list[RawQueryWarning]:
    warnings: list[RawQueryWarning] = []
    if request.n_rows is not None:
        warnings.append(
            RawQueryWarning(
                code='WINDOW_LATEST_ROWS_MUTABLE',
                message='n_rows uses latest rows and can drift as new trades arrive',
            )
        )
    if request.n_random is not None:
        warnings.append(
            RawQueryWarning(
                code='WINDOW_RANDOM_SAMPLE',
                message='n_random is sampling-oriented and not intended for canonical replay windows',
            )
        )
    return warnings


def _build_historical_warnings(
    *, n_latest_rows: int | None, n_random_rows: int | None
) -> list[RawQueryWarning]:
    warnings: list[RawQueryWarning] = []
    if n_latest_rows is not None:
        warnings.append(
            RawQueryWarning(
                code='WINDOW_LATEST_ROWS_MUTABLE',
                message='n_latest_rows uses latest rows and can drift as new trades arrive',
            )
        )
    if n_random_rows is not None:
        warnings.append(
            RawQueryWarning(
                code='WINDOW_RANDOM_SAMPLE',
                message='n_random_rows is sampling-oriented and not intended for canonical replay windows',
            )
        )
    return warnings


async def _run_native_query_with_limits(
    callback: Any,
    *args: Any,
    **kwargs: Any,
) -> Any:
    global _queued_requests

    request_is_queued = False
    async with _queue_lock:
        if _queued_requests >= QUERY_MAX_QUEUE:
            raise HTTPException(
                status_code=503,
                detail={
                    'code': 'QUERY_QUEUE_LIMIT_REACHED',
                    'message': 'Query queue limit reached',
                },
            )
        _queued_requests += 1
        request_is_queued = True

    try:
        async with _query_semaphore:
            async with _queue_lock:
                if request_is_queued:
                    _queued_requests -= 1
                    request_is_queued = False
            return await run_in_threadpool(callback, *args, **kwargs)
    finally:
        if request_is_queued:
            async with _queue_lock:
                _queued_requests -= 1


def _historical_dataset_for_source(source: str) -> RawQueryDataset:
    if source not in HISTORICAL_SOURCE_TO_DATASET:
        raise RuntimeError(f'Unsupported historical source={source}')
    return HISTORICAL_SOURCE_TO_DATASET[source]


def _build_historical_envelope(
    *,
    frame: pl.DataFrame,
    mode: RawQueryMode,
    dataset: RawQueryDataset,
    warnings: list[RawQueryWarning],
    rights_state: RightsState,
    rights_provisional: bool,
) -> dict[str, Any]:
    return {
        'mode': mode,
        'source': dataset,
        'sources': [dataset],
        'row_count': frame.height,
        'schema': [
            {'name': name, 'dtype': str(dtype)}
            for name, dtype in frame.schema.items()
        ],
        'warnings': [warning.model_dump() for warning in warnings],
        'rows': frame.to_dicts(),
        'rights_state': rights_state,
        'rights_provisional': rights_provisional,
        'freshness': None,
        'view_id': None,
        'view_version': None,
    }


def _parse_historical_trades_request(payload: dict[str, Any]) -> HistoricalSpotTradesRequest:
    try:
        return HistoricalSpotTradesRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc


def _parse_historical_klines_request(payload: dict[str, Any]) -> HistoricalSpotKlinesRequest:
    try:
        return HistoricalSpotKlinesRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc


def _parse_historical_etf_request(
    payload: dict[str, Any],
) -> HistoricalETFDailyMetricsRequest:
    try:
        return HistoricalETFDailyMetricsRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc


def _parse_historical_fred_request(
    payload: dict[str, Any],
) -> HistoricalFREDSeriesMetricsRequest:
    try:
        return HistoricalFREDSeriesMetricsRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc


def _parse_historical_bitcoin_request(
    payload: dict[str, Any],
) -> HistoricalBitcoinDatasetRequest:
    try:
        return HistoricalBitcoinDatasetRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc


def _resolve_single_source_or_raise(request: RawQueryRequest) -> RawQueryDataset:
    if len(request.sources) != 1:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'QUERY_CONTRACT_ERROR',
                'message': (
                    'Exactly one source is currently supported per request '
                    '(sources length must be 1)'
                ),
            },
        )
    return request.sources[0]


def _parse_iso_datetime_to_utc(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_aligned_freshness(
    *, envelope: dict[str, Any]
) -> tuple[RawQueryFreshness | None, list[RawQueryWarning]]:
    rows_payload_obj = envelope.get('rows')
    if not isinstance(rows_payload_obj, list):
        return RawQueryFreshness(as_of_utc=None, lag_seconds=None), []
    rows_payload = cast(list[Any], rows_payload_obj)
    if len(rows_payload) == 0:
        return RawQueryFreshness(as_of_utc=None, lag_seconds=None), []

    freshest: datetime | None = None
    for row_obj in rows_payload:
        if not isinstance(row_obj, dict):
            continue
        row = cast(dict[str, Any], row_obj)
        for candidate_key in ('valid_to_utc_exclusive', 'aligned_at_utc'):
            raw_value = row.get(candidate_key)
            if not isinstance(raw_value, str) or raw_value.strip() == '':
                continue
            try:
                parsed = _parse_iso_datetime_to_utc(raw_value)
            except ValueError:
                continue
            if freshest is None or parsed > freshest:
                freshest = parsed

    if freshest is None:
        return RawQueryFreshness(as_of_utc=None, lag_seconds=None), []

    lag_seconds = int((datetime.now(UTC) - freshest).total_seconds())
    freshest_iso = freshest.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    freshness = RawQueryFreshness(
        as_of_utc=freshest_iso,
        lag_seconds=lag_seconds,
    )
    if lag_seconds <= ALIGNED_FRESHNESS_MAX_AGE_SECONDS:
        return freshness, []

    return freshness, [
        RawQueryWarning(
            code='ALIGNED_FRESHNESS_STALE',
            message=(
                'Aligned data freshness exceeded max age threshold: '
                f'lag_seconds={lag_seconds} '
                f'max_age_seconds={ALIGNED_FRESHNESS_MAX_AGE_SECONDS}'
            ),
        )
    ]


def _map_dagster_status(status: str) -> ExportStatus:
    if status in {'QUEUED', 'NOT_STARTED', 'STARTING'}:
        return 'queued'
    if status in {'STARTED', 'CANCELING'}:
        return 'running'
    if status == 'SUCCESS':
        return 'succeeded'
    if status in {'FAILURE', 'CANCELED'}:
        return 'failed'
    raise RuntimeError(f'Unsupported Dagster run status: {status}')


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


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


def _read_export_tags(
    tags: dict[str, str],
) -> tuple[
    RawExportMode,
    ExportFormat,
    RawQueryDataset,
    str,
    RightsState,
    bool,
    str | None,
    int | None,
]:
    return read_export_tags(tags)


def _read_export_artifact(
    *, export_id: str, export_format: ExportFormat
) -> RawExportArtifact | None:
    export_root = Path(_require_env('ORIGO_EXPORT_ROOT_DIR'))
    metadata_path = export_root / export_id / 'metadata.json'
    if not metadata_path.exists():
        return None

    metadata = _expect_dict(
        json.loads(metadata_path.read_text(encoding='utf-8')), 'Export metadata'
    )

    metadata_format = metadata.get('format')
    if metadata_format != export_format:
        raise RuntimeError(
            f'Export metadata format mismatch: expected={export_format} got={metadata_format}'
        )

    artifact_path_raw = metadata.get('path')
    if not isinstance(artifact_path_raw, str) or artifact_path_raw.strip() == '':
        raise RuntimeError('Export metadata path must be a non-empty string')
    artifact_path = Path(artifact_path_raw)
    if not artifact_path.exists():
        raise RuntimeError(f'Export artifact file missing: {artifact_path}')

    row_count_raw = metadata.get('row_count')
    if not isinstance(row_count_raw, int):
        raise RuntimeError('Export metadata row_count must be an integer')

    checksum_raw = metadata.get('checksum_sha256')
    if not isinstance(checksum_raw, str) or checksum_raw.strip() == '':
        raise RuntimeError('Export metadata checksum_sha256 must be a non-empty string')

    return RawExportArtifact(
        format=export_format,
        uri=str(artifact_path.resolve()),
        row_count=row_count_raw,
        checksum_sha256=checksum_raw,
    )


def _read_export_failure(export_id: str) -> _ExportFailure | None:
    export_root = Path(_require_env('ORIGO_EXPORT_ROOT_DIR'))
    failure_path = export_root / export_id / 'error.json'
    if not failure_path.exists():
        return None

    failure_payload = _expect_dict(
        json.loads(failure_path.read_text(encoding='utf-8')), 'Export failure payload'
    )

    error_code = failure_payload.get('error_code')
    if not isinstance(error_code, str) or error_code.strip() == '':
        raise RuntimeError(
            'Export failure payload error_code must be a non-empty string'
        )

    error_message = failure_payload.get('error_message')
    if not isinstance(error_message, str) or error_message.strip() == '':
        raise RuntimeError(
            'Export failure payload error_message must be a non-empty string'
        )

    return _ExportFailure(error_code=error_code, error_message=error_message)


def _build_export_audit_payload(request: RawExportRequest) -> dict[str, Any]:
    request_payload = request.model_dump(mode='json')
    auth_token = request_payload.pop('auth_token', None)
    request_payload['auth_token_present'] = bool(
        isinstance(auth_token, str) and auth_token.strip() != ''
    )
    canonical = json.dumps(request_payload, sort_keys=True, separators=(',', ':'))
    request_sha256 = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    return {
        'dataset': request.dataset,
        'format': request.format,
        'mode': request.mode,
        'strict': request.strict,
        'request_sha256': request_sha256,
    }


def _audit_or_raise_http(
    *,
    event_type: str,
    export_id: str | None,
    payload: dict[str, Any],
) -> None:
    try:
        get_export_audit_log().append_event(
            event_type=event_type,
            export_id=export_id,
            payload=payload,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'EXPORT_AUDIT_WRITE_ERROR',
                'message': str(exc),
            },
        ) from exc


def _classify_terminal_failure(
    *, dagster_status: str, persisted_failure: _ExportFailure | None
) -> _ExportFailure:
    if persisted_failure is not None:
        return persisted_failure
    if dagster_status == 'CANCELED':
        return _ExportFailure(
            error_code='EXPORT_RUN_CANCELED',
            error_message='Export run was canceled',
        )
    return _ExportFailure(
        error_code='EXPORT_RUN_FAILED',
        error_message=f'Export run ended in Dagster status={dagster_status}',
    )


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


async def _handle_historical_spot_trades(
    *,
    source: str,
    request: HistoricalSpotTradesRequest,
    x_clickhouse_token: str | None,
) -> RawQueryResponse:
    dataset = _historical_dataset_for_source(source)
    filters_payload = (
        [
            filter_clause.model_dump(mode='json')
            for filter_clause in request.filters
        ]
        if request.filters is not None
        else None
    )
    try:
        query_rights_decision = resolve_query_rights(
            dataset=dataset,
            auth_token=x_clickhouse_token,
        )
    except RightsGateError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': exc.code, 'message': str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'QUERY_RIGHTS_RESOLUTION_ERROR',
                'message': str(exc),
            },
        ) from exc

    try:
        query_rights_state, query_rights_provisional = (
            query_rights_decision.response_metadata()
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'QUERY_RIGHTS_METADATA_ERROR',
                'message': str(exc),
            },
        ) from exc

    warnings = _build_historical_warnings(
        n_latest_rows=request.n_latest_rows,
        n_random_rows=request.n_random_rows,
    )
    if request.strict and warnings:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'STRICT_MODE_WARNING_FAILURE',
                'message': 'Warnings present while strict=true',
                'warnings': [warning.model_dump() for warning in warnings],
            },
        )

    try:
        frame = await _run_native_query_with_limits(
            query_spot_trades_data,
            source=source,
            mode=request.mode,
            start_date=request.start_date,
            end_date=request.end_date,
            n_latest_rows=request.n_latest_rows,
            n_random_rows=request.n_random_rows,
            fields=request.fields,
            filters=filters_payload,
            include_datetime_col=request.include_datetime_col,
            auth_token=x_clickhouse_token,
            show_summary=False,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_RUNTIME_ERROR', 'message': str(exc)},
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_BACKEND_ERROR', 'message': str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_UNKNOWN_ERROR', 'message': str(exc)},
        ) from exc

    if frame.height == 0:
        raise HTTPException(
            status_code=404,
            detail={
                'code': 'HISTORICAL_NO_DATA',
                'message': 'No rows found for query window',
            },
        )

    envelope = _build_historical_envelope(
        frame=frame,
        mode=request.mode,
        dataset=dataset,
        warnings=warnings,
        rights_state=query_rights_state,
        rights_provisional=query_rights_provisional,
    )
    return RawQueryResponse.model_validate(envelope)


async def _handle_historical_spot_klines(
    *,
    source: str,
    request: HistoricalSpotKlinesRequest,
    x_clickhouse_token: str | None,
) -> RawQueryResponse:
    dataset = _historical_dataset_for_source(source)
    filters_payload = (
        [
            filter_clause.model_dump(mode='json')
            for filter_clause in request.filters
        ]
        if request.filters is not None
        else None
    )
    try:
        query_rights_decision = resolve_query_rights(
            dataset=dataset,
            auth_token=x_clickhouse_token,
        )
    except RightsGateError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': exc.code, 'message': str(exc)},
        ) from exc

    try:
        query_rights_state, query_rights_provisional = (
            query_rights_decision.response_metadata()
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'QUERY_RIGHTS_METADATA_ERROR',
                'message': str(exc),
            },
        ) from exc

    warnings = _build_historical_warnings(
        n_latest_rows=request.n_latest_rows,
        n_random_rows=request.n_random_rows,
    )
    if request.strict and warnings:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'STRICT_MODE_WARNING_FAILURE',
                'message': 'Warnings present while strict=true',
                'warnings': [warning.model_dump() for warning in warnings],
            },
        )

    try:
        frame = await _run_native_query_with_limits(
            query_spot_klines_data,
            source=source,
            mode=request.mode,
            start_date=request.start_date,
            end_date=request.end_date,
            n_latest_rows=request.n_latest_rows,
            n_random_rows=request.n_random_rows,
            fields=request.fields,
            filters=filters_payload,
            kline_size=request.kline_size,
            auth_token=x_clickhouse_token,
            show_summary=False,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_RUNTIME_ERROR', 'message': str(exc)},
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_BACKEND_ERROR', 'message': str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_UNKNOWN_ERROR', 'message': str(exc)},
        ) from exc

    if frame.height == 0:
        raise HTTPException(
            status_code=404,
            detail={
                'code': 'HISTORICAL_NO_DATA',
                'message': 'No rows found for query window',
            },
        )

    envelope = _build_historical_envelope(
        frame=frame,
        mode=request.mode,
        dataset=dataset,
        warnings=warnings,
        rights_state=query_rights_state,
        rights_provisional=query_rights_provisional,
    )
    return RawQueryResponse.model_validate(envelope)


async def _handle_historical_etf_daily_metrics(
    *,
    request: HistoricalETFDailyMetricsRequest,
    x_clickhouse_token: str | None,
) -> RawQueryResponse:
    dataset: RawQueryDataset = 'etf_daily_metrics'
    filters_payload = (
        [
            filter_clause.model_dump(mode='json')
            for filter_clause in request.filters
        ]
        if request.filters is not None
        else None
    )
    try:
        query_rights_decision = resolve_query_rights(
            dataset=dataset,
            auth_token=x_clickhouse_token,
        )
    except RightsGateError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': exc.code, 'message': str(exc)},
        ) from exc

    try:
        query_rights_state, query_rights_provisional = (
            query_rights_decision.response_metadata()
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'QUERY_RIGHTS_METADATA_ERROR',
                'message': str(exc),
            },
        ) from exc

    warnings = _build_historical_warnings(
        n_latest_rows=request.n_latest_rows,
        n_random_rows=request.n_random_rows,
    )
    try:
        etf_quality_warnings = await run_in_threadpool(
            build_etf_daily_quality_warnings,
            x_clickhouse_token,
            ETF_DAILY_STALE_MAX_AGE_DAYS,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_WARNING_RUNTIME_ERROR',
                'message': str(exc),
            },
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_WARNING_BACKEND_ERROR',
                'message': str(exc),
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_WARNING_UNKNOWN_ERROR',
                'message': str(exc),
            },
        ) from exc
    warnings.extend(etf_quality_warnings)
    if request.strict and warnings:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'STRICT_MODE_WARNING_FAILURE',
                'message': 'Warnings present while strict=true',
                'warnings': [warning.model_dump() for warning in warnings],
            },
        )

    try:
        frame = await _run_native_query_with_limits(
            query_etf_daily_metrics_data,
            mode=request.mode,
            start_date=request.start_date,
            end_date=request.end_date,
            n_latest_rows=request.n_latest_rows,
            n_random_rows=request.n_random_rows,
            fields=request.fields,
            filters=filters_payload,
            auth_token=x_clickhouse_token,
            show_summary=False,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_RUNTIME_ERROR', 'message': str(exc)},
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_BACKEND_ERROR', 'message': str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_UNKNOWN_ERROR', 'message': str(exc)},
        ) from exc

    if frame.height == 0:
        raise HTTPException(
            status_code=404,
            detail={
                'code': 'HISTORICAL_NO_DATA',
                'message': 'No rows found for query window',
            },
        )

    envelope = _build_historical_envelope(
        frame=frame,
        mode=request.mode,
        dataset=dataset,
        warnings=warnings,
        rights_state=query_rights_state,
        rights_provisional=query_rights_provisional,
    )
    return RawQueryResponse.model_validate(envelope)


async def _handle_historical_fred_series_metrics(
    *,
    request: HistoricalFREDSeriesMetricsRequest,
    x_clickhouse_token: str | None,
) -> RawQueryResponse:
    dataset: RawQueryDataset = 'fred_series_metrics'
    filters_payload = (
        [
            filter_clause.model_dump(mode='json')
            for filter_clause in request.filters
        ]
        if request.filters is not None
        else None
    )
    try:
        query_rights_decision = resolve_query_rights(
            dataset=dataset,
            auth_token=x_clickhouse_token,
        )
    except RightsGateError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': exc.code, 'message': str(exc)},
        ) from exc

    try:
        query_rights_state, query_rights_provisional = (
            query_rights_decision.response_metadata()
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'QUERY_RIGHTS_METADATA_ERROR',
                'message': str(exc),
            },
        ) from exc

    warnings = _build_historical_warnings(
        n_latest_rows=request.n_latest_rows,
        n_random_rows=request.n_random_rows,
    )
    try:
        fred_publish_warnings = await run_in_threadpool(
            build_fred_publish_freshness_warnings,
            x_clickhouse_token,
            FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_WARNING_RUNTIME_ERROR',
                'message': str(exc),
            },
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_WARNING_BACKEND_ERROR',
                'message': str(exc),
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_WARNING_UNKNOWN_ERROR',
                'message': str(exc),
            },
        ) from exc
    warnings.extend(fred_publish_warnings)

    try:
        await run_in_threadpool(
            emit_fred_warning_alerts_and_audit,
            warnings=warnings,
            dataset=dataset,
            mode=request.mode,
            strict=request.strict,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_ALERT_AUDIT_RUNTIME_ERROR',
                'message': str(exc),
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'HISTORICAL_ALERT_AUDIT_UNKNOWN_ERROR',
                'message': str(exc),
            },
        ) from exc

    if request.strict and warnings:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'STRICT_MODE_WARNING_FAILURE',
                'message': 'Warnings present while strict=true',
                'warnings': [warning.model_dump() for warning in warnings],
            },
        )

    try:
        frame = await _run_native_query_with_limits(
            query_fred_series_metrics_data,
            mode=request.mode,
            start_date=request.start_date,
            end_date=request.end_date,
            n_latest_rows=request.n_latest_rows,
            n_random_rows=request.n_random_rows,
            fields=request.fields,
            filters=filters_payload,
            auth_token=x_clickhouse_token,
            show_summary=False,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_RUNTIME_ERROR', 'message': str(exc)},
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_BACKEND_ERROR', 'message': str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_UNKNOWN_ERROR', 'message': str(exc)},
        ) from exc

    if frame.height == 0:
        raise HTTPException(
            status_code=404,
            detail={
                'code': 'HISTORICAL_NO_DATA',
                'message': 'No rows found for query window',
            },
        )

    envelope = _build_historical_envelope(
        frame=frame,
        mode=request.mode,
        dataset=dataset,
        warnings=warnings,
        rights_state=query_rights_state,
        rights_provisional=query_rights_provisional,
    )
    return RawQueryResponse.model_validate(envelope)


async def _handle_historical_bitcoin_dataset(
    *,
    dataset: RawQueryDataset,
    request: HistoricalBitcoinDatasetRequest,
    x_clickhouse_token: str | None,
) -> RawQueryResponse:
    filters_payload = (
        [
            filter_clause.model_dump(mode='json')
            for filter_clause in request.filters
        ]
        if request.filters is not None
        else None
    )
    try:
        query_rights_decision = resolve_query_rights(
            dataset=dataset,
            auth_token=x_clickhouse_token,
        )
    except RightsGateError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': exc.code, 'message': str(exc)},
        ) from exc

    try:
        query_rights_state, query_rights_provisional = (
            query_rights_decision.response_metadata()
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'QUERY_RIGHTS_METADATA_ERROR',
                'message': str(exc),
            },
        ) from exc

    warnings = _build_historical_warnings(
        n_latest_rows=request.n_latest_rows,
        n_random_rows=request.n_random_rows,
    )
    if request.strict and warnings:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'STRICT_MODE_WARNING_FAILURE',
                'message': 'Warnings present while strict=true',
                'warnings': [warning.model_dump() for warning in warnings],
            },
        )

    try:
        frame = await _run_native_query_with_limits(
            query_bitcoin_dataset_data,
            dataset=dataset,
            mode=request.mode,
            start_date=request.start_date,
            end_date=request.end_date,
            n_latest_rows=request.n_latest_rows,
            n_random_rows=request.n_random_rows,
            fields=request.fields,
            filters=filters_payload,
            auth_token=x_clickhouse_token,
            show_summary=False,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={'code': 'HISTORICAL_CONTRACT_ERROR', 'message': str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_RUNTIME_ERROR', 'message': str(exc)},
        ) from exc
    except DatabaseError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_BACKEND_ERROR', 'message': str(exc)},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'HISTORICAL_UNKNOWN_ERROR', 'message': str(exc)},
        ) from exc

    if frame.height == 0:
        raise HTTPException(
            status_code=404,
            detail={
                'code': 'HISTORICAL_NO_DATA',
                'message': 'No rows found for query window',
            },
        )

    envelope = _build_historical_envelope(
        frame=frame,
        mode=request.mode,
        dataset=dataset,
        warnings=warnings,
        rights_state=query_rights_state,
        rights_provisional=query_rights_provisional,
    )
    return RawQueryResponse.model_validate(envelope)


@app.post('/v1/historical/binance/spot/trades', response_model=RawQueryResponse)
async def historical_binance_spot_trades(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_spot_trades(
        source='binance',
        request=_parse_historical_trades_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/binance/spot/klines', response_model=RawQueryResponse)
async def historical_binance_spot_klines(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_spot_klines(
        source='binance',
        request=_parse_historical_klines_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/okx/spot/trades', response_model=RawQueryResponse)
async def historical_okx_spot_trades(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_spot_trades(
        source='okx',
        request=_parse_historical_trades_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/okx/spot/klines', response_model=RawQueryResponse)
async def historical_okx_spot_klines(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_spot_klines(
        source='okx',
        request=_parse_historical_klines_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bybit/spot/trades', response_model=RawQueryResponse)
async def historical_bybit_spot_trades(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_spot_trades(
        source='bybit',
        request=_parse_historical_trades_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bybit/spot/klines', response_model=RawQueryResponse)
async def historical_bybit_spot_klines(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_spot_klines(
        source='bybit',
        request=_parse_historical_klines_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/etf/daily_metrics', response_model=RawQueryResponse)
async def historical_etf_daily_metrics(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_etf_daily_metrics(
        request=_parse_historical_etf_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/fred/series_metrics', response_model=RawQueryResponse)
async def historical_fred_series_metrics(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_fred_series_metrics(
        request=_parse_historical_fred_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bitcoin/block_headers', response_model=RawQueryResponse)
async def historical_bitcoin_block_headers(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_block_headers',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bitcoin/block_transactions', response_model=RawQueryResponse)
async def historical_bitcoin_block_transactions(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_block_transactions',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bitcoin/mempool_state', response_model=RawQueryResponse)
async def historical_bitcoin_mempool_state(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_mempool_state',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bitcoin/block_fee_totals', response_model=RawQueryResponse)
async def historical_bitcoin_block_fee_totals(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_block_fee_totals',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post(
    '/v1/historical/bitcoin/block_subsidy_schedule',
    response_model=RawQueryResponse,
)
async def historical_bitcoin_block_subsidy_schedule(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_block_subsidy_schedule',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post(
    '/v1/historical/bitcoin/network_hashrate_estimate',
    response_model=RawQueryResponse,
)
async def historical_bitcoin_network_hashrate_estimate(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_network_hashrate_estimate',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/historical/bitcoin/circulating_supply', response_model=RawQueryResponse)
async def historical_bitcoin_circulating_supply(
    payload: dict[str, Any],
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    return await _handle_historical_bitcoin_dataset(
        dataset='bitcoin_circulating_supply',
        request=_parse_historical_bitcoin_request(payload),
        x_clickhouse_token=x_clickhouse_token,
    )


@app.post('/v1/raw/query', response_model=RawQueryResponse)
async def query_raw(
    request: RawQueryRequest,
    x_clickhouse_token: str | None = Header(default=None, alias='X-ClickHouse-Token'),
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    global _queued_requests, _queued_aligned_requests

    is_aligned_mode = request.mode == 'aligned_1s'
    queue_limit = ALIGNED_QUERY_MAX_QUEUE if is_aligned_mode else QUERY_MAX_QUEUE
    queue_error_code = (
        'ALIGNED_QUERY_QUEUE_LIMIT_REACHED'
        if is_aligned_mode
        else 'QUERY_QUEUE_LIMIT_REACHED'
    )
    queue_error_message = (
        'Aligned query queue limit reached'
        if is_aligned_mode
        else 'Query queue limit reached'
    )
    queue_lock = _queue_aligned_lock if is_aligned_mode else _queue_lock
    request_is_queued = False

    async with queue_lock:
        current_queue_size = (
            _queued_aligned_requests if is_aligned_mode else _queued_requests
        )
        if current_queue_size >= queue_limit:
            raise HTTPException(
                status_code=503,
                detail={
                    'code': queue_error_code,
                    'message': queue_error_message,
                },
            )
        if is_aligned_mode:
            _queued_aligned_requests += 1
        else:
            _queued_requests += 1
        request_is_queued = True

    try:
        semaphore = _aligned_query_semaphore if is_aligned_mode else _query_semaphore
        async with semaphore:
            async with queue_lock:
                if request_is_queued:
                    if is_aligned_mode:
                        _queued_aligned_requests -= 1
                    else:
                        _queued_requests -= 1
                    request_is_queued = False

            query_source = _resolve_single_source_or_raise(request)
            filters_payload = (
                [
                    filter_clause.model_dump(mode='json')
                    for filter_clause in request.filters
                ]
                if request.filters is not None
                else None
            )
            try:
                query_rights_decision = resolve_query_rights(
                    dataset=query_source,
                    auth_token=x_clickhouse_token,
                )
            except RightsGateError as exc:
                raise HTTPException(
                    status_code=409,
                    detail={'code': exc.code, 'message': str(exc)},
                ) from exc
            try:
                query_rights_state, query_rights_provisional = (
                    query_rights_decision.response_metadata()
                )
            except RuntimeError as exc:
                raise HTTPException(
                    status_code=503,
                    detail={
                        'code': 'QUERY_RIGHTS_METADATA_ERROR',
                        'message': str(exc),
                    },
                ) from exc
            try:
                if request.mode == 'native':
                    envelope = await run_in_threadpool(
                        query_native_wide_rows_envelope,
                        dataset=query_source,
                        select_cols=request.fields,
                        n_rows=request.n_rows,
                        n_random=request.n_random,
                        time_range=request.time_range,
                        auth_token=x_clickhouse_token,
                        filters=filters_payload,
                    )
                elif request.mode == 'aligned_1s':
                    if query_source not in _ALIGNED_QUERY_DATASETS:
                        raise HTTPException(
                            status_code=409,
                            detail={
                                'code': 'QUERY_CONTRACT_ERROR',
                                'message': (
                                    'aligned_1s mode does not support source='
                                    f'{query_source}'
                                ),
                            },
                        )
                    envelope = await run_in_threadpool(
                        query_aligned_wide_rows_envelope,
                        dataset=query_source,
                        select_cols=request.fields,
                        n_rows=request.n_rows,
                        n_random=request.n_random,
                        time_range=request.time_range,
                        auth_token=x_clickhouse_token,
                        filters=filters_payload,
                    )
                else:
                    raise ValueError(f'Unsupported query mode: {request.mode}')
            except ValueError as exc:
                raise HTTPException(
                    status_code=409,
                    detail={'code': 'QUERY_CONTRACT_ERROR', 'message': str(exc)},
                ) from exc
            except RuntimeError as exc:
                raise HTTPException(
                    status_code=503,
                    detail={'code': 'QUERY_RUNTIME_ERROR', 'message': str(exc)},
                ) from exc
            except DatabaseError as exc:
                raise HTTPException(
                    status_code=503,
                    detail={'code': 'QUERY_BACKEND_ERROR', 'message': str(exc)},
                ) from exc
            except Exception as exc:
                raise HTTPException(
                    status_code=503,
                    detail={'code': 'QUERY_UNKNOWN_ERROR', 'message': str(exc)},
                ) from exc

            warnings = _build_warnings(request)
            freshness: RawQueryFreshness | None = None
            if query_source == 'etf_daily_metrics':
                try:
                    etf_quality_warnings = await run_in_threadpool(
                        build_etf_daily_quality_warnings,
                        x_clickhouse_token,
                        ETF_DAILY_STALE_MAX_AGE_DAYS,
                    )
                except RuntimeError as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_WARNING_RUNTIME_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                except DatabaseError as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_WARNING_BACKEND_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                except Exception as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_WARNING_UNKNOWN_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                warnings.extend(etf_quality_warnings)

            if query_source == 'fred_series_metrics':
                try:
                    fred_publish_warnings = await run_in_threadpool(
                        build_fred_publish_freshness_warnings,
                        x_clickhouse_token,
                        FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS,
                    )
                except RuntimeError as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_WARNING_RUNTIME_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                except DatabaseError as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_WARNING_BACKEND_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                except Exception as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_WARNING_UNKNOWN_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                warnings.extend(fred_publish_warnings)

            if query_source == 'fred_series_metrics':
                try:
                    await run_in_threadpool(
                        emit_fred_warning_alerts_and_audit,
                        warnings=warnings,
                        dataset=query_source,
                        mode=request.mode,
                        strict=request.strict,
                    )
                except RuntimeError as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_ALERT_AUDIT_RUNTIME_ERROR',
                            'message': str(exc),
                        },
                    ) from exc
                except Exception as exc:
                    raise HTTPException(
                        status_code=503,
                        detail={
                            'code': 'QUERY_ALERT_AUDIT_UNKNOWN_ERROR',
                            'message': str(exc),
                        },
                    ) from exc

            if request.mode == 'aligned_1s':
                aligned_freshness, aligned_freshness_warnings = _build_aligned_freshness(
                    envelope=envelope
                )
                freshness = aligned_freshness
                warnings.extend(aligned_freshness_warnings)

            if request.strict and warnings:
                raise HTTPException(
                    status_code=409,
                    detail={
                        'code': 'STRICT_MODE_WARNING_FAILURE',
                        'message': 'Warnings present while strict=true',
                        'warnings': [warning.model_dump() for warning in warnings],
                    },
                )

            if envelope['row_count'] == 0:
                raise HTTPException(
                    status_code=404,
                    detail={
                        'code': 'QUERY_NO_DATA',
                        'message': 'No rows found for query window',
                    },
                )

            envelope['warnings'] = [warning.model_dump() for warning in warnings]
            envelope['freshness'] = (
                freshness.model_dump() if freshness is not None else None
            )
            envelope['sources'] = request.sources
            envelope['view_id'] = request.view_id
            envelope['view_version'] = request.view_version
            envelope['rights_state'] = query_rights_state
            envelope['rights_provisional'] = query_rights_provisional
            return RawQueryResponse.model_validate(envelope)
    finally:
        if request_is_queued:
            async with queue_lock:
                if is_aligned_mode:
                    _queued_aligned_requests -= 1
                else:
                    _queued_requests -= 1


@app.post('/v1/raw/export', response_model=RawExportSubmitResponse, status_code=202)
async def submit_raw_export(
    request: RawExportRequest,
    _: None = Depends(_require_internal_api_key),
) -> RawExportSubmitResponse:
    global _pending_export_dispatch_requests

    if request.mode == 'aligned_1s' and request.dataset not in _ALIGNED_QUERY_DATASETS:
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'EXPORT_CONTRACT_ERROR',
                'message': (
                    'aligned_1s mode does not support dataset='
                    f'{request.dataset}'
                ),
            },
        )

    audit_payload = _build_export_audit_payload(request)

    try:
        rights_decision = resolve_export_rights(
            dataset=request.dataset,
            auth_token=request.auth_token,
        )
    except RightsGateError as exc:
        _audit_or_raise_http(
            event_type='export_submit_rejected',
            export_id=None,
            payload={
                **audit_payload,
                'failure_code': exc.code,
                'failure_message': str(exc),
            },
        )
        raise HTTPException(
            status_code=409,
            detail={'code': exc.code, 'message': str(exc)},
        ) from exc

    if request.auth_token is not None and request.auth_token.strip() != '':
        _audit_or_raise_http(
            event_type='export_submit_rejected',
            export_id=None,
                payload={
                    **audit_payload,
                    'rights_state': rights_decision.rights_state,
                    'rights_provisional': rights_decision.rights_provisional,
                    'failure_code': 'EXPORT_AUTH_TOKEN_UNSUPPORTED',
                    'failure_message': (
                        'auth_token is not supported for export dispatch in this slice'
                ),
            },
        )
        raise HTTPException(
            status_code=409,
            detail={
                'code': 'EXPORT_AUTH_TOKEN_UNSUPPORTED',
                'message': 'auth_token is not supported for export dispatch in this slice',
            },
        )

    async with _pending_export_dispatch_lock:
        if _pending_export_dispatch_requests >= EXPORT_MAX_QUEUE:
            _audit_or_raise_http(
                event_type='export_submit_rejected',
                export_id=None,
                payload={
                    **audit_payload,
                    'rights_state': rights_decision.rights_state,
                    'rights_provisional': rights_decision.rights_provisional,
                    'failure_code': 'EXPORT_QUEUE_LIMIT_REACHED',
                    'failure_message': 'Export queue limit reached',
                },
            )
            raise HTTPException(
                status_code=503,
                detail={
                    'code': 'EXPORT_QUEUE_LIMIT_REACHED',
                    'message': 'Export queue limit reached',
                },
            )
        _pending_export_dispatch_requests += 1

    request_payload = request.model_dump(exclude={'auth_token'})
    try:
        async with _export_dispatch_semaphore:
            try:
                run_snapshot = await run_in_threadpool(
                    launch_export_run,
                    mode=request.mode,
                    export_format=request.format,
                    dataset=request.dataset,
                    request_payload=request_payload,
                    source=rights_decision.source,
                    rights_state=rights_decision.rights_state,
                    rights_provisional=rights_decision.rights_provisional,
                )
            except DagsterGraphQLError as exc:
                _audit_or_raise_http(
                    event_type='export_submit_failed',
                    export_id=None,
                    payload={
                        **audit_payload,
                        'rights_state': rights_decision.rights_state,
                        'rights_provisional': rights_decision.rights_provisional,
                        'failure_code': 'EXPORT_DISPATCH_ERROR',
                        'failure_message': str(exc),
                    },
                )
                raise HTTPException(
                    status_code=503,
                    detail={'code': 'EXPORT_DISPATCH_ERROR', 'message': str(exc)},
                ) from exc
    finally:
        async with _pending_export_dispatch_lock:
            _pending_export_dispatch_requests -= 1

    status = _map_dagster_status(run_snapshot.status)
    async with _export_status_lock:
        _last_export_status[run_snapshot.run_id] = status

    _audit_or_raise_http(
        event_type='export_submit_accepted',
        export_id=run_snapshot.run_id,
        payload={
            **audit_payload,
            'rights_state': rights_decision.rights_state,
            'rights_provisional': rights_decision.rights_provisional,
            'source': rights_decision.source,
            'status': status,
        },
    )

    return RawExportSubmitResponse(
        export_id=run_snapshot.run_id,
        status=status,
        submitted_at=run_snapshot.creation_time,
        status_path=f'/v1/raw/export/{run_snapshot.run_id}',
        rights_state=rights_decision.rights_state,
        rights_provisional=rights_decision.rights_provisional,
        view_id=request.view_id,
        view_version=request.view_version,
    )


@app.get('/v1/raw/export/{export_id}', response_model=RawExportStatusResponse)
async def get_raw_export_status(
    export_id: str,
    _: None = Depends(_require_internal_api_key),
) -> RawExportStatusResponse:
    try:
        run_snapshot = await run_in_threadpool(get_run_snapshot, run_id=export_id)
    except DagsterRunNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                'code': 'EXPORT_NOT_FOUND',
                'message': str(exc),
            },
        )
    except DagsterGraphQLError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'EXPORT_STATUS_ERROR', 'message': str(exc)},
        ) from exc

    try:
        (
            mode,
            export_format,
            dataset,
            source,
            rights_state,
            rights_provisional,
            view_id,
            view_version,
        ) = _read_export_tags(run_snapshot.tags)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'EXPORT_STATUS_METADATA_ERROR', 'message': str(exc)},
        ) from exc

    raw_status = run_snapshot.status
    status = _map_dagster_status(raw_status)
    try:
        artifact = _read_export_artifact(
            export_id=run_snapshot.run_id, export_format=export_format
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={'code': 'EXPORT_ARTIFACT_METADATA_ERROR', 'message': str(exc)},
        ) from exc

    if status == 'succeeded' and artifact is None:
        raise HTTPException(
            status_code=503,
            detail={
                'code': 'EXPORT_ARTIFACT_MISSING',
                'message': 'Run succeeded but export artifact metadata is missing',
            },
        )

    if status == 'failed':
        try:
            persisted_failure = _read_export_failure(export_id=run_snapshot.run_id)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=503,
                detail={'code': 'EXPORT_FAILURE_METADATA_ERROR', 'message': str(exc)},
            ) from exc
        failure = _classify_terminal_failure(
            dagster_status=raw_status,
            persisted_failure=persisted_failure,
        )
        error_code = failure.error_code
        error_message = failure.error_message
    else:
        error_code = None
        error_message = None

    should_emit_transition_event = False
    previous_status: ExportStatus | None = None
    async with _export_status_lock:
        previous_status = _last_export_status.get(run_snapshot.run_id)
        if previous_status != status:
            _last_export_status[run_snapshot.run_id] = status
            should_emit_transition_event = True

    if should_emit_transition_event:
        transition_payload: dict[str, Any] = {
            'dataset': dataset,
            'source': source,
            'rights_state': rights_state,
            'rights_provisional': rights_provisional,
            'format': export_format,
            'from_status': previous_status,
            'to_status': status,
        }
        if error_code is not None:
            transition_payload['error_code'] = error_code
        if error_message is not None:
            transition_payload['error_message'] = error_message
        _audit_or_raise_http(
            event_type='export_status_transition',
            export_id=run_snapshot.run_id,
            payload=transition_payload,
        )

    return RawExportStatusResponse(
        export_id=run_snapshot.run_id,
        status=status,
        mode=mode,
        format=export_format,
        dataset=dataset,
        source=source,
        rights_state=rights_state,
        rights_provisional=rights_provisional,
        view_id=view_id,
        view_version=view_version,
        submitted_at=run_snapshot.creation_time,
        updated_at=run_snapshot.update_time,
        artifact=artifact,
        error_code=error_code,
        error_message=error_message,
    )
