from __future__ import annotations

import asyncio
import os

from clickhouse_connect.driver.exceptions import DatabaseError
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool

from origo.data._internal.generic_endpoints import (
    query_binance_native_wide_rows_envelope,
)

from .schemas import RawQueryRequest, RawQueryResponse, RawQueryWarning

app = FastAPI(title='Origo Raw API', version='0.1.0')

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

if QUERY_MAX_CONCURRENCY <= 0:
    raise RuntimeError('ORIGO_QUERY_MAX_CONCURRENCY must be > 0')
if QUERY_MAX_QUEUE < 0:
    raise RuntimeError('ORIGO_QUERY_MAX_QUEUE must be >= 0')

_query_semaphore = asyncio.Semaphore(QUERY_MAX_CONCURRENCY)
_pending_requests = 0
_pending_lock = asyncio.Lock()


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


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


@app.post('/v1/raw/query', response_model=RawQueryResponse)
async def query_raw(
    request: RawQueryRequest,
    _: None = Depends(_require_internal_api_key),
) -> RawQueryResponse:
    global _pending_requests

    async with _pending_lock:
        if _pending_requests >= QUERY_MAX_QUEUE:
            raise HTTPException(
                status_code=503,
                detail={
                    'code': 'QUERY_QUEUE_LIMIT_REACHED',
                    'message': 'Query queue limit reached',
                },
            )
        _pending_requests += 1

    try:
        async with _query_semaphore:
            try:
                envelope = await run_in_threadpool(
                    query_binance_native_wide_rows_envelope,
                    dataset=request.dataset,
                    select_cols=request.fields,
                    month_year=request.month_year,
                    n_rows=request.n_rows,
                    n_random=request.n_random,
                    time_range=request.time_range,
                    include_datetime_col=request.include_datetime,
                    auth_token=request.auth_token,
                )
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
            return RawQueryResponse.model_validate(envelope)
    finally:
        async with _pending_lock:
            _pending_requests -= 1
