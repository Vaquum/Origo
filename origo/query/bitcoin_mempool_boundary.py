from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, cast

from clickhouse_connect import get_client as _raw_get_client

from .native_core import (
    MonthWindow,
    QueryWindow,
    TimeRangeWindow,
    resolve_clickhouse_http_settings,
)

get_client = cast(Any, _raw_get_client)

_BITCOIN_MEMPOOL_SOURCE_ID = 'bitcoin_core'
_BITCOIN_MEMPOOL_STREAM_ID = 'bitcoin_mempool_state'
_BITCOIN_MEMPOOL_NATIVE_TABLE = 'canonical_bitcoin_mempool_state_native_v1'


def _close_client_quietly(client: Any) -> None:
    try:
        client.close()
    except Exception:
        return


def resolve_bitcoin_mempool_capture_boundary_or_raise(
    *,
    auth_token: str | None,
) -> datetime:
    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        compression=True,
    )
    try:
        partition_rows = client.query(
            
                'SELECT min(partition_id) '
                'FROM ('
                'SELECT partition_id, argMax(state, recorded_at_utc) AS state '
                f'FROM {settings.database}.canonical_backfill_partition_proofs '
                f"WHERE source_id = '{_BITCOIN_MEMPOOL_SOURCE_ID}' "
                f"AND stream_id = '{_BITCOIN_MEMPOOL_STREAM_ID}' "
                'GROUP BY partition_id'
                ") WHERE state = 'proved_complete'"
            
        ).result_rows
        if len(partition_rows) != 1:
            raise RuntimeError(
                'Expected one earliest terminal mempool partition row while resolving '
                'bitcoin_mempool_state capture boundary'
            )
        partition_id = partition_rows[0][0]
        if partition_id is None:
            raise RuntimeError(
                'bitcoin_mempool_state capture boundary is unknown because no '
                'terminal proved partition exists'
            )
        boundary_rows = client.query(
            
                f'SELECT min(snapshot_at) FROM {settings.database}.{_BITCOIN_MEMPOOL_NATIVE_TABLE} '
                f"WHERE toDate(snapshot_at) = toDate('{partition_id}')"
            
        ).result_rows
    finally:
        _close_client_quietly(client)

    if len(boundary_rows) != 1:
        raise RuntimeError(
            'Expected one earliest mempool snapshot boundary row while resolving '
            'bitcoin_mempool_state capture boundary'
        )
    boundary_raw = boundary_rows[0][0]
    if boundary_raw is None:
        raise RuntimeError(
            'bitcoin_mempool_state capture boundary partition has no native rows'
        )
    if isinstance(boundary_raw, datetime):
        boundary = boundary_raw
    else:
        normalized = str(boundary_raw).replace(' ', 'T')
        boundary = datetime.fromisoformat(normalized)
    if boundary.tzinfo is None:
        boundary = boundary.replace(tzinfo=UTC)
    return boundary.astimezone(UTC)


def enforce_bitcoin_mempool_query_window_or_raise(
    *,
    window: QueryWindow,
    auth_token: str | None,
) -> None:
    boundary = resolve_bitcoin_mempool_capture_boundary_or_raise(auth_token=auth_token)
    if isinstance(window, TimeRangeWindow):
        requested_start = datetime.fromisoformat(
            window.start_iso.replace('Z', '+00:00')
        ).astimezone(UTC)
        if requested_start < boundary:
            raise ValueError(
                'bitcoin_mempool_state availability begins at '
                f'{boundary.strftime("%Y-%m-%dT%H:%M:%SZ")}; '
                f'requested start={window.start_iso}'
            )
        return
    if isinstance(window, MonthWindow):
        requested_start = date(window.year, window.month, 1)
        if requested_start < boundary.date():
            raise ValueError(
                'bitcoin_mempool_state availability begins on '
                f'{boundary.date().isoformat()}; '
                f'requested month starts on {requested_start.isoformat()}'
            )


def enforce_bitcoin_mempool_historical_window_or_raise(
    *,
    start_date: date | None,
    end_date: date | None,
    auth_token: str | None,
) -> None:
    boundary = resolve_bitcoin_mempool_capture_boundary_or_raise(auth_token=auth_token)
    boundary_date = boundary.date()
    if start_date is not None and start_date < boundary_date:
        raise ValueError(
            'bitcoin_mempool_state availability begins on '
            f'{boundary_date.isoformat()}; requested start_date={start_date.isoformat()}'
        )
    if start_date is None and end_date is not None and end_date < boundary_date:
        raise ValueError(
            'bitcoin_mempool_state availability begins on '
            f'{boundary_date.isoformat()}; requested end_date={end_date.isoformat()}'
        )
