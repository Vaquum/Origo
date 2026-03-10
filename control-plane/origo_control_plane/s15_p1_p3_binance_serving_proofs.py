from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.data._internal.generic_endpoints import (
    query_aligned,
    query_aligned_wide_rows_envelope,
    query_native,
    query_native_wide_rows_envelope,
)
from origo.events.precision import canonicalize_payload_json_with_precision
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.binance_aligned_projector import (
    project_binance_futures_trades_aligned,
    project_binance_spot_agg_trades_aligned,
    project_binance_spot_trades_aligned,
)
from origo_control_plane.utils.binance_native_projector import (
    project_binance_futures_trades_native,
    project_binance_spot_agg_trades_native,
    project_binance_spot_trades_native,
)

Dataset = Literal['spot_trades', 'spot_agg_trades', 'futures_trades']

_PROOF_DB_SUFFIX = '_s15_p1_p3_proof'
_SLICE_DIR = Path('spec/slices/slice-15-binance-event-sourcing-port')
_WINDOW_START_ISO = '2024-01-04T00:00:00Z'
_WINDOW_END_ISO = '2024-01-04T00:00:03Z'


@dataclass(frozen=True)
class FixtureEvent:
    source_offset_or_equivalent: str
    source_event_time_utc: datetime
    payload_raw: bytes


@dataclass(frozen=True)
class StreamFixture:
    dataset: Dataset
    stream_id: Dataset
    partition_id: str
    events: tuple[FixtureEvent, ...]


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _parse_iso_utc(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _to_ms(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1000)


def _canonical_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _normalize_float(value: Any, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal, str)):
        raise RuntimeError(f'{label} must be numeric, got {type(value).__name__}')
    as_decimal = Decimal(str(value))
    return float(as_decimal.quantize(Decimal('0.000000000001')))


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must not be bool')
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(f'{label} must be int-compatible') from exc
    raise RuntimeError(f'{label} must be int-compatible')


def _require_datetime(value: Any, *, label: str) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        return _parse_iso_utc(value)
    raise RuntimeError(f'{label} must be datetime or ISO string')


def _canonical_payload_for_event(*, stream_id: Dataset, payload_raw: bytes) -> dict[str, Any]:
    canonical_json = canonicalize_payload_json_with_precision(
        source_id='binance',
        stream_id=stream_id,
        payload_raw=payload_raw,
        payload_encoding='utf-8',
    )
    parsed = json.loads(canonical_json)
    if not isinstance(parsed, dict):
        raise RuntimeError('Canonical payload must decode to object')
    raw_map = cast(dict[Any, Any], parsed)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError('Canonical payload keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _fixtures() -> tuple[StreamFixture, ...]:
    partition_id = '2024-01-04'
    return (
        StreamFixture(
            dataset='spot_trades',
            stream_id='spot_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='7001',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":7001,"price":"42000.10000000","qty":"0.01000000",'
                        b'"quote_qty":"420.00100000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='7002',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 700000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"7002","price":"42000.20000000","qty":"0.02000000",'
                        b'"quote_qty":"840.00400000","is_buyer_maker":true,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='7003',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"7003","price":"41999.90000000","qty":"0.03000000",'
                        b'"quote_qty":"1259.99700000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='7004',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 900000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"7004","price":"42000.40000000","qty":"0.04000000",'
                        b'"quote_qty":"1680.01600000","is_buyer_maker":true,"is_best_match":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='7005',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 2, 100000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"7005","price":"42000.30000000","qty":"0.05000000",'
                        b'"quote_qty":"2100.01500000","is_buyer_maker":false,"is_best_match":true}'
                    ),
                ),
            ),
        ),
        StreamFixture(
            dataset='spot_agg_trades',
            stream_id='spot_agg_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='8001',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 200000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":8001,"price":"42000.05000000","qty":"0.11000000",'
                        b'"first_trade_id":7999,"last_trade_id":8001,"is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='8002',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 900000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":"8002","price":"42000.25000000","qty":"0.12000000",'
                        b'"first_trade_id":"8002","last_trade_id":"8004","is_buyer_maker":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='8003',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 600000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":"8003","price":"41999.95000000","qty":"0.09000000",'
                        b'"first_trade_id":"8005","last_trade_id":"8008","is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='8004',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 2, 200000, tzinfo=UTC),
                    payload_raw=(
                        b'{"agg_trade_id":"8004","price":"42000.35000000","qty":"0.13000000",'
                        b'"first_trade_id":"8009","last_trade_id":"8012","is_buyer_maker":true}'
                    ),
                ),
            ),
        ),
        StreamFixture(
            dataset='futures_trades',
            stream_id='futures_trades',
            partition_id=partition_id,
            events=(
                FixtureEvent(
                    source_offset_or_equivalent='9001',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 0, 150000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":9001,"price":"42010.10000000","qty":"0.21000000",'
                        b'"quote_qty":"8822.12100000","is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9002',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 200000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"9002","price":"42010.20000000","qty":"0.18000000",'
                        b'"quote_qty":"7561.83600000","is_buyer_maker":true}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9003',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 1, 800000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"9003","price":"42009.90000000","qty":"0.24000000",'
                        b'"quote_qty":"10082.37600000","is_buyer_maker":false}'
                    ),
                ),
                FixtureEvent(
                    source_offset_or_equivalent='9004',
                    source_event_time_utc=datetime(2024, 1, 4, 0, 0, 2, 400000, tzinfo=UTC),
                    payload_raw=(
                        b'{"trade_id":"9004","price":"42010.40000000","qty":"0.20000000",'
                        b'"quote_qty":"8402.08000000","is_buyer_maker":true}'
                    ),
                ),
            ),
        ),
    )


@contextmanager
def _override_clickhouse_database(database: str):
    previous_value = os.environ.get('CLICKHOUSE_DATABASE')
    os.environ['CLICKHOUSE_DATABASE'] = database
    try:
        yield
    finally:
        if previous_value is None:
            os.environ.pop('CLICKHOUSE_DATABASE', None)
        else:
            os.environ['CLICKHOUSE_DATABASE'] = previous_value


def _expected_native_rows(fixture: StreamFixture) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in fixture.events:
        payload = _canonical_payload_for_event(
            stream_id=fixture.stream_id,
            payload_raw=event.payload_raw,
        )
        timestamp_ms = _to_ms(event.source_event_time_utc)

        if fixture.dataset == 'spot_trades':
            rows.append(
                {
                    'trade_id': _require_int(payload.get('trade_id'), label='trade_id'),
                    'timestamp': timestamp_ms,
                    'price': _normalize_float(payload.get('price'), label='price'),
                    'quantity': _normalize_float(payload.get('qty'), label='qty'),
                    'quote_quantity': _normalize_float(
                        payload.get('quote_qty'),
                        label='quote_qty',
                    ),
                    'is_buyer_maker': 1
                    if bool(payload.get('is_buyer_maker'))
                    else 0,
                    'is_best_match': 1 if bool(payload.get('is_best_match')) else 0,
                    'datetime': event.source_event_time_utc.isoformat(),
                }
            )
        elif fixture.dataset == 'spot_agg_trades':
            rows.append(
                {
                    'agg_trade_id': _require_int(
                        payload.get('agg_trade_id'),
                        label='agg_trade_id',
                    ),
                    'timestamp': timestamp_ms,
                    'price': _normalize_float(payload.get('price'), label='price'),
                    'quantity': _normalize_float(payload.get('qty'), label='qty'),
                    'first_trade_id': _require_int(
                        payload.get('first_trade_id'),
                        label='first_trade_id',
                    ),
                    'last_trade_id': _require_int(
                        payload.get('last_trade_id'),
                        label='last_trade_id',
                    ),
                    'is_buyer_maker': 1
                    if bool(payload.get('is_buyer_maker'))
                    else 0,
                    'datetime': event.source_event_time_utc.isoformat(),
                }
            )
        else:
            rows.append(
                {
                    'futures_trade_id': _require_int(
                        payload.get('trade_id'),
                        label='trade_id',
                    ),
                    'timestamp': timestamp_ms,
                    'price': _normalize_float(payload.get('price'), label='price'),
                    'quantity': _normalize_float(payload.get('qty'), label='qty'),
                    'quote_quantity': _normalize_float(
                        payload.get('quote_qty'),
                        label='quote_qty',
                    ),
                    'is_buyer_maker': 1
                    if bool(payload.get('is_buyer_maker'))
                    else 0,
                    'datetime': event.source_event_time_utc.isoformat(),
                }
            )

    if fixture.dataset == 'spot_trades':
        rows.sort(key=lambda row: (cast(str, row['datetime']), cast(int, row['trade_id'])))
    elif fixture.dataset == 'spot_agg_trades':
        rows.sort(
            key=lambda row: (
                cast(str, row['datetime']),
                cast(int, row['agg_trade_id']),
            )
        )
    else:
        rows.sort(
            key=lambda row: (
                cast(str, row['datetime']),
                cast(int, row['futures_trade_id']),
            )
        )

    return rows


def _expected_aligned_rows(fixture: StreamFixture) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[dict[str, Any]]] = {}
    for event in fixture.events:
        payload = _canonical_payload_for_event(
            stream_id=fixture.stream_id,
            payload_raw=event.payload_raw,
        )
        second_bucket = event.source_event_time_utc.replace(microsecond=0)
        bucket_rows = buckets.get(second_bucket)
        if bucket_rows is None:
            bucket_rows = []
            buckets[second_bucket] = bucket_rows
        bucket_rows.append(
            {
                'source_offset_or_equivalent': event.source_offset_or_equivalent,
                'price': Decimal(str(payload.get('price'))),
                'qty': Decimal(str(payload.get('qty'))),
                'quote_qty': Decimal(str(payload.get('quote_qty')))
                if payload.get('quote_qty') is not None
                else Decimal(str(payload.get('price'))) * Decimal(str(payload.get('qty'))),
            }
        )

    output: list[dict[str, Any]] = []
    for aligned_at_utc in sorted(buckets):
        ordered = sorted(
            buckets[aligned_at_utc],
            key=lambda row: int(cast(str, row['source_offset_or_equivalent'])),
        )
        prices = [cast(Decimal, row['price']) for row in ordered]
        quantity_sum = sum((cast(Decimal, row['qty']) for row in ordered), Decimal('0'))
        quote_sum = sum((cast(Decimal, row['quote_qty']) for row in ordered), Decimal('0'))
        output.append(
            {
                'aligned_at_utc': aligned_at_utc.isoformat(),
                'open_price': _normalize_float(prices[0], label='open_price'),
                'high_price': _normalize_float(max(prices), label='high_price'),
                'low_price': _normalize_float(min(prices), label='low_price'),
                'close_price': _normalize_float(prices[-1], label='close_price'),
                'quantity_sum': _normalize_float(quantity_sum, label='quantity_sum'),
                'quote_volume_sum': _normalize_float(quote_sum, label='quote_volume_sum'),
                'trade_count': len(ordered),
            }
        )

    return output


def _actual_native_rows(dataset: Dataset) -> list[dict[str, Any]]:
    if dataset == 'spot_trades':
        select_cols: tuple[str, ...] = (
            'trade_id',
            'timestamp',
            'price',
            'quantity',
            'quote_quantity',
            'is_buyer_maker',
            'is_best_match',
            'datetime',
        )
    elif dataset == 'spot_agg_trades':
        select_cols = (
            'agg_trade_id',
            'timestamp',
            'price',
            'quantity',
            'first_trade_id',
            'last_trade_id',
            'is_buyer_maker',
            'datetime',
        )
    else:
        select_cols = (
            'futures_trade_id',
            'timestamp',
            'price',
            'quantity',
            'quote_quantity',
            'is_buyer_maker',
            'datetime',
        )

    frame = query_native(
        dataset=dataset,
        select_cols=select_cols,
        time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
        include_datetime_col=True,
        datetime_iso_output=False,
        show_summary=False,
        auth_token=None,
    )

    rows: list[dict[str, Any]] = []
    for raw_row in frame.to_dicts():
        normalized: dict[str, Any] = {}
        for key, value in raw_row.items():
            if key == 'datetime':
                normalized[key] = _require_datetime(value, label='datetime').isoformat()
            elif key in {'price', 'quantity', 'quote_quantity'}:
                normalized[key] = _normalize_float(value, label=key)
            elif key in {'is_buyer_maker', 'is_best_match'}:
                if isinstance(value, bool):
                    normalized[key] = 1 if value else 0
                else:
                    normalized[key] = _require_int(value, label=key)
            elif key in {'trade_id', 'agg_trade_id', 'futures_trade_id', 'timestamp', 'first_trade_id', 'last_trade_id'}:
                normalized[key] = _require_int(value, label=key)
            else:
                normalized[key] = value
        rows.append(normalized)

    return rows


def _actual_aligned_rows(dataset: Dataset) -> list[dict[str, Any]]:
    frame = query_aligned(
        dataset=dataset,
        select_cols=(
            'aligned_at_utc',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'quantity_sum',
            'quote_volume_sum',
            'trade_count',
        ),
        time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
        datetime_iso_output=False,
        show_summary=False,
        auth_token=None,
    )

    rows: list[dict[str, Any]] = []
    for raw_row in frame.to_dicts():
        rows.append(
            {
                'aligned_at_utc': _require_datetime(
                    raw_row.get('aligned_at_utc'),
                    label='aligned_at_utc',
                ).isoformat(),
                'open_price': _normalize_float(raw_row.get('open_price'), label='open_price'),
                'high_price': _normalize_float(raw_row.get('high_price'), label='high_price'),
                'low_price': _normalize_float(raw_row.get('low_price'), label='low_price'),
                'close_price': _normalize_float(raw_row.get('close_price'), label='close_price'),
                'quantity_sum': _normalize_float(raw_row.get('quantity_sum'), label='quantity_sum'),
                'quote_volume_sum': _normalize_float(
                    raw_row.get('quote_volume_sum'),
                    label='quote_volume_sum',
                ),
                'trade_count': _require_int(raw_row.get('trade_count'), label='trade_count'),
            }
        )

    return rows


def _acceptance_for_dataset(dataset: Dataset) -> dict[str, Any]:
    native_envelope = query_native_wide_rows_envelope(
        dataset=dataset,
        select_cols=None,
        time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
        include_datetime_col=True,
        auth_token=None,
    )
    if native_envelope.get('mode') != 'native' or native_envelope.get('source') != dataset:
        raise RuntimeError(f'S15-P1 native envelope mismatch for dataset={dataset}')
    native_row_count = _require_int(native_envelope.get('row_count'), label='row_count')
    if native_row_count <= 0:
        raise RuntimeError(f'S15-P1 expected native rows for dataset={dataset}')

    aligned_envelope = query_aligned_wide_rows_envelope(
        dataset=dataset,
        select_cols=None,
        time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
        auth_token=None,
    )
    if aligned_envelope.get('mode') != 'aligned_1s' or aligned_envelope.get('source') != dataset:
        raise RuntimeError(f'S15-P1 aligned envelope mismatch for dataset={dataset}')
    aligned_row_count = _require_int(aligned_envelope.get('row_count'), label='row_count')
    if aligned_row_count <= 0:
        raise RuntimeError(f'S15-P1 expected aligned rows for dataset={dataset}')

    native_rows = cast(list[dict[str, Any]], native_envelope.get('rows'))
    aligned_rows = cast(list[dict[str, Any]], aligned_envelope.get('rows'))

    return {
        'native': {
            'row_count': native_row_count,
            'schema': native_envelope.get('schema'),
            'rows_hash_sha256': _canonical_hash(native_rows),
        },
        'aligned_1s': {
            'row_count': aligned_row_count,
            'schema': aligned_envelope.get('schema'),
            'rows_hash_sha256': _canonical_hash(aligned_rows),
        },
    }


def _parity_for_dataset(fixture: StreamFixture) -> dict[str, Any]:
    expected_native_rows = _expected_native_rows(fixture)
    actual_native_rows = _actual_native_rows(fixture.dataset)

    expected_aligned_rows = _expected_aligned_rows(fixture)
    actual_aligned_rows = _actual_aligned_rows(fixture.dataset)

    expected_native_hash = _canonical_hash(expected_native_rows)
    actual_native_hash = _canonical_hash(actual_native_rows)
    expected_aligned_hash = _canonical_hash(expected_aligned_rows)
    actual_aligned_hash = _canonical_hash(actual_aligned_rows)

    native_match = expected_native_hash == actual_native_hash
    aligned_match = expected_aligned_hash == actual_aligned_hash

    if not native_match:
        raise RuntimeError(
            'S15-P2 native parity mismatch '
            f'dataset={fixture.dataset} expected={expected_native_hash} actual={actual_native_hash}'
        )
    if not aligned_match:
        raise RuntimeError(
            'S15-P2 aligned parity mismatch '
            f'dataset={fixture.dataset} expected={expected_aligned_hash} actual={actual_aligned_hash}'
        )

    return {
        'dataset': fixture.dataset,
        'native': {
            'row_count': len(actual_native_rows),
            'expected_rows_hash_sha256': expected_native_hash,
            'actual_rows_hash_sha256': actual_native_hash,
            'match': native_match,
        },
        'aligned_1s': {
            'row_count': len(actual_aligned_rows),
            'expected_rows_hash_sha256': expected_aligned_hash,
            'actual_rows_hash_sha256': actual_aligned_hash,
            'match': aligned_match,
        },
    }


def _source_payload_checksums(fixtures: tuple[StreamFixture, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for fixture in fixtures:
        payload[fixture.dataset] = {
            event.source_offset_or_equivalent: hashlib.sha256(event.payload_raw).hexdigest()
            for event in fixture.events
        }
    return payload


def _project_all_streams(
    *,
    client: ClickHouseClient,
    database: str,
    fixtures: tuple[StreamFixture, ...],
    run_id: str,
    projected_at_utc: datetime,
) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for fixture in fixtures:
        partition_ids = {fixture.partition_id}
        if fixture.dataset == 'spot_trades':
            native_summary = project_binance_spot_trades_native(
                client=client,
                database=database,
                partition_ids=partition_ids,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
            )
            aligned_summary = project_binance_spot_trades_aligned(
                client=client,
                database=database,
                partition_ids=partition_ids,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
            )
        elif fixture.dataset == 'spot_agg_trades':
            native_summary = project_binance_spot_agg_trades_native(
                client=client,
                database=database,
                partition_ids=partition_ids,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
            )
            aligned_summary = project_binance_spot_agg_trades_aligned(
                client=client,
                database=database,
                partition_ids=partition_ids,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
            )
        else:
            native_summary = project_binance_futures_trades_native(
                client=client,
                database=database,
                partition_ids=partition_ids,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
            )
            aligned_summary = project_binance_futures_trades_aligned(
                client=client,
                database=database,
                partition_ids=partition_ids,
                run_id=run_id,
                projected_at_utc=projected_at_utc,
            )

        summaries[fixture.dataset] = {
            'native': native_summary.to_dict(),
            'aligned_1s': aligned_summary.to_dict(),
        }

    return summaries


def _write_all_events(
    *,
    writer: CanonicalEventWriter,
    fixtures: tuple[StreamFixture, ...],
    run_id: str,
) -> dict[str, list[str]]:
    statuses: dict[str, list[str]] = {}
    for fixture in fixtures:
        stream_statuses: list[str] = []
        for event in fixture.events:
            write_result = writer.write_event(
                CanonicalEventWriteInput(
                    source_id='binance',
                    stream_id=fixture.stream_id,
                    partition_id=fixture.partition_id,
                    source_offset_or_equivalent=event.source_offset_or_equivalent,
                    source_event_time_utc=event.source_event_time_utc,
                    ingested_at_utc=event.source_event_time_utc,
                    payload_content_type='application/json',
                    payload_encoding='utf-8',
                    payload_raw=event.payload_raw,
                    run_id=run_id,
                )
            )
            stream_statuses.append(write_result.status)
        statuses[fixture.dataset] = stream_statuses
    return statuses


def _run_projection_pass(
    *,
    pass_id: str,
    fixtures: tuple[StreamFixture, ...],
) -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}_{pass_id}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        writer = CanonicalEventWriter(client=admin_client, database=proof_database)
        write_statuses = _write_all_events(
            writer=writer,
            fixtures=fixtures,
            run_id=f's15-p1-p3-{pass_id}-ingest',
        )
        for fixture in fixtures:
            expected = ['inserted'] * len(fixture.events)
            observed = write_statuses[fixture.dataset]
            if observed != expected:
                raise RuntimeError(
                    'S15-P1/P2/P3 expected all canonical writes inserted '
                    f'dataset={fixture.dataset} expected={expected} observed={observed}'
                )

        projection_summaries = _project_all_streams(
            client=admin_client,
            database=proof_database,
            fixtures=fixtures,
            run_id=f's15-p1-p3-{pass_id}-project',
            projected_at_utc=datetime(2024, 1, 4, 0, 5, 0, tzinfo=UTC),
        )

        with _override_clickhouse_database(proof_database):
            acceptance = {
                fixture.dataset: _acceptance_for_dataset(fixture.dataset)
                for fixture in fixtures
            }
            parity = {
                fixture.dataset: _parity_for_dataset(fixture)
                for fixture in fixtures
            }

            fingerprints = {
                fixture.dataset: {
                    'native': {
                        'rows_hash_sha256': _canonical_hash(
                            _actual_native_rows(fixture.dataset)
                        )
                    },
                    'aligned_1s': {
                        'rows_hash_sha256': _canonical_hash(
                            _actual_aligned_rows(fixture.dataset)
                        )
                    },
                }
                for fixture in fixtures
            }

        return {
            'proof_database': proof_database,
            'write_statuses': write_statuses,
            'projection_summaries': projection_summaries,
            'acceptance': acceptance,
            'parity': parity,
            'fingerprints': fingerprints,
            'source_payload_sha256': _source_payload_checksums(fixtures),
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def run_s15_p1_p3_proofs() -> dict[str, Any]:
    fixtures = _fixtures()
    run_1 = _run_projection_pass(pass_id='run_1', fixtures=fixtures)
    run_2 = _run_projection_pass(pass_id='run_2', fixtures=fixtures)

    deterministic_match = run_1['fingerprints'] == run_2['fingerprints']
    if not deterministic_match:
        raise RuntimeError('S15-P3 determinism failed: fingerprint mismatch between runs')

    for fixture in fixtures:
        dataset = fixture.dataset
        parity_native_match = cast(bool, run_1['parity'][dataset]['native']['match'])
        parity_aligned_match = cast(bool, run_1['parity'][dataset]['aligned_1s']['match'])
        if not parity_native_match or not parity_aligned_match:
            raise RuntimeError(f'S15-P2 parity failed for dataset={dataset}')

    return {
        'proof_scope': 'Slice 15 S15-P1/S15-P2/S15-P3 Binance serving proofs',
        'window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'datasets': [fixture.dataset for fixture in fixtures],
        'run_1': run_1,
        'run_2': run_2,
        'deterministic_match': deterministic_match,
    }


def main() -> None:
    payload = run_s15_p1_p3_proofs()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)

    acceptance_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 15 S15-P1 fixed-window acceptance for native + aligned_1s',
        'window': payload['window'],
        'acceptance': payload['run_1']['acceptance'],
    }
    parity_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 15 S15-P2 parity checks versus fixture baselines',
        'window': payload['window'],
        'parity': payload['run_1']['parity'],
        'source_payload_sha256': payload['run_1']['source_payload_sha256'],
    }
    determinism_payload = {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 15 S15-P3 replay determinism for native + aligned_1s',
        'window': payload['window'],
        'run_1_fingerprints': payload['run_1']['fingerprints'],
        'run_2_fingerprints': payload['run_2']['fingerprints'],
        'deterministic_match': payload['deterministic_match'],
    }
    baseline_fixture_payload = {
        'column_key': {
            'rows_hash_sha256': 'SHA256 of canonicalized row payload for exact window.',
            'source_payload_sha256': 'SHA256 of raw fixture payload per source offset.',
        },
        'deterministic_match': payload['deterministic_match'],
        'fixture_window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'source_checksums': payload['run_1']['source_payload_sha256'],
        'run_1_fingerprints': payload['run_1']['fingerprints'],
        'run_2_fingerprints': payload['run_2']['fingerprints'],
    }

    (_SLICE_DIR / 'proof-s15-p1-acceptance.json').write_text(
        json.dumps(acceptance_payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    (_SLICE_DIR / 'proof-s15-p2-parity.json').write_text(
        json.dumps(parity_payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    (_SLICE_DIR / 'proof-s15-p3-determinism.json').write_text(
        json.dumps(determinism_payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    (_SLICE_DIR / 'baseline-fixture-2024-01-04_2024-01-04.json').write_text(
        json.dumps(baseline_fixture_payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )

    print(
        json.dumps(
            {'deterministic_match': payload['deterministic_match']},
            sort_keys=True,
        )
    )


if __name__ == '__main__':
    main()
