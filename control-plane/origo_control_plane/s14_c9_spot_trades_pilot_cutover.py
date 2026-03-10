from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.aligned_projector import (
    AlignedProjectionPolicyInput,
    CanonicalAligned1sProjector,
    CanonicalAlignedPolicyStore,
)
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_c9_proof'
_PILOT_VIEW_ID = 'spot_trades_event_pilot'
_PILOT_VIEW_VERSION = 1
_NATIVE_PROJECTOR_ID = 'pilot_spot_trades_native_v1'
_ALIGNED_PROJECTOR_ID = 'pilot_spot_trades_aligned_v1'


@dataclass(frozen=True)
class _ProjectionRunStats:
    batches_processed: int
    events_processed: int
    rows_written: int
    last_checkpoint_status: str | None


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _require_bool(value: Any, *, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in {0, 1}:
            return value == 1
    raise RuntimeError(f'{label} must be bool or 0/1 int, got {value!r}')


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be integer, got {value!r}')
    return value


def _require_datetime(value: Any, *, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise RuntimeError(f'{label} must be datetime, got {type(value).__name__}')
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _format_decimal_8(value: Any, *, label: str) -> str:
    decimal_value: Decimal
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must not be bool')
    if isinstance(value, (int, float, str, Decimal)):
        decimal_value = Decimal(str(value))
    else:
        raise RuntimeError(f'{label} has unsupported numeric type: {type(value).__name__}')
    return format(decimal_value.quantize(Decimal('0.00000001')), '.8f')


def _format_datetime64_ms(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return normalized.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _iso_utc_ms(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return normalized.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def _rows_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _insert_legacy_spot_trades_sample(
    *, client: ClickHouseClient, database: str
) -> list[dict[str, Any]]:
    sample_rows: list[dict[str, Any]] = [
        {
            'trade_id': 101,
            'datetime': datetime(2024, 1, 1, 0, 0, 0, 100000, tzinfo=UTC),
            'price': 41000.1,
            'quantity': 0.01,
            'is_buyer_maker': 0,
            'is_best_match': 1,
        },
        {
            'trade_id': 102,
            'datetime': datetime(2024, 1, 1, 0, 0, 0, 800000, tzinfo=UTC),
            'price': 41000.2,
            'quantity': 0.02,
            'is_buyer_maker': 1,
            'is_best_match': 1,
        },
        {
            'trade_id': 103,
            'datetime': datetime(2024, 1, 1, 0, 0, 1, 200000, tzinfo=UTC),
            'price': 41000.3,
            'quantity': 0.015,
            'is_buyer_maker': 0,
            'is_best_match': 1,
        },
        {
            'trade_id': 104,
            'datetime': datetime(2024, 1, 1, 0, 0, 1, 900000, tzinfo=UTC),
            'price': 40999.9,
            'quantity': 0.025,
            'is_buyer_maker': 1,
            'is_best_match': 1,
        },
        {
            'trade_id': 105,
            'datetime': datetime(2024, 1, 1, 0, 0, 2, 100000, tzinfo=UTC),
            'price': 41000.4,
            'quantity': 0.005,
            'is_buyer_maker': 0,
            'is_best_match': 1,
        },
    ]

    insert_rows: list[tuple[object, ...]] = []
    for row in sample_rows:
        price = float(cast(float, row['price']))
        quantity = float(cast(float, row['quantity']))
        quote_quantity = float(Decimal(str(price)) * Decimal(str(quantity)))
        datetime_utc = _require_datetime(row['datetime'], label='sample.datetime')
        timestamp_ms = int(datetime_utc.timestamp() * 1000)
        insert_rows.append(
            (
                _require_int(row['trade_id'], label='sample.trade_id'),
                price,
                quantity,
                quote_quantity,
                timestamp_ms,
                _require_int(row['is_buyer_maker'], label='sample.is_buyer_maker'),
                _require_int(row['is_best_match'], label='sample.is_best_match'),
                datetime_utc,
            )
        )

    client.execute(
        f'''
        INSERT INTO {database}.binance_trades
        (
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            is_best_match,
            datetime
        )
        VALUES
        ''',
        insert_rows,
    )
    return sample_rows


def _ingest_legacy_window_to_canonical_events(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
    start_utc: datetime,
    end_utc: datetime,
    run_label: str,
) -> dict[str, Any]:
    rows = client.execute(
        f'''
        SELECT
            trade_id,
            timestamp,
            price,
            quantity,
            quote_quantity,
            is_buyer_maker,
            is_best_match,
            datetime
        FROM {database}.binance_trades
        WHERE datetime >= toDateTime64(%(start_utc)s, 3, 'UTC')
            AND datetime < toDateTime64(%(end_utc)s, 3, 'UTC')
        ORDER BY datetime ASC, trade_id ASC
        ''',
        {
            'start_utc': _format_datetime64_ms(start_utc),
            'end_utc': _format_datetime64_ms(end_utc),
        },
    )

    writer = CanonicalEventWriter(client=client, database=database)
    statuses: list[str] = []
    for row in rows:
        trade_id = _require_int(row[0], label='legacy.trade_id')
        timestamp_ms = _require_int(row[1], label='legacy.timestamp')
        source_event_time_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
        payload = {
            'trade_id': trade_id,
            'price': _format_decimal_8(row[2], label='legacy.price'),
            'qty': _format_decimal_8(row[3], label='legacy.quantity'),
            'quote_qty': _format_decimal_8(row[4], label='legacy.quote_quantity'),
            'is_buyer_maker': _require_bool(
                row[5], label='legacy.is_buyer_maker'
            ),
            'is_best_match': _require_bool(row[6], label='legacy.is_best_match'),
        }
        payload_raw = json.dumps(
            payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True
        ).encode('utf-8')

        result = writer.write_event(
            CanonicalEventWriteInput(
                source_id=stream_key.source_id,
                stream_id=stream_key.stream_id,
                partition_id=stream_key.partition_id,
                source_offset_or_equivalent=str(trade_id),
                source_event_time_utc=source_event_time_utc,
                ingested_at_utc=source_event_time_utc,
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=payload_raw,
            )
        )
        statuses.append(result.status)

    return {
        'run_label': run_label,
        'rows_selected': len(rows),
        'statuses': statuses,
        'inserted': statuses.count('inserted'),
        'duplicate': statuses.count('duplicate'),
    }


def _project_native_from_canonical_events(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
    run_id: str,
    projected_at_utc: datetime,
) -> _ProjectionRunStats:
    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_NATIVE_PROJECTOR_ID,
        stream_key=stream_key,
        batch_size=1000,
    )
    runtime.start()
    batches_processed = 0
    events_processed = 0
    rows_written = 0
    last_checkpoint_status: str | None = None
    try:
        while True:
            batch = runtime.fetch_next_batch()
            if batch == []:
                break
            batches_processed += 1
            events_processed += len(batch)

            insert_rows: list[tuple[object, ...]] = []
            for event in batch:
                payload_obj = json.loads(event.payload_json)
                if not isinstance(payload_obj, dict):
                    raise RuntimeError('Canonical payload_json must decode to object')
                payload = cast(dict[str, Any], payload_obj)

                source_event_time_utc = event.source_event_time_utc
                if source_event_time_utc is None:
                    raise RuntimeError(
                        'Native projection requires source_event_time_utc for spot_trades events'
                    )
                source_event_time_utc = source_event_time_utc.astimezone(UTC)
                timestamp_ms = int(source_event_time_utc.timestamp() * 1000)

                trade_id = _require_int(payload.get('trade_id'), label='payload.trade_id')
                price = float(
                    Decimal(
                        _format_decimal_8(payload.get('price'), label='payload.price')
                    )
                )
                quantity = float(
                    Decimal(
                        _format_decimal_8(payload.get('qty'), label='payload.qty')
                    )
                )
                quote_quantity = float(
                    Decimal(
                        _format_decimal_8(
                            payload.get('quote_qty'), label='payload.quote_qty'
                        )
                    )
                )
                is_buyer_maker = 1 if _require_bool(
                    payload.get('is_buyer_maker'), label='payload.is_buyer_maker'
                ) else 0
                is_best_match = 1 if _require_bool(
                    payload.get('is_best_match'), label='payload.is_best_match'
                ) else 0

                insert_rows.append(
                    (
                        trade_id,
                        timestamp_ms,
                        price,
                        quantity,
                        quote_quantity,
                        is_buyer_maker,
                        is_best_match,
                        source_event_time_utc,
                        event.event_id,
                        event.source_offset_or_equivalent,
                        source_event_time_utc,
                        event.ingested_at_utc.astimezone(UTC),
                    )
                )

            client.execute(
                f'''
                INSERT INTO {database}.canonical_binance_spot_trades_native_v1
                (
                    trade_id,
                    timestamp,
                    price,
                    quantity,
                    quote_quantity,
                    is_buyer_maker,
                    is_best_match,
                    datetime,
                    event_id,
                    source_offset_or_equivalent,
                    source_event_time_utc,
                    ingested_at_utc
                )
                VALUES
                ''',
                insert_rows,
            )
            rows_written += len(insert_rows)

            checkpoint_result = runtime.commit_checkpoint(
                last_event=batch[-1],
                run_id=run_id,
                checkpointed_at_utc=projected_at_utc,
                state={
                    'projection': 'canonical_binance_spot_trades_native_v1',
                    'rows_written': len(insert_rows),
                },
            )
            last_checkpoint_status = checkpoint_result.status
    finally:
        runtime.stop()

    return _ProjectionRunStats(
        batches_processed=batches_processed,
        events_processed=events_processed,
        rows_written=rows_written,
        last_checkpoint_status=last_checkpoint_status,
    )


def _project_aligned_from_canonical_events(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
    run_id: str,
    projected_at_utc: datetime,
) -> dict[str, Any]:
    policy_store = CanonicalAlignedPolicyStore(client=client, database=database)
    policy_result = policy_store.record_policy(
        AlignedProjectionPolicyInput(
            view_id=_PILOT_VIEW_ID,
            view_version=_PILOT_VIEW_VERSION,
            stream_key=stream_key,
            bucket_size_seconds=1,
            tier_policy='hot_1s_warm_1m',
            retention_hot_days=7,
            retention_warm_days=90,
            recorded_by_run_id=run_id,
            recorded_at_utc=projected_at_utc,
        )
    )

    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_ALIGNED_PROJECTOR_ID,
        stream_key=stream_key,
        batch_size=1000,
    )
    aligned_projector = CanonicalAligned1sProjector(client=client, database=database)
    backfill_result = aligned_projector.backfill_from_runtime(
        runtime=runtime,
        policy=policy_result.policy_state,
        run_id=run_id,
        projector_id=_ALIGNED_PROJECTOR_ID,
        projected_at_utc=projected_at_utc,
    )
    runtime.stop()
    return {
        'policy_status': policy_result.status,
        'policy_revision': policy_result.policy_state.policy_revision,
        'backfill': {
            'batches_processed': backfill_result.batches_processed,
            'events_processed': backfill_result.events_processed,
            'rows_written': backfill_result.rows_written,
            'last_checkpoint_status': backfill_result.last_checkpoint_status,
        },
    }


def _query_baseline_native_rows(
    *,
    client: ClickHouseClient,
    database: str,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    rows = client.execute(
        f'''
        SELECT
            trade_id,
            timestamp,
            price,
            quantity,
            quote_quantity,
            is_buyer_maker,
            is_best_match,
            datetime
        FROM {database}.binance_trades
        WHERE datetime >= toDateTime64(%(start_utc)s, 3, 'UTC')
            AND datetime < toDateTime64(%(end_utc)s, 3, 'UTC')
        ORDER BY datetime ASC, trade_id ASC
        ''',
        {
            'start_utc': _format_datetime64_ms(start_utc),
            'end_utc': _format_datetime64_ms(end_utc),
        },
    )
    return [
        {
            'trade_id': int(row[0]),
            'timestamp': int(row[1]),
            'price': float(row[2]),
            'quantity': float(row[3]),
            'quote_quantity': float(row[4]),
            'is_buyer_maker': int(row[5]),
            'is_best_match': int(row[6]),
            'datetime': _iso_utc_ms(_require_datetime(row[7], label='baseline.datetime')),
        }
        for row in rows
    ]


def _query_pilot_native_rows(
    *,
    client: ClickHouseClient,
    database: str,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    rows = client.execute(
        f'''
        SELECT
            trade_id,
            timestamp,
            price,
            quantity,
            quote_quantity,
            is_buyer_maker,
            is_best_match,
            datetime
        FROM {database}.canonical_binance_spot_trades_native_v1
        WHERE datetime >= toDateTime64(%(start_utc)s, 3, 'UTC')
            AND datetime < toDateTime64(%(end_utc)s, 3, 'UTC')
        ORDER BY datetime ASC, trade_id ASC
        ''',
        {
            'start_utc': _format_datetime64_ms(start_utc),
            'end_utc': _format_datetime64_ms(end_utc),
        },
    )
    return [
        {
            'trade_id': int(row[0]),
            'timestamp': int(row[1]),
            'price': float(row[2]),
            'quantity': float(row[3]),
            'quote_quantity': float(row[4]),
            'is_buyer_maker': int(row[5]),
            'is_best_match': int(row[6]),
            'datetime': _iso_utc_ms(_require_datetime(row[7], label='pilot.datetime')),
        }
        for row in rows
    ]


def _query_baseline_aligned_rows(
    *,
    client: ClickHouseClient,
    database: str,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    rows = client.execute(
        f'''
        SELECT
            toDateTime64(toStartOfSecond(toDateTime64(datetime, 3, 'UTC')), 3, 'UTC') AS aligned_at_utc,
            argMin(price, trade_id) AS open_price,
            max(price) AS high_price,
            min(price) AS low_price,
            argMax(price, trade_id) AS close_price,
            sum(quantity) AS quantity_sum,
            sum(price * quantity) AS quote_volume_sum,
            count() AS trade_count
        FROM {database}.binance_trades
        WHERE datetime >= toDateTime64(%(start_utc)s, 3, 'UTC')
            AND datetime < toDateTime64(%(end_utc)s, 3, 'UTC')
        GROUP BY aligned_at_utc
        ORDER BY aligned_at_utc ASC
        ''',
        {
            'start_utc': _format_datetime64_ms(start_utc),
            'end_utc': _format_datetime64_ms(end_utc),
        },
    )
    return [
        {
            'aligned_at_utc': _iso_utc_ms(
                _require_datetime(row[0], label='baseline.aligned_at_utc')
            ),
            'open_price': float(row[1]),
            'high_price': float(row[2]),
            'low_price': float(row[3]),
            'close_price': float(row[4]),
            'quantity_sum': float(row[5]),
            'quote_volume_sum': float(row[6]),
            'trade_count': int(row[7]),
        }
        for row in rows
    ]


def _query_pilot_aligned_rows(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    rows = client.execute(
        f'''
        SELECT
            aligned_at_utc,
            payload_rows_json
        FROM {database}.canonical_aligned_1s_aggregates
        WHERE view_id = %(view_id)s
            AND view_version = %(view_version)s
            AND source_id = %(source_id)s
            AND stream_id = %(stream_id)s
            AND partition_id = %(partition_id)s
            AND aligned_at_utc >= toDateTime64(%(start_utc)s, 3, 'UTC')
            AND aligned_at_utc < toDateTime64(%(end_utc)s, 3, 'UTC')
        ORDER BY aligned_at_utc ASC
        ''',
        {
            'view_id': _PILOT_VIEW_ID,
            'view_version': _PILOT_VIEW_VERSION,
            'source_id': stream_key.source_id,
            'stream_id': stream_key.stream_id,
            'partition_id': stream_key.partition_id,
            'start_utc': _format_datetime64_ms(start_utc),
            'end_utc': _format_datetime64_ms(end_utc),
        },
    )

    output: list[dict[str, Any]] = []
    for row in rows:
        aligned_at_utc = _require_datetime(row[0], label='pilot.aligned_at_utc')
        payload_rows_obj = json.loads(str(row[1]))
        if not isinstance(payload_rows_obj, dict):
            raise RuntimeError('Aligned payload_rows_json must decode to object')
        payload_rows_dict = cast(dict[str, Any], payload_rows_obj)
        payload_rows_raw = payload_rows_dict.get('rows')
        if not isinstance(payload_rows_raw, list):
            raise RuntimeError('Aligned payload_rows_json rows must be non-empty list')
        payload_rows = cast(list[Any], payload_rows_raw)
        if payload_rows == []:
            raise RuntimeError('Aligned payload_rows_json rows must be non-empty list')

        prices: list[float] = []
        quantities: list[float] = []
        for event_row in payload_rows:
            if not isinstance(event_row, dict):
                raise RuntimeError('Aligned payload row must be object')
            event_row_dict = cast(dict[str, Any], event_row)
            payload_json = event_row_dict.get('payload_json')
            if not isinstance(payload_json, str):
                raise RuntimeError('Aligned payload row must include payload_json string')
            payload_obj = json.loads(payload_json)
            if not isinstance(payload_obj, dict):
                raise RuntimeError('payload_json must decode to object')
            payload = cast(dict[str, Any], payload_obj)
            prices.append(
                float(
                    Decimal(
                        _format_decimal_8(payload.get('price'), label='payload.price')
                    )
                )
            )
            quantities.append(
                float(
                    Decimal(_format_decimal_8(payload.get('qty'), label='payload.qty'))
                )
            )

        output.append(
            {
                'aligned_at_utc': _iso_utc_ms(aligned_at_utc),
                'open_price': prices[0],
                'high_price': max(prices),
                'low_price': min(prices),
                'close_price': prices[-1],
                'quantity_sum': float(sum(quantities)),
                'quote_volume_sum': float(
                    sum(price * quantity for price, quantity in zip(prices, quantities))
                ),
                'trade_count': len(prices),
            }
        )

    return output


def _assert_parity(
    *,
    label: str,
    baseline_rows: list[dict[str, Any]],
    pilot_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline_hash = _rows_hash(baseline_rows)
    pilot_hash = _rows_hash(pilot_rows)
    if baseline_hash != pilot_hash:
        raise RuntimeError(
            f'S14-C9 pilot parity failed for {label}: '
            f'baseline_hash={baseline_hash} pilot_hash={pilot_hash}'
        )
    return {
        'row_count': len(baseline_rows),
        'baseline_hash_sha256': baseline_hash,
        'pilot_hash_sha256': pilot_hash,
    }


def run_s14_c9_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='spot_trades',
        partition_id='btcusdt',
    )
    start_utc = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_utc = datetime(2024, 1, 1, 0, 0, 3, tzinfo=UTC)

    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        sample_rows = _insert_legacy_spot_trades_sample(
            client=admin_client,
            database=proof_database,
        )

        ingest_run_1 = _ingest_legacy_window_to_canonical_events(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            start_utc=start_utc,
            end_utc=end_utc,
            run_label='ingest_run_1',
        )
        ingest_run_2 = _ingest_legacy_window_to_canonical_events(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            start_utc=start_utc,
            end_utc=end_utc,
            run_label='ingest_run_2_replay',
        )
        if ingest_run_1['inserted'] != len(sample_rows):
            raise RuntimeError(
                'S14-C9 pilot expected first ingest run to insert all sample rows'
            )
        if ingest_run_2['duplicate'] != len(sample_rows):
            raise RuntimeError(
                'S14-C9 pilot expected replay ingest run to classify all rows as duplicate'
            )

        native_run_1 = _project_native_from_canonical_events(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            run_id='s14-c9-native-run-1',
            projected_at_utc=datetime(2024, 1, 1, 0, 10, 0, tzinfo=UTC),
        )
        native_run_2 = _project_native_from_canonical_events(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            run_id='s14-c9-native-run-2',
            projected_at_utc=datetime(2024, 1, 1, 0, 11, 0, tzinfo=UTC),
        )

        aligned_run_1 = _project_aligned_from_canonical_events(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            run_id='s14-c9-aligned-run-1',
            projected_at_utc=datetime(2024, 1, 1, 0, 12, 0, tzinfo=UTC),
        )
        aligned_run_2 = _project_aligned_from_canonical_events(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            run_id='s14-c9-aligned-run-2',
            projected_at_utc=datetime(2024, 1, 1, 0, 13, 0, tzinfo=UTC),
        )

        baseline_native = _query_baseline_native_rows(
            client=admin_client,
            database=proof_database,
            start_utc=start_utc,
            end_utc=end_utc,
        )
        pilot_native = _query_pilot_native_rows(
            client=admin_client,
            database=proof_database,
            start_utc=start_utc,
            end_utc=end_utc,
        )
        baseline_aligned = _query_baseline_aligned_rows(
            client=admin_client,
            database=proof_database,
            start_utc=start_utc,
            end_utc=end_utc,
        )
        pilot_aligned = _query_pilot_aligned_rows(
            client=admin_client,
            database=proof_database,
            stream_key=stream_key,
            start_utc=start_utc,
            end_utc=end_utc,
        )

        native_parity = _assert_parity(
            label='native',
            baseline_rows=baseline_native,
            pilot_rows=pilot_native,
        )
        aligned_parity = _assert_parity(
            label='aligned_1s',
            baseline_rows=baseline_aligned,
            pilot_rows=pilot_aligned,
        )

        return {
            'proof_scope': (
                'Slice 14 S14-C9 pilot cutover for spot_trades through canonical '
                'events into native and aligned serving projections'
            ),
            'proof_database': proof_database,
            'pilot_view': {
                'view_id': _PILOT_VIEW_ID,
                'view_version': _PILOT_VIEW_VERSION,
            },
            'legacy_seed_rows': len(sample_rows),
            'ingest_runs': [ingest_run_1, ingest_run_2],
            'native_projection_runs': {
                'run_1': {
                    'batches_processed': native_run_1.batches_processed,
                    'events_processed': native_run_1.events_processed,
                    'rows_written': native_run_1.rows_written,
                    'last_checkpoint_status': native_run_1.last_checkpoint_status,
                },
                'run_2_replay': {
                    'batches_processed': native_run_2.batches_processed,
                    'events_processed': native_run_2.events_processed,
                    'rows_written': native_run_2.rows_written,
                    'last_checkpoint_status': native_run_2.last_checkpoint_status,
                },
            },
            'aligned_projection_runs': {
                'run_1': aligned_run_1,
                'run_2_replay': aligned_run_2,
            },
            'parity': {
                'native': native_parity,
                'aligned_1s': aligned_parity,
            },
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_c9_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c9-pilot-cutover.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
