from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.data._internal.generic_endpoints import query_aligned, query_native
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.bybit_aligned_projector import (
    project_bybit_spot_trades_aligned,
)
from origo_control_plane.utils.bybit_canonical_event_ingest import (
    BybitSpotTradeEvent,
    parse_bybit_spot_trade_csv,
    write_bybit_spot_trades_to_canonical,
)
from origo_control_plane.utils.bybit_native_projector import (
    project_bybit_spot_trades_native,
)

_PROOF_DB_SUFFIX_RUN_1 = '_s19_p1_p3_proof_run_1'
_PROOF_DB_SUFFIX_RUN_2 = '_s19_p1_p3_proof_run_2'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-19-bybit-event-sourcing-port'
)
_WINDOW_START_ISO = '2024-01-04T00:00:00Z'
_WINDOW_END_ISO = '2024-01-04T00:00:03Z'
_INGESTED_AT_UTC = datetime(2026, 3, 10, 22, 0, 0, tzinfo=UTC)
_FIXTURE_DATE = '2024-01-04'

_NATIVE_COLUMNS: tuple[str, ...] = (
    'symbol',
    'trade_id',
    'trd_match_id',
    'side',
    'price',
    'size',
    'quote_quantity',
    'timestamp',
    'datetime',
    'tick_direction',
    'gross_value',
    'home_notional',
    'foreign_notional',
)
_ALIGNED_COLUMNS: tuple[str, ...] = (
    'aligned_at_utc',
    'open_price',
    'high_price',
    'low_price',
    'close_price',
    'quantity_sum',
    'quote_volume_sum',
    'trade_count',
)


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
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


def _bybit_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day
    end_utc_exclusive = parsed_day + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _build_fixture_csv_bytes() -> bytes:
    return (
        b'timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n'
        b'1704326400.1000,BTCUSDT,Buy,0.010,42500.10,PlusTick,m-3001,4.25001e+11,0.010,425.001\n'
        b'1704326400.7000,BTCUSDT,Sell,0.020,42500.20,MinusTick,m-3002,8.50004e+11,0.020,850.004\n'
        b'1704326401.1000,BTCUSDT,Buy,0.015,42499.90,ZeroPlusTick,m-3003,6.374985e+11,0.015,637.4985\n'
        b'1704326401.9000,BTCUSDT,Sell,0.030,42500.40,MinusTick,m-3004,1.275012e+12,0.030,1275.012\n'
        b'1704326402.1000,BTCUSDT,Buy,0.025,42500.30,PlusTick,m-3005,1.0625075e+12,0.025,1062.5075\n'
    )


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat()
    if isinstance(value, float):
        return float(Decimal(str(value)).quantize(Decimal('0.000000000001')))
    if isinstance(value, dict):
        value_map = cast(dict[str, Any], value)
        normalized_map: dict[str, Any] = {}
        for key in sorted(value_map):
            normalized_map[key] = _normalize_scalar(value_map[key])
        return normalized_map
    if isinstance(value, list):
        items = cast(list[Any], value)
        return [_normalize_scalar(item) for item in items]
    return value


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized: dict[str, Any] = {}
        for key in sorted(row):
            normalized[key] = _normalize_scalar(row[key])
        normalized_rows.append(normalized)
    return normalized_rows


def _rows_hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _native_fingerprint(rows: list[dict[str, Any]]) -> dict[str, Any]:
    trade_ids = sorted(cast(int, row['trade_id']) for row in rows)
    size_sum = sum(Decimal(str(cast(float, row['size']))) for row in rows)
    quote_quantity_sum = sum(
        Decimal(str(cast(float, row['quote_quantity'])))
        for row in rows
    )
    return {
        'row_count': len(rows),
        'first_trade_id': trade_ids[0] if trade_ids else None,
        'last_trade_id': trade_ids[-1] if trade_ids else None,
        'size_sum': format(size_sum, 'f'),
        'quote_quantity_sum': format(quote_quantity_sum, 'f'),
        'rows_hash_sha256': _rows_hash(rows),
    }


def _aligned_fingerprint(rows: list[dict[str, Any]]) -> dict[str, Any]:
    trade_count_total = sum(cast(int, row['trade_count']) for row in rows)
    quantity_sum_total = sum(
        Decimal(str(cast(float, row['quantity_sum'])))
        for row in rows
    )
    quote_volume_sum_total = sum(
        Decimal(str(cast(float, row['quote_volume_sum'])))
        for row in rows
    )
    return {
        'row_count': len(rows),
        'trade_count_total': trade_count_total,
        'quantity_sum_total': format(quantity_sum_total, 'f'),
        'quote_volume_sum_total': format(quote_volume_sum_total, 'f'),
        'rows_hash_sha256': _rows_hash(rows),
    }


def _to_legacy_row(event: BybitSpotTradeEvent) -> tuple[object, ...]:
    return (
        event.symbol,
        event.trade_id,
        event.trd_match_id,
        event.side,
        float(Decimal(event.price_text)),
        float(Decimal(event.size_text)),
        float(Decimal(event.quote_quantity_text)),
        event.timestamp,
        event.event_time_utc,
        event.tick_direction,
        float(Decimal(event.gross_value_text)),
        float(Decimal(event.home_notional_text)),
        float(Decimal(event.foreign_notional_text)),
    )


def _seed_legacy_bybit_table(
    *,
    client: ClickHouseClient,
    database: str,
    events: list[BybitSpotTradeEvent],
) -> None:
    rows = [_to_legacy_row(event) for event in events]
    client.execute(
        f'''
        INSERT INTO {database}.bybit_spot_trades
        (
            symbol,
            trade_id,
            trd_match_id,
            side,
            price,
            size,
            quote_quantity,
            timestamp,
            datetime,
            tick_direction,
            gross_value,
            home_notional,
            foreign_notional
        )
        VALUES
        ''',
        rows,
    )


def _to_clickhouse_datetime64_literal(value: str) -> str:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _query_legacy_native_rows(*, client: ClickHouseClient, database: str) -> list[dict[str, Any]]:
    start_ch = _to_clickhouse_datetime64_literal(_WINDOW_START_ISO)
    end_ch = _to_clickhouse_datetime64_literal(_WINDOW_END_ISO)
    rows = client.execute(
        f'''
        SELECT
            symbol,
            trade_id,
            trd_match_id,
            side,
            price,
            size,
            quote_quantity,
            timestamp,
            datetime,
            tick_direction,
            gross_value,
            home_notional,
            foreign_notional
        FROM {database}.bybit_spot_trades
        WHERE datetime >= toDateTime64(%(start)s, 3, 'UTC')
            AND datetime < toDateTime64(%(end)s, 3, 'UTC')
        ORDER BY datetime ASC, trade_id ASC
        ''',
        {
            'start': start_ch,
            'end': end_ch,
        },
    )
    return _normalize_rows(
        [
            {
                'symbol': row[0],
                'trade_id': row[1],
                'trd_match_id': row[2],
                'side': row[3],
                'price': row[4],
                'size': row[5],
                'quote_quantity': row[6],
                'timestamp': row[7],
                'datetime': row[8],
                'tick_direction': row[9],
                'gross_value': row[10],
                'home_notional': row[11],
                'foreign_notional': row[12],
            }
            for row in rows
        ]
    )


def _query_legacy_aligned_rows(*, client: ClickHouseClient, database: str) -> list[dict[str, Any]]:
    start_ch = _to_clickhouse_datetime64_literal(_WINDOW_START_ISO)
    end_ch = _to_clickhouse_datetime64_literal(_WINDOW_END_ISO)
    rows = client.execute(
        f'''
        SELECT
            toDateTime64(toStartOfSecond(datetime), 3, 'UTC') AS aligned_at_utc,
            argMin(price, trade_id) AS open_price,
            max(price) AS high_price,
            min(price) AS low_price,
            argMax(price, trade_id) AS close_price,
            sum(size) AS quantity_sum,
            sum(price * size) AS quote_volume_sum,
            count() AS trade_count
        FROM {database}.bybit_spot_trades
        WHERE datetime >= toDateTime64(%(start)s, 3, 'UTC')
            AND datetime < toDateTime64(%(end)s, 3, 'UTC')
        GROUP BY aligned_at_utc
        ORDER BY aligned_at_utc ASC
        ''',
        {
            'start': start_ch,
            'end': end_ch,
        },
    )
    return _normalize_rows(
        [
            {
                'aligned_at_utc': row[0],
                'open_price': row[1],
                'high_price': row[2],
                'low_price': row[3],
                'close_price': row[4],
                'quantity_sum': row[5],
                'quote_volume_sum': row[6],
                'trade_count': row[7],
            }
            for row in rows
        ]
    )


def _run_single_database(*, settings: MigrationSettings, proof_database: str) -> dict[str, Any]:
    proof_settings = MigrationSettings(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=proof_database,
    )
    runner = MigrationRunner(settings=proof_settings)
    admin_client = _build_clickhouse_client(settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        fixture_csv = _build_fixture_csv_bytes()
        fixture_csv_sha256 = hashlib.sha256(fixture_csv).hexdigest()
        fixture_zip_sha256 = fixture_csv_sha256
        day_start_ts_utc_ms, day_end_ts_utc_ms = _bybit_day_window_utc_ms(_FIXTURE_DATE)
        events = parse_bybit_spot_trade_csv(
            csv_content=fixture_csv,
            date_str=_FIXTURE_DATE,
            day_start_ts_utc_ms=day_start_ts_utc_ms,
            day_end_ts_utc_ms=day_end_ts_utc_ms,
        )

        _seed_legacy_bybit_table(client=admin_client, database=proof_database, events=events)

        write_summary = write_bybit_spot_trades_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s19-p1-p3-proof-write',
            ingested_at_utc=_INGESTED_AT_UTC,
        )
        if write_summary['rows_processed'] != len(events):
            raise RuntimeError('S19-P1 expected all fixture events to be processed by writer')

        partition_ids = {event.partition_id for event in events}
        native_projection_summary = project_bybit_spot_trades_native(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s19-p1-p3-proof-native-projector',
            projected_at_utc=datetime(2026, 3, 10, 22, 0, 5, tzinfo=UTC),
        )
        aligned_projection_summary = project_bybit_spot_trades_aligned(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s19-p1-p3-proof-aligned-projector',
            projected_at_utc=datetime(2026, 3, 10, 22, 0, 6, tzinfo=UTC),
        )
        aligned_projection_rows_raw = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_aligned_1s_aggregates
            WHERE source_id = 'bybit'
                AND stream_id = 'bybit_spot_trades'
                AND view_id = 'aligned_1s_raw'
                AND view_version = 1
            '''
        )
        aligned_projection_rows = int(aligned_projection_rows_raw[0][0])

        with _override_clickhouse_database(proof_database):
            native_rows = _normalize_rows(
                query_native(
                    dataset='bybit_spot_trades',
                    select_cols=list(_NATIVE_COLUMNS),
                    time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
                    datetime_iso_output=False,
                ).to_dicts()
            )
            aligned_rows = _normalize_rows(
                query_aligned(
                    dataset='bybit_spot_trades',
                    select_cols=list(_ALIGNED_COLUMNS),
                    time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
                    datetime_iso_output=False,
                ).to_dicts()
            )

        legacy_native_rows = _query_legacy_native_rows(
            client=admin_client,
            database=proof_database,
        )
        legacy_aligned_rows = _query_legacy_aligned_rows(
            client=admin_client,
            database=proof_database,
        )

        native_parity = native_rows == legacy_native_rows
        aligned_parity = aligned_rows == legacy_aligned_rows
        if not native_parity or not aligned_parity:
            native_hash = _rows_hash(native_rows)
            legacy_native_hash = _rows_hash(legacy_native_rows)
            aligned_hash = _rows_hash(aligned_rows)
            legacy_aligned_hash = _rows_hash(legacy_aligned_rows)
            raise RuntimeError(
                'S19-P2 parity mismatch against legacy Bybit baselines: '
                f'native_parity={native_parity} aligned_parity={aligned_parity} '
                f'native_hash={native_hash} legacy_native_hash={legacy_native_hash} '
                f'aligned_hash={aligned_hash} legacy_aligned_hash={legacy_aligned_hash} '
                f'aligned_projection_summary={aligned_projection_summary.to_dict()} '
                f'aligned_projection_rows={aligned_projection_rows}'
            )

        return {
            'write_summary': write_summary,
            'native_projection_summary': native_projection_summary.to_dict(),
            'aligned_projection_summary': aligned_projection_summary.to_dict(),
            'aligned_projection_rows': aligned_projection_rows,
            'native_rows': native_rows,
            'aligned_rows': aligned_rows,
            'native_fingerprint': _native_fingerprint(native_rows),
            'aligned_fingerprint': _aligned_fingerprint(aligned_rows),
            'source_checksums': {
                _FIXTURE_DATE: {
                    'zip_sha256': fixture_zip_sha256,
                    'csv_sha256': fixture_csv_sha256,
                }
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


def run_s19_p1_p3_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    run_1_database = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_1}'
    run_2_database = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_2}'

    run_1 = _run_single_database(settings=base_settings, proof_database=run_1_database)
    run_2 = _run_single_database(settings=base_settings, proof_database=run_2_database)

    deterministic_match = (
        run_1['native_fingerprint'] == run_2['native_fingerprint']
        and run_1['aligned_fingerprint'] == run_2['aligned_fingerprint']
    )
    if not deterministic_match:
        raise RuntimeError('S19-P3 determinism failure: run fingerprints differ')

    acceptance_payload = {
        'proof_scope': (
            'Slice 19 S19-P1 fixed-window acceptance for Bybit native and aligned_1s '
            'from canonical projection serving paths'
        ),
        'window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'row_counts': {
            'native': run_1['native_fingerprint']['row_count'],
            'aligned_1s': run_1['aligned_fingerprint']['row_count'],
        },
        'native_fingerprint': run_1['native_fingerprint'],
        'aligned_fingerprint': run_1['aligned_fingerprint'],
        'source_checksums': run_1['source_checksums'],
        'acceptance_verified': True,
    }

    parity_payload = {
        'proof_scope': (
            'Slice 19 S19-P2 parity checks versus legacy Bybit fixture baselines '
            '(legacy table SQL projections)'
        ),
        'window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'native_parity_match': run_1['parity']['native'],
        'aligned_parity_match': run_1['parity']['aligned_1s'],
        'parity_verified': True,
    }

    determinism_payload = {
        'proof_scope': (
            'Slice 19 S19-P3 replay determinism for Bybit native and aligned_1s '
            'after event-sourcing cutover'
        ),
        'run_1_fingerprints': {
            'native': run_1['native_fingerprint'],
            'aligned_1s': run_1['aligned_fingerprint'],
        },
        'run_2_fingerprints': {
            'native': run_2['native_fingerprint'],
            'aligned_1s': run_2['aligned_fingerprint'],
        },
        'deterministic_match': deterministic_match,
    }

    baseline_fixture = {
        'fixture_window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'source_checksums': run_1['source_checksums'],
        'run_1_fingerprints': {
            'bybit_spot_trades': {
                'native': run_1['native_fingerprint'],
                'aligned_1s': run_1['aligned_fingerprint'],
            }
        },
        'run_2_fingerprints': {
            'bybit_spot_trades': {
                'native': run_2['native_fingerprint'],
                'aligned_1s': run_2['aligned_fingerprint'],
            }
        },
        'deterministic_match': deterministic_match,
            'column_key': {
            'row_count': 'Total rows returned in the fixture window for this run.',
            'first_trade_id': 'Minimum trade_id in native result rows.',
            'last_trade_id': 'Maximum trade_id in native result rows.',
            'size_sum': 'Deterministic sum of native size values.',
            'quote_quantity_sum': 'Deterministic sum of native quote_quantity values.',
            'trade_count_total': 'Deterministic sum of aligned trade_count across buckets.',
            'quantity_sum_total': 'Deterministic sum of aligned quantity_sum values.',
            'quote_volume_sum_total': 'Deterministic sum of aligned quote_volume_sum values.',
            'rows_hash_sha256': 'SHA256 of canonicalized result rows for this run.',
                'source_checksums': 'Fixture source checksums (zip and csv sha256).',
        },
    }

    return {
        'proof_s19_p1': acceptance_payload,
        'proof_s19_p2': parity_payload,
        'proof_s19_p3': determinism_payload,
        'baseline_fixture': baseline_fixture,
    }


def main() -> None:
    payload = run_s19_p1_p3_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)

    p1_path = _SLICE_DIR / 'proof-s19-p1-acceptance.json'
    p2_path = _SLICE_DIR / 'proof-s19-p2-parity.json'
    p3_path = _SLICE_DIR / 'proof-s19-p3-determinism.json'
    baseline_path = _SLICE_DIR / 'baseline-fixture-2024-01-04_2024-01-04.json'

    p1_path.write_text(
        json.dumps(payload['proof_s19_p1'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    p2_path.write_text(
        json.dumps(payload['proof_s19_p2'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    p3_path.write_text(
        json.dumps(payload['proof_s19_p3'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    baseline_path.write_text(
        json.dumps(payload['baseline_fixture'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
