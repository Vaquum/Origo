from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.data._internal.generic_endpoints import query_aligned, query_native
from origo.fred.canonical_event_ingest import (
    build_fred_canonical_payload,
    write_fred_long_metrics_to_canonical,
)
from origo.fred.normalize import FREDLongMetricRow
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.fred_aligned_projector import (
    project_fred_series_metrics_aligned,
)
from origo_control_plane.utils.fred_native_projector import (
    project_fred_series_metrics_native,
)

_PROOF_DB_SUFFIX_RUN_1 = '_s17_p1_p3_proof_run_1'
_PROOF_DB_SUFFIX_RUN_2 = '_s17_p1_p3_proof_run_2'
_SLICE_DIR = Path('spec/slices/slice-17-fred-event-sourcing-port')
_WINDOW_START_ISO = '2026-03-08T00:00:00Z'
_WINDOW_END_ISO = '2026-03-11T00:00:00Z'
_INGESTED_AT_UTC = datetime(2026, 3, 10, 16, 0, 0, tzinfo=UTC)

_NATIVE_COLUMNS: tuple[str, ...] = (
    'metric_id',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'observed_at_utc',
    'dimensions_json',
    'provenance_json',
    'ingested_at_utc',
)

_ALIGNED_COLUMNS: tuple[str, ...] = (
    'aligned_at_utc',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'dimensions_json',
    'provenance_json',
    'latest_ingested_at_utc',
    'records_in_bucket',
    'valid_from_utc',
    'valid_to_utc_exclusive',
)


@dataclass(frozen=True)
class FREDFixtureRow:
    source_id: str
    metric_name: str
    metric_unit: str
    observed_day: str
    metric_value_string: str
    last_updated_utc: str
    series_id: str


def _parse_iso_utc(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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


def _fixtures() -> tuple[FREDFixtureRow, ...]:
    return (
        FREDFixtureRow(
            source_id='fred_fedfunds',
            metric_name='effective_federal_funds_rate',
            metric_unit='Percent',
            observed_day='2026-03-08',
            metric_value_string='4.33',
            last_updated_utc='2026-03-08T12:00:00Z',
            series_id='FEDFUNDS',
        ),
        FREDFixtureRow(
            source_id='fred_fedfunds',
            metric_name='effective_federal_funds_rate',
            metric_unit='Percent',
            observed_day='2026-03-09',
            metric_value_string='4.35',
            last_updated_utc='2026-03-09T12:00:00Z',
            series_id='FEDFUNDS',
        ),
        FREDFixtureRow(
            source_id='fred_dgs10',
            metric_name='treasury_yield_10y',
            metric_unit='Percent',
            observed_day='2026-03-08',
            metric_value_string='4.10',
            last_updated_utc='2026-03-08T20:00:00Z',
            series_id='DGS10',
        ),
        FREDFixtureRow(
            source_id='fred_dgs10',
            metric_name='treasury_yield_10y',
            metric_unit='Percent',
            observed_day='2026-03-10',
            metric_value_string='.',
            last_updated_utc='2026-03-10T20:00:00Z',
            series_id='DGS10',
        ),
    )


def _to_metric_value_float(value: str) -> float | None:
    if value == '.':
        return None
    return float(Decimal(value))


def _build_dimensions_json(fixture: FREDFixtureRow) -> str:
    return json.dumps(
        {
            'frequency': 'Daily',
            'observation_date': fixture.observed_day,
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': fixture.series_id,
        },
        sort_keys=True,
        separators=(',', ':'),
    )


def _build_provenance_json(fixture: FREDFixtureRow) -> str:
    return json.dumps(
        {
            'fetched_from': 'fred_api',
            'frequency': 'Daily',
            'last_updated_utc': fixture.last_updated_utc,
            'realtime_end': fixture.observed_day,
            'realtime_start': fixture.observed_day,
            'registry_version': '2026-03-10-s17-fixture',
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': fixture.series_id,
            'source_uri': f'fred://series/{fixture.series_id}',
            'units': fixture.metric_unit,
        },
        sort_keys=True,
        separators=(',', ':'),
    )


def _build_rows() -> list[FREDLongMetricRow]:
    rows: list[FREDLongMetricRow] = []
    for fixture in _fixtures():
        observed_at_utc = _parse_iso_utc(f'{fixture.observed_day}T00:00:00Z')
        metric_id = f'{fixture.source_id}:{fixture.observed_day}:{fixture.metric_name}'
        rows.append(
            FREDLongMetricRow(
                metric_id=metric_id,
                source_id=fixture.source_id,
                metric_name=fixture.metric_name,
                metric_unit=fixture.metric_unit,
                metric_value_string=fixture.metric_value_string,
                metric_value_int=None,
                metric_value_float=_to_metric_value_float(fixture.metric_value_string),
                metric_value_bool=None,
                observed_at_utc=observed_at_utc,
                dimensions_json=_build_dimensions_json(fixture),
                provenance_json=_build_provenance_json(fixture),
            )
        )
    rows.sort(
        key=lambda row: (row.source_id, row.metric_name, row.observed_at_utc, row.metric_id)
    )
    return rows


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
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


def _hash_rows_per_day(
    *,
    rows: list[dict[str, Any]],
    day_key_field: str,
) -> dict[str, str]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        day_value_raw = row.get(day_key_field)
        if not isinstance(day_value_raw, str):
            raise RuntimeError(f'Expected string day key field {day_key_field}')
        day = day_value_raw[:10]
        existing = grouped.get(day)
        if existing is None:
            grouped[day] = [row]
        else:
            existing.append(row)
    output: dict[str, str] = {}
    for day in sorted(grouped):
        output[day] = _rows_hash(grouped[day])
    return output


def _sum_metric_value_float(rows: list[dict[str, Any]]) -> str:
    running = Decimal('0')
    for row in rows:
        value = row.get('metric_value_float')
        if value is None:
            continue
        if isinstance(value, (float, int)):
            running += Decimal(str(value))
        elif isinstance(value, str):
            running += Decimal(value)
        else:
            raise RuntimeError(f'Unsupported metric_value_float value type: {type(value)}')
    return format(running, 'f')


def _native_fingerprint(rows: list[dict[str, Any]]) -> dict[str, Any]:
    offsets = sorted(cast(str, row['metric_id']) for row in rows)
    return {
        'row_count': len(rows),
        'first_offset': offsets[0] if offsets else None,
        'last_offset': offsets[-1] if offsets else None,
        'metric_value_float_sum': _sum_metric_value_float(rows),
        'hash_per_day': _hash_rows_per_day(rows=rows, day_key_field='observed_at_utc'),
        'rows_hash_sha256': _rows_hash(rows),
    }


def _aligned_fingerprint(rows: list[dict[str, Any]]) -> dict[str, Any]:
    offsets = sorted(
        (
            f"{cast(str, row['source_id'])}:"
            f"{cast(str, row['metric_name'])}:"
            f"{cast(str, row['aligned_at_utc'])}"
        )
        for row in rows
    )
    return {
        'row_count': len(rows),
        'first_offset': offsets[0] if offsets else None,
        'last_offset': offsets[-1] if offsets else None,
        'metric_value_float_sum': _sum_metric_value_float(rows),
        'hash_per_day': _hash_rows_per_day(rows=rows, day_key_field='aligned_at_utc'),
        'rows_hash_sha256': _rows_hash(rows),
    }


def _expected_native_rows(rows: list[FREDLongMetricRow]) -> list[dict[str, Any]]:
    expected_rows: list[dict[str, Any]] = []
    for row in rows:
        expected_rows.append(
            {
                'metric_id': row.metric_id,
                'source_id': row.source_id,
                'metric_name': row.metric_name,
                'metric_unit': row.metric_unit,
                'metric_value_string': row.metric_value_string,
                'metric_value_int': row.metric_value_int,
                'metric_value_float': row.metric_value_float,
                'metric_value_bool': row.metric_value_bool,
                'observed_at_utc': row.observed_at_utc,
                'dimensions_json': row.dimensions_json,
                'provenance_json': row.provenance_json,
                'ingested_at_utc': _INGESTED_AT_UTC,
            }
        )
    expected_rows.sort(
        key=lambda item: (
            cast(datetime, item['observed_at_utc']),
            cast(str, item['source_id']),
            cast(str, item['metric_name']),
            cast(str, item['metric_id']),
        )
    )
    return _normalize_rows(expected_rows)


def _expected_aligned_rows(rows: list[FREDLongMetricRow]) -> list[dict[str, Any]]:
    window_start = _parse_iso_utc(_WINDOW_START_ISO)
    window_end = _parse_iso_utc(_WINDOW_END_ISO)
    by_series: dict[tuple[str, str], list[FREDLongMetricRow]] = {}
    for row in rows:
        key = (row.source_id, row.metric_name)
        existing = by_series.get(key)
        if existing is None:
            by_series[key] = [row]
        else:
            existing.append(row)

    expected: list[dict[str, Any]] = []
    for (source_id, metric_name), series_rows in sorted(by_series.items()):
        sorted_rows = sorted(
            series_rows,
            key=lambda item: (item.observed_at_utc, item.metric_id),
        )
        for index, row in enumerate(sorted_rows):
            next_start = (
                sorted_rows[index + 1].observed_at_utc
                if index + 1 < len(sorted_rows)
                else window_end
            )
            valid_from = max(row.observed_at_utc, window_start)
            valid_to = min(next_start, window_end)
            if valid_to <= valid_from:
                continue
            expected.append(
                {
                    'aligned_at_utc': row.observed_at_utc,
                    'source_id': source_id,
                    'metric_name': metric_name,
                    'metric_unit': row.metric_unit,
                    'metric_value_string': row.metric_value_string,
                    'metric_value_int': row.metric_value_int,
                    'metric_value_float': row.metric_value_float,
                    'metric_value_bool': row.metric_value_bool,
                    'dimensions_json': row.dimensions_json,
                    'provenance_json': row.provenance_json,
                    'latest_ingested_at_utc': _INGESTED_AT_UTC,
                    'records_in_bucket': 1,
                    'valid_from_utc': valid_from,
                    'valid_to_utc_exclusive': valid_to,
                }
            )
    expected.sort(
        key=lambda item: (
            cast(str, item['source_id']),
            cast(str, item['metric_name']),
            cast(datetime, item['valid_from_utc']),
        )
    )
    return _normalize_rows(expected)


def _source_payload_checksums(rows: list[FREDLongMetricRow]) -> dict[str, dict[str, str]]:
    by_day: dict[str, list[bytes]] = {}
    for row in rows:
        day = row.observed_at_utc.date().isoformat()
        payload_raw = json.dumps(
            build_fred_canonical_payload(row=row),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        ).encode('utf-8')
        existing = by_day.get(day)
        if existing is None:
            by_day[day] = [payload_raw]
        else:
            existing.append(payload_raw)

    checksums: dict[str, dict[str, str]] = {}
    for day in sorted(by_day):
        payloads = sorted(by_day[day])
        digest = hashlib.sha256()
        for payload in payloads:
            digest.update(payload)
        day_hash = digest.hexdigest()
        checksums[day] = {
            'zip_sha256': day_hash,
            'csv_sha256': day_hash,
        }
    return checksums


def _run_once(*, settings: MigrationSettings, proof_database: str) -> dict[str, Any]:
    proof_settings = MigrationSettings(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=proof_database,
    )
    runner = MigrationRunner(settings=proof_settings)
    admin_client = _build_clickhouse_client(settings)
    rows = _build_rows()
    expected_native = _expected_native_rows(rows)
    expected_aligned = _expected_aligned_rows(rows)

    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        write_summary = write_fred_long_metrics_to_canonical(
            client=admin_client,
            database=proof_database,
            rows=rows,
            run_id='s17-p1-p3-proof',
            ingested_at_utc=_INGESTED_AT_UTC,
        )
        if write_summary.rows_inserted != len(rows) or write_summary.rows_duplicate != 0:
            raise RuntimeError(
                'S17-P1/P3 expected full insert on first pass, '
                f'observed={write_summary.to_dict()}'
            )

        partition_ids = {row.observed_at_utc.date().isoformat() for row in rows}
        native_projection_summary = project_fred_series_metrics_native(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s17-p1-p3-native-projector',
            projected_at_utc=datetime(2026, 3, 10, 16, 5, 0, tzinfo=UTC),
        )
        aligned_projection_summary = project_fred_series_metrics_aligned(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s17-p1-p3-aligned-projector',
            projected_at_utc=datetime(2026, 3, 10, 16, 6, 0, tzinfo=UTC),
        )

        with _override_clickhouse_database(proof_database):
            native_frame = query_native(
                dataset='fred_series_metrics',
                select_cols=list(_NATIVE_COLUMNS),
                time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
                include_datetime_col=True,
                datetime_iso_output=False,
                show_summary=False,
                auth_token=None,
            )
            aligned_frame = query_aligned(
                dataset='fred_series_metrics',
                select_cols=list(_ALIGNED_COLUMNS),
                time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
                datetime_iso_output=False,
                show_summary=False,
                auth_token=None,
            )

        observed_native = _normalize_rows(native_frame.to_dicts())
        observed_aligned = _normalize_rows(aligned_frame.to_dicts())
        if observed_native != expected_native:
            raise RuntimeError(
                'S17-P1 native parity mismatch between expected and observed rows'
            )
        if observed_aligned != expected_aligned:
            raise RuntimeError(
                'S17-P1 aligned parity mismatch between expected and observed rows'
            )

        return {
            'proof_database': proof_database,
            'write_summary': write_summary.to_dict(),
            'native_projection_summary': native_projection_summary.to_dict(),
            'aligned_projection_summary': aligned_projection_summary.to_dict(),
            'expected_native_rows': expected_native,
            'expected_aligned_rows': expected_aligned,
            'observed_native_rows': observed_native,
            'observed_aligned_rows': observed_aligned,
            'native_rows_hash_sha256': _rows_hash(observed_native),
            'aligned_rows_hash_sha256': _rows_hash(observed_aligned),
            'native_fingerprint': _native_fingerprint(observed_native),
            'aligned_fingerprint': _aligned_fingerprint(observed_aligned),
            'source_checksums': _source_payload_checksums(rows),
            'native_row_count': len(observed_native),
            'aligned_row_count': len(observed_aligned),
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def run_s17_p1_p3_proofs() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    run_1_db = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_1}'
    run_2_db = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_2}'
    run_1 = _run_once(settings=base_settings, proof_database=run_1_db)
    run_2 = _run_once(settings=base_settings, proof_database=run_2_db)

    deterministic_match = (
        run_1['native_rows_hash_sha256'] == run_2['native_rows_hash_sha256']
        and run_1['aligned_rows_hash_sha256'] == run_2['aligned_rows_hash_sha256']
    )
    if not deterministic_match:
        raise RuntimeError('S17-P3 determinism failed: run hashes do not match')

    p1_payload = {
        'proof_scope': (
            'Slice 17 S17-P1 fixed-window acceptance for FRED native and aligned_1s'
        ),
        'window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'native_row_count': run_1['native_row_count'],
        'aligned_row_count': run_1['aligned_row_count'],
        'acceptance_verified': True,
    }
    p2_payload = {
        'proof_scope': (
            'Slice 17 S17-P2 parity verification against FRED fixture baselines'
        ),
        'native_rows_hash_sha256': run_1['native_rows_hash_sha256'],
        'aligned_rows_hash_sha256': run_1['aligned_rows_hash_sha256'],
        'parity_verified': True,
    }
    p3_payload = {
        'proof_scope': (
            'Slice 17 S17-P3 replay determinism for FRED native and aligned_1s after cutover'
        ),
        'run_1': {
            'native_rows_hash_sha256': run_1['native_rows_hash_sha256'],
            'aligned_rows_hash_sha256': run_1['aligned_rows_hash_sha256'],
        },
        'run_2': {
            'native_rows_hash_sha256': run_2['native_rows_hash_sha256'],
            'aligned_rows_hash_sha256': run_2['aligned_rows_hash_sha256'],
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
            'fred_series_metrics': {
                'native': run_1['native_fingerprint'],
                'aligned_1s': run_1['aligned_fingerprint'],
            }
        },
        'run_2_fingerprints': {
            'fred_series_metrics': {
                'native': run_2['native_fingerprint'],
                'aligned_1s': run_2['aligned_fingerprint'],
            }
        },
        'deterministic_match': deterministic_match,
        'column_key': {
            'source_checksums': (
                'Daily canonical source payload checksums. zip_sha256/csv_sha256 fields are '
                'populated for uniform fixture schema even though FRED source format is JSON.'
            ),
            'row_count': 'Total rows returned in the fixture window for this run.',
            'first_offset': 'Lexicographically smallest deterministic identity in result rows.',
            'last_offset': 'Lexicographically largest deterministic identity in result rows.',
            'metric_value_float_sum': (
                'Deterministic sum of non-null metric_value_float values in the result window.'
            ),
            'hash_per_day': 'Per-day SHA256 of canonicalized rows in that day bucket.',
            'rows_hash_sha256': 'SHA256 of canonicalized full result rows for this run.',
        },
    }

    return {
        'p1': p1_payload,
        'p2': p2_payload,
        'p3': p3_payload,
        'baseline_fixture': baseline_fixture,
    }


def main() -> None:
    payload = run_s17_p1_p3_proofs()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)

    p1_path = _SLICE_DIR / 'proof-s17-p1-acceptance.json'
    p2_path = _SLICE_DIR / 'proof-s17-p2-parity.json'
    p3_path = _SLICE_DIR / 'proof-s17-p3-determinism.json'
    baseline_path = _SLICE_DIR / 'baseline-fixture-2026-03-08_2026-03-10.json'

    p1_path.write_text(
        json.dumps(payload['p1'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    p2_path.write_text(
        json.dumps(payload['p2'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    p3_path.write_text(
        json.dumps(payload['p3'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    baseline_path.write_text(
        json.dumps(payload['baseline_fixture'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload['p1'], indent=2, sort_keys=True))
    print(json.dumps(payload['p2'], indent=2, sort_keys=True))
    print(json.dumps(payload['p3'], indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
