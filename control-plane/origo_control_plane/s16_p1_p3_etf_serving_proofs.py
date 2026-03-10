from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.data._internal.generic_endpoints import query_aligned, query_native
from origo.scraper.contracts import (
    MetricValue,
    NormalizedMetricRecord,
    ProvenanceMetadata,
)
from origo.scraper.etf_canonical_event_ingest import (
    build_etf_canonical_payload,
    write_etf_normalized_records_to_canonical,
)
from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.utils.etf_aligned_projector import (
    project_etf_daily_metrics_aligned,
)
from origo_control_plane.utils.etf_native_projector import (
    project_etf_daily_metrics_native,
)

_PROOF_DB_SUFFIX_RUN_1 = '_s16_p1_p3_proof_run_1'
_PROOF_DB_SUFFIX_RUN_2 = '_s16_p1_p3_proof_run_2'
_SLICE_DIR = Path('spec/slices/slice-16-etf-event-sourcing-port')
_WINDOW_START_ISO = '2026-03-08T00:00:00Z'
_WINDOW_END_ISO = '2026-03-10T00:00:00Z'

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
class ETFDayFixture:
    source_id: str
    issuer: str
    ticker: str
    observed_day: str
    btc_units: float
    total_net_assets_usd: float
    holdings_row_count: int


def _parse_iso_utc(value: str) -> datetime:
    normalized = value.replace('Z', '+00:00')
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _require_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None:
        raise RuntimeError(f'{label} must be timezone-aware')
    return value.astimezone(UTC)


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


def _fixtures() -> tuple[ETFDayFixture, ...]:
    return (
        ETFDayFixture(
            source_id='etf_ishares_ibit_daily',
            issuer='iShares',
            ticker='IBIT',
            observed_day='2026-03-08',
            btc_units=285123.125,
            total_net_assets_usd=12345678901.23,
            holdings_row_count=1,
        ),
        ETFDayFixture(
            source_id='etf_ishares_ibit_daily',
            issuer='iShares',
            ticker='IBIT',
            observed_day='2026-03-09',
            btc_units=286000.5,
            total_net_assets_usd=12400000123.45,
            holdings_row_count=1,
        ),
        ETFDayFixture(
            source_id='etf_fidelity_fbtc_daily',
            issuer='Fidelity',
            ticker='FBTC',
            observed_day='2026-03-08',
            btc_units=178123.75,
            total_net_assets_usd=7654321000.55,
            holdings_row_count=1,
        ),
        ETFDayFixture(
            source_id='etf_fidelity_fbtc_daily',
            issuer='Fidelity',
            ticker='FBTC',
            observed_day='2026-03-09',
            btc_units=178500.875,
            total_net_assets_usd=7665000099.0,
            holdings_row_count=1,
        ),
    )


def _provenance_for_fixture(fixture: ETFDayFixture) -> ProvenanceMetadata:
    observed_at_utc = _parse_iso_utc(f'{fixture.observed_day}T00:00:00Z')
    fetched_at_utc = observed_at_utc.replace(hour=4)
    parsed_at_utc = observed_at_utc.replace(hour=4, minute=5)
    normalized_at_utc = observed_at_utc.replace(hour=4, minute=6)
    artifact_seed = f'{fixture.source_id}:{fixture.observed_day}'
    artifact_sha256 = hashlib.sha256(artifact_seed.encode('utf-8')).hexdigest()
    return ProvenanceMetadata(
        source_id=fixture.source_id,
        source_uri=f'https://example.com/{fixture.ticker.lower()}/{fixture.observed_day}.json',
        artifact_id=f'{fixture.ticker.lower()}-{fixture.observed_day}',
        artifact_sha256=artifact_sha256,
        fetch_method='http',
        parser_name='s16-proof-fixture',
        parser_version='1.0.0',
        fetched_at_utc=fetched_at_utc,
        parsed_at_utc=parsed_at_utc,
        normalized_at_utc=normalized_at_utc,
    )


def _metric_records_for_fixture(fixture: ETFDayFixture) -> list[NormalizedMetricRecord]:
    observed_at_utc = _parse_iso_utc(f'{fixture.observed_day}T00:00:00Z')
    dimensions = {
        'issuer': fixture.issuer,
        'ticker': fixture.ticker,
    }
    provenance = _provenance_for_fixture(fixture)

    metric_values: tuple[tuple[str, MetricValue, str | None], ...] = (
        ('issuer', fixture.issuer, None),
        ('ticker', fixture.ticker, None),
        ('as_of_date', fixture.observed_day, None),
        ('btc_units', fixture.btc_units, 'BTC'),
        ('total_net_assets_usd', fixture.total_net_assets_usd, 'USD'),
        ('holdings_row_count', fixture.holdings_row_count, 'COUNT'),
    )

    records: list[NormalizedMetricRecord] = []
    for metric_name, metric_value, metric_unit in metric_values:
        records.append(
            NormalizedMetricRecord(
                metric_id=f'{fixture.source_id}:{fixture.observed_day}:{metric_name}',
                source_id=fixture.source_id,
                metric_name=metric_name,
                metric_value=metric_value,
                metric_unit=metric_unit,
                observed_at_utc=observed_at_utc,
                dimensions=dimensions,
                provenance=provenance,
            )
        )
    return records


def _build_all_records() -> list[NormalizedMetricRecord]:
    records: list[NormalizedMetricRecord] = []
    for fixture in _fixtures():
        records.extend(_metric_records_for_fixture(fixture))
    return records


def _metric_value_channels(
    value: MetricValue,
) -> tuple[str | None, int | None, float | None, int | None]:
    if value is None:
        return (None, None, None, None)
    if isinstance(value, bool):
        return ('true' if value else 'false', None, None, 1 if value else 0)
    if isinstance(value, int):
        return (str(value), value, float(value), None)
    if isinstance(value, float):
        text_value = format(value, '.17g')
        try:
            float_value = float(Decimal(text_value))
        except InvalidOperation as exc:
            raise RuntimeError(f'Invalid float fixture value: {value}') from exc
        return (text_value, None, float_value, None)
    return (value, None, None, None)


def _canonical_json(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return _require_utc(value, label='datetime').isoformat()
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


def _expected_native_rows(
    *,
    records: list[NormalizedMetricRecord],
    ingested_at_utc: datetime,
) -> list[dict[str, Any]]:
    normalized_ingested_at_utc = _require_utc(ingested_at_utc, label='ingested_at_utc')
    rows: list[dict[str, Any]] = []
    for record in records:
        (
            metric_value_string,
            metric_value_int,
            metric_value_float,
            metric_value_bool,
        ) = _metric_value_channels(record.metric_value)
        rows.append(
            {
                'metric_id': record.metric_id,
                'source_id': record.source_id,
                'metric_name': record.metric_name,
                'metric_unit': record.metric_unit,
                'metric_value_string': metric_value_string,
                'metric_value_int': metric_value_int,
                'metric_value_float': metric_value_float,
                'metric_value_bool': metric_value_bool,
                'observed_at_utc': record.observed_at_utc,
                'dimensions_json': _canonical_json(record.dimensions),
                'provenance_json': (
                    _canonical_json(cast(dict[str, Any], build_etf_canonical_payload(record=record)['provenance']))
                    if build_etf_canonical_payload(record=record)['provenance'] is not None
                    else None
                ),
                'ingested_at_utc': normalized_ingested_at_utc,
            }
        )
    rows.sort(
        key=lambda row: (
            cast(datetime, row['observed_at_utc']),
            cast(str, row['metric_id']),
        )
    )
    return _normalize_rows(rows)


def _expected_aligned_rows(
    *,
    records: list[NormalizedMetricRecord],
    ingested_at_utc: datetime,
) -> list[dict[str, Any]]:
    normalized_ingested_at_utc = _require_utc(ingested_at_utc, label='ingested_at_utc')
    window_end = _parse_iso_utc(_WINDOW_END_ISO)

    by_series: dict[tuple[str, str], list[NormalizedMetricRecord]] = {}
    for record in records:
        key = (record.source_id, record.metric_name)
        existing = by_series.get(key)
        if existing is None:
            by_series[key] = [record]
        else:
            existing.append(record)

    output_rows: list[dict[str, Any]] = []
    for (source_id, metric_name), series_records in sorted(by_series.items()):
        ordered = sorted(series_records, key=lambda item: item.observed_at_utc)
        for index, record in enumerate(ordered):
            next_observed = (
                ordered[index + 1].observed_at_utc
                if index + 1 < len(ordered)
                else window_end
            )
            (
                metric_value_string,
                metric_value_int,
                metric_value_float,
                metric_value_bool,
            ) = _metric_value_channels(record.metric_value)
            output_rows.append(
                {
                    'aligned_at_utc': record.observed_at_utc,
                    'source_id': source_id,
                    'metric_name': metric_name,
                    'metric_unit': record.metric_unit,
                    'metric_value_string': metric_value_string,
                    'metric_value_int': metric_value_int,
                    'metric_value_float': metric_value_float,
                    'metric_value_bool': metric_value_bool,
                    'dimensions_json': _canonical_json(record.dimensions),
                    'provenance_json': (
                        _canonical_json(
                            cast(
                                dict[str, Any],
                                build_etf_canonical_payload(record=record)['provenance'],
                            )
                        )
                        if build_etf_canonical_payload(record=record)['provenance']
                        is not None
                        else None
                    ),
                    'latest_ingested_at_utc': normalized_ingested_at_utc,
                    'records_in_bucket': 1,
                    'valid_from_utc': record.observed_at_utc,
                    'valid_to_utc_exclusive': next_observed,
                }
            )

    output_rows.sort(
        key=lambda row: (
            cast(str, row['source_id']),
            cast(str, row['metric_name']),
            cast(datetime, row['valid_from_utc']),
        )
    )
    return _normalize_rows(output_rows)


def _source_checksums(records: list[NormalizedMetricRecord]) -> dict[str, dict[str, str]]:
    checksums: dict[str, dict[str, str]] = {}
    for record in records:
        payload = build_etf_canonical_payload(record=record)
        payload_raw = json.dumps(
            payload,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        ).encode('utf-8')
        checksums[record.metric_id] = {
            'source_payload_sha256': hashlib.sha256(payload_raw).hexdigest(),
        }
    return checksums


def _execute_run(*, proof_database: str) -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )
    records = _build_all_records()
    partition_ids = sorted(
        {record.observed_at_utc.astimezone(UTC).date().isoformat() for record in records}
    )
    ingested_at_utc = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        write_summary = write_etf_normalized_records_to_canonical(
            client=admin_client,
            database=proof_database,
            records=records,
            run_id='s16-p1-p3-proof',
            ingested_at_utc=ingested_at_utc,
        )
        if write_summary.rows_inserted != len(records):
            raise RuntimeError(
                'S16-P1 expected all canonical rows inserted, '
                f'expected={len(records)} observed={write_summary.rows_inserted}'
            )

        native_projection_summary = project_etf_daily_metrics_native(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s16-p1-p3-proof',
            projected_at_utc=datetime(2026, 3, 10, 12, 1, 0, tzinfo=UTC),
        )
        aligned_projection_summary = project_etf_daily_metrics_aligned(
            client=admin_client,
            database=proof_database,
            partition_ids=partition_ids,
            run_id='s16-p1-p3-proof',
            projected_at_utc=datetime(2026, 3, 10, 12, 2, 0, tzinfo=UTC),
        )

        with _override_clickhouse_database(proof_database):
            native_frame = query_native(
                dataset='etf_daily_metrics',
                select_cols=list(_NATIVE_COLUMNS),
                time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
                include_datetime_col=True,
                datetime_iso_output=False,
                show_summary=False,
            )
            aligned_frame = query_aligned(
                dataset='etf_daily_metrics',
                select_cols=list(_ALIGNED_COLUMNS),
                time_range=(_WINDOW_START_ISO, _WINDOW_END_ISO),
                datetime_iso_output=False,
                show_summary=False,
            )

        observed_native_rows = _normalize_rows(native_frame.to_dicts())
        observed_aligned_rows = _normalize_rows(aligned_frame.to_dicts())
        expected_native_rows = _expected_native_rows(
            records=records,
            ingested_at_utc=ingested_at_utc,
        )
        expected_aligned_rows = _expected_aligned_rows(
            records=records,
            ingested_at_utc=ingested_at_utc,
        )

        if observed_native_rows != expected_native_rows:
            raise RuntimeError('S16-P2 parity mismatch in native ETF rows')
        if observed_aligned_rows != expected_aligned_rows:
            raise RuntimeError('S16-P2 parity mismatch in aligned ETF rows')

        return {
            'proof_database': proof_database,
            'fixture_record_count': len(records),
            'partition_ids': partition_ids,
            'write_summary': write_summary.to_dict(),
            'native_projection_summary': native_projection_summary.to_dict(),
            'aligned_projection_summary': aligned_projection_summary.to_dict(),
            'native_row_count': len(observed_native_rows),
            'aligned_row_count': len(observed_aligned_rows),
            'native_rows_hash_sha256': _rows_hash(observed_native_rows),
            'aligned_rows_hash_sha256': _rows_hash(observed_aligned_rows),
            'source_checksums': _source_checksums(records),
            'native_rows': observed_native_rows,
            'aligned_rows': observed_aligned_rows,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def run_s16_p1_p3_proofs() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    run_1 = _execute_run(
        proof_database=f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_1}'
    )
    run_2 = _execute_run(
        proof_database=f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_2}'
    )

    acceptance = {
        'proof_scope': (
            'Slice 16 S16-P1 acceptance for ETF native and aligned_1s event-driven '
            'serving projections'
        ),
        'window_start_utc': _WINDOW_START_ISO,
        'window_end_utc': _WINDOW_END_ISO,
        'fixture_record_count': run_1['fixture_record_count'],
        'native_row_count': run_1['native_row_count'],
        'aligned_row_count': run_1['aligned_row_count'],
        'write_summary': run_1['write_summary'],
        'native_projection_summary': run_1['native_projection_summary'],
        'aligned_projection_summary': run_1['aligned_projection_summary'],
        'acceptance_passed': True,
    }

    parity = {
        'proof_scope': (
            'Slice 16 S16-P2 parity for ETF native and aligned_1s outputs '
            'against deterministic fixture expectations'
        ),
        'window_start_utc': _WINDOW_START_ISO,
        'window_end_utc': _WINDOW_END_ISO,
        'native_rows_hash_sha256': run_1['native_rows_hash_sha256'],
        'aligned_rows_hash_sha256': run_1['aligned_rows_hash_sha256'],
        'native_parity_match': True,
        'aligned_parity_match': True,
    }

    deterministic_match = (
        run_1['native_rows_hash_sha256'] == run_2['native_rows_hash_sha256']
        and run_1['aligned_rows_hash_sha256'] == run_2['aligned_rows_hash_sha256']
    )
    if not deterministic_match:
        raise RuntimeError('S16-P3 determinism mismatch between run_1 and run_2')

    determinism = {
        'proof_scope': (
            'Slice 16 S16-P3 replay determinism for ETF native and aligned_1s '
            'event-driven serving projections'
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
        'column_key': {
            'rows_hash_sha256': 'SHA256 of canonicalized row payload for exact window.',
            'source_payload_sha256': 'SHA256 of canonical ETF payload bytes by metric_id.',
        },
        'deterministic_match': deterministic_match,
        'fixture_window': {
            'start_utc': _WINDOW_START_ISO,
            'end_utc': _WINDOW_END_ISO,
        },
        'source_checksums': run_1['source_checksums'],
        'run_1_fingerprints': {
            'etf_daily_metrics': {
                'native': {
                    'rows_hash_sha256': run_1['native_rows_hash_sha256'],
                },
                'aligned_1s': {
                    'rows_hash_sha256': run_1['aligned_rows_hash_sha256'],
                },
            }
        },
        'run_2_fingerprints': {
            'etf_daily_metrics': {
                'native': {
                    'rows_hash_sha256': run_2['native_rows_hash_sha256'],
                },
                'aligned_1s': {
                    'rows_hash_sha256': run_2['aligned_rows_hash_sha256'],
                },
            }
        },
    }

    return {
        'acceptance': acceptance,
        'parity': parity,
        'determinism': determinism,
        'baseline_fixture': baseline_fixture,
    }


def main() -> None:
    payload = run_s16_p1_p3_proofs()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)

    acceptance_path = _SLICE_DIR / 'proof-s16-p1-acceptance.json'
    parity_path = _SLICE_DIR / 'proof-s16-p2-parity.json'
    determinism_path = _SLICE_DIR / 'proof-s16-p3-determinism.json'
    baseline_path = _SLICE_DIR / 'baseline-fixture-2026-03-08_2026-03-09.json'

    acceptance_path.write_text(
        json.dumps(payload['acceptance'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    parity_path.write_text(
        json.dumps(payload['parity'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    determinism_path.write_text(
        json.dumps(payload['determinism'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    baseline_path.write_text(
        json.dumps(payload['baseline_fixture'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
