from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo.fred.canonical_event_ingest import (
    build_fred_canonical_payload,
    write_fred_long_metrics_to_canonical,
)
from origo.fred.normalize import FREDLongMetricRow
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s17_p6_proof'
_SLICE_DIR = Path('spec/slices/slice-17-fred-event-sourcing-port')


@dataclass(frozen=True)
class _FixtureRecordSpec:
    source_id: str
    metric_name: str
    metric_unit: str
    observed_day: str
    metric_value_string: str
    last_updated_utc: str
    series_id: str


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _parse_observed_utc(day: str) -> datetime:
    return datetime.fromisoformat(f'{day}T00:00:00+00:00').astimezone(UTC)


def _dimensions_json(series_id: str, observed_day: str) -> str:
    return json.dumps(
        {
            'frequency': 'Daily',
            'observation_date': observed_day,
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': series_id,
        },
        sort_keys=True,
        separators=(',', ':'),
    )


def _provenance_json(spec: _FixtureRecordSpec) -> str:
    return json.dumps(
        {
            'fetched_from': 'fred_api',
            'frequency': 'Daily',
            'last_updated_utc': spec.last_updated_utc,
            'realtime_end': spec.observed_day,
            'realtime_start': spec.observed_day,
            'registry_version': '2026-03-10-s17-fixture',
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': spec.series_id,
            'source_uri': f'fred://series/{spec.series_id}',
            'units': spec.metric_unit,
        },
        sort_keys=True,
        separators=(',', ':'),
    )


def _record_specs() -> tuple[_FixtureRecordSpec, ...]:
    return (
        _FixtureRecordSpec(
            source_id='fred_fedfunds',
            metric_name='effective_federal_funds_rate',
            metric_unit='Percent',
            observed_day='2026-03-09',
            metric_value_string='4.35',
            last_updated_utc='2026-03-09T12:00:00Z',
            series_id='FEDFUNDS',
        ),
        _FixtureRecordSpec(
            source_id='fred_dgs10',
            metric_name='treasury_yield_10y',
            metric_unit='Percent',
            observed_day='2026-03-09',
            metric_value_string='4.12',
            last_updated_utc='2026-03-09T20:00:00Z',
            series_id='DGS10',
        ),
        _FixtureRecordSpec(
            source_id='fred_dgs10',
            metric_name='treasury_yield_10y',
            metric_unit='Percent',
            observed_day='2026-03-10',
            metric_value_string='.',
            last_updated_utc='2026-03-10T20:00:00Z',
            series_id='DGS10',
        ),
    )


def _records() -> list[FREDLongMetricRow]:
    records: list[FREDLongMetricRow] = []
    for spec in _record_specs():
        records.append(
            FREDLongMetricRow(
                metric_id=f'{spec.source_id}:{spec.observed_day}:{spec.metric_name}',
                source_id=spec.source_id,
                metric_name=spec.metric_name,
                metric_unit=spec.metric_unit,
                metric_value_string=spec.metric_value_string,
                metric_value_int=None,
                metric_value_float=(
                    None
                    if spec.metric_value_string == '.'
                    else float(spec.metric_value_string)
                ),
                metric_value_bool=None,
                observed_at_utc=_parse_observed_utc(spec.observed_day),
                dimensions_json=_dimensions_json(spec.series_id, spec.observed_day),
                provenance_json=_provenance_json(spec),
            )
        )
    return records


def _payload_raw_for_record(record: FREDLongMetricRow) -> bytes:
    payload = build_fred_canonical_payload(row=record)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    ).encode('utf-8')


def _payload_raw_to_bytes(value: Any, *, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode('utf-8')
    raise RuntimeError(f'{label} must be bytes-compatible')


def run_s17_p6_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    records = _records()
    record_by_metric_id = {record.metric_id: record for record in records}

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        write_summary = write_fred_long_metrics_to_canonical(
            client=admin_client,
            database=proof_database,
            rows=records,
            run_id='s17-p6-proof',
            ingested_at_utc=datetime(2026, 3, 10, 19, 0, 0, tzinfo=UTC),
        )
        if write_summary.rows_inserted != len(records) or write_summary.rows_duplicate != 0:
            raise RuntimeError(
                'S17-P6 expected all rows inserted once, '
                f'observed={write_summary.to_dict()}'
            )

        stored_rows = admin_client.execute(
            f'''
            SELECT
                source_offset_or_equivalent,
                payload_raw,
                payload_sha256_raw,
                payload_json
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'fred'
                AND stream_id = 'fred_series_metrics'
            ORDER BY source_offset_or_equivalent ASC
            '''
        )

        fidelity_rows: list[dict[str, Any]] = []
        for row in stored_rows:
            metric_id = str(row[0])
            fixture_record = record_by_metric_id.get(metric_id)
            if fixture_record is None:
                raise RuntimeError(f'S17-P6 unknown metric_id in canonical log: {metric_id}')

            expected_payload_raw = _payload_raw_for_record(fixture_record)
            observed_payload_raw = _payload_raw_to_bytes(
                row[1],
                label='canonical_event_log.payload_raw',
            )
            observed_payload_sha256 = str(row[2])
            observed_payload_json = str(row[3])

            expected_payload_sha256 = hashlib.sha256(expected_payload_raw).hexdigest()
            if observed_payload_raw != expected_payload_raw:
                raise RuntimeError(f'S17-P6 payload_raw mismatch for metric_id={metric_id}')
            if observed_payload_sha256 != expected_payload_sha256:
                raise RuntimeError(
                    f'S17-P6 payload_sha256_raw mismatch for metric_id={metric_id}'
                )

            canonicalized_json = canonicalize_payload_json_with_precision(
                source_id='fred',
                stream_id='fred_series_metrics',
                payload_raw=expected_payload_raw,
                payload_encoding='utf-8',
            )
            if observed_payload_json != canonicalized_json:
                raise RuntimeError(
                    f'S17-P6 payload_json precision-canonical mismatch for metric_id={metric_id}'
                )

            fidelity_rows.append(
                {
                    'metric_id': metric_id,
                    'payload_sha256_raw': observed_payload_sha256,
                    'payload_json_sha256': hashlib.sha256(
                        observed_payload_json.encode('utf-8')
                    ).hexdigest(),
                    'raw_fidelity_match': True,
                    'precision_roundtrip_match': True,
                }
            )

        invalid_precision_error: str
        invalid_payload = (
            b'{"record_source_id":"fred_fedfunds",'
            b'"metric_id":"invalid-precision",'
            b'"metric_name":"effective_federal_funds_rate",'
            b'"metric_unit":"Percent",'
            b'"metric_value_kind":"float",'
            b'"metric_value_text":"4.37",'
            b'"metric_value_bool":null,'
            b'"observed_at_utc":"2026-03-11T00:00:00+00:00",'
            b'"dimensions":{"frequency":"Daily","observation_date":"2026-03-11","seasonal_adjustment":"Not Seasonally Adjusted","series_id":"FEDFUNDS"},'
            b'"provenance":null,'
            b'"unexpected_numeric":1}'
        )
        try:
            canonicalize_payload_json_with_precision(
                source_id='fred',
                stream_id='fred_series_metrics',
                payload_raw=invalid_payload,
                payload_encoding='utf-8',
            )
        except RuntimeError as exc:
            invalid_precision_error = str(exc)
        else:
            raise RuntimeError(
                'S17-P6 expected precision guardrail rejection for unexpected numeric field'
            )

        return {
            'proof_scope': (
                'Slice 17 S17-P6 raw fidelity and precision proof for FRED canonical ingest'
            ),
            'proof_database': proof_database,
            'write_summary': write_summary.to_dict(),
            'fidelity_rows': fidelity_rows,
            'invalid_precision_guardrail_error': invalid_precision_error,
            'raw_fidelity_and_precision_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s17_p6_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s17-p6-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
