from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.precision import canonicalize_payload_json_with_precision
from origo.scraper.contracts import NormalizedMetricRecord, ProvenanceMetadata
from origo.scraper.etf_canonical_event_ingest import (
    build_etf_canonical_payload,
    write_etf_normalized_records_to_canonical,
)
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s16_p6_proof'
_SLICE_DIR = Path('spec/slices/slice-16-etf-event-sourcing-port')


@dataclass(frozen=True)
class _FixtureRecordSpec:
    source_id: str
    issuer: str
    ticker: str
    observed_day: str
    metric_name: str
    metric_value: str | int | float | bool | None
    metric_unit: str | None


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _parse_observed_utc(day: str) -> datetime:
    return datetime.fromisoformat(f'{day}T00:00:00+00:00').astimezone(UTC)


def _provenance(source_id: str, observed_day: str) -> ProvenanceMetadata:
    observed = _parse_observed_utc(observed_day)
    return ProvenanceMetadata(
        source_id=source_id,
        source_uri=f'https://example.com/{source_id}/{observed_day}.json',
        artifact_id=f'{source_id}-{observed_day}',
        artifact_sha256=f'fixture-{source_id}-{observed_day}',
        fetch_method='http',
        parser_name='s16-p6-proof',
        parser_version='1.0.0',
        fetched_at_utc=observed.replace(hour=4),
        parsed_at_utc=observed.replace(hour=4, minute=5),
        normalized_at_utc=observed.replace(hour=4, minute=6),
    )


def _record_specs() -> tuple[_FixtureRecordSpec, ...]:
    return (
        _FixtureRecordSpec(
            source_id='etf_ishares_ibit_daily',
            issuer='iShares',
            ticker='IBIT',
            observed_day='2026-03-09',
            metric_name='btc_units',
            metric_value=286000.5,
            metric_unit='BTC',
        ),
        _FixtureRecordSpec(
            source_id='etf_ishares_ibit_daily',
            issuer='iShares',
            ticker='IBIT',
            observed_day='2026-03-09',
            metric_name='as_of_date',
            metric_value='2026-03-09',
            metric_unit=None,
        ),
        _FixtureRecordSpec(
            source_id='etf_fidelity_fbtc_daily',
            issuer='Fidelity',
            ticker='FBTC',
            observed_day='2026-03-09',
            metric_name='holdings_row_count',
            metric_value=1,
            metric_unit='COUNT',
        ),
    )


def _records() -> list[NormalizedMetricRecord]:
    records: list[NormalizedMetricRecord] = []
    for spec in _record_specs():
        records.append(
            NormalizedMetricRecord(
                metric_id=(
                    f'{spec.source_id}:{spec.observed_day}:{spec.metric_name}'
                ),
                source_id=spec.source_id,
                metric_name=spec.metric_name,
                metric_value=spec.metric_value,
                metric_unit=spec.metric_unit,
                observed_at_utc=_parse_observed_utc(spec.observed_day),
                dimensions={'issuer': spec.issuer, 'ticker': spec.ticker},
                provenance=_provenance(spec.source_id, spec.observed_day),
            )
        )
    return records


def _payload_raw_for_record(record: NormalizedMetricRecord) -> bytes:
    payload = build_etf_canonical_payload(record=record)
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


def run_s16_p6_proof() -> dict[str, Any]:
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

        write_summary = write_etf_normalized_records_to_canonical(
            client=admin_client,
            database=proof_database,
            records=records,
            run_id='s16-p6-proof',
            ingested_at_utc=datetime(2026, 3, 10, 15, 0, 0, tzinfo=UTC),
        )
        if write_summary.rows_inserted != len(records) or write_summary.rows_duplicate != 0:
            raise RuntimeError(
                'S16-P6 expected all rows inserted once, '
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
            WHERE source_id = 'etf'
                AND stream_id = 'etf_daily_metrics'
            ORDER BY source_offset_or_equivalent ASC
            '''
        )

        fidelity_rows: list[dict[str, Any]] = []
        for row in stored_rows:
            metric_id = str(row[0])
            fixture_record = record_by_metric_id.get(metric_id)
            if fixture_record is None:
                raise RuntimeError(f'S16-P6 unknown metric_id in canonical log: {metric_id}')

            expected_payload_raw = _payload_raw_for_record(fixture_record)
            observed_payload_raw = _payload_raw_to_bytes(
                row[1],
                label='canonical_event_log.payload_raw',
            )
            observed_payload_sha256 = str(row[2])
            observed_payload_json = str(row[3])

            expected_payload_sha256 = hashlib.sha256(expected_payload_raw).hexdigest()
            if observed_payload_raw != expected_payload_raw:
                raise RuntimeError(f'S16-P6 payload_raw mismatch for metric_id={metric_id}')
            if observed_payload_sha256 != expected_payload_sha256:
                raise RuntimeError(
                    f'S16-P6 payload_sha256_raw mismatch for metric_id={metric_id}'
                )

            canonicalized_json = canonicalize_payload_json_with_precision(
                source_id='etf',
                stream_id='etf_daily_metrics',
                payload_raw=expected_payload_raw,
                payload_encoding='utf-8',
            )
            if observed_payload_json != canonicalized_json:
                raise RuntimeError(
                    f'S16-P6 payload_json precision-canonical mismatch for metric_id={metric_id}'
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
            b'{"record_source_id":"etf_ishares_ibit_daily",'
            b'"metric_id":"invalid-precision",'
            b'"metric_name":"btc_units",'
            b'"metric_unit":"BTC",'
            b'"metric_value_kind":"float",'
            b'"metric_value_text":"1.0",'
            b'"metric_value_bool":null,'
            b'"observed_at_utc":"2026-03-09T00:00:00+00:00",'
            b'"dimensions":{"issuer":"iShares","ticker":"IBIT"},'
            b'"provenance":null,'
            b'"unexpected_numeric":1}'
        )
        try:
            canonicalize_payload_json_with_precision(
                source_id='etf',
                stream_id='etf_daily_metrics',
                payload_raw=invalid_payload,
                payload_encoding='utf-8',
            )
        except RuntimeError as exc:
            invalid_precision_error = str(exc)
        else:
            raise RuntimeError(
                'S16-P6 expected precision guardrail rejection for unexpected numeric field'
            )

        return {
            'proof_scope': (
                'Slice 16 S16-P6 raw fidelity and precision proof for ETF canonical ingest'
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
    payload = run_s16_p6_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s16-p6-raw-fidelity-precision.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
