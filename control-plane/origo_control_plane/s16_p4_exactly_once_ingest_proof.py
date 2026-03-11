from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.scraper.contracts import NormalizedMetricRecord, ProvenanceMetadata
from origo.scraper.etf_canonical_event_ingest import (
    write_etf_normalized_records_to_canonical,
)
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s16_p4_proof'
_SLICE_DIR = Path('spec/slices/slice-16-etf-event-sourcing-port')


@dataclass(frozen=True)
class _FixtureSpec:
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
    value = datetime.fromisoformat(f'{day}T00:00:00+00:00')
    return value.astimezone(UTC)


def _provenance(source_id: str, observed_day: str) -> ProvenanceMetadata:
    observed_at_utc = _parse_observed_utc(observed_day)
    return ProvenanceMetadata(
        source_id=source_id,
        source_uri=f'https://example.com/{source_id}/{observed_day}.json',
        artifact_id=f'{source_id}-{observed_day}',
        artifact_sha256=f'sha256-{source_id}-{observed_day}',
        fetch_method='http',
        parser_name='s16-p4-proof',
        parser_version='1.0.0',
        fetched_at_utc=observed_at_utc.replace(hour=4),
        parsed_at_utc=observed_at_utc.replace(hour=4, minute=5),
        normalized_at_utc=observed_at_utc.replace(hour=4, minute=6),
    )


def _fixture_specs() -> tuple[_FixtureSpec, ...]:
    return (
        _FixtureSpec(
            source_id='etf_ishares_ibit_daily',
            issuer='iShares',
            ticker='IBIT',
            observed_day='2026-03-08',
            metric_name='btc_units',
            metric_value=285123.125,
            metric_unit='BTC',
        ),
        _FixtureSpec(
            source_id='etf_ishares_ibit_daily',
            issuer='iShares',
            ticker='IBIT',
            observed_day='2026-03-08',
            metric_name='total_net_assets_usd',
            metric_value=12345678901.23,
            metric_unit='USD',
        ),
        _FixtureSpec(
            source_id='etf_fidelity_fbtc_daily',
            issuer='Fidelity',
            ticker='FBTC',
            observed_day='2026-03-08',
            metric_name='btc_units',
            metric_value=178123.75,
            metric_unit='BTC',
        ),
        _FixtureSpec(
            source_id='etf_fidelity_fbtc_daily',
            issuer='Fidelity',
            ticker='FBTC',
            observed_day='2026-03-08',
            metric_name='total_net_assets_usd',
            metric_value=7654321000.55,
            metric_unit='USD',
        ),
    )


def _records() -> list[NormalizedMetricRecord]:
    output: list[NormalizedMetricRecord] = []
    for spec in _fixture_specs():
        output.append(
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
    return output


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def run_s16_p4_proof() -> dict[str, Any]:
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
    phase_1_records = records[:2]
    phase_2_records = records

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        phase_1_summary = write_etf_normalized_records_to_canonical(
            client=admin_client,
            database=proof_database,
            records=phase_1_records,
            run_id='s16-p4-phase-1',
            ingested_at_utc=datetime(2026, 3, 10, 13, 0, 0, tzinfo=UTC),
        )
        if phase_1_summary.rows_inserted != len(phase_1_records):
            raise RuntimeError('S16-P4 phase-1 expected all rows inserted')
        if phase_1_summary.rows_duplicate != 0:
            raise RuntimeError('S16-P4 phase-1 expected zero duplicate rows')

        phase_2_summary = write_etf_normalized_records_to_canonical(
            client=admin_client,
            database=proof_database,
            records=phase_2_records,
            run_id='s16-p4-phase-2',
            ingested_at_utc=datetime(2026, 3, 10, 13, 1, 0, tzinfo=UTC),
        )
        if phase_2_summary.rows_inserted != 2 or phase_2_summary.rows_duplicate != 2:
            raise RuntimeError(
                'S16-P4 phase-2 expected inserted=2 duplicate=2, '
                f'observed={phase_2_summary.to_dict()}'
            )

        phase_3_summary = write_etf_normalized_records_to_canonical(
            client=admin_client,
            database=proof_database,
            records=phase_2_records,
            run_id='s16-p4-phase-3',
            ingested_at_utc=datetime(2026, 3, 10, 13, 2, 0, tzinfo=UTC),
        )
        if phase_3_summary.rows_inserted != 0 or phase_3_summary.rows_duplicate != 4:
            raise RuntimeError(
                'S16-P4 phase-3 expected inserted=0 duplicate=4, '
                f'observed={phase_3_summary.to_dict()}'
            )

        persisted_rows = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'etf'
                AND stream_id = 'etf_daily_metrics'
                AND partition_id = '2026-03-08'
            '''
        )
        persisted_count = _require_int(
            persisted_rows[0][0],
            label='persisted_count',
        )
        if persisted_count != len(records):
            raise RuntimeError(
                f'S16-P4 persisted count mismatch expected={len(records)} '
                f'observed={persisted_count}'
            )

        duplicate_identity_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent, count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'etf'
                AND stream_id = 'etf_daily_metrics'
                AND partition_id = '2026-03-08'
            GROUP BY source_offset_or_equivalent
            HAVING count() > 1
            '''
        )
        if duplicate_identity_rows != []:
            raise RuntimeError(
                'S16-P4 duplicate identity rows found: '
                f'{duplicate_identity_rows}'
            )

        return {
            'proof_scope': (
                'Slice 16 S16-P4 exactly-once ingest for ETF canonical events '
                'under duplicate replay and crash/restart simulation'
            ),
            'proof_database': proof_database,
            'fixture_event_count': len(records),
            'phase_1_pre_crash_partial_ingest': phase_1_summary.to_dict(),
            'phase_2_after_restart_full_replay': phase_2_summary.to_dict(),
            'phase_3_post_recovery_duplicate_replay': phase_3_summary.to_dict(),
            'persisted_row_count': persisted_count,
            'duplicate_identity_rows': duplicate_identity_rows,
            'exactly_once_verified': True,
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s16_p4_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s16-p4-exactly-once-ingest.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
