from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.fred.canonical_event_ingest import write_fred_long_metrics_to_canonical
from origo.fred.normalize import FREDLongMetricRow
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s17_p4_proof'
_SLICE_DIR = Path('spec/slices/slice-17-fred-event-sourcing-port')


@dataclass(frozen=True)
class _FixtureSpec:
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
    value = datetime.fromisoformat(f'{day}T00:00:00+00:00')
    return value.astimezone(UTC)


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


def _provenance_json(
    *, source_id: str, series_id: str, observed_day: str, last_updated_utc: str, metric_unit: str
) -> str:
    return json.dumps(
        {
            'fetched_from': 'fred_api',
            'frequency': 'Daily',
            'last_updated_utc': last_updated_utc,
            'realtime_end': observed_day,
            'realtime_start': observed_day,
            'registry_version': '2026-03-10-s17-fixture',
            'seasonal_adjustment': 'Not Seasonally Adjusted',
            'series_id': series_id,
            'source_uri': f'fred://series/{series_id}',
            'units': metric_unit,
            'source_id': source_id,
        },
        sort_keys=True,
        separators=(',', ':'),
    )


def _fixture_specs() -> tuple[_FixtureSpec, ...]:
    return (
        _FixtureSpec(
            source_id='fred_fedfunds',
            metric_name='effective_federal_funds_rate',
            metric_unit='Percent',
            observed_day='2026-03-08',
            metric_value_string='4.33',
            last_updated_utc='2026-03-08T12:00:00Z',
            series_id='FEDFUNDS',
        ),
        _FixtureSpec(
            source_id='fred_fedfunds',
            metric_name='effective_federal_funds_rate',
            metric_unit='Percent',
            observed_day='2026-03-09',
            metric_value_string='4.35',
            last_updated_utc='2026-03-09T12:00:00Z',
            series_id='FEDFUNDS',
        ),
        _FixtureSpec(
            source_id='fred_dgs10',
            metric_name='treasury_yield_10y',
            metric_unit='Percent',
            observed_day='2026-03-08',
            metric_value_string='4.10',
            last_updated_utc='2026-03-08T20:00:00Z',
            series_id='DGS10',
        ),
        _FixtureSpec(
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
    output: list[FREDLongMetricRow] = []
    for spec in _fixture_specs():
        metric_id = f'{spec.source_id}:{spec.observed_day}:{spec.metric_name}'
        metric_value_float = None if spec.metric_value_string == '.' else float(spec.metric_value_string)
        output.append(
            FREDLongMetricRow(
                metric_id=metric_id,
                source_id=spec.source_id,
                metric_name=spec.metric_name,
                metric_unit=spec.metric_unit,
                metric_value_string=spec.metric_value_string,
                metric_value_int=None,
                metric_value_float=metric_value_float,
                metric_value_bool=None,
                observed_at_utc=_parse_observed_utc(spec.observed_day),
                dimensions_json=_dimensions_json(spec.series_id, spec.observed_day),
                provenance_json=_provenance_json(
                    source_id=spec.source_id,
                    series_id=spec.series_id,
                    observed_day=spec.observed_day,
                    last_updated_utc=spec.last_updated_utc,
                    metric_unit=spec.metric_unit,
                ),
            )
        )
    return output


def _require_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{label} must be int, got {value!r}')
    return value


def run_s17_p4_proof() -> dict[str, Any]:
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

        phase_1_summary = write_fred_long_metrics_to_canonical(
            client=admin_client,
            database=proof_database,
            rows=phase_1_records,
            run_id='s17-p4-phase-1',
            ingested_at_utc=datetime(2026, 3, 10, 17, 0, 0, tzinfo=UTC),
        )
        if phase_1_summary.rows_inserted != len(phase_1_records):
            raise RuntimeError('S17-P4 phase-1 expected all rows inserted')
        if phase_1_summary.rows_duplicate != 0:
            raise RuntimeError('S17-P4 phase-1 expected zero duplicate rows')

        phase_2_summary = write_fred_long_metrics_to_canonical(
            client=admin_client,
            database=proof_database,
            rows=phase_2_records,
            run_id='s17-p4-phase-2',
            ingested_at_utc=datetime(2026, 3, 10, 17, 1, 0, tzinfo=UTC),
        )
        if phase_2_summary.rows_inserted != 2 or phase_2_summary.rows_duplicate != 2:
            raise RuntimeError(
                'S17-P4 phase-2 expected inserted=2 duplicate=2, '
                f'observed={phase_2_summary.to_dict()}'
            )

        phase_3_summary = write_fred_long_metrics_to_canonical(
            client=admin_client,
            database=proof_database,
            rows=phase_2_records,
            run_id='s17-p4-phase-3',
            ingested_at_utc=datetime(2026, 3, 10, 17, 2, 0, tzinfo=UTC),
        )
        if phase_3_summary.rows_inserted != 0 or phase_3_summary.rows_duplicate != 4:
            raise RuntimeError(
                'S17-P4 phase-3 expected inserted=0 duplicate=4, '
                f'observed={phase_3_summary.to_dict()}'
            )

        persisted_rows = admin_client.execute(
            f'''
            SELECT count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'fred'
                AND stream_id = 'fred_series_metrics'
                AND partition_id IN ('2026-03-08', '2026-03-09', '2026-03-10')
            '''
        )
        persisted_count = _require_int(
            persisted_rows[0][0],
            label='persisted_count',
        )
        if persisted_count != len(records):
            raise RuntimeError(
                f'S17-P4 persisted count mismatch expected={len(records)} '
                f'observed={persisted_count}'
            )

        duplicate_identity_rows = admin_client.execute(
            f'''
            SELECT source_offset_or_equivalent, count()
            FROM {proof_database}.canonical_event_log
            WHERE source_id = 'fred'
                AND stream_id = 'fred_series_metrics'
                AND partition_id IN ('2026-03-08', '2026-03-09', '2026-03-10')
            GROUP BY source_offset_or_equivalent
            HAVING count() > 1
            '''
        )
        if duplicate_identity_rows != []:
            raise RuntimeError(
                'S17-P4 duplicate identity rows found: '
                f'{duplicate_identity_rows}'
            )

        return {
            'proof_scope': (
                'Slice 17 S17-P4 exactly-once ingest for FRED canonical events '
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
    payload = run_s17_p4_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)
    output_path = _SLICE_DIR / 'proof-s17-p4-exactly-once-ingest.json'
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
