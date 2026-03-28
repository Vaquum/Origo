from __future__ import annotations

from datetime import UTC, datetime

from origo.fred import (
    FREDRawSeriesBundle,
    normalize_fred_raw_bundles_to_long_metrics_or_raise,
)
from origo.fred.contracts import FREDSeriesRegistryEntry


def test_normalize_fred_raw_bundles_to_long_metrics_uses_persisted_payload_contract() -> None:
    bundle = FREDRawSeriesBundle(
        source_id='fred_fedfunds',
        series_id='FEDFUNDS',
        source_uri='fred://series/FEDFUNDS',
        fetched_at_utc=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
        registry_version='2026-03-06-s6-c1',
        metadata_payload={
            'seriess': [
                {
                    'id': 'FEDFUNDS',
                    'title': 'Effective Federal Funds Rate',
                    'units': 'Percent',
                    'frequency': 'Monthly',
                    'seasonal_adjustment': 'Not Seasonally Adjusted',
                    'observation_start': '1954-07-01',
                    'observation_end': '2026-03-01',
                    'last_updated': '2026-03-18 15:01:00-05',
                    'popularity': 99,
                    'notes': 'Test metadata payload',
                }
            ]
        },
        observations_payload={
            'observations': [
                {
                    'realtime_start': '2026-03-01',
                    'realtime_end': '2026-03-01',
                    'date': '2026-02-01',
                    'value': '4.33',
                },
                {
                    'realtime_start': '2026-03-01',
                    'realtime_end': '2026-03-01',
                    'date': '2026-03-01',
                    'value': '4.35',
                },
            ]
        },
    )

    rows = normalize_fred_raw_bundles_to_long_metrics_or_raise(
        bundles=[bundle],
        registry_entries=[
            FREDSeriesRegistryEntry(
                series_id='FEDFUNDS',
                source_id='fred_fedfunds',
                metric_name='effective_federal_funds_rate',
                metric_unit='Percent',
                frequency_hint='Monthly',
            )
        ],
        registry_version='2026-03-06-s6-c1',
    )

    assert [row.source_id for row in rows] == ['fred_fedfunds', 'fred_fedfunds']
    assert [row.observed_at_utc.isoformat() for row in rows] == [
        '2026-02-01T00:00:00+00:00',
        '2026-03-01T00:00:00+00:00',
    ]
    assert [row.metric_value_string for row in rows] == ['4.33', '4.35']
    assert all('FEDFUNDS' in row.dimensions_json for row in rows)
