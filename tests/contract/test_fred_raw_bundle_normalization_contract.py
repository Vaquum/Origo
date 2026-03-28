from __future__ import annotations

import json
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
    assert all(
        row.provenance_json is not None and 'last_updated_utc' not in row.provenance_json
        for row in rows
    )


def test_normalize_fred_raw_bundles_to_long_metrics_ignores_metadata_last_updated_in_row_identity() -> None:
    registry_entries = [
        FREDSeriesRegistryEntry(
            series_id='FEDFUNDS',
            source_id='fred_fedfunds',
            metric_name='effective_federal_funds_rate',
            metric_unit='Percent',
            frequency_hint='Monthly',
        )
    ]
    base_observations_payload: dict[str, object] = {
        'output_type': 1,
        'observations': [
            {
                'realtime_start': '2026-03-01',
                'realtime_end': '2026-03-01',
                'date': '2026-03-01',
                'value': '4.35',
            }
        ],
    }

    def _bundle(last_updated: str) -> FREDRawSeriesBundle:
        return FREDRawSeriesBundle(
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
                        'last_updated': last_updated,
                        'popularity': 99,
                        'notes': 'Test metadata payload',
                    }
                ]
            },
            observations_payload=base_observations_payload,
        )

    rows_1 = normalize_fred_raw_bundles_to_long_metrics_or_raise(
        bundles=[_bundle('2026-03-18 15:01:00-05')],
        registry_entries=registry_entries,
        registry_version='2026-03-06-s6-c1',
    )
    rows_2 = normalize_fred_raw_bundles_to_long_metrics_or_raise(
        bundles=[_bundle('2026-03-28 15:01:00-05')],
        registry_entries=registry_entries,
        registry_version='2026-03-06-s6-c1',
    )

    assert [(row.metric_id, row.provenance_json) for row in rows_1] == [
        (row.metric_id, row.provenance_json) for row in rows_2
    ]


def test_normalize_fred_raw_bundles_to_long_metrics_expands_output_type_2_vintages() -> None:
    bundle = FREDRawSeriesBundle(
        source_id='fred_cpiaucsl',
        series_id='CPIAUCSL',
        source_uri='fred://series/CPIAUCSL',
        fetched_at_utc=datetime(2026, 3, 28, 12, 0, tzinfo=UTC),
        registry_version='2026-03-06-s6-c1',
        metadata_payload={
            'seriess': [
                {
                    'id': 'CPIAUCSL',
                    'title': 'Consumer Price Index for All Urban Consumers: All Items',
                    'units': 'Index 1982-1984=100',
                    'frequency': 'Monthly',
                    'seasonal_adjustment': 'Seasonally Adjusted',
                    'observation_start': '1947-01-01',
                    'observation_end': '2026-03-01',
                    'last_updated': '2026-03-18 07:00:00-05',
                    'popularity': 99,
                    'notes': 'Test metadata payload',
                }
            ]
        },
        observations_payload={
            'output_type': 2,
            'observations': [
                {
                    'date': '1947-01-01',
                    'CPIAUCSL_20260311': '21.48',
                    'CPIAUCSL_20260312': '21.48',
                }
            ],
        },
    )

    rows = normalize_fred_raw_bundles_to_long_metrics_or_raise(
        bundles=[bundle],
        registry_entries=[
            FREDSeriesRegistryEntry(
                series_id='CPIAUCSL',
                source_id='fred_cpiaucsl',
                metric_name='consumer_price_index_all_items',
                metric_unit='Index 1982-1984=100',
                frequency_hint='Monthly',
            )
        ],
        registry_version='2026-03-06-s6-c1',
    )

    assert len(rows) == 2
    assert [row.metric_value_string for row in rows] == ['21.48', '21.48']
    assert rows[0].metric_id != rows[1].metric_id
    realtime_pairs = sorted(
        (
            json.loads(row.provenance_json or '{}')['realtime_start'],
            json.loads(row.provenance_json or '{}')['realtime_end'],
        )
        for row in rows
    )
    assert realtime_pairs == [
        ('2026-03-11', '2026-03-11'),
        ('2026-03-12', '2026-03-12'),
    ]
