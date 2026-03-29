from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

from origo.fred import FREDClient, FREDSeriesRegistryEntry, build_fred_raw_bundles


def _metadata_payload(*, series_id: str, observation_start: str, observation_end: str) -> dict[str, object]:
    return {
        'seriess': [
            {
                'id': series_id,
                'title': f'Test {series_id}',
                'units': 'Percent',
                'frequency': 'Monthly',
                'seasonal_adjustment': 'Not Seasonally Adjusted',
                'observation_start': observation_start,
                'observation_end': observation_end,
                'last_updated': '2026-03-29 00:00:00-05',
                'popularity': 1,
                'notes': 'Test metadata payload',
            }
        ]
    }


class _FakeFREDClient:
    def __init__(self, metadata_payloads: dict[str, dict[str, object]]) -> None:
        self._metadata_payloads = metadata_payloads
        self.revision_history_calls: list[dict[str, Any]] = []
        self.latest_snapshot_calls: list[dict[str, Any]] = []

    def fetch_series_metadata_payload(self, *, series_id: str) -> dict[str, object]:
        return self._metadata_payloads[series_id]

    def fetch_series_revision_history_payload(self, **kwargs: Any) -> dict[str, object]:
        self.revision_history_calls.append(kwargs)
        series_id = kwargs['series_id']
        return {
            'output_type': 2,
            'observations': [
                {
                    'date': kwargs['observation_start'].isoformat(),
                    f'{series_id}_20260329': '1.00',
                }
            ],
        }

    def fetch_series_observations_payload(self, **kwargs: Any) -> dict[str, object]:
        self.latest_snapshot_calls.append(kwargs)
        return {
            'output_type': 1,
            'observations': [
                {
                    'realtime_start': kwargs['observation_start'].isoformat(),
                    'realtime_end': kwargs['observation_end'].isoformat(),
                    'date': kwargs['observation_start'].isoformat(),
                    'value': '1.00',
                }
            ],
        }


def test_build_fred_raw_bundles_skips_series_with_no_overlap() -> None:
    client = _FakeFREDClient(
        {
            'FEDFUNDS': _metadata_payload(
                series_id='FEDFUNDS',
                observation_start='1954-07-01',
                observation_end='2026-03-01',
            ),
            'CPIAUCSL': _metadata_payload(
                series_id='CPIAUCSL',
                observation_start='1947-01-01',
                observation_end='2026-03-01',
            ),
        }
    )

    bundles = build_fred_raw_bundles(
        client=cast(FREDClient, client),
        registry_entries=[
            FREDSeriesRegistryEntry(
                series_id='FEDFUNDS',
                source_id='fred_fedfunds',
                metric_name='effective_federal_funds_rate',
            ),
            FREDSeriesRegistryEntry(
                series_id='CPIAUCSL',
                source_id='fred_cpiaucsl',
                metric_name='consumer_price_index_all_items',
            ),
        ],
        registry_version='2026-03-06-s6-c1',
        observation_start=date(1947, 1, 1),
        observation_end=date(1948, 12, 1),
        observations_mode='revision_history',
    )

    assert [bundle.series_id for bundle in bundles] == ['CPIAUCSL']
    assert client.latest_snapshot_calls == []
    assert client.revision_history_calls == [
        {
            'series_id': 'CPIAUCSL',
            'observation_start': date(1947, 1, 1),
            'observation_end': date(1948, 12, 1),
            'realtime_start': date(1776, 7, 4),
            'realtime_end': date(9999, 12, 31),
        }
    ]


def test_build_fred_raw_bundles_fails_loud_when_window_overlaps_no_series() -> None:
    client = _FakeFREDClient(
        {
            'FEDFUNDS': _metadata_payload(
                series_id='FEDFUNDS',
                observation_start='1954-07-01',
                observation_end='2026-03-01',
            )
        }
    )

    with pytest.raises(
        RuntimeError,
        match='FRED raw bundle request did not overlap any configured series',
    ):
        build_fred_raw_bundles(
            client=cast(FREDClient, client),
            registry_entries=[
                FREDSeriesRegistryEntry(
                    series_id='FEDFUNDS',
                    source_id='fred_fedfunds',
                    metric_name='effective_federal_funds_rate',
                )
            ],
            registry_version='2026-03-06-s6-c1',
            observation_start=date(1947, 1, 1),
            observation_end=date(1948, 12, 1),
            observations_mode='revision_history',
        )

    assert client.revision_history_calls == []
    assert client.latest_snapshot_calls == []
