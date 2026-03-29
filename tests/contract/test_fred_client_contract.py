from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from origo.fred.client import FREDAPIConfig, FREDClient, load_fred_api_config_from_env


def _build_client(
    *, revision_history_initial_vintage_dates_per_request: int = 275
) -> FREDClient:
    return FREDClient(
        FREDAPIConfig(
            api_key='test-key',
            timeout_seconds=1.0,
            revision_history_initial_vintage_dates_per_request=(
                revision_history_initial_vintage_dates_per_request
            ),
        )
    )


def test_fetch_all_series_vintage_dates_pages_until_expected_count(
    monkeypatch: Any,
) -> None:
    client = _build_client()
    captured_calls: list[tuple[str, dict[str, str]]] = []

    def _fake_request_json(*, endpoint: str, params: dict[str, str]) -> dict[str, object]:
        captured_calls.append((endpoint, dict(params)))
        assert endpoint == 'series/vintagedates'
        if params['offset'] == '0':
            return {
                'count': 3,
                'offset': 0,
                'limit': 2,
                'vintage_dates': [
                    '2026-03-11',
                    '2026-03-12',
                ],
            }
        if params['offset'] == '2':
            return {
                'count': 3,
                'offset': 2,
                'limit': 2,
                'vintage_dates': ['2026-03-13'],
            }
        raise AssertionError(f'unexpected vintagedates request params={params!r}')

    monkeypatch.setattr(client, '_request_json', _fake_request_json)

    vintage_dates = client.fetch_all_series_vintage_dates(
        series_id='CPIAUCSL',
        realtime_start=date(1776, 7, 4),
        realtime_end=date(9999, 12, 31),
    )

    assert vintage_dates == [
        date(2026, 3, 11),
        date(2026, 3, 12),
        date(2026, 3, 13),
    ]
    assert captured_calls == [
        (
            'series/vintagedates',
            {
                'series_id': 'CPIAUCSL',
                'limit': '10000',
                'offset': '0',
                'sort_order': 'asc',
                'realtime_start': '1776-07-04',
                'realtime_end': '9999-12-31',
            },
        ),
        (
            'series/vintagedates',
            {
                'series_id': 'CPIAUCSL',
                'limit': '10000',
                'offset': '2',
                'sort_order': 'asc',
                'realtime_start': '1776-07-04',
                'realtime_end': '9999-12-31',
            },
        ),
    ]


def test_fetch_series_revision_history_payload_batches_vintage_dates_for_output_type_2(
    monkeypatch: Any,
) -> None:
    client = _build_client(revision_history_initial_vintage_dates_per_request=2)
    vintage_dates = [
        date(2026, 3, 11),
        date(2026, 3, 12),
        date(2026, 3, 13),
    ]
    captured_observation_calls: list[dict[str, str]] = []

    def _fake_fetch_all_series_vintage_dates(**_: Any) -> list[date]:
        return vintage_dates

    monkeypatch.setattr(
        client,
        'fetch_all_series_vintage_dates',
        _fake_fetch_all_series_vintage_dates,
    )

    def _fake_fetch_series_observations_payload(**kwargs: Any) -> dict[str, object]:
        vintage_dates_param = kwargs['vintage_dates']
        captured_observation_calls.append(
            {
                'series_id': kwargs['series_id'],
                'observation_start': kwargs['observation_start'].isoformat(),
                'observation_end': kwargs['observation_end'].isoformat(),
                'vintage_dates': ','.join(
                    vintage_date.isoformat() for vintage_date in vintage_dates_param
                ),
                'output_type': str(kwargs['output_type']),
                'sort_order': kwargs['sort_order'],
            }
        )
        if vintage_dates_param == vintage_dates[:2]:
            return {
                'output_type': 2,
                'observations': [
                    {
                        'date': '1947-01-01',
                        'CPIAUCSL_20260311': '21.48',
                        'CPIAUCSL_20260312': '21.48',
                    }
                ],
            }
        if vintage_dates_param == vintage_dates[2:]:
            return {
                'output_type': 2,
                'observations': [
                    {
                        'date': '1947-01-01',
                        'CPIAUCSL_20260313': '21.49',
                    }
                ],
            }
        raise AssertionError(f'unexpected observation request kwargs={kwargs!r}')

    monkeypatch.setattr(
        client,
        'fetch_series_observations_payload',
        _fake_fetch_series_observations_payload,
    )
    payload = client.fetch_series_revision_history_payload(
        series_id='CPIAUCSL',
        observation_start=date(1947, 1, 1),
        observation_end=date(1947, 1, 1),
        realtime_start=date(1776, 7, 4),
        realtime_end=date(9999, 12, 31),
    )

    assert payload['output_type'] == 2
    assert payload['request_chunk_count'] == 2
    assert payload['request_chunk_size_limit'] == 2
    assert payload['request_chunk_hard_limit'] == 2000
    assert payload['requested_vintage_dates'] == [
        '2026-03-11',
        '2026-03-12',
        '2026-03-13',
    ]
    assert payload['observations'] == [
        {
            'date': '1947-01-01',
            'CPIAUCSL_20260311': '21.48',
            'CPIAUCSL_20260312': '21.48',
        },
        {
            'date': '1947-01-01',
            'CPIAUCSL_20260313': '21.49',
        },
    ]
    assert captured_observation_calls == [
        {
            'series_id': 'CPIAUCSL',
            'observation_start': '1947-01-01',
            'observation_end': '1947-01-01',
            'vintage_dates': '2026-03-11,2026-03-12',
            'output_type': '2',
            'sort_order': 'asc',
        },
        {
            'series_id': 'CPIAUCSL',
            'observation_start': '1947-01-01',
            'observation_end': '1947-01-01',
            'vintage_dates': '2026-03-13',
            'output_type': '2',
            'sort_order': 'asc',
        },
    ]


@pytest.mark.parametrize(
    ('failure_message', 'expected_calls'),
    [
        (
            'FRED request failed with HTTP error, endpoint=series/observations '
            'status_code=414 body=Request-URI Too Long',
            [
                '2026-03-11,2026-03-12,2026-03-13',
                '2026-03-11',
                '2026-03-12,2026-03-13',
            ],
        ),
        (
            'FRED request failed with HTTP error, endpoint=series/observations '
            'status_code=400 body=<H1>Bad Request</H1>',
            [
                '2026-03-11,2026-03-12,2026-03-13',
                '2026-03-11',
                '2026-03-12,2026-03-13',
            ],
        ),
        (
            'FRED request transport failure, endpoint=series/observations, reason=timed_out',
            [
                '2026-03-11,2026-03-12,2026-03-13',
                '2026-03-11',
                '2026-03-12,2026-03-13',
            ],
        ),
    ],
)
def test_fetch_series_revision_history_payload_splits_retryable_vintage_date_requests(
    monkeypatch: Any,
    failure_message: str,
    expected_calls: list[str],
) -> None:
    client = _build_client(revision_history_initial_vintage_dates_per_request=3)
    vintage_dates = [
        date(2026, 3, 11),
        date(2026, 3, 12),
        date(2026, 3, 13),
    ]
    captured_observation_calls: list[str] = []

    def _fake_fetch_all_series_vintage_dates(**_: Any) -> list[date]:
        return vintage_dates

    monkeypatch.setattr(
        client,
        'fetch_all_series_vintage_dates',
        _fake_fetch_all_series_vintage_dates,
    )

    def _fake_fetch_series_observations_payload(**kwargs: Any) -> dict[str, object]:
        vintage_date_chunk = kwargs['vintage_dates']
        chunk_label = ','.join(vintage_date.isoformat() for vintage_date in vintage_date_chunk)
        captured_observation_calls.append(chunk_label)
        if len(vintage_date_chunk) == 3:
            raise RuntimeError(failure_message)
        if vintage_date_chunk == vintage_dates[:1]:
            return {
                'output_type': 2,
                'observations': [
                    {
                        'date': '1947-01-01',
                        'CPIAUCSL_20260311': '21.48',
                    }
                ],
            }
        if vintage_date_chunk == vintage_dates[1:]:
            return {
                'output_type': 2,
                'observations': [
                    {
                        'date': '1947-01-01',
                        'CPIAUCSL_20260312': '21.48',
                        'CPIAUCSL_20260313': '21.49',
                    }
                ],
            }
        raise AssertionError(f'unexpected observation request kwargs={kwargs!r}')

    monkeypatch.setattr(
        client,
        'fetch_series_observations_payload',
        _fake_fetch_series_observations_payload,
    )

    payload = client.fetch_series_revision_history_payload(
        series_id='CPIAUCSL',
        observation_start=date(1947, 1, 1),
        observation_end=date(1947, 1, 1),
        realtime_start=date(1776, 7, 4),
        realtime_end=date(9999, 12, 31),
    )

    assert payload['request_chunk_count'] == 2
    assert payload['observations'] == [
        {
            'date': '1947-01-01',
            'CPIAUCSL_20260311': '21.48',
        },
        {
            'date': '1947-01-01',
            'CPIAUCSL_20260312': '21.48',
            'CPIAUCSL_20260313': '21.49',
        },
    ]
    assert captured_observation_calls == expected_calls


def test_fetch_series_observations_payload_rejects_oversized_vintage_date_requests() -> None:
    client = _build_client()
    oversized_vintage_dates = [
        date.fromordinal(date(2020, 1, 1).toordinal() + offset)
        for offset in range(2001)
    ]

    with pytest.raises(
        ValueError,
        match='vintage_dates exceeds FRED output_type=2 request limit',
    ):
        client.fetch_series_observations_payload(
            series_id='CPIAUCSL',
            vintage_dates=oversized_vintage_dates,
            output_type=2,
        )


def test_load_fred_api_config_from_env_requires_revision_history_window(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('FRED_API_KEY', 'test-key')
    monkeypatch.setenv('ORIGO_FRED_HTTP_TIMEOUT_SECONDS', '20')
    monkeypatch.setenv(
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST',
        '275',
    )

    config = load_fred_api_config_from_env()

    assert config.api_key == 'test-key'
    assert config.timeout_seconds == 20.0
    assert config.revision_history_initial_vintage_dates_per_request == 275


def test_load_fred_api_config_from_env_rejects_invalid_revision_history_window(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('FRED_API_KEY', 'test-key')
    monkeypatch.setenv('ORIGO_FRED_HTTP_TIMEOUT_SECONDS', '20')
    monkeypatch.setenv(
        'ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST',
        '2001',
    )

    with pytest.raises(
        RuntimeError,
        match='ORIGO_FRED_REVISION_HISTORY_INITIAL_VINTAGE_DATES_PER_REQUEST exceeds '
        'FRED output_type=2 request limit',
    ):
        load_fred_api_config_from_env()
