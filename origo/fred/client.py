from __future__ import annotations

import json
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .contracts import (
    FREDObservation,
    FREDSeriesMetadata,
    FREDSeriesRegistryEntry,
    FREDSeriesSnapshot,
)

_FRED_BASE_URL = 'https://api.stlouisfed.org/fred'
_FRED_FILE_TYPE = 'json'
_DEFAULT_USER_AGENT = 'origo-fred-client/1.0'
_LAST_UPDATED_SUFFIX_PATTERN = re.compile(r'([+-]\d{2})(?::?(\d{2}))?$')
_FRED_OUTPUT_TYPE_VINTAGE_COLUMN_PATTERN = re.compile(r'^(?P<series_id>[A-Z0-9]+)_(?P<vintage>\d{8})$')
_FRED_VINTAGE_DATES_MAX_LIMIT = 10000
_FRED_OUTPUT_TYPE_2_MAX_VINTAGE_DATES_PER_REQUEST = 2000


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _require_positive_float_env(name: str) -> float:
    raw_value = _require_env(name)
    try:
        parsed = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a float, got '{raw_value}'") from exc
    if parsed <= 0:
        raise RuntimeError(f'{name} must be > 0, got {parsed}')
    return parsed


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'Invalid {label}: {value}') from exc


def _parse_last_updated_utc(raw: str) -> datetime:
    normalized = raw.strip()
    if normalized == '':
        raise RuntimeError('last_updated must be non-empty')

    suffix_match = _LAST_UPDATED_SUFFIX_PATTERN.search(normalized)
    if suffix_match is None:
        raise RuntimeError(
            'last_updated must include timezone offset, got '
            f"'{normalized}'"
        )

    hours_component = suffix_match.group(1)
    minutes_component = suffix_match.group(2) or '00'
    normalized_suffix = f'{hours_component}{minutes_component}'
    normalized = (
        normalized[: suffix_match.start()]
        + normalized_suffix
        + normalized[suffix_match.end() :]
    )

    try:
        parsed = datetime.strptime(normalized, '%Y-%m-%d %H:%M:%S%z')
    except ValueError as exc:
        raise RuntimeError(f'Invalid last_updated: {raw}') from exc
    return parsed.astimezone(UTC)


def _normalize_observation_value(raw_value: str) -> float | None:
    value = raw_value.strip()
    if value == '.':
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f'Invalid observation value: {raw_value}') from exc


def _require_object(payload: object, label: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise RuntimeError(f'{label} must be a JSON object')
    raw_mapping = cast(dict[object, object], payload)
    for raw_key in raw_mapping:
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
    return cast(dict[str, object], raw_mapping)


def _require_list(payload: object, label: str) -> list[object]:
    if not isinstance(payload, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[object], payload)


def _require_string(payload: dict[str, object], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{context}.{key} must be a non-empty string')
    return value


def _require_int(payload: dict[str, object], key: str, context: str) -> int:
    value = payload.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(f'{context}.{key} must be an integer') from exc
    raise RuntimeError(f'{context}.{key} must be an integer')


def _parse_json_object(raw_body: bytes, context: str) -> dict[str, object]:
    try:
        decoded = json.loads(raw_body.decode('utf-8'))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'Failed to decode JSON for {context}') from exc
    return _require_object(decoded, context)


def _normalize_series_metadata(
    *, series_id: str, payload: dict[str, object]
) -> FREDSeriesMetadata:
    raw_series_entries = _require_list(payload.get('seriess'), 'FRED payload.seriess')
    if len(raw_series_entries) != 1:
        raise RuntimeError(
            f'FRED payload.seriess must contain exactly one item, got {len(raw_series_entries)}'
        )

    first_item = _require_object(raw_series_entries[0], 'payload.seriess[0]')
    payload_series_id = _require_string(first_item, 'id', 'payload.seriess[0]')
    if payload_series_id != series_id:
        raise RuntimeError(
            'Requested series_id does not match metadata response, got '
            f'{series_id} != {payload_series_id}'
        )

    return FREDSeriesMetadata(
        series_id=payload_series_id,
        title=_require_string(first_item, 'title', 'payload.seriess[0]'),
        units=_require_string(first_item, 'units', 'payload.seriess[0]'),
        frequency=_require_string(first_item, 'frequency', 'payload.seriess[0]'),
        seasonal_adjustment=_require_string(
            first_item, 'seasonal_adjustment', 'payload.seriess[0]'
        ),
        observation_start=_parse_date(
            _require_string(first_item, 'observation_start', 'payload.seriess[0]'),
            'observation_start',
        ),
        observation_end=_parse_date(
            _require_string(first_item, 'observation_end', 'payload.seriess[0]'),
            'observation_end',
        ),
        last_updated_utc=_parse_last_updated_utc(
            _require_string(first_item, 'last_updated', 'payload.seriess[0]')
        ),
        popularity=_require_int(first_item, 'popularity', 'payload.seriess[0]'),
        notes=_require_string(first_item, 'notes', 'payload.seriess[0]'),
    )


def _normalize_series_observations(
    *, series_id: str, payload: dict[str, object]
) -> list[FREDObservation]:
    raw_output_type = payload.get('output_type', 1)
    if isinstance(raw_output_type, int):
        output_type = raw_output_type
    elif isinstance(raw_output_type, str):
        try:
            output_type = int(raw_output_type)
        except ValueError as exc:
            raise RuntimeError('FRED payload.output_type must be an integer when set') from exc
    else:
        raise RuntimeError('FRED payload.output_type must be an integer when set')

    if output_type == 1:
        return _normalize_output_type_1_observations(series_id=series_id, payload=payload)
    if output_type == 2:
        return _normalize_output_type_2_observations(series_id=series_id, payload=payload)
    raise RuntimeError(
        f'FRED observations output_type={output_type} is not supported by the Origo normalizer'
    )


def _normalize_series_vintage_dates(
    *, series_id: str, payload: dict[str, object]
) -> list[date]:
    raw_vintage_dates = _require_list(
        payload.get('vintage_dates'), 'FRED payload.vintage_dates'
    )
    vintage_dates: list[date] = []
    seen_vintage_dates: set[date] = set()
    for index, raw_vintage_date in enumerate(raw_vintage_dates):
        if not isinstance(raw_vintage_date, str) or raw_vintage_date.strip() == '':
            raise RuntimeError(
                f'payload.vintage_dates[{index}] must be a non-empty string'
            )
        vintage_date = _parse_date(
            raw_vintage_date,
            f'payload.vintage_dates[{index}]',
        )
        if vintage_date in seen_vintage_dates:
            raise RuntimeError(
                f'FRED vintage dates must be unique for series_id={series_id}, '
                f'duplicate={vintage_date.isoformat()}'
            )
        seen_vintage_dates.add(vintage_date)
        vintage_dates.append(vintage_date)
    return vintage_dates


def _is_series_observations_uri_too_long_error(exc: RuntimeError) -> bool:
    message = str(exc)
    return (
        'endpoint=series/observations' in message
        and 'status_code=414' in message
    )


def _normalize_output_type_1_observations(
    *, series_id: str, payload: dict[str, object]
) -> list[FREDObservation]:
    raw_observations = _require_list(
        payload.get('observations'), 'FRED payload.observations'
    )
    if len(raw_observations) == 0:
        raise RuntimeError(f'FRED observations are empty for series_id={series_id}')

    observations: list[FREDObservation] = []
    for index, raw_observation in enumerate(raw_observations):
        context = f'payload.observations[{index}]'
        payload_observation = _require_object(raw_observation, context)
        raw_value = _require_string(payload_observation, 'value', context)
        observations.append(
            FREDObservation(
                series_id=series_id,
                realtime_start=_parse_date(
                    _require_string(payload_observation, 'realtime_start', context),
                    f'{context}.realtime_start',
                ),
                realtime_end=_parse_date(
                    _require_string(payload_observation, 'realtime_end', context),
                    f'{context}.realtime_end',
                ),
                observation_date=_parse_date(
                    _require_string(payload_observation, 'date', context),
                    f'{context}.date',
                ),
                value=_normalize_observation_value(raw_value),
                raw_value=raw_value,
            )
        )

    return observations


def _normalize_output_type_2_observations(
    *, series_id: str, payload: dict[str, object]
) -> list[FREDObservation]:
    raw_observations = _require_list(
        payload.get('observations'), 'FRED payload.observations'
    )
    if len(raw_observations) == 0:
        raise RuntimeError(f'FRED observations are empty for series_id={series_id}')

    observations: list[FREDObservation] = []
    for index, raw_observation in enumerate(raw_observations):
        context = f'payload.observations[{index}]'
        payload_observation = _require_object(raw_observation, context)
        observation_date = _parse_date(
            _require_string(payload_observation, 'date', context),
            f'{context}.date',
        )
        vintage_column_count = 0
        for raw_key, raw_value in payload_observation.items():
            if raw_key == 'date':
                continue
            vintage_match = _FRED_OUTPUT_TYPE_VINTAGE_COLUMN_PATTERN.fullmatch(raw_key)
            if vintage_match is None:
                raise RuntimeError(
                    f'{context} contains unexpected output_type=2 key {raw_key!r}'
                )
            vintage_series_id = vintage_match.group('series_id')
            if vintage_series_id != series_id:
                raise RuntimeError(
                    f'{context} vintage column series_id mismatch: '
                    f'{vintage_series_id!r} != {series_id!r}'
                )
            if not isinstance(raw_value, str) or raw_value.strip() == '':
                raise RuntimeError(f'{context}.{raw_key} must be a non-empty string')
            vintage_date = _parse_date(
                (
                    f'{vintage_match.group("vintage")[0:4]}-'
                    f'{vintage_match.group("vintage")[4:6]}-'
                    f'{vintage_match.group("vintage")[6:8]}'
                ),
                f'{context}.{raw_key}',
            )
            observations.append(
                FREDObservation(
                    series_id=series_id,
                    realtime_start=vintage_date,
                    realtime_end=vintage_date,
                    observation_date=observation_date,
                    value=_normalize_observation_value(raw_value),
                    raw_value=raw_value,
                )
            )
            vintage_column_count += 1
        if vintage_column_count == 0:
            raise RuntimeError(
                f'{context} output_type=2 row must contain at least one vintage column'
            )

    observations.sort(
        key=lambda observation: (
            observation.observation_date,
            observation.realtime_start,
            observation.realtime_end,
            observation.raw_value,
        )
    )
    return observations


def normalize_fred_series_metadata_payload_or_raise(
    *, series_id: str, payload: dict[str, object]
) -> FREDSeriesMetadata:
    if series_id.strip() == '':
        raise ValueError('series_id must be non-empty')
    return _normalize_series_metadata(series_id=series_id, payload=payload)


def normalize_fred_series_observations_payload_or_raise(
    *, series_id: str, payload: dict[str, object]
) -> list[FREDObservation]:
    if series_id.strip() == '':
        raise ValueError('series_id must be non-empty')
    return _normalize_series_observations(series_id=series_id, payload=payload)


def normalize_fred_series_vintage_dates_payload_or_raise(
    *, series_id: str, payload: dict[str, object]
) -> list[date]:
    if series_id.strip() == '':
        raise ValueError('series_id must be non-empty')
    return _normalize_series_vintage_dates(series_id=series_id, payload=payload)


@dataclass(frozen=True)
class FREDAPIConfig:
    api_key: str
    timeout_seconds: float
    base_url: str = _FRED_BASE_URL
    user_agent: str = _DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if self.api_key.strip() == '':
            raise ValueError('api_key must be non-empty')
        if self.timeout_seconds <= 0:
            raise ValueError('timeout_seconds must be > 0')
        if self.base_url.strip() == '':
            raise ValueError('base_url must be non-empty')
        if self.user_agent.strip() == '':
            raise ValueError('user_agent must be non-empty')


def load_fred_api_config_from_env() -> FREDAPIConfig:
    return FREDAPIConfig(
        api_key=_require_env('FRED_API_KEY'),
        timeout_seconds=_require_positive_float_env('ORIGO_FRED_HTTP_TIMEOUT_SECONDS'),
    )


class FREDClient:
    def __init__(self, config: FREDAPIConfig) -> None:
        self._config = config

    def _request_json(
        self,
        *,
        endpoint: str,
        params: dict[str, str],
    ) -> dict[str, object]:
        request_query = {
            **params,
            'api_key': self._config.api_key,
            'file_type': _FRED_FILE_TYPE,
        }
        query_string = urlencode(request_query)
        request_url = (
            f'{self._config.base_url.rstrip("/")}/{endpoint.lstrip("/")}?{query_string}'
        )

        request = Request(
            url=request_url,
            headers={'User-Agent': self._config.user_agent},
            method='GET',
        )

        try:
            with urlopen(request, timeout=self._config.timeout_seconds) as response:
                status_code = response.getcode()
                raw_body = response.read()
        except HTTPError as exc:
            raw_error = exc.read().decode('utf-8', errors='replace')
            raise RuntimeError(
                'FRED request failed with HTTP error, '
                f'endpoint={endpoint} status_code={exc.code} body={raw_error}'
            ) from exc
        except URLError as exc:
            raise RuntimeError(
                f'FRED request transport failure, endpoint={endpoint}, reason={exc.reason}'
            ) from exc

        if status_code != 200:
            body_text = raw_body.decode('utf-8', errors='replace')
            raise RuntimeError(
                'FRED request returned non-200 status, '
                f'endpoint={endpoint} status_code={status_code} body={body_text}'
            )

        payload = _parse_json_object(raw_body, f'FRED endpoint={endpoint}')

        raw_error_code = payload.get('error_code')
        if raw_error_code is not None:
            error_code = (
                str(raw_error_code)
                if isinstance(raw_error_code, (int, str))
                else repr(raw_error_code)
            )
            raw_error_message = payload.get('error_message')
            if not isinstance(raw_error_message, str):
                raise RuntimeError(
                    'FRED payload returned error_code without a valid error_message'
                )
            raise RuntimeError(
                'FRED API returned error payload, '
                f'error_code={error_code} error_message={raw_error_message}'
            )

        return payload

    def fetch_series_metadata(self, *, series_id: str) -> FREDSeriesMetadata:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        payload = self.fetch_series_metadata_payload(series_id=series_id)
        return normalize_fred_series_metadata_payload_or_raise(
            series_id=series_id,
            payload=payload,
        )

    def fetch_series_metadata_payload(self, *, series_id: str) -> dict[str, object]:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        return self._request_json(endpoint='series', params={'series_id': series_id})

    def fetch_series_observations(
        self,
        *,
        series_id: str,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
        vintage_dates: Sequence[date] | None = None,
        output_type: Literal[1, 2, 3, 4] = 1,
        sort_order: Literal['asc', 'desc'] = 'asc',
        limit: int | None = None,
    ) -> list[FREDObservation]:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        if limit is not None and limit <= 0:
            raise ValueError(f'limit must be > 0 when set, got {limit}')

        payload = self.fetch_series_observations_payload(
            series_id=series_id,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            vintage_dates=vintage_dates,
            output_type=output_type,
            sort_order=sort_order,
            limit=limit,
        )
        return normalize_fred_series_observations_payload_or_raise(
            series_id=series_id,
            payload=payload,
        )

    def fetch_series_observations_payload(
        self,
        *,
        series_id: str,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
        vintage_dates: Sequence[date] | None = None,
        output_type: Literal[1, 2, 3, 4] = 1,
        sort_order: Literal['asc', 'desc'] = 'asc',
        limit: int | None = None,
    ) -> dict[str, object]:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        if limit is not None and limit <= 0:
            raise ValueError(f'limit must be > 0 when set, got {limit}')
        if realtime_start is not None and realtime_end is not None and realtime_start > realtime_end:
            raise ValueError(
                'realtime_start must be <= realtime_end, got '
                f'{realtime_start.isoformat()} > {realtime_end.isoformat()}'
            )
        if vintage_dates is not None and (realtime_start is not None or realtime_end is not None):
            raise ValueError(
                'vintage_dates cannot be combined with realtime_start/realtime_end'
            )
        params: dict[str, str] = {'series_id': series_id, 'sort_order': sort_order}
        if observation_start is not None:
            params['observation_start'] = observation_start.isoformat()
        if observation_end is not None:
            params['observation_end'] = observation_end.isoformat()
        if realtime_start is not None:
            params['realtime_start'] = realtime_start.isoformat()
        if realtime_end is not None:
            params['realtime_end'] = realtime_end.isoformat()
        if vintage_dates is not None:
            normalized_vintage_dates: list[str] = []
            seen_vintage_dates: set[date] = set()
            for vintage_date in vintage_dates:
                if vintage_date in seen_vintage_dates:
                    raise ValueError(
                        f'vintage_dates must be unique, got duplicate={vintage_date.isoformat()}'
                    )
                seen_vintage_dates.add(vintage_date)
                normalized_vintage_dates.append(vintage_date.isoformat())
            if normalized_vintage_dates == []:
                raise ValueError('vintage_dates must be non-empty when provided')
            if len(normalized_vintage_dates) > _FRED_OUTPUT_TYPE_2_MAX_VINTAGE_DATES_PER_REQUEST:
                raise ValueError(
                    'vintage_dates exceeds FRED output_type=2 request limit, '
                    f'got {len(normalized_vintage_dates)} > '
                    f'{_FRED_OUTPUT_TYPE_2_MAX_VINTAGE_DATES_PER_REQUEST}'
                )
            params['vintage_dates'] = ','.join(normalized_vintage_dates)
        params['output_type'] = str(output_type)
        if limit is not None:
            params['limit'] = str(limit)
        return self._request_json(endpoint='series/observations', params=params)

    def fetch_series_vintage_dates_payload(
        self,
        *,
        series_id: str,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
        limit: int = _FRED_VINTAGE_DATES_MAX_LIMIT,
        offset: int = 0,
        sort_order: Literal['asc', 'desc'] = 'asc',
    ) -> dict[str, object]:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        if limit <= 0 or limit > _FRED_VINTAGE_DATES_MAX_LIMIT:
            raise ValueError(
                f'limit must be between 1 and {_FRED_VINTAGE_DATES_MAX_LIMIT}, got {limit}'
            )
        if offset < 0:
            raise ValueError(f'offset must be >= 0, got {offset}')
        if realtime_start is not None and realtime_end is not None and realtime_start > realtime_end:
            raise ValueError(
                'realtime_start must be <= realtime_end, got '
                f'{realtime_start.isoformat()} > {realtime_end.isoformat()}'
            )
        params: dict[str, str] = {
            'series_id': series_id,
            'limit': str(limit),
            'offset': str(offset),
            'sort_order': sort_order,
        }
        if realtime_start is not None:
            params['realtime_start'] = realtime_start.isoformat()
        if realtime_end is not None:
            params['realtime_end'] = realtime_end.isoformat()
        return self._request_json(endpoint='series/vintagedates', params=params)

    def fetch_all_series_vintage_dates(
        self,
        *,
        series_id: str,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> list[date]:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        all_vintage_dates: list[date] = []
        expected_count: int | None = None
        offset = 0
        while True:
            payload = self.fetch_series_vintage_dates_payload(
                series_id=series_id,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
                limit=_FRED_VINTAGE_DATES_MAX_LIMIT,
                offset=offset,
                sort_order='asc',
            )
            if expected_count is None:
                expected_count = _require_int(
                    payload,
                    'count',
                    'FRED payload',
                )
            page_vintage_dates = normalize_fred_series_vintage_dates_payload_or_raise(
                series_id=series_id,
                payload=payload,
            )
            if page_vintage_dates == []:
                if expected_count != 0 and offset < expected_count:
                    raise RuntimeError(
                        'FRED vintage date pagination ended before expected count was reached, '
                        f'series_id={series_id} offset={offset} expected_count={expected_count}'
                    )
                break
            all_vintage_dates.extend(page_vintage_dates)
            offset += len(page_vintage_dates)
            if offset >= expected_count:
                break
        return all_vintage_dates

    def fetch_series_revision_history_payload(
        self,
        *,
        series_id: str,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date,
        realtime_end: date,
    ) -> dict[str, object]:
        if series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        vintage_dates = self.fetch_all_series_vintage_dates(
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        if vintage_dates == []:
            raise RuntimeError(
                f'FRED revision history returned no vintage dates for series_id={series_id}'
            )

        merged_observations: list[object] = []
        chunk_payloads: list[dict[str, object]] = []

        def _fetch_revision_history_chunk(
            *,
            vintage_date_chunk: Sequence[date],
        ) -> list[dict[str, object]]:
            try:
                return [
                    self.fetch_series_observations_payload(
                        series_id=series_id,
                        observation_start=observation_start,
                        observation_end=observation_end,
                        vintage_dates=vintage_date_chunk,
                        output_type=2,
                        sort_order='asc',
                        limit=None,
                    )
                ]
            except RuntimeError as exc:
                if (
                    _is_series_observations_uri_too_long_error(exc)
                    and len(vintage_date_chunk) > 1
                ):
                    midpoint = len(vintage_date_chunk) // 2
                    return _fetch_revision_history_chunk(
                        vintage_date_chunk=vintage_date_chunk[:midpoint]
                    ) + _fetch_revision_history_chunk(
                        vintage_date_chunk=vintage_date_chunk[midpoint:]
                    )
                raise

        for start_index in range(
            0,
            len(vintage_dates),
            _FRED_OUTPUT_TYPE_2_MAX_VINTAGE_DATES_PER_REQUEST,
        ):
            vintage_date_chunk = vintage_dates[
                start_index : start_index + _FRED_OUTPUT_TYPE_2_MAX_VINTAGE_DATES_PER_REQUEST
            ]
            for chunk_payload in _fetch_revision_history_chunk(
                vintage_date_chunk=vintage_date_chunk
            ):
                merged_observations.extend(
                    _require_list(
                        chunk_payload.get('observations'),
                        'FRED payload.observations',
                    )
                )
                chunk_payloads.append(chunk_payload)

        return {
            'output_type': 2,
            'sort_order': 'asc',
            'realtime_start': realtime_start.isoformat(),
            'realtime_end': realtime_end.isoformat(),
            'observation_start': (
                observation_start.isoformat() if observation_start is not None else None
            ),
            'observation_end': (
                observation_end.isoformat() if observation_end is not None else None
            ),
            'requested_vintage_dates': [
                vintage_date.isoformat() for vintage_date in vintage_dates
            ],
            'request_chunk_count': len(chunk_payloads),
            'request_chunk_size_limit': _FRED_OUTPUT_TYPE_2_MAX_VINTAGE_DATES_PER_REQUEST,
            'observations': merged_observations,
        }


def build_fred_client_from_env() -> FREDClient:
    return FREDClient(load_fred_api_config_from_env())


def fetch_registry_snapshots(
    *,
    client: FREDClient,
    registry_entries: Sequence[FREDSeriesRegistryEntry],
    observation_limit: int = 5,
) -> list[FREDSeriesSnapshot]:
    if len(registry_entries) == 0:
        raise ValueError('registry_entries must be non-empty')
    if observation_limit <= 0:
        raise ValueError(f'observation_limit must be > 0, got {observation_limit}')

    snapshots: list[FREDSeriesSnapshot] = []
    for entry in registry_entries:
        metadata = client.fetch_series_metadata(series_id=entry.series_id)
        observations = client.fetch_series_observations(
            series_id=entry.series_id,
            sort_order='desc',
            limit=observation_limit,
        )
        if entry.metric_unit is not None and entry.metric_unit != metadata.units:
            raise RuntimeError(
                'Registry metric_unit must match FRED metadata units, '
                f'series_id={entry.series_id} registry={entry.metric_unit} metadata={metadata.units}'
            )
        snapshots.append(
            FREDSeriesSnapshot(
                registry_entry=entry,
                metadata=metadata,
                observations=observations,
            )
        )
    return snapshots
