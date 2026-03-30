from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import requests

from origo_control_plane.config import require_int_env

_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS_ENV = 'ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS'
_OKX_SOURCE_HTTP_TIMEOUT_SECONDS_ENV = 'ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS'
_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS_ENV = 'ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS'

OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT = (
    'https://www.okx.com/priapi/v5/broker/public/trade-data/download-link'
)
OKX_SPOT_INSTRUMENT_ID = 'BTC-USDT'
OKX_TRADE_HISTORY_MODULE = '1'
OKX_SOURCE_DAY_UTC_OFFSET_HOURS = 8

BYBIT_BASE_URL = 'https://public.bybit.com/trading/BTCUSDT/'
BYBIT_SYMBOL = 'BTCUSDT'


class ExchangeSourceHttpError(RuntimeError):
    def __init__(
        self,
        *,
        message: str,
        status_code: int | None,
        error_kind: str,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_kind = error_kind


def _load_positive_timeout_seconds_or_raise(name: str) -> int:
    value = require_int_env(name)
    if value <= 0:
        raise RuntimeError(f'{name} must be > 0, got {value}')
    return value


def load_binance_source_request_timeout_seconds_or_raise() -> int:
    return _load_positive_timeout_seconds_or_raise(
        _BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS_ENV
    )


def load_okx_source_request_timeout_seconds_or_raise() -> int:
    return _load_positive_timeout_seconds_or_raise(_OKX_SOURCE_HTTP_TIMEOUT_SECONDS_ENV)


def load_bybit_source_request_timeout_seconds_or_raise() -> int:
    return _load_positive_timeout_seconds_or_raise(
        _BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS_ENV
    )


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return value


def verify_md5_base64_or_raise(*, payload: bytes, content_md5_b64: str) -> None:
    digest_md5 = hashlib.md5(payload).digest()
    actual_b64 = base64.b64encode(digest_md5).decode('ascii')
    if actual_b64 != content_md5_b64:
        raise RuntimeError(
            'Content-MD5 mismatch: '
            f'expected={content_md5_b64} actual={actual_b64}'
        )


def okx_source_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day - timedelta(hours=OKX_SOURCE_DAY_UTC_OFFSET_HOURS)
    end_utc_exclusive = start_utc + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def resolve_okx_daily_file_url_or_raise(
    *,
    date_str: str,
    session: Any | None = None,
) -> tuple[str, str]:
    begin_ms, _ = okx_source_day_window_utc_ms(date_str)
    payload = {
        'module': OKX_TRADE_HISTORY_MODULE,
        'instType': 'SPOT',
        'instQueryParam': {'instIdList': [OKX_SPOT_INSTRUMENT_ID]},
        'dateQuery': {
            'dateAggrType': 'daily',
            'begin': str(begin_ms),
            'end': str(begin_ms),
        },
    }
    requester = session if session is not None else requests
    response = requester.post(
        OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT,
        json=payload,
        timeout=load_okx_source_request_timeout_seconds_or_raise(),
    )
    if response.status_code == 429:
        retry_after = response.headers.get('Retry-After')
        raise ExchangeSourceHttpError(
            message=(
                'OKX download-link endpoint rate-limited the request: '
                f'date={date_str} retry_after={retry_after!r}'
            ),
            status_code=429,
            error_kind='http_429',
        )
    response.raise_for_status()

    body = _expect_dict(response.json(), 'OKX download-link response')
    code = body.get('code')
    if code != '0':
        raise RuntimeError(f'OKX download-link API returned non-zero code: {code}')

    raw_data = _expect_dict(body.get('data'), 'OKX download-link response.data')
    raw_details = _expect_list(raw_data.get('details'), 'OKX download-link response.details')
    if len(raw_details) != 1:
        raise RuntimeError(
            'OKX download-link response data.details must contain exactly one item '
            f'for date={date_str}, got={raw_details}'
        )
    detail = _expect_dict(raw_details[0], 'OKX download-link response.details[0]')
    raw_group_details = _expect_list(
        detail.get('groupDetails'),
        'OKX download-link response.details[0].groupDetails',
    )
    if len(raw_group_details) != 1:
        raise RuntimeError(
            'OKX download-link response detail.groupDetails must contain exactly one '
            f'item for date={date_str}, got={raw_group_details}'
        )
    group_detail = _expect_dict(
        raw_group_details[0],
        'OKX download-link response.details[0].groupDetails[0]',
    )

    filename = _expect_non_empty_str(
        group_detail.get('filename'),
        'OKX download-link response filename',
    )
    url = _expect_non_empty_str(
        group_detail.get('url'),
        'OKX download-link response url',
    )

    expected_filename = f'{OKX_SPOT_INSTRUMENT_ID}-trades-{date_str}.zip'
    if filename != expected_filename:
        raise RuntimeError(
            'OKX download-link filename mismatch for requested day: '
            f'expected={expected_filename} got={filename}'
        )
    return filename, url


def resolve_bybit_daily_file_url(*, date_str: str) -> tuple[str, str]:
    filename = f'{BYBIT_SYMBOL}{date_str}.csv.gz'
    return filename, BYBIT_BASE_URL + filename
