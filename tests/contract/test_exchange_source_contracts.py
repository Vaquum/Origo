from __future__ import annotations

from typing import Any, ClassVar

import pytest
from origo_control_plane.utils import exchange_source_contracts as contracts


def test_exchange_source_timeout_loaders_require_positive_ints(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv('ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS', '0')
    with pytest.raises(RuntimeError, match='must be > 0'):
        contracts.load_binance_source_request_timeout_seconds_or_raise()

    monkeypatch.setenv('ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS', '-1')
    with pytest.raises(RuntimeError, match='must be > 0'):
        contracts.load_okx_source_request_timeout_seconds_or_raise()

    monkeypatch.setenv('ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS', '15')
    assert contracts.load_bybit_source_request_timeout_seconds_or_raise() == 15


def test_resolve_okx_daily_file_url_uses_okx_timeout_env(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    class _FakeResponse:
        status_code = 200
        headers: ClassVar[dict[str, str]] = {}

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {
                'code': '0',
                'data': {
                    'details': [
                        {
                            'groupDetails': [
                                {
                                    'filename': 'BTC-USDT-trades-2024-01-01.zip',
                                    'url': 'https://example.test/okx.zip',
                                }
                            ]
                        }
                    ]
                },
            }

    class _FakeSession:
        def post(self, url: str, *, json: dict[str, Any], timeout: int) -> _FakeResponse:
            captured['url'] = url
            captured['json'] = json
            captured['timeout'] = timeout
            return _FakeResponse()

    monkeypatch.setenv('ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS', '17')

    filename, url = contracts.resolve_okx_daily_file_url_or_raise(
        date_str='2024-01-01',
        session=_FakeSession(),
    )

    assert filename == 'BTC-USDT-trades-2024-01-01.zip'
    assert url == 'https://example.test/okx.zip'
    assert captured['timeout'] == 17
