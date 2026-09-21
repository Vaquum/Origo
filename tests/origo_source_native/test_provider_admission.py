"""Request-admission races; clock and HTTP status are controlled, not market data."""

from collections.abc import Callable
from pathlib import Path

import pytest
import requests

from origo.sources.adapters import binance_daily as daily
from origo.sources.contracts import SourceError

URL = 'https://fapi.binance.com/fapi/v1/trades'


class Clock:
    def __init__(self) -> None:
        self.instant = 1000.0
        self.on_sleep: Callable[[], None] | None = None

    def time(self) -> float:
        return self.instant

    def monotonic(self) -> float:
        return self.instant

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        callback, self.on_sleep = self.on_sleep, None
        if callback is not None:
            callback()
        self.instant += seconds


def response(status: int, **headers: str) -> requests.Response:
    value = requests.Response()
    value.status_code = status
    value.headers.update(headers)
    value._content = b'[]'
    return value


@pytest.mark.parametrize('lane', ['shared', 'live'])
@pytest.mark.parametrize('verdict', ['retry_after', 'backstop', 'ban'])
def test_waiting_request_rechecks_later_provider_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lane: str,
    verdict: str,
) -> None:
    clock = Clock()
    monkeypatch.setattr(daily, 'time', clock)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    budget = daily._Budget(tmp_path, URL)
    with budget:
        assert budget.reserve(5, lane) is None
    calls: list[float] = []

    def transport(*args: object, **kwargs: object) -> requests.Response:
        calls.append(clock.instant)
        return response(200)

    monkeypatch.setattr(daily, '_request', transport)

    def impose_hold() -> None:
        if verdict == 'backstop':
            limited = response(200, **{'X-MBX-USED-WEIGHT-1M': '1920'})
        else:
            limited = response(418 if verdict == 'ban' else 429, **{'Retry-After': '30'})
        with budget:
            budget.settle(limited)

    clock.on_sleep = impose_hold
    if verdict == 'ban':
        with pytest.raises(SourceError, match='circuit is open'):
            daily.get_response(
                URL, params={'symbol': 'BTCUSDT', 'limit': 1000}, weight=5, lane=lane
            )
        assert calls == []
    else:
        daily.get_response(URL, params={'symbol': 'BTCUSDT', 'limit': 1000}, weight=5, lane=lane)
        assert len(calls) == 1
        assert calls[0] >= 1000 + (60 if verdict == 'backstop' else 30)


def test_pending_admission_does_not_spend_or_extend_future_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    monkeypatch.setattr(daily, 'time', clock)
    budget = daily._Budget(tmp_path, URL)
    with budget:
        assert budget.reserve(200, 'shared') is None
        first = budget.reserve(200, 'shared')
        assert first is not None
        assert budget.reserve(200, 'shared') == first
        assert budget.reserve(200, 'shared') == first
        clock.instant = first + 0.01
        assert budget.reserve(200, 'shared') is None
        following = budget.reserve(200, 'shared')
        assert following is not None
        assert following > clock.instant


def test_malformed_retry_after_blocks_further_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    monkeypatch.setattr(daily, 'time', clock)
    budget = daily._Budget(tmp_path, URL)
    with budget:
        with pytest.raises(SourceError, match='admission blocked'):
            budget.settle(response(429, **{'Retry-After': 'not-a-duration'}))
        with pytest.raises(SourceError, match='circuit is open'):
            budget.reserve(5, 'live')
        clock.instant += 60
        assert budget.reserve(5, 'live') is None
