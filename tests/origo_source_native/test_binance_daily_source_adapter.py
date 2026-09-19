from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters import binance_spot_rest as rest

from .acceptance_cases import SPOT_CASE, assert_archive_rest_equal

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/binance/spot'
ARCHIVES = FIXTURES / 'daily/trades/revisioned'
REST = FIXTURES / 'rest/trades'


def archive_response(url: str) -> daily.Response:
    return daily.Response((ARCHIVES / url.rsplit('/', 1)[-1]).read_bytes(), {}, 200)


def test_real_spot_archive_obeys_binance_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    adapter = daily.BinanceSpotDaily()
    partition = adapter.partition('2017-08-17')
    revision = adapter.fetch(partition)
    assert revision.row_count == 3427
    assert revision.key == 'a23fe0dae54b7143d5cffb42184e1a05afe7124aaca641cd438297ce83c9f9ec'
    assert next(iter(revision.rows()))[0] == 0
    adapter.revalidate(partition, revision)
    for day in ('2024-12-31', '2025-01-01'):
        body = (ARCHIVES / f'BTCUSDT-trades-{day}.csv').read_bytes()
        provenance = json.loads((ARCHIVES / f'BTCUSDT-trades-{day}.provenance.json').read_text())
        assert hashlib.sha256(body).hexdigest() == provenance['selected_sha256']
        rows = tuple(daily.spot_csv_rows(body, adapter.partition(day)))
        assert len(rows) == 12000
        assert all(len(str(row[4])) == (13 if day.startswith('2024') else 16) for row in rows)
        assert all(isinstance(row[-1], datetime) and row[-1].tzinfo == UTC for row in rows)
        with pytest.raises(ValueError, match='unique ordered'):
            tuple(
                daily.spot_csv_rows(
                    body + body.splitlines(keepends=True)[0], adapter.partition(day)
                )
            )
        with pytest.raises(ValueError, match='unsigned integers'):
            tuple(
                daily.spot_csv_rows(
                    b'trade_id,price,quantity,quote_quantity,timestamp,maker,best\n' + body,
                    adapter.partition(day),
                )
            )
    original = archive_response

    def corrupt(url: str) -> daily.Response:
        result = original(url)
        return (
            result
            if url.endswith('CHECKSUM')
            else daily.Response(result.body + b'changed', {}, 200)
        )

    monkeypatch.setattr(daily, 'get_response', corrupt)
    with pytest.raises(RuntimeError, match='checksum mismatch'):
        adapter.fetch(partition)


def test_real_spot_closed_minutes_obey_binance_provisional_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provenance = json.loads((REST / 'provenance.json').read_text())
    remaining = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> daily.Response:
        expected = remaining.pop(0)
        assert url == expected['url']
        assert params == expected['params']
        assert headers == {}
        assert weight == (4 if url.endswith('/aggTrades') else 25)
        body = (REST / expected['file']).read_bytes()
        assert hashlib.sha256(body).hexdigest() == expected['sha256']
        return daily.Response(body, expected['response_headers'], expected['status'])

    monkeypatch.delenv('BINANCE_SPOT_REST_BASE_URL', raising=False)
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinanceSpotProvisional()
    key = datetime.fromisoformat(provenance['minute_start']).strftime('%Y-%m-%dT%H:%M:%SZ')
    partition = adapter.partition(key)
    now = partition.end + timedelta(minutes=10, seconds=30)
    covered = (partition, adapter.partition('2025-01-01T00:02:00Z'))
    candidates = adapter.candidates(now, partition.start, covered)
    assert [item.key for item in candidates] == [
        '2025-01-01T00:10:00Z',
        '2025-01-01T00:01:00Z',
        '2025-01-01T00:03:00Z',
        '2025-01-01T00:04:00Z',
        '2025-01-01T00:05:00Z',
        '2025-01-01T00:06:00Z',
    ]
    canonical = daily.BinanceSpotDaily().partition('2025-01-01')
    assert adapter.candidates(now, partition.start, (canonical,)) == ()
    assert adapter.candidates(partition.start, partition.start, ()) == ()
    late = now + timedelta(days=3)
    assert all(
        item.start >= late.replace(second=0) - timedelta(hours=36, minutes=1)
        for item in adapter.candidates(late, partition.start, ())
    )
    assert daily.BinanceSpotDaily().candidate(now).key == '2024-12-31'
    revision = adapter.fetch(partition)
    rows = tuple(revision.rows())
    assert not remaining
    assert len(rows) > 1000
    assert len({row[0] for row in rows}) == len(rows)
    assert all(partition.start <= row[-1] < partition.end for row in rows)
    archive_rows = tuple(
        daily.spot_csv_rows(
            (ARCHIVES / 'BTCUSDT-trades-2025-01-01.csv').read_bytes(),
            daily.BinanceSpotDaily().partition('2025-01-01'),
        )
    )
    assert_archive_rest_equal(
        tuple(row for row in archive_rows if partition.start <= row[-1] < partition.end),
        rows,
        case=SPOT_CASE,
    )


def test_real_empty_minute_requires_two_ticks_and_later_boundary_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provenance = json.loads((REST / 'empty-provenance.json').read_text())
    expected = list(provenance['requests'])
    now = datetime.fromisoformat(expected[0]['captured_at'])

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> daily.Response:
        item = expected.pop(0)
        assert url == item['url'] and params == item['params']
        body = (REST / item['file']).read_bytes()
        assert hashlib.sha256(body).hexdigest() == item['sha256']
        return daily.Response(body, {}, item['status'])

    monkeypatch.delenv('BINANCE_SPOT_REST_BASE_URL', raising=False)
    monkeypatch.setattr(rest, 'get_response', captured)
    monkeypatch.setattr(rest, 'now_utc', lambda: now)
    adapter = rest.BinanceSpotProvisional()
    partition = adapter.partition('2017-08-17T00:00:00Z')
    first = adapter.fetch(partition)
    assert not first.complete and first.row_count == 0
    now = datetime.fromisoformat(expected[0]['captured_at'])
    second = adapter.fetch(partition, first.evidence_json)
    assert second.complete and second.row_count == 0
    assert not expected and tuple(second.rows()) == ()


def test_binance_rate_budget_survives_workers_and_respects_retry_headers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    clock = [1000.0]
    responses: list[requests.Response] = []
    calls: list[str] = []
    for status, headers in (
        (200, {}),
        (429, {'Retry-After': '3'}),
        (418, {'Retry-After': '5'}),
        (200, {}),
    ):
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers)
        response._content = b''
        responses.append(response)

    def request(url: str, params: object, headers: object) -> requests.Response:
        calls.append(url)
        return responses.pop(0)

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    monkeypatch.setattr(daily.time, 'time', lambda: clock[0])
    monkeypatch.setattr(daily.time, 'sleep', sleep)
    monkeypatch.setattr(daily, '_request', request)
    daily.get_response('https://api.binance.com/api/v3/historicalTrades', weight=60)
    assert clock[0] == 1000.0
    state = tmp_path / 'binance_rest_budget.api_binance_com.state'
    assert state.read_text() == '1001.000000 0.000000'
    # Another host paces from its own budget: no sleep despite the hot api budget.
    with pytest.raises(RuntimeError, match='HTTP 429'):
        daily.get_response('https://fapi.binance.com/fapi/v1/aggTrades', weight=4)
    assert clock[0] == 1000.0
    with pytest.raises(RuntimeError, match='HTTP 418'):
        daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=4)
    assert clock[0] == 1001.0
    # The api 418 circuits only api: fapi waits out its own Retry-After and proceeds.
    daily.get_response('https://fapi.binance.com/fapi/v1/aggTrades', weight=4)
    assert clock[0] == 1003.0
    assert len(calls) == 4


def test_weighted_requests_touch_the_worker_heartbeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import requests

    response = requests.Response()
    response.status_code = 200
    response._content = b'[]'
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    monkeypatch.setattr(daily, '_request', lambda url, params, headers: response)
    beat = tmp_path / 'worker.heartbeat'
    monkeypatch.setenv('ORIGO_WORKER_HEARTBEAT', str(beat))
    daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=20)
    assert float(beat.read_text()) > 0
    # Dagster runs never set the variable: no heartbeat is written.
    monkeypatch.delenv('ORIGO_WORKER_HEARTBEAT')
    beat.unlink()
    daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=20)
    assert not beat.exists()


def _canned_response(
    *, status: int = 200, used: str = '0', retry: str | None = None
) -> object:
    import requests

    response = requests.Response()
    response.status_code = status
    response._content = b'[]'
    response.headers['X-MBX-USED-WEIGHT-1M'] = used
    if retry is not None:
        response.headers['Retry-After'] = retry
    return response


def _paced_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> list[float]:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    monkeypatch.delenv('ORIGO_WORKER_HEARTBEAT', raising=False)
    sleeps: list[float] = []
    monkeypatch.setattr(daily.time, 'sleep', sleeps.append)
    return sleeps


def _budget_state(tmp_path: Path, host: str) -> tuple[float, float]:
    text = (tmp_path / f'binance_rest_budget.{host}.state').read_text().strip()
    first, second = text.split()
    return float(first), float(second)


def test_weighted_budgets_are_independent_per_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _paced_setup(tmp_path, monkeypatch)
    monkeypatch.setattr(
        daily, '_request', lambda url, params, headers: _canned_response()
    )
    daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=600)
    daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=600)
    assert sleeps == [0.0, 0.0]
    assert (tmp_path / 'binance_rest_budget.fapi_binance_com.state').exists()
    assert (tmp_path / 'binance_rest_budget.api_binance_com.state').exists()
    daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=600)
    assert sleeps[2] == pytest.approx(25.0, abs=1.0)


def test_spot_aliases_share_one_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _paced_setup(tmp_path, monkeypatch)
    monkeypatch.setattr(
        daily, '_request', lambda url, params, headers: _canned_response()
    )
    daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=600)
    assert not (tmp_path / 'binance_rest_budget.api1_binance_com.state').exists()
    daily.get_response('https://api1.binance.com/api/v3/aggTrades', weight=600)
    assert sleeps == [0.0, pytest.approx(10.0, abs=1.0)]


def test_weighted_rate_and_used_weight_backstop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import time as time_module

    sleeps = _paced_setup(tmp_path, monkeypatch)
    monkeypatch.setattr(
        daily, '_request', lambda url, params, headers: _canned_response()
    )
    before = time_module.time()
    daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=600)
    anchor, _ = _budget_state(tmp_path, 'fapi_binance_com')
    assert anchor - before == pytest.approx(25.0, abs=1.0)
    monkeypatch.setattr(
        daily, '_request', lambda url, params, headers: _canned_response(used='2000')
    )
    during = time_module.time()
    daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=600)
    held, _ = _budget_state(tmp_path, 'fapi_binance_com')
    assert held - during >= 60.0
    daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=600)
    assert sleeps[-1] >= 55.0
    # The same used weight is below the spot backstop: no hold on api.
    monkeypatch.setattr(
        daily, '_request', lambda url, params, headers: _canned_response(used='2000')
    )
    calm = time_module.time()
    daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=600)
    api_held, _ = _budget_state(tmp_path, 'api_binance_com')
    assert api_held - calm == pytest.approx(10.0, abs=1.0)


def test_rate_circuit_opens_only_for_the_limited_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.contracts import SourceError

    _paced_setup(tmp_path, monkeypatch)
    calls: list[str] = []

    def limited(url: str, params: object, headers: object) -> object:
        calls.append(url)
        return _canned_response(status=418, retry='30')

    monkeypatch.setattr(daily, '_request', limited)
    with pytest.raises(SourceError) as banned:
        daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=200)
    assert banned.value.code == 'PROVIDER_HTTP_418'
    with pytest.raises(SourceError) as circuited:
        daily.get_response('https://fapi.binance.com/fapi/v1/historicalTrades', weight=200)
    assert circuited.value.code == 'PROVIDER_RATE_CIRCUIT'
    assert len(calls) == 1
    monkeypatch.setattr(
        daily, '_request', lambda url, params, headers: _canned_response()
    )
    daily.get_response('https://api.binance.com/api/v3/aggTrades', weight=4)
