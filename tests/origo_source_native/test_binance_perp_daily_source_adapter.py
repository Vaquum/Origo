from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from origo.sources.adapters import binance_perp_columnar as columnar
from origo.sources.adapters import binance_perp_daily as daily
from origo.sources.adapters import binance_perp_rest as rest
from origo.sources.adapters.binance_daily import Response
from origo.sources.contracts import SourceError

from .acceptance_cases import PERP_CASE, assert_archive_rest_equal

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures'
ARCHIVES = FIXTURES / 'daily/trades/BTCUSDT'
REST = FIXTURES / 'rest/trades'


def archive_response(url: str) -> Response:
    return Response((ARCHIVES / url.rsplit('/', 1)[-1]).read_bytes(), {}, 200)


def _day_csv(day: str) -> bytes:
    with zipfile.ZipFile(ARCHIVES / f'BTCUSDT-trades-{day}.zip') as archive:
        return archive.read(f'BTCUSDT-trades-{day}.csv')


def test_real_perp_archive_obeys_binance_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    adapter = daily.BinancePerpDaily()
    partition = adapter.partition('2019-09-08')
    revision = adapter.fetch(partition)
    assert revision.row_count == 3754
    assert revision.key == '22a2a3b8b90deab1376a0d2112af9bdce1ccaeaa0db4cbd149d61dd26e311074'
    assert next(iter(revision.rows()))[0] == 1
    adapter.revalidate(partition, revision)
    rows = tuple(daily.perp_csv_rows(_day_csv('2019-09-08'), partition))
    table = columnar.perp_table(_day_csv('2019-09-08'), partition)
    assert len(rows) == table.num_rows == 3754
    assert rows[0][0] == rows[-1][0] - 3753 == 1
    assert table.column('trade_id').to_pylist() == [row[0] for row in rows]
    provenance = json.loads(
        (ARCHIVES / 'BTCUSDT-trades-2019-09-08.provenance.json').read_text()
    )
    assert provenance['official_row_count'] == 3754
    assert provenance['zip_sha256'] == revision.key
    original = archive_response

    def corrupt(url: str) -> Response:
        result = original(url)
        return (
            result
            if url.endswith('CHECKSUM')
            else Response(result.body + b'changed', {}, 200)
        )

    monkeypatch.setattr(daily, 'get_response', corrupt)
    with pytest.raises(RuntimeError, match='checksum mismatch'):
        adapter.fetch(partition)


def test_perp_archives_accept_an_official_header_or_none() -> None:
    adapter = daily.BinancePerpDaily()
    headered = _day_csv('2024-04-20')
    assert headered.splitlines()[0].decode() == columnar.HEADER
    prefix = b'\n'.join(headered.splitlines()[:5001]) + b'\n'
    rows = tuple(daily.perp_csv_rows(prefix, adapter.partition('2024-04-20')))
    assert len(rows) == 5000
    assert rows[0][0] == 4915991486
    table = columnar.perp_table(headered, adapter.partition('2024-04-20'))
    assert table.num_rows == 3203773
    assert table.column('trade_id').to_pylist()[0] == 4915991486
    with pytest.raises(ValueError, match='exactly six fields'):
        tuple(
            daily.perp_csv_rows(
                prefix + b'1,2,3,4,5,True,True\n', adapter.partition('2024-04-20')
            )
        )
    with pytest.raises(ValueError, match='unsigned integers'):
        tuple(
            daily.perp_csv_rows(
                b'\n'.join(headered.splitlines()[1:2000] + headered.splitlines()[:1]) + b'\n',
                adapter.partition('2024-04-20'),
            )
        )


def test_perp_field_parsers_reject_bad_input() -> None:
    adapter = daily.BinancePerpDaily()
    partition = adapter.partition('2019-09-08')
    assert daily.timestamp_datetime(1567965470575) == datetime(
        2019, 9, 8, 17, 57, 50, 575000, tzinfo=UTC
    )
    assert daily.timestamp_datetime(1567965470575000) == datetime(
        2019, 9, 8, 17, 57, 50, 575000, tzinfo=UTC
    )
    with pytest.raises(ValueError, match='milliseconds or microseconds'):
        daily.timestamp_datetime(1567965470)
    with pytest.raises(ValueError, match='must be positive'):
        daily.parse_decimal('0')
    with pytest.raises(ValueError, match='Invalid Binance decimal'):
        daily.parse_decimal('abc')
    body = _day_csv('2019-09-08')
    with pytest.raises(ValueError, match='unique ordered'):
        tuple(
            daily.perp_csv_rows(
                body + body.splitlines(keepends=True)[0], partition
            )
        )
    with pytest.raises(ValueError, match='outside its partition'):
        tuple(
            daily.perp_csv_rows(
                body + b'9999999999,10000.0,0.001,10.0,1567987200000,true\n', partition
            )
        )
    with pytest.raises(ValueError, match='precedes the declared first day'):
        adapter.partition('2019-09-07')
    assert adapter.candidate(datetime(2019, 9, 10, 12, tzinfo=UTC)).key == '2019-09-09'


def _rest_responses(name: str) -> tuple[dict, dict[str, bytes]]:
    provenance = json.loads((REST / name).read_text())
    bodies = {}
    for request in provenance['requests']:
        body = (REST / request['file']).read_bytes()
        assert hashlib.sha256(body).hexdigest() == request['sha256']
        bodies[request['file']] = body
    return provenance, bodies


def test_real_perp_closed_minutes_obey_binance_provisional_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    provenance, bodies = _rest_responses('provenance.json')
    assert provenance['minute_start'] == '2026-09-16T20:00:00+00:00'
    calls = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, object], headers: dict[str, str], weight: int
    ) -> Response:
        expected = calls.pop(0)
        assert url == expected['url'] and params == expected['params']
        assert headers == {'X-MBX-APIKEY': '0' * 64}
        assert weight == (20 if url.endswith('aggTrades') else 200)
        return Response(bodies[expected['file']], {}, expected['status'])

    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinancePerpProvisional()
    now = datetime(2026, 9, 16, 21, 30, tzinfo=UTC)
    covered = (
        adapter.partition('2026-09-16T20:01:00Z'),
        adapter.partition('2026-09-16T20:02:00Z'),
    )
    current, *missing = adapter.candidates(now, datetime(2026, 9, 16, 20, tzinfo=UTC), covered)
    assert current.key == '2026-09-16T21:29:00Z'
    assert [part.key for part in missing] == [
        '2026-09-16T20:00:00Z',
        '2026-09-16T20:03:00Z',
        '2026-09-16T20:04:00Z',
        '2026-09-16T20:05:00Z',
        '2026-09-16T20:06:00Z',
    ]
    minute = adapter.partition('2026-09-16T20:00:00Z')
    revision = adapter.fetch(minute)
    rows = tuple(revision.rows())
    assert calls == [] and len(rows) > 1000
    assert rows == tuple(sorted(rows, key=lambda row: row[0]))
    assert all(isinstance(row[1], Decimal) for row in rows)
    assert all(minute.start <= row[-1] < minute.end for row in rows)
    archive_rows = tuple(
        daily.perp_csv_rows(
            (ARCHIVES / 'minute-2026-09-16T20-00.csv').read_bytes(),
            daily.BinancePerpDaily().partition('2026-09-16'),
        )
    )
    assert len(rows) == 2325
    assert_archive_rest_equal(archive_rows, rows, case=PERP_CASE)


def test_real_empty_minute_requires_two_ticks_and_later_boundary_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    provenance, bodies = _rest_responses('empty-provenance.json')
    calls = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, object], headers: dict[str, str], weight: int
    ) -> Response:
        expected = calls.pop(0)
        assert url == expected['url'] and params == expected['params']
        assert headers == {'X-MBX-APIKEY': '0' * 64}
        assert weight == 20
        return Response(bodies[expected['file']], {}, expected['status'])

    monkeypatch.setattr(rest, 'get_response', captured)
    now = datetime.fromisoformat(provenance['requests'][0]['captured_at'])
    monkeypatch.setattr(rest, 'now_utc', lambda: now)
    adapter = rest.BinancePerpProvisional()
    partition = adapter.partition('2019-09-08T00:00:00Z')
    first = adapter.fetch(partition)
    assert first.row_count == 0 and not first.complete
    assert calls != []
    now = datetime.fromisoformat(calls[0]['captured_at'])
    monkeypatch.setattr(rest, 'now_utc', lambda: now)
    second = adapter.fetch(partition, first.evidence_json)
    assert second.row_count == 0 and second.complete and calls == []
    assert tuple(second.rows()) == ()


def test_perp_provisional_fetch_requires_an_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('BINANCE_API_KEY', raising=False)
    adapter = rest.BinancePerpProvisional()
    with pytest.raises(SourceError, match='BINANCE_API_KEY is required') as error:
        adapter.fetch(adapter.partition('2026-09-16T20:00:00Z'))
    assert error.value.code == 'PROVIDER_CREDENTIAL_MISSING'


def test_perp_paging_uses_thousand_trade_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 2325-trade minute completes in one locator plus four 1000-trade pages."""
    import json as json_module

    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    monkeypatch.setattr(rest, 'now_utc', lambda: datetime(2026, 9, 16, 21, 30, tzinfo=UTC))
    start_ms = 1789588800000
    end_ms = 1789588860000
    first_id = 100000
    backtrack: list[dict[str, Any]] = [
        {
            'id': trade_id,
            'price': '76043.60',
            'qty': '0.002',
            'quoteQty': '152.08',
            'time': start_ms - 1,
            'isBuyerMaker': False,
        }
        for trade_id in range(first_id - 1000, first_id)
    ]
    in_minute: list[dict[str, Any]] = [
        {
            'id': trade_id,
            'price': '76043.60',
            'qty': '0.002',
            'quoteQty': '152.08',
            'time': start_ms + (trade_id - first_id) * 25,
            'isBuyerMaker': False,
        }
        for trade_id in range(first_id, first_id + 2325)
    ]
    boundary: list[dict[str, Any]] = [
        {
            'id': first_id + 2325,
            'price': '76043.60',
            'qty': '0.002',
            'quoteQty': '152.08',
            'time': end_ms,
            'isBuyerMaker': False,
        }
    ]
    ledger = backtrack + in_minute + boundary
    seen: list[dict[str, object]] = []

    def captured(
        url: str, *, params: dict[str, object], headers: dict[str, str], weight: int
    ) -> Response:
        assert headers == {'X-MBX-APIKEY': '0' * 64}
        if url.endswith('aggTrades'):
            assert weight == 20
            body = json_module.dumps(
                [{'f': first_id, 'l': first_id, 'T': start_ms}]
            ).encode()
            return Response(body, {}, 200)
        assert weight == 200
        assert params['limit'] == 1000
        seen.append(dict(params))
        from_id = cast(int, params['fromId'])
        page = [row for row in ledger if from_id <= row['id'] < from_id + 1000]
        return Response(json_module.dumps(page).encode(), {}, 200)

    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinancePerpProvisional()
    revision = adapter.fetch(adapter.partition('2026-09-16T20:00:00Z'))
    assert [call['fromId'] for call in seen] == [99000, 100000, 101000, 102000]
    assert len(tuple(revision.rows())) == 2325
