from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from origo.sources.adapters import binance_spot_agg_columnar as columnar
from origo.sources.adapters import binance_spot_agg_daily as daily
from origo.sources.adapters import binance_spot_agg_rest as rest
from origo.sources.adapters.binance_daily import Response

from .acceptance_cases import SPOT_AGG_CASE, assert_archive_rest_equal

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/binance/spot'
ARCHIVES = FIXTURES / 'daily/aggtrades/BTCUSDT'
REST = FIXTURES / 'rest/aggtrades'


def archive_response(url: str) -> Response:
    return Response((ARCHIVES / url.rsplit('/', 1)[-1]).read_bytes(), {}, 200)


def _day_csv(day: str) -> bytes:
    with zipfile.ZipFile(ARCHIVES / f'BTCUSDT-aggTrades-{day}.zip') as archive:
        return archive.read(f'BTCUSDT-aggTrades-{day}.csv')


def test_real_spot_agg_archive_obeys_binance_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    adapter = daily.BinanceSpotAggDaily()
    partition = adapter.partition('2017-08-17')
    revision = adapter.fetch(partition)
    assert revision.row_count == 3089
    assert revision.key == '4790d51a88d6a73b492c24f08acc2f04d4112e20cbad823cd6b648cc85c18dd7'
    assert next(iter(revision.rows()))[0] == 0
    adapter.revalidate(partition, revision)
    rows = tuple(daily.agg_csv_rows(_day_csv('2017-08-17'), partition))
    table = columnar.agg_table(_day_csv('2017-08-17'), partition)
    assert len(rows) == table.num_rows == 3089
    assert rows[0][0] == rows[-1][0] - 3088 == 0
    assert table.column('agg_trade_id').to_pylist() == [row[0] for row in rows]
    provenance = json.loads(
        (ARCHIVES / 'BTCUSDT-aggTrades-2017-08-17.provenance.json').read_text()
    )
    assert provenance['official_row_count'] == 3089
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


def test_spot_agg_archives_are_headerless_eight_column_rows() -> None:
    adapter = daily.BinanceSpotAggDaily()
    assert daily.BinanceSpotAggDaily.HEADER is None
    assert columnar.HEADER is None
    body = _day_csv('2024-04-20')
    assert body.splitlines()[0].decode().split(',')[0] == '2980094258'
    prefix = b'\n'.join(body.splitlines()[:5001]) + b'\n'
    rows = tuple(daily.agg_csv_rows(prefix, adapter.partition('2024-04-20')))
    assert len(rows) == 5001
    assert rows[0][0] == 2980094258
    table = columnar.agg_table(body, adapter.partition('2024-04-20'))
    assert table.num_rows == 1066539
    assert table.column('agg_trade_id').to_pylist()[0] == 2980094258
    with pytest.raises(ValueError, match='exactly eight fields'):
        tuple(
            daily.agg_csv_rows(
                prefix + b'1,2,3,4,5,6,True\n', adapter.partition('2024-04-20')
            )
        )
    with pytest.raises(ValueError, match='unsigned integers'):
        tuple(
            daily.agg_csv_rows(
                b'-1,2,3,4,5,1713571200005,True,True\n', adapter.partition('2024-04-20')
            )
        )


def test_spot_agg_field_parsers_reject_bad_input() -> None:
    adapter = daily.BinanceSpotAggDaily()
    partition = adapter.partition('2017-08-17')
    assert daily.timestamp_datetime(1502942428322) == datetime(
        2017, 8, 17, 4, 0, 28, 322000, tzinfo=UTC
    )
    assert daily.timestamp_datetime(1502942428322000) == datetime(
        2017, 8, 17, 4, 0, 28, 322000, tzinfo=UTC
    )
    with pytest.raises(ValueError, match='milliseconds or microseconds'):
        daily.timestamp_datetime(1502942428)
    with pytest.raises(ValueError, match='must be positive'):
        daily.parse_decimal('0')
    with pytest.raises(ValueError, match='Invalid Binance decimal'):
        daily.parse_decimal('abc')
    body = _day_csv('2017-08-17')
    with pytest.raises(ValueError, match='unique ordered'):
        tuple(
            daily.agg_csv_rows(
                body + body.splitlines(keepends=True)[0], partition
            )
        )
    with pytest.raises(ValueError, match='outside its partition'):
        tuple(
            daily.agg_csv_rows(
                body + b'9999999999,10000.0,0.001,9999999998,9999999999,1503014400000,True,True\n',
                partition,
            )
        )
    with pytest.raises(ValueError, match='precedes the declared first day'):
        adapter.partition('2017-08-16')
    assert adapter.candidate(datetime(2017, 8, 19, 12, tzinfo=UTC)).key == '2017-08-18'


def _rest_responses(name: str) -> tuple[dict, dict[str, bytes]]:
    provenance = json.loads((REST / name).read_text())
    bodies = {}
    for request in provenance['requests']:
        body = (REST / request['file']).read_bytes()
        assert hashlib.sha256(body).hexdigest() == request['sha256']
        bodies[request['file']] = body
    return provenance, bodies


def test_real_spot_agg_closed_minutes_obey_binance_provisional_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('BINANCE_API_KEY', raising=False)
    provenance, bodies = _rest_responses('provenance.json')
    assert provenance['minute_start'] == '2026-09-16T02:30:00+00:00'
    calls = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, object], headers: dict[str, str], weight: int
    ) -> Response:
        expected = calls.pop(0)
        assert url == expected['url'] and params == expected['params']
        assert headers == {}
        assert weight == 4
        return Response(bodies[expected['file']], {}, expected['status'])

    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinanceSpotAggProvisional()
    now = datetime(2026, 9, 16, 4, 0, tzinfo=UTC)
    covered = (
        adapter.partition('2026-09-16T02:31:00Z'),
        adapter.partition('2026-09-16T02:32:00Z'),
    )
    current, *missing = adapter.candidates(now, datetime(2026, 9, 16, 2, 30, tzinfo=UTC), covered)
    assert current.key == '2026-09-16T03:59:00Z'
    assert [part.key for part in missing] == [
        '2026-09-16T02:30:00Z',
        '2026-09-16T02:33:00Z',
        '2026-09-16T02:34:00Z',
        '2026-09-16T02:35:00Z',
        '2026-09-16T02:36:00Z',
    ]
    minute = adapter.partition('2026-09-16T02:30:00Z')
    revision = adapter.fetch(minute)
    rows = tuple(revision.rows())
    assert calls == [] and len(rows) > 1000
    assert rows == tuple(sorted(rows, key=lambda row: row[0]))
    assert all(isinstance(row[1], Decimal) for row in rows)
    assert all(minute.start <= row[-1] < minute.end for row in rows)
    archive_rows = tuple(
        daily.agg_csv_rows(
            (ARCHIVES / 'minute-2026-09-16T02-30.csv').read_bytes(),
            daily.BinanceSpotAggDaily().partition('2026-09-16'),
        )
    )
    assert len(rows) == 2439
    assert_archive_rest_equal(archive_rows, rows, case=SPOT_AGG_CASE)


def test_spot_agg_empty_minute_requires_two_ticks_and_later_boundary_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('BINANCE_API_KEY', raising=False)
    provenance, bodies = _rest_responses('empty-provenance.json')
    calls = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, object], headers: dict[str, str], weight: int
    ) -> Response:
        expected = calls.pop(0)
        assert url == expected['url'] and params == expected['params']
        assert headers == {}
        assert weight == 4
        return Response(bodies[expected['file']], {}, expected['status'])

    monkeypatch.setattr(rest, 'get_response', captured)
    now = datetime.fromisoformat(provenance['requests'][0]['captured_at'])
    monkeypatch.setattr(rest, 'now_utc', lambda: now)
    adapter = rest.BinanceSpotAggProvisional()
    partition = adapter.partition('2017-08-17T00:00:00Z')
    first = adapter.fetch(partition)
    assert first.row_count == 0 and not first.complete
    assert calls != []
    now = datetime.fromisoformat(calls[0]['captured_at'])
    monkeypatch.setattr(rest, 'now_utc', lambda: now)
    second = adapter.fetch(partition, first.evidence_json)
    assert second.row_count == 0 and second.complete and calls == []
    assert tuple(second.rows()) == ()


_QUIRK_FIRST = b'2980094258,64000.10,0.00100,5000000000,5000000000,1713571200005,True,True\n'
_QUIRK_SENTINEL = b'2980094259,0,0,-1,-1,1713571199005,False,True\n'
_QUIRK_SECOND = b'2980094260,64001.00,0.00200,5000000001,5000000001,1713571201005,True,True\n'


def test_spot_agg_cleaning_drops_sentinels_and_exact_duplicates() -> None:
    adapter = daily.BinanceSpotAggDaily()
    partition = adapter.partition('2024-04-20')
    body = _QUIRK_FIRST + _QUIRK_SENTINEL + _QUIRK_SECOND + _QUIRK_FIRST
    cleaned, dropped = adapter.clean_rows(body, partition)
    assert dropped == {'sentinel_rows': 1, 'duplicate_rows': 1}
    assert cleaned == _QUIRK_FIRST + _QUIRK_SECOND
    rows = tuple(daily.agg_csv_rows(cleaned, partition))
    table = columnar.agg_table(cleaned, partition)
    assert len(rows) == table.num_rows == 2
    assert [row[0] for row in rows] == [2980094258, 2980094260]


def test_spot_agg_cleaning_rejects_reused_ids_with_different_rows() -> None:
    adapter = daily.BinanceSpotAggDaily()
    partition = adapter.partition('2024-04-20')
    altered = _QUIRK_FIRST.replace(b'64000.10', b'64000.20')
    with pytest.raises(ValueError, match='repeats with different content'):
        adapter.clean_rows(_QUIRK_FIRST + _QUIRK_SECOND + altered, partition)


def test_spot_agg_cleaning_passes_clean_archives_through_untouched() -> None:
    adapter = daily.BinanceSpotAggDaily()
    partition = adapter.partition('2017-08-17')
    body = _day_csv('2017-08-17')
    cleaned, dropped = adapter.clean_rows(body, partition)
    assert dropped == {} and cleaned is body


def test_spot_agg_fetch_records_dropped_rows_in_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = daily.BinanceSpotAggDaily()
    partition = adapter.partition('2024-04-20')
    csv_body = _QUIRK_FIRST + _QUIRK_SENTINEL + _QUIRK_SECOND + _QUIRK_FIRST
    member = 'BTCUSDT-aggTrades-2024-04-20'
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr(member + '.csv', csv_body)
    zip_body = buffer.getvalue()
    digest = hashlib.sha256(zip_body).hexdigest()

    def served(url: str) -> Response:
        if url.endswith('CHECKSUM'):
            return Response(f'{digest}  {member}.zip\n'.encode(), {}, 200)
        return Response(zip_body, {}, 200)

    monkeypatch.setattr(daily, 'get_response', served)
    revision = adapter.fetch(partition)
    assert revision.key == digest and revision.row_count == 2
    evidence = json.loads(revision.evidence_json)
    assert evidence['dropped_rows'] == {'sentinel_rows': 1, 'duplicate_rows': 1}
    assert evidence['csv_sha256'] == hashlib.sha256(csv_body).hexdigest()
