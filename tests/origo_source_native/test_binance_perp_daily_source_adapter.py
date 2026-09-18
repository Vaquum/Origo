from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from origo.sources.adapters import binance_perp_columnar as columnar
from origo.sources.adapters import binance_perp_daily as daily
from origo.sources.adapters.binance_daily import Response

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
