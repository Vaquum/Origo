from __future__ import annotations

import gzip
import hashlib
import json
import tarfile
from pathlib import Path
from typing import NotRequired, TypedDict, cast

import pytest

from origo.law_catalog import build_catalog
from origo.sources.adapters import binance_perp_daily as daily
from origo.sources.adapters import binance_perp_rest as rest
from origo.sources.adapters.binance_daily import REST_HOST_BUDGETS, Response
from origo.sources.adapters.binance_perp_agg_rest import BinancePerpAggProvisional
from origo.sources.adapters.binance_provisional import BinanceProvisionalBase
from origo.sources.adapters.binance_spot_agg_rest import BinanceSpotAggProvisional
from origo.sources.adapters.binance_spot_rest import BinanceSpotProvisional
from origo.sources.contracts import Row
from origo.sources.hashing import content_hash

from .acceptance_cases import PERP_CASE, assert_archive_rest_equal
from .test_binance_perp_daily_source_adapter import ARCHIVES, _rest_responses


def test_real_fixed_start_reduces_weight_without_changing_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    provenance, bodies = _rest_responses('provenance.json')
    baseline = provenance['requests']
    pending = [baseline[0], *baseline[2:]]
    weights: list[int] = []

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> Response:
        request = pending.pop(0)
        assert url == request['url'] and params == request['params']
        assert headers == {'X-MBX-APIKEY': '0' * 64}
        weights.append(weight)
        return Response(bodies[request['file']], request['response_headers'], 200)

    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinancePerpProvisional()
    revision = adapter.fetch(adapter.partition('2026-09-16T20:00:00Z'))
    rows = tuple(revision.rows())
    archive = tuple(
        daily.perp_csv_rows(
            (ARCHIVES / 'minute-2026-09-16T20-00.csv').read_bytes(),
            daily.BinancePerpDaily().partition('2026-09-16'),
        )
    )
    assert not pending and revision.complete and revision.row_count == 2325
    assert_archive_rest_equal(archive, rows, case=PERP_CASE)
    assert revision.key == content_hash(archive, schema_version=1)
    assert sum(weights) == 1220 < 20 + (len(baseline) - 1) * 200 == 1420
    assert [row[0] for row in rows[:3]] == [8086999560, 8086999561, 8086999562]
    evidence = json.loads(revision.evidence_json)['requests']
    assert len(evidence) == 7
    assert evidence[1]['params']['fromId'] == 8086999063
    assert evidence[1]['used_weight_1m'] == 465
    assert all('egress_ip' not in request for request in evidence)


@pytest.mark.parametrize('fault', ['recover', 'no_boundary', 'page_cap', 'unordered', 'schema'])
def test_missing_boundary_recovers_once_within_page_budget(
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    """Fault injection withholds/reorders recorded rows; it is not a provider capture."""
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    provenance, bodies = _rest_responses('provenance.json')
    raw_pages = {
        request['params']['fromId']: bodies[request['file']]
        for request in provenance['requests'][1:]
    }
    in_minute = cast(list[dict[str, object]], json.loads(bodies['page-02.json']))
    starts: list[int] = []

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> Response:
        if url.endswith('aggTrades'):
            assert weight == 20
            return Response(bodies['locator.json'], {}, 200)
        assert weight == 200 and params['limit'] == 500
        start = cast(int, params['fromId'])
        starts.append(start)
        if len(starts) == 1:
            # Withholding all pre-minute rows makes the nearer start unproved.
            page = in_minute
            if fault == 'unordered':
                page = list(reversed(in_minute))
            elif fault == 'schema':
                page = [{key: value for key, value in in_minute[0].items() if key != 'qty'}]
            body = json.dumps(page).encode()
        elif fault == 'no_boundary' and len(starts) == 2:
            body = bodies['page-02.json']
        elif fault == 'page_cap' and len(starts) >= 4:
            # Truncate genuine pages to one row; no invented trade or timestamp.
            body = json.dumps([in_minute[len(starts) - 4]]).encode()
        else:
            body = raw_pages[start]
        return Response(body, {}, 200)

    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinancePerpProvisional()
    partition = adapter.partition('2026-09-16T20:00:00Z')
    if fault == 'page_cap':
        monkeypatch.setattr(rest.BinancePerpProvisional, 'PAGE_CAP', 100)
    if fault == 'recover':
        revision = adapter.fetch(partition)
        archive = tuple(
            daily.perp_csv_rows(
                (ARCHIVES / 'minute-2026-09-16T20-00.csv').read_bytes(),
                daily.BinancePerpDaily().partition('2026-09-16'),
            )
        )
        assert_archive_rest_equal(archive, tuple(revision.rows()), case=PERP_CASE)
        assert revision.key == content_hash(archive, schema_version=1)
        evidence = json.loads(revision.evidence_json)['requests']
        assert len(evidence) == 9 and 20 + len(starts) * 200 == 1620
    else:
        message = {
            'no_boundary': 'never reached pre-minute evidence',
            'page_cap': '100-page cap',
            'unordered': 'unordered',
            'schema': 'qty must be decimal text',
        }[fault]
        with pytest.raises((RuntimeError, ValueError), match=message):
            adapter.fetch(partition)
    if fault in ('unordered', 'schema'):
        assert starts == [8086999063]
    else:
        assert starts[:2] == [8086999063, 8086998563]
        assert starts.count(8086998563) == 1
    if fault == 'page_cap':
        assert len(starts) == 100


class _RecordedRequest(TypedDict):
    file: str
    url: str
    params: dict[str, str | int]
    status: int | None
    response_headers: dict[str, str]
    sha256: str
    origin: str
    captured_at: NotRequired[str]


_BUSY = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures/high_volume_trades'
_BUSY_KEY = '2026-09-21T08:38:00Z'


def _busy_responses() -> tuple[list[_RecordedRequest], dict[str, bytes]]:
    provenance = cast(dict[str, object], json.loads((_BUSY / 'provenance.json').read_text()))
    assert provenance['partition_key'] == _BUSY_KEY
    assert provenance['locator_origin'] == 'verified_aggregate_archive'
    requests = cast(list[_RecordedRequest], provenance['requests'])
    assert requests[0]['origin'] == 'archive_derived'
    assert requests[0]['status'] is None and requests[0]['response_headers'] == {}
    assert 'captured_at' not in requests[0]
    locator_provenance = cast(
        dict[str, object], json.loads((_BUSY / 'locator.provenance.json').read_text())
    )
    assert locator_provenance['origin'] == 'verified_aggregate_archive'
    assert str(locator_provenance['checksum_line']).split()[0] == locator_provenance['zip_sha256']
    source_row = str(locator_provenance['source_csv_row'])
    assert (
        hashlib.sha256(source_row.encode()).hexdigest()
        == locator_provenance['source_csv_row_sha256']
    )
    fields = source_row.strip().split(',')
    assert json.loads((_BUSY / requests[0]['file']).read_bytes()) == [
        {
            'a': int(fields[0]),
            'f': int(fields[3]),
            'l': int(fields[4]),
            'T': int(fields[5]),
        }
    ]
    bodies = {requests[0]['file']: (_BUSY / requests[0]['file']).read_bytes()}
    with tarfile.open(_BUSY / str(provenance['historical_payloads_file']), 'r:gz') as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        expected = [request['file'] for request in requests[1:]]
        assert len(names) == len(set(names)) == len(expected) == len(set(expected))
        assert set(names) == set(expected) and all(member.isfile() for member in members)
        for request in requests[1:]:
            stream = archive.extractfile(request['file'])
            assert stream is not None
            with stream:
                bodies[request['file']] = stream.read()
    for request in requests:
        assert hashlib.sha256(bodies[request['file']]).hexdigest() == request['sha256']
    for request in requests[1:]:
        assert request['origin'] == 'http_capture' and request['status'] == 200
        assert request['captured_at']
    return requests, bodies


def _busy_archive() -> tuple[Row, ...]:
    body = gzip.decompress((_BUSY / 'minute.csv.gz').read_bytes())
    provenance = cast(dict[str, object], json.loads((_BUSY / 'minute.provenance.json').read_text()))
    assert hashlib.sha256(body).hexdigest() == provenance['selected_sha256']
    assert str(provenance['checksum_line']).split()[0] == provenance['zip_sha256']
    assert str(provenance['source_url']).startswith('https://data.binance.vision/')
    rows = tuple(
        daily.perp_csv_rows(body, daily.BinancePerpDaily().partition(str(provenance['date'])))
    )
    assert len(rows) == provenance['selected_rows'] == 129751
    assert int(str(provenance['row_stop'])) - int(str(provenance['row_start'])) == len(rows)
    assert rows[0][0] == 8100178650 and rows[-1][0] == 8100308576
    return rows


def test_busy_perp_minute_exceeds_100_pages_and_matches_archive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    monkeypatch.delenv('ORIGO_BINANCE_PERP_EGRESS_IPS', raising=False)
    requests, bodies = _busy_responses()
    assert len(requests) == 262, 'The fixture must contain one locator and all 261 HTTP pages.'
    calls: list[_RecordedRequest] = []

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> Response:
        request = requests[len(calls)]
        assert url == request['url'] and params == request['params']
        assert headers == {'X-MBX-APIKEY': '0' * 64}
        assert weight == (20 if not calls else 200)
        calls.append(request)
        # The locator is archive-derived; only historical pages are HTTP captures.
        return Response(bodies[request['file']], request['response_headers'], 200)

    monkeypatch.setattr(rest, 'get_response', captured)
    adapter = rest.BinancePerpProvisional()
    revision = adapter.fetch(adapter.partition(_BUSY_KEY))
    archive = _busy_archive()
    assert len(calls) == len(requests) == 262  # One locator, 261 actual historical pages.
    assert 100 < len(calls) - 1 < adapter.PAGE_CAP == 512
    assert revision.complete and revision.row_count == len(archive)
    assert_archive_rest_equal(archive, tuple(revision.rows()), case=PERP_CASE)
    assert revision.key == content_hash(archive, schema_version=1)
    evidence = json.loads(revision.evidence_json)['requests']
    assert len(evidence) == len(calls)
    assert evidence[1]['params']['fromId'] == 8100178150
    assert all(request['params']['limit'] == 500 for request in calls[1:])


def test_perp_page_cap_fails_without_end_witness(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    monkeypatch.delenv('ORIGO_BINANCE_PERP_EGRESS_IPS', raising=False)
    requests, bodies = _busy_responses()
    calls: list[_RecordedRequest] = []

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> Response:
        request = requests[len(calls)]
        assert url == request['url'] and params == request['params']
        assert weight == (20 if not calls else 200)
        calls.append(request)
        return Response(bodies[request['file']], request['response_headers'], 200)

    monkeypatch.setattr(rest, 'get_response', captured)
    with monkeypatch.context() as limited:
        limited.setattr(rest.BinancePerpProvisional, 'PAGE_CAP', 150)
        adapter = rest.BinancePerpProvisional()
        with pytest.raises(RuntimeError, match='exceeded the 150-page cap'):
            adapter.fetch(adapter.partition(_BUSY_KEY))
    assert len(calls) == 151

    # Delivery fault: shorten genuine pages to distinct actual in-minute rows.
    # Preserve the entire first page, including its real pre-start witness.
    first = cast(list[dict[str, object]], json.loads(bodies[requests[1]['file']]))
    later = [
        row
        for request in requests[2:]
        for row in cast(list[dict[str, object]], json.loads(bodies[request['file']]))
        if 1789979880000 <= cast(int, row['time']) < 1789979940000
    ][:511]
    assert len(later) == len({cast(int, row['id']) for row in later}) == 511
    delivered = [first, *[[row] for row in later]]
    starts: list[int] = []
    weights: list[int] = []

    def shortened(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> Response:
        weights.append(weight)
        if url.endswith('aggTrades'):
            assert weight == 20
            return Response(bodies[requests[0]['file']], {}, 200)
        assert params['limit'] == 500 and weight == 200
        expected = 8100178150 if not starts else cast(int, delivered[len(starts) - 1][-1]['id']) + 1
        assert params['fromId'] == expected
        page = delivered[len(starts)]
        starts.append(expected)
        return Response(json.dumps(page).encode(), {}, 200)

    monkeypatch.setattr(rest, 'get_response', shortened)
    adapter = rest.BinancePerpProvisional()
    assert adapter.PAGE_CAP == 512
    with pytest.raises(RuntimeError, match='exceeded the 512-page cap'):
        adapter.fetch(adapter.partition(_BUSY_KEY))
    assert len(starts) == 512 and weights == [20, *([200] * 512)]


def test_perp_fallback_counts_against_page_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    monkeypatch.delenv('ORIGO_BINANCE_PERP_EGRESS_IPS', raising=False)
    provenance, bodies = _rest_responses('provenance.json')
    pages = {
        request['params']['fromId']: bodies[request['file']]
        for request in provenance['requests'][1:]
    }
    archive = tuple(
        daily.perp_csv_rows(
            (ARCHIVES / 'minute-2026-09-16T20-00.csv').read_bytes(),
            daily.BinancePerpDaily().partition('2026-09-16'),
        )
    )
    for cap in (8, 7):
        starts: list[int] = []
        weights: list[int] = []

        def captured(
            url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
        ) -> Response:
            weights.append(weight)
            if url.endswith('aggTrades'):
                return Response(bodies['locator.json'], {}, 200)
            assert params['limit'] == 500
            starts.append(cast(int, params['fromId']))
            body = bodies['page-02.json'] if len(starts) == 1 else pages[starts[-1]]
            return Response(body, {}, 200)

        monkeypatch.setattr(rest, 'get_response', captured)
        monkeypatch.setattr(rest.BinancePerpProvisional, 'PAGE_CAP', cap)
        adapter = rest.BinancePerpProvisional()
        partition = adapter.partition('2026-09-16T20:00:00Z')
        if cap == 8:
            revision = adapter.fetch(partition)
            assert revision.complete
            assert_archive_rest_equal(archive, tuple(revision.rows()), case=PERP_CASE)
            assert revision.key == content_hash(archive, schema_version=1)
        else:
            with pytest.raises(RuntimeError, match='exceeded the 7-page cap'):
                adapter.fetch(partition)
        assert starts[:2] == [8086999063, 8086998563]
        assert starts.count(8086998563) == 1 and len(starts) == cap
        assert weights == [20, *([200] * cap)]


def test_page_caps_preserve_other_sources_and_request_budgets() -> None:
    assert BinanceProvisionalBase.PAGE_CAP == 100
    adapters = {
        'binance_spot_trades': BinanceSpotProvisional(),
        'binance_spot_aggtrades': BinanceSpotAggProvisional(),
        'binance_perp_aggtrades': BinancePerpAggProvisional(),
        'binance_perp_trades': rest.BinancePerpProvisional(),
    }
    catalog = build_catalog('c034318225c416fbccd67139fcba7c7afe54f0f7')
    gates = {gate['id']: gate for gate in catalog['gates']}
    for source, adapter in adapters.items():
        expected = 512 if source == 'binance_perp_trades' else 100
        assert adapter.PAGE_CAP == expected
        assert gates[f'provider.response_completeness.page_cap:{source}']['thresholds'] == {
            'pages': expected
        }
    perp = rest.BinancePerpProvisional()
    assert (perp.PAGE_LIMIT, perp.WEIGHT_HISTORICAL, perp.WEIGHT_LOCATOR) == (500, 200, 20)
    assert REST_HOST_BUDGETS['fapi.binance.com'] == (24, 1920)
