from __future__ import annotations

import json
from typing import cast

import pytest

from origo.sources.adapters import binance_perp_daily as daily
from origo.sources.adapters import binance_perp_rest as rest
from origo.sources.adapters.binance_daily import Response
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
