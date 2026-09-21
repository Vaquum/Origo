"""Authentic recorded recent trades; local replay is not a production soak."""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from origo.sources.adapters.binance_daily import Response
from origo.sources.adapters.binance_perp_rest import historical_row
from origo.sources.contracts import Partition
from origo.steady_state.capture_repair import repair_minute
from origo.steady_state.trade_spool import CaptureMiss, PageCost, SealedMinute, TradeSpool, parse_recent_trades
from origo.workers.trade_capture import TradeCapture, check_spool

ROOT = Path(__file__).resolve().parents[1] / 'fixtures/steady_state/perp_recent'


def recordings() -> list[dict]:
    provenance = json.loads((ROOT / 'provenance.json').read_text())
    payload = (ROOT / provenance['response_bundle']).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == provenance['sha256']
    rows = [json.loads(line) for line in gzip.decompress(payload).splitlines()]
    assert len(rows) == provenance['request_count']
    return rows


def replay(spool: TradeSpool, records: list[dict]) -> list:
    outcomes = []
    for record in records:
        body = base64.b64decode(record['body_base64'])
        assert hashlib.sha256(body).hexdigest() == record['body_sha256']
        times = iter((datetime.fromisoformat(record['captured_at']),
                      datetime.fromisoformat(record['completed_at'])))
        def transport(url: str, *, params: dict, headers: dict, weight: int, lane: str) -> Response:
            assert url == record['url'] and params == record['params']
            assert headers == {} and weight == 5 and lane == 'live'
            return Response(body, {}, record['status'])
        capture = TradeCapture(spool, base_url='https://fapi.binance.com',
                               clock=lambda: next(times), transport=transport)
        outcomes.append(capture.step())
    return outcomes


def test_capture_preserves_real_values_and_only_seals_bracketed_minutes(tmp_path: Path) -> None:
    records = recordings()
    spool = TradeSpool.create(tmp_path / 'capture.sqlite', historical_row)
    try:
        outcomes = replay(spool, records)
        sealed = [minute for outcome in outcomes for minute in outcome.sealed]
        assert len(sealed) == 2 and len(set(sealed)) == 2
        assert all(outcome.closed_reason is None for outcome in outcomes)
        now = datetime.fromisoformat(records[-1]['completed_at'])
        assert check_spool(spool.path, now=now) == 0
        assert check_spool(spool.path, now=now + timedelta(seconds=181)) == 1
        assert check_spool(tmp_path / 'missing.sqlite', now=now) == 1
        assert not (tmp_path / 'missing.sqlite').exists()
        first = spool.sealed_minute(sealed[0])
        assert isinstance(first, SealedMinute)
        before = spool.connection.execute('SELECT count(*) FROM trades').fetchone()[0]
        rejected = spool.acknowledge(sealed[0], content_hash='different-approved-input',
                                     generation='isolated-review', now=now)
        assert rejected.sealed and not rejected.hash_matched and rejected.rows_released == 0
        assert spool.connection.execute('SELECT count(*) FROM trades').fetchone()[0] == before
    finally:
        spool.close()


def test_missing_capture_pages_are_repaired_by_exact_overlapping_native_rows(tmp_path: Path) -> None:
    records = recordings()
    all_rows = {}
    for record in records:
        for row in json.loads(base64.b64decode(record['body_base64'])):
            if row['id'] in all_rows:
                assert all_rows[row['id']] == row
            all_rows[row['id']] = row
    anchor = datetime(2026, 9, 21, 15, 11, tzinfo=UTC)
    full = TradeSpool.create(tmp_path / 'full.sqlite', historical_row)
    partial = TradeSpool.create(tmp_path / 'partial.sqlite', historical_row)
    try:
        replay(full, records)
        kept = [r for r in records if not (anchor + timedelta(seconds=10)
                <= datetime.fromisoformat(r['captured_at']) < anchor + timedelta(seconds=28))]
        replay(partial, kept)
        expected = full.sealed_minute(anchor)
        assert isinstance(expected, SealedMinute)
        assert isinstance(partial.sealed_minute(anchor), CaptureMiss)
        requests = []
        ordered = [all_rows[key] for key in sorted(all_rows)]
        def page(from_id: int) -> Response:
            # Normalized local replay of authentic rows, not claimed HTTP provenance.
            requests.append(from_id)
            rows = [row for row in ordered if row['id'] >= from_id][:500]
            return Response(json.dumps(rows).encode(), {}, 200)
        minute = Partition(anchor.strftime('%Y-%m-%dT%H:%M:%SZ'),
                           anchor, anchor + timedelta(minutes=1), True)
        repaired = repair_minute(partial, minute, page)
        assert repaired is not None and repaired.complete
        assert tuple(repaired.rows()) == expected.rows
        assert repaired.content_hash == expected.content_hash
        assert len(requests) >= 1
        assert len(requests) < (len(expected.rows) + 499) // 500
        evidence = json.loads(repaired.evidence_json)
        assert len(evidence['requests']) == len(requests)
        assert all(row['limit'] == 500 and row['weight'] == 200 for row in evidence['requests'])
    finally:
        full.close()
        partial.close()
