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
from origo.steady_state.trade_spool import (
    CaptureMiss,
    PageCost,
    SealedMinute,
    TradeSpool,
    parse_recent_trades,
)
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
        times = iter(
            (
                datetime.fromisoformat(record['captured_at']),
                datetime.fromisoformat(record['completed_at']),
            )
        )

        def transport(url: str, *, params: dict, headers: dict, weight: int, lane: str) -> Response:
            assert url == record['url'] and params == record['params']
            assert headers == {} and weight == 5 and lane == 'live'
            return Response(body, {}, record['status'])

        capture = TradeCapture(
            spool,
            base_url='https://fapi.binance.com',
            clock=lambda: next(times),
            transport=transport,
        )
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
        rejected = spool.acknowledge(
            sealed[0],
            content_hash='different-approved-input',
            generation='isolated-review',
            now=now,
        )
        assert rejected.sealed and not rejected.hash_matched and rejected.rows_released == 0
        assert spool.connection.execute('SELECT count(*) FROM trades').fetchone()[0] == before
    finally:
        spool.close()


def test_missing_capture_pages_are_repaired_by_exact_overlapping_native_rows(
    tmp_path: Path,
) -> None:
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
        kept = [
            r
            for r in records
            if not (
                anchor + timedelta(seconds=10)
                <= datetime.fromisoformat(r['captured_at'])
                < anchor + timedelta(seconds=28)
            )
        ]
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

        minute = Partition(
            anchor.strftime('%Y-%m-%dT%H:%M:%SZ'), anchor, anchor + timedelta(minutes=1), True
        )
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


def test_acknowledged_responses_prune_without_discarding_an_unaccepted_head(tmp_path: Path) -> None:
    records = recordings()
    spool = TradeSpool.create(tmp_path / 'retention.sqlite', historical_row)
    try:
        outcomes = replay(spool, records)
        closed = [minute for outcome in outcomes for minute in outcome.sealed]
        accepted = spool.sealed_minute(closed[0])
        assert isinstance(accepted, SealedMinute)
        cutoff = int(closed[0].timestamp() * 1000)
        before_head = spool.connection.execute(
            'SELECT id FROM trades WHERE time < ? ORDER BY id',
            (cutoff,),
        ).fetchall()
        assert before_head, 'The authentic partial capture head must stay unaccepted.'
        before_responses = spool.connection.execute('SELECT count(*) FROM responses').fetchone()[0]
        now = datetime.fromisoformat(records[-1]['completed_at']) + timedelta(seconds=61)
        outcome = spool.acknowledge(
            closed[0],
            content_hash=accepted.content_hash,
            generation='accepted-retention-test',
            now=now,
        )
        assert outcome.hash_matched and outcome.rows_released > 0
        after_responses = spool.connection.execute('SELECT count(*) FROM responses').fetchone()[0]
        assert after_responses < before_responses
        assert (
            spool.connection.execute(
                'SELECT id FROM trades WHERE time < ? ORDER BY id',
                (cutoff,),
            ).fetchall()
            == before_head
        )
        assert isinstance(spool.sealed_minute(closed[1]), SealedMinute)
    finally:
        spool.close()


def test_repeated_old_provider_page_does_not_refresh_capture_health(tmp_path: Path) -> None:
    record = recordings()[-1]
    body = base64.b64decode(record['body_base64'])
    rows = parse_recent_trades(body)
    now = datetime.fromisoformat(record['completed_at'])
    spool = TradeSpool.create(tmp_path / 'stale.sqlite', historical_row)
    try:
        for observed in (now, now + timedelta(seconds=181)):
            spool.record(
                rows,
                captured_at=observed,
                completed_at=observed,
                status=200,
                body_sha256=hashlib.sha256(body).hexdigest(),
                cost=PageCost(5),
            )
        assert check_spool(spool.path, now=now + timedelta(seconds=181)) == 1
    finally:
        spool.close()


def test_absent_ancillary_rpi_metadata_stays_unknown_not_false() -> None:
    # The archived six-field raw product has no RPI label. Removing only that
    # ancillary field must not invent a value or change owned native trade values.
    source = json.loads(base64.b64decode(recordings()[0]['body_base64']))[0]
    without_rpi = {key: value for key, value in source.items() if key != 'isRPITrade'}
    (parsed,) = parse_recent_trades(json.dumps([without_rpi]).encode())
    assert parsed.is_rpi is None
    assert 'isRPITrade' not in parsed.provider_row()
    assert historical_row(parsed.provider_row()) == historical_row(source)
    with pytest.raises(ValueError, match='boolean'):
        parse_recent_trades(json.dumps([{**source, 'isRPITrade': 'unknown'}]).encode())


def test_repair_boundary_survives_acknowledging_the_previous_minute(tmp_path: Path) -> None:
    records = recordings()
    target = datetime(2026, 9, 21, 15, 12, tzinfo=UTC)
    full = TradeSpool.create(tmp_path / 'whole.sqlite', historical_row)
    partial = TradeSpool.create(tmp_path / 'gapped.sqlite', historical_row)
    try:
        replay(full, records)
        kept = [
            record
            for record in records
            if not (
                target + timedelta(seconds=10)
                <= datetime.fromisoformat(record['captured_at'])
                < target + timedelta(seconds=28)
            )
        ]
        replay(partial, kept)
        prior = partial.sealed_minute(target - timedelta(minutes=1))
        assert isinstance(prior, SealedMinute)
        now = datetime.fromisoformat(records[-1]['completed_at']) + timedelta(seconds=61)
        partial.acknowledge(
            target - timedelta(minutes=1),
            content_hash=prior.content_hash,
            generation='accepted-before-gap',
            now=now,
        )
        preserved = partial.connection.execute(
            'SELECT count(*) FROM trades WHERE time >= ? AND time < ?',
            (
                int((target - timedelta(minutes=1)).timestamp() * 1000),
                int(target.timestamp() * 1000),
            ),
        ).fetchone()[0]
        assert preserved == 1
        rows_by_id = {}
        for record in records:
            for row in json.loads(base64.b64decode(record['body_base64'])):
                rows_by_id[row['id']] = row
        ordered = [rows_by_id[key] for key in sorted(rows_by_id)]

        def page(from_id: int) -> Response:
            rows = [row for row in ordered if row['id'] >= from_id][:500]
            return Response(json.dumps(rows).encode(), {}, 200)

        interval = Partition(
            target.strftime('%Y-%m-%dT%H:%M:%SZ'), target, target + timedelta(minutes=1), True
        )
        repaired = repair_minute(partial, interval, page)
        expected = full.sealed_minute(target)
        assert isinstance(expected, SealedMinute)
        assert repaired is not None and repaired.content_hash == expected.content_hash
        assert tuple(repaired.rows()) == expected.rows
    finally:
        full.close()
        partial.close()
