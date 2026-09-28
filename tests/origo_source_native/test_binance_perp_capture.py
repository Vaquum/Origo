from __future__ import annotations

import base64
import csv
import fcntl
import gzip
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pytest
import requests

from origo.sources.adapters import binance_daily as transport
from origo.sources.adapters import binance_perp_spool as spool
from origo.sources.adapters.binance_perp_rest import BinancePerpProvisional
from origo.sources.contracts import SourceError
from origo.workers import perp_capture as capture
from origo.workers.runtime import heartbeat_is_fresh, heartbeat_path, touch_heartbeat

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures/recent_trades'
SMALL = FIXTURES / '2026-09-21'


def _records(root: Path = SMALL) -> list[dict[str, object]]:
    provenance = json.loads((root / 'provenance.json').read_text())
    bundle = root / provenance['response_bundle']
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() == provenance['sha256']
    with gzip.open(bundle, 'rt') as stream:
        records = [json.loads(line) for line in stream]
    for record in records:
        assert record['url'] == capture.RECENT_TRADES_URL
        assert record['params'] == {'symbol': 'BTCUSDT', 'limit': 1000}
        assert record['authentication'] == 'none' and record['status'] == 200
        assert hashlib.sha256(_body(record)).hexdigest() == record['body_sha256']
    return records


def _body(record: Mapping[str, object]) -> bytes:
    return base64.b64decode(str(record['body_base64']), validate=True)


def _append(root: Path, records: list[dict[str, object]]) -> None:
    for record in records:
        spool.append_capture(
            root, capture.parse_recent(_body(record)),
            received_at=datetime.fromisoformat(str(record['completed_at'])),
            evidence={key: value for key, value in record.items() if key != 'body_base64'},
        )


def _acceptance() -> Path:
    manifest = FIXTURES / 'acceptance.json'
    assert manifest.is_file(), (
        'S461 acceptance is incomplete: provide a preregistered genuine recent capture '
        'covering >=17,967 trades in one complete minute, historical parity and the '
        'later checksum-verified archive. The 2026-09-21 corpus does not qualify.'
    )
    metadata = json.loads(manifest.read_text())
    assert metadata['origin'] == 'genuine_recent_capture'
    assert metadata['row_count'] >= 17967
    root = FIXTURES / metadata['directory']
    assert root.resolve().is_relative_to(FIXTURES.resolve())
    registration = json.loads((root / 'registration.json').read_text())
    assert datetime.fromisoformat(registration['registered_at']) <= datetime.fromisoformat(
        registration['start']
    )
    assert registration['interval_seconds'] == 0.5
    return root


def _archive_rows(root: Path) -> tuple[tuple[object, ...], ...]:
    provenance = json.loads((root / 'archive.provenance.json').read_text())
    assert provenance['archive_sha256'] == provenance['checksum_line'].split()[0]
    assert provenance['url'].startswith('https://data.binance.vision/data/futures/um/daily/trades/')
    payload = (root / provenance['extract_file']).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == provenance['extract_sha256']
    return tuple(spool.historical_row({
        'id': int(row[0]), 'price': row[1], 'qty': row[2], 'quoteQty': row[3],
        'time': int(row[4]), 'isBuyerMaker': row[5] == 'true',
    }) for row in csv.reader(io.StringIO(payload.decode())))


def test_recent_capture_matches_historical_and_archive(tmp_path: Path) -> None:
    root = _acceptance()
    records = _records(root)
    _append(tmp_path, records)
    metadata = json.loads((FIXTURES / 'acceptance.json').read_text())
    revision = spool.read_spooled_revision(
        tmp_path, BinancePerpProvisional().partition(metadata['partition_key'])
    )
    assert revision is not None and revision.complete and revision.row_count >= 17967
    archive = _archive_rows(root)
    history: dict[int, tuple[object, ...]] = {}
    with gzip.open(root / 'historical.jsonl.gz', 'rt') as stream:
        for line in stream:
            request = json.loads(line)
            assert request['url'].endswith('/fapi/v1/historicalTrades')
            assert request['status'] == 200 and request['params']['limit'] == 500
            assert hashlib.sha256(_body(request)).hexdigest() == request['body_sha256']
            for raw in capture.parse_recent(_body(request)):
                row = spool.historical_row(raw)
                history[int(str(row[0]))] = row
    rows = tuple(revision.rows())
    assert rows == archive == tuple(history[int(str(row[0]))] for row in rows)


def test_small_authentic_capture_matches_available_archive(tmp_path: Path) -> None:
    records = _records()
    _append(tmp_path, records)
    archive = _archive_rows(SMALL)
    gathered: list[tuple[object, ...]] = []
    for minute, count in [('15:11', 9948), ('15:12', 6647)]:
        revision = spool.read_spooled_revision(
            tmp_path, BinancePerpProvisional().partition(f'2026-09-21T{minute}:00Z')
        )
        assert revision is not None and revision.complete and revision.row_count == count
        gathered.extend(revision.rows())
    assert tuple(gathered) == archive
    history = json.loads((SMALL / 'historical_crosscheck.json').read_text())
    assert hashlib.sha256(_body(history)).hexdigest() == history['body_sha256']
    observed = {row[0]: row for row in gathered}
    crosscheck = tuple(spool.historical_row(raw) for raw in capture.parse_recent(_body(history)))
    assert len(crosscheck) == 499
    assert all(observed[row[0]] == row for row in crosscheck)


def test_overlap_preserves_real_id_skips_and_rejects_conflicts(tmp_path: Path) -> None:
    records = _records()
    first = capture.parse_recent(_body(records[0]))
    second = capture.parse_recent(_body(records[1]))
    assert any(int(str(b['id'])) - int(str(a['id'])) > 1 for a, b in pairwise(first))
    received = datetime.fromisoformat(str(records[0]['completed_at']))
    initial = spool.append_capture(tmp_path, first, received_at=received)
    next_commit = spool.append_capture(tmp_path, second, received_at=received + timedelta(seconds=1))
    assert next_commit.segment_id == initial.segment_id and next_commit.advanced
    repeated = spool.append_capture(tmp_path, second, received_at=received + timedelta(seconds=2))
    assert not repeated.advanced and repeated.last_durable_capture_at == next_commit.last_durable_capture_at
    # An explicit conflict fault changes one recorded trade; it is never fixture evidence.
    conflict = [dict(row) for row in second]
    conflict[-1]['price'] = first[0]['price']
    assert conflict[-1]['price'] != second[-1]['price']
    with pytest.raises(SourceError, match='conflict'):
        spool.append_capture(tmp_path, conflict, received_at=received + timedelta(seconds=3))


def test_capture_obeys_ip_budget_and_provider_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _records()[0]
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    calls: list[tuple[str, Mapping[str, str | int] | None, str | None]] = []

    def request(url: str, params: Mapping[str, str | int] | None,
                headers: Mapping[str, str] | None, egress_ip: str | None = None) -> requests.Response:
        assert headers is None
        with sqlite3.connect(tmp_path / 'spool' / 'capture.sqlite3') as database:
            intent = json.loads(database.execute(
                'SELECT evidence FROM polls ORDER BY seq DESC LIMIT 1'
            ).fetchone()[0])
        assert intent['outcome'] == 'pending' and intent['request_weight_upper_bound'] == 5
        calls.append((url, params, egress_ip))
        response = requests.Response()
        response.status_code = {1: 429, 3: 418}.get(len(calls), 200)
        response.headers = requests.structures.CaseInsensitiveDict({
            'Retry-After': '1', 'X-Test-Excluded': 'deliberate header filtering fault',
        })
        response._content = _body(record)
        return response

    monkeypatch.setattr(transport, '_request', request)
    worker = capture.PerpCapture(tmp_path / 'spool', capture.status_path(tmp_path))
    with pytest.raises(SourceError, match='HTTP 429'):
        worker.step()
    state = next((tmp_path / 'locks').glob('*.state'))
    cooldown = float(state.read_text().split()[0])
    assert cooldown > time.time()
    started = time.monotonic()
    worker.step()
    assert time.monotonic() - started >= 0.9
    assert calls == [(capture.RECENT_TRADES_URL, {'symbol': 'BTCUSDT', 'limit': 1000},
                      capture.CAPTURE_EGRESS_IP)] * 2
    assert '.37.27.112.140.' in state.name
    assert transport.REST_HOST_BUDGETS['fapi.binance.com'] == (24, 1920)
    assert capture.RECENT_TRADES_WEIGHT == 5
    with pytest.raises(SourceError, match='HTTP 418'):
        worker.step()
    assert float(state.read_text().split()[1]) > time.time()
    with pytest.raises(SourceError, match='circuit is open'):
        worker.step()
    assert len(calls) == 3
    with sqlite3.connect(tmp_path / 'spool' / 'capture.sqlite3') as database:
        attempts = [(first, json.loads(evidence)) for first, evidence in database.execute(
            'SELECT first_id,evidence FROM polls ORDER BY seq'
        )]
    assert len(attempts) == 4
    assert [value['outcome'] for _, value in attempts] == ['failed', 'succeeded', 'failed', 'failed']
    assert [value['error_code'] for _, value in attempts] == [
        'PROVIDER_HTTP_429', None, 'PROVIDER_HTTP_418', 'PROVIDER_RATE_CIRCUIT',
    ]
    assert all(first is None for index, (first, _) in enumerate(attempts) if index != 1)
    assert all(value['request_weight_upper_bound'] == 5 for _, value in attempts)
    assert attempts[1][1]['headers'] == {'Retry-After': '1'}


def test_capture_continues_during_slow_repair_and_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = _records()
    _append(tmp_path / 'spool', records[:1])
    body = _body(records[1])

    def response(*args: object, **kwargs: object) -> transport.Response:
        return transport.Response(body, {}, 200, capture.CAPTURE_EGRESS_IP)

    monkeypatch.setattr(capture, 'get_response', response)
    # A real SQLite repair writer lock must not serialize the independent capture writer.
    with spool._database(tmp_path / 'spool', 'repair', create=True) as repair:
        repair.execute('BEGIN IMMEDIATE')
        began = time.monotonic()
        commit = capture.PerpCapture(tmp_path / 'spool', capture.status_path(tmp_path)).step()
        assert commit.advanced and time.monotonic() - began < 2
        repair.rollback()


def test_authentic_busy_capture_stays_within_resource_budget(tmp_path: Path) -> None:
    root = _acceptance()
    records = _records(root)
    began = time.monotonic()
    _append(tmp_path, records)
    recorded_seconds = (datetime.fromisoformat(str(records[-1]['completed_at']))
                        - datetime.fromisoformat(str(records[0]['captured_at']))).total_seconds()
    assert time.monotonic() - began <= recorded_seconds / 2
    assert spool.spool_bytes(tmp_path) <= 16 * 1024**3
    # This gate requires actual process/resource observations, not estimated row sizes.
    measurements = json.loads((root / 'resources.json').read_text())
    assert measurements['origin'] == 'measured_capture_process'
    assert measurements['max_rss_bytes'] <= 512 * 1024**2
    assert measurements['cpu_limit'] == 1
    assert measurements['peak_minute_payload_bytes'] <= 32 * 1024**2
    assert measurements['unresolved_breaks_after_repair'] == 0
    assert measurements['repair_queue_end'] <= measurements['repair_queue_start']


def test_closeout_rejects_missing_or_unknown_observations() -> None:
    original = json.loads((Path(__file__).resolve().parents[1]
        / 'fixtures/law/overview-baseline-58b45de.json').read_text())['report']
    start = datetime.fromisoformat(original['sampling_slot'])
    with pytest.raises(ValueError, match='361 consecutive'):
        capture.validate_closeout([original], start=start, end=start + timedelta(hours=6))
    # Duplication and corruption of a genuine observation cannot manufacture a six-hour window.
    with pytest.raises(ValueError):
        capture.validate_closeout([original] * 361, start=start, end=start + timedelta(hours=6))
    unknown = json.loads(json.dumps(original))
    feed = next(feed for feed in unknown['feeds'] if feed['source_key'] == 'binance_perp_trades')
    feed['predicates']['R1']['status'] = 'UNKNOWN'
    with pytest.raises(ValueError, match='UNKNOWN'):
        capture.validate_closeout([unknown] * 361, start=start, end=start + timedelta(hours=6))


def test_stalled_capture_lock_remains_visible_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lockroot = tmp_path / 'locks'
    lockroot.mkdir()
    state = lockroot / 'binance_rest_budget.fapi_binance_com.37.27.112.140.state'
    state.write_text(f'{time.time() + 1:.6f} 0.000000')
    saved = state.read_text()
    heartbeat = heartbeat_path(tmp_path, capture.FEED)
    code = '''
import os
import sqlite3
from pathlib import Path
from origo.workers.runtime import touch_heartbeat,start_watchdog
from origo.sources.adapters.binance_daily import get_response
p=Path(os.environ['TEST_CAPTURE_HEARTBEAT'])
touch_heartbeat(p)
start_watchdog(p,max_age_seconds=1,poll_seconds=0.05)
get_response('https://fapi.binance.com/fapi/v1/trades',params={'symbol':'BTCUSDT','limit':1000},weight=5,egress_ip='37.27.112.140')
'''
    environment = {**os.environ, 'ORIGO_SOURCE_LOCK_DIR': str(lockroot),
                   'TEST_CAPTURE_HEARTBEAT': str(heartbeat), 'ORIGO_WORKER_HEARTBEAT': str(heartbeat)}
    with state.open('a+') as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        completed = subprocess.run([sys.executable, '-c', code], env=environment,
                                   timeout=15, capture_output=True)
        assert completed.returncode == 3, completed.stderr.decode()
        assert state.read_text() == saved
        assert not heartbeat_is_fresh(heartbeat, max_age_seconds=1, now=time.time())
        fcntl.flock(held, fcntl.LOCK_UN)
    body = _body(_records()[0])

    def request(*args: object, **kwargs: object) -> requests.Response:
        response = requests.Response()
        response.status_code = 200
        response._content = body
        return response

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(lockroot))
    monkeypatch.setattr(transport, '_request', request)
    worker = capture.PerpCapture(tmp_path / 'spool', capture.status_path(tmp_path))
    assert worker.step().advanced
    touch_heartbeat(heartbeat)
    assert capture.check_capture(tmp_path, now=datetime.now(UTC)) == 0
