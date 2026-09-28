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
from collections.abc import Callable, Iterable, Mapping
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


def _instant(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    assert parsed.utcoffset() == timedelta(0), 'Recorded times must be UTC.'
    return parsed


def _failed(record: Mapping[str, object]) -> bool:
    return record.get('status') != 200 or bool(record.get('error_code'))


def _verify_body(record: Mapping[str, object]) -> None:
    status = record.get('status')
    if status is not None:
        assert type(status) is int and 100 <= status <= 599
        assert hashlib.sha256(_body(record)).hexdigest() == record['body_sha256']
    else:
        assert 'body_base64' not in record and 'body_sha256' not in record
    if _failed(record):
        assert isinstance(record.get('error_code'), str) and record['error_code']
    else:
        assert status == 200


def _records() -> list[dict[str, object]]:
    provenance = json.loads((SMALL / 'provenance.json').read_text())
    bundle = SMALL / provenance['response_bundle']
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() == provenance['sha256']
    with gzip.open(bundle, 'rt') as stream:
        records = [json.loads(line) for line in stream]
    assert len(records) == provenance['request_count'] == 181
    for record in records:
        assert record['url'] == capture.RECENT_TRADES_URL
        assert record['params'] == {'symbol': 'BTCUSDT', 'limit': 1000}
        assert record['authentication'] == 'none'
        assert _instant(record['captured_at']) <= _instant(record['completed_at'])
        _verify_body(record)
    return records


def _body(record: Mapping[str, object]) -> bytes:
    return base64.b64decode(str(record['body_base64']), validate=True)


def _append(root: Path, records: Iterable[dict[str, object]]) -> tuple[int, int]:
    attempts = failures = 0
    for record in records:
        evidence = {key: value for key, value in record.items() if key != 'body_base64'}
        evidence['request_weight_upper_bound'] = 5
        attempt = spool.begin_capture_attempt(root, evidence)
        if _failed(record):
            evidence['outcome'] = 'failed'
            spool.finish_capture_attempt(root, attempt, evidence)
            failures += 1
        else:
            evidence['outcome'] = 'succeeded'
            spool.append_capture(
                root, capture.parse_recent(_body(record)),
                received_at=_instant(record['completed_at']), evidence=evidence, attempt_id=attempt,
            )
        attempts += 1
    with sqlite3.connect(root / 'capture.sqlite3') as connection:
        assert connection.execute('SELECT count() FROM polls').fetchone()[0] == attempts
    return attempts, failures


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


def test_recorded_recent_capture_matches_archive_and_partial_historical_crosscheck(tmp_path: Path) -> None:
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


def test_replay_keeps_failed_attempt_cost_without_creating_rows(tmp_path: Path) -> None:
    original = _records()[:2]
    # Transport fault injection drops one actual response; this is not acceptance evidence.
    failed = {key: value for key, value in original[1].items()
              if key not in ('status', 'body_base64', 'body_sha256')}
    failed['error_code'] = 'PROVIDER_TRANSPORT_FAILED'
    _verify_body(failed)
    assert _append(tmp_path, [original[0], failed]) == (2, 1)
    with sqlite3.connect(tmp_path / 'capture.sqlite3') as connection:
        assert connection.execute('SELECT count() FROM trades').fetchone()[0] == 1000
        attempts = connection.execute('SELECT first_id,evidence FROM polls ORDER BY seq').fetchall()
    assert attempts[1][0] is None
    assert json.loads(attempts[1][1])['request_weight_upper_bound'] == 5
    assert json.loads(attempts[1][1])['error_code'] == 'PROVIDER_TRANSPORT_FAILED'


def test_recorded_historical_bridge_replays_exact_original_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = _records()
    history = json.loads((SMALL / 'historical_crosscheck.json').read_text())
    _verify_body(history)
    historical_rows = capture.parse_recent(_body(history))
    observed = {int(str(row['id'])): row for record in records
                for row in capture.parse_recent(_body(record))}
    left_id = int(str(historical_rows[0]['id']))
    right_id = int(str(historical_rows[100]['id']))
    groups = [[row for key, row in sorted(observed.items()) if key <= left_id],
              [row for key, row in sorted(observed.items()) if key >= right_id]]
    # Delivery fault withholds actual rows between genuine historical witnesses.
    # Only the unmodified provider response below is presented as historical HTTP evidence.
    for rows in groups:
        for offset in range(0, len(rows), 999):
            spool.append_capture(tmp_path, rows[max(0, offset - 1):offset + 999],
                                 received_at=_instant(records[-1]['completed_at']))
    minute = datetime.fromtimestamp(int(str(historical_rows[0]['time'])) / 1000, UTC)
    key = minute.replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
    partition = BinancePerpProvisional().partition(key)
    assert spool.read_spooled_revision(tmp_path, partition) is None
    calls = 0

    def response(url: str, *, params: Mapping[str, str | int], headers: Mapping[str, str],
                 weight: int, egress_ip: str) -> transport.Response:
        nonlocal calls
        assert url == history['url'] and params == history['params']
        assert weight == 200 and egress_ip == '37.27.112.144'
        assert headers == {'X-MBX-APIKEY': 'isolated-recorded-replay'}
        calls += 1
        return transport.Response(_body(history), {}, 200, egress_ip)

    monkeypatch.setenv('BINANCE_API_KEY', 'isolated-recorded-replay')
    monkeypatch.setattr(spool, 'get_response', response)
    spool.repair_spooled_gaps(tmp_path, partition, egress_ip='37.27.112.144')
    state = spool.capture_state(tmp_path)
    assert calls == 1 and state is not None and state.pending_gaps == 0
    actual: list[tuple[object, ...]] = []
    for key in ('2026-09-21T15:11:00Z', '2026-09-21T15:12:00Z'):
        revision = spool.read_spooled_revision(tmp_path, BinancePerpProvisional().partition(key))
        assert revision is not None
        actual.extend(revision.rows())
    assert tuple(actual) == _archive_rows(SMALL)


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


def _replay_historical_busy_minute(root: Path) -> dict[str, int | float]:
    import resource

    from .test_binance_perp_efficiency import _BUSY_KEY, _busy_archive, _busy_responses

    # Offline delivery of unchanged historical HTTP trades with one repeated boundary row.
    # These pages do not represent recent-endpoint captures or network throughput.
    requests, bodies = _busy_responses()
    previous: dict[str, object] | None = None
    started, cpu_started = time.monotonic(), time.process_time()
    for request in requests[1:]:
        rows = capture.parse_recent(bodies[request['file']])
        delivery = ([previous] if previous is not None else []) + rows
        spool.append_capture(root, delivery, received_at=_instant(request['captured_at']))
        previous = rows[-1]
    partition = BinancePerpProvisional().partition(_BUSY_KEY)
    revision = spool.read_spooled_revision(root, partition)
    assert revision is not None and revision.complete and revision.row_count == 129751
    assert tuple(revision.rows()) == _busy_archive()
    with sqlite3.connect(root / 'capture.sqlite3') as connection:
        payload = connection.execute('SELECT sum(length(payload)) FROM trades').fetchone()[0]
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == 'linux':
        # ru_maxrss includes inherited pre-exec residency; VmHWM belongs to this exec image.
        rss = int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines()
                       if line.startswith('VmHWM:'))) * 1024
    elif sys.platform != 'darwin':
        rss *= 1024
    return {
        'rows': revision.row_count, 'spool_bytes': spool.spool_bytes(root),
        'payload_bytes': payload, 'max_rss_bytes': rss,
        'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu_started,
    }


def test_historical_busy_minute_offline_delivery_stays_within_spool_and_process_bounds(
    tmp_path: Path, record_property: Callable[[str, object], None],
) -> None:
    code = """
import json,sys
from pathlib import Path
from tests.origo_source_native.test_binance_perp_capture import _replay_historical_busy_minute
print(json.dumps(_replay_historical_busy_minute(Path(sys.argv[1]))))
"""
    completed = subprocess.run([sys.executable, '-c', code, str(tmp_path)],
                               timeout=120, capture_output=True, text=True, check=True)
    measured = json.loads(completed.stdout)
    assert measured['rows'] == 129751
    assert 0 < measured['max_rss_bytes'] <= 512 * 1024**2
    assert 0 < measured['spool_bytes'] < spool.SPOOL_LIMIT_BYTES == 16 * 1024**3
    assert 0 < measured['payload_bytes'] <= 32 * 1024**2
    # Expose measured processing headroom without imposing host-speed-dependent CI gates.
    for name, value in measured.items():
        record_property(name, value)


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
