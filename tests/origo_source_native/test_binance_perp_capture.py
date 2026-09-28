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
from collections.abc import Iterable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pytest
import requests

from origo.sources.adapters import binance_daily as transport
from origo.sources.adapters import binance_perp_spool as spool
from origo.sources.adapters.binance_perp_rest import BinancePerpProvisional
from origo.sources.contracts import Revision, SourceError
from origo.workers import perp_capture as capture
from origo.workers.runtime import heartbeat_is_fresh, heartbeat_path, touch_heartbeat

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures/recent_trades'
SMALL = FIXTURES / '2026-09-21'


def _bundles(root: Path, provenance: Mapping[str, object]) -> Iterator[dict[str, object]]:
    bundles = provenance.get('response_bundles')
    if bundles is None:
        bundles = [{'path': provenance['response_bundle'], 'sha256': provenance['sha256'],
                    'attempts': provenance['request_count']}]
    names: set[str] = set()
    total = 0
    for bundle in bundles:
        name = bundle['path']
        assert name not in names, 'A repeated bundle cannot substitute for missing attempts.'
        names.add(name)
        path = root / name
        assert path.resolve().is_relative_to(root.resolve())
        with path.open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == bundle['sha256']
        count = 0
        with gzip.open(path, 'rt') as stream:
            for line in stream:
                assert line.endswith('\n'), 'An interrupted response record is not evidence.'
                count += 1
                yield json.loads(line)
        assert count == bundle['attempts'], 'Bundle attempt accounting differs.'
        total += count
    assert total == provenance['request_count'], 'Attempts were omitted from the bundle inventory.'


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


def _recent_attempts(root: Path = SMALL, *, registered: bool = False) -> Iterator[dict[str, object]]:
    provenance = json.loads((root / 'provenance.json').read_text())
    records = _bundles(root, provenance)
    if registered:
        records = _registered_attempts(root, records, provenance)
    for record in records:
        assert record['url'] == capture.RECENT_TRADES_URL
        assert record['params'] == {'symbol': 'BTCUSDT', 'limit': 1000}
        assert record['authentication'] == 'none'
        assert _instant(record['captured_at']) <= _instant(record['completed_at'])
        _verify_body(record)
        yield record


def _records(root: Path = SMALL) -> list[dict[str, object]]:
    return list(_recent_attempts(root))


def _registered_attempts(
    root: Path, records: Iterable[dict[str, object]], provenance: Mapping[str, object]
) -> Iterator[dict[str, object]]:
    registration_bytes = (root / 'registration.json').read_bytes()
    assert hashlib.sha256(registration_bytes).hexdigest() == provenance['registration_sha256']
    registration = json.loads(registration_bytes)
    assert registration['interval_seconds'] == 0.5 and registration['weight'] == 5
    assert registration['endpoint'] == capture.RECENT_TRADES_URL
    assert registration['params'] == {'symbol': 'BTCUSDT', 'limit': 1000}
    start, end = (_instant(registration[key]) for key in ('start', 'end'))
    assert _instant(registration['registered_at']) <= start < end
    assert provenance['start'] == registration['start'] and provenance['end'] == registration['end']
    result = json.loads((root / 'result.json').read_text())
    assert result['stop_reason'] == 'interval_complete', 'A partial registered interval cannot pass.'
    assert _instant(result['finished_at']) >= end
    if 'response_bundles' in provenance:
        assert result['response_bundles'] == provenance['response_bundles']
    else:
        assert result['sha256'] == provenance['sha256']
    count = failures = 0
    previous_end = start
    for record in records:
        assert record['attempt'] == count, 'Missing, repeated or reordered capture attempts.'
        began, finished = (_instant(record[key]) for key in ('captured_at', 'completed_at'))
        assert start <= began < end and previous_end <= began <= finished
        assert record['request_weight'] == 5
        elapsed = record['elapsed_seconds']
        assert type(elapsed) in (int, float) and elapsed >= 0
        if record.get('status') is not None:
            assert isinstance(record['response_headers'], dict)
        if count == 0:
            assert (began - start).total_seconds() <= 1, 'Capture did not cover the registered start.'
        count += 1
        failures += int(_failed(record))
        previous_end = finished
        yield record
    assert count > 0 and count == result['attempts'] == provenance['request_count']
    assert failures == result['failures'] == provenance['failures']
    assert result['total_requested_weight'] == count * 5
    assert previous_end >= end - timedelta(seconds=1), 'The registered tail is missing.'


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


def _acceptance() -> tuple[Path, dict[str, object]]:
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
    partition = BinancePerpProvisional().partition(metadata['partition_key'])
    assert _instant(registration['start']) <= partition.start
    assert partition.end <= _instant(registration['end'])
    return root, metadata


def _historical_attempts(root: Path) -> Iterator[dict[str, object]]:
    provenance = json.loads((root / 'historical.provenance.json').read_text())
    count = failures = 0
    for record in _bundles(root, provenance):
        assert record['attempt'] == count
        assert record['purpose'] in ('bridge', 'parity')
        assert record['url'] == 'https://fapi.binance.com/fapi/v1/historicalTrades'
        assert record['params']['symbol'] == 'BTCUSDT' and record['params']['limit'] == 500
        assert type(record['params']['fromId']) is int
        assert record['request_weight'] == 200
        assert _instant(record['captured_at']) <= _instant(record['completed_at'])
        assert 'X-MBX-APIKEY' not in record.get('request_headers', {})
        _verify_body(record)
        count += 1
        failures += int(_failed(record))
        yield record
    assert failures == provenance['failures']
    assert provenance['total_requested_weight'] == count * 200


def _repair_recorded(
    root: Path, records: Iterable[dict[str, object]], monkeypatch: pytest.MonkeyPatch
) -> tuple[int, int]:
    pending = iter(record for record in records if record['purpose'] == 'bridge')
    current = next(pending, None)
    attempts = failures = 0
    recorded_error: str | None = None
    clock = datetime.now(UTC)

    def response(url: str, *, params: Mapping[str, str | int], headers: Mapping[str, str],
                 weight: int, egress_ip: str) -> transport.Response:
        nonlocal current, attempts, failures, clock, recorded_error
        assert current is not None, 'Repair requested an unrecorded historical page.'
        record = current
        assert url == record['url'] and params == record['params']
        assert weight == 200 and egress_ip == '37.27.112.144'
        assert headers == {'X-MBX-APIKEY': 'isolated-recorded-replay'}
        attempts += 1
        current = next(pending, None)
        clock = _instant(record.get('completed_at', record['captured_at']))
        if _failed(record):
            failures += 1
            recorded_error = str(record['error_code'])
            raise SourceError(recorded_error, 'Recorded historical transport failure.')
        return transport.Response(_body(record), record.get('response_headers', {}), 200, egress_ip)

    with monkeypatch.context() as patch:
        patch.setenv('BINANCE_API_KEY', 'isolated-recorded-replay')
        patch.setattr(spool, 'get_response', response)
        patch.setattr(spool, 'now_utc', lambda: clock)
        while current is not None:
            before, previous_failures = attempts, failures
            recorded_error = None
            partition = BinancePerpProvisional().partition(current['partition_key'])
            clock = _instant(current['captured_at'])
            try:
                spool.repair_spooled_gaps(root, partition, egress_ip='37.27.112.144')
            except SourceError as error:
                assert failures == previous_failures + 1 and error.code == recorded_error
                assert attempts > before, 'An unrecorded repair failure occurred.'
            assert 0 < attempts - before <= spool.REPAIR_PAGE_BUDGET
    state = spool.capture_state(root)
    assert state is not None and state.pending_gaps == 0, 'Recorded bridges leave unresolved gaps.'
    return attempts, failures


def _busy_revision(
    root: Path, metadata: Mapping[str, object], registration: Mapping[str, object]
) -> Revision:
    partition = BinancePerpProvisional().partition(metadata['partition_key'])
    with sqlite3.connect(root / 'capture.sqlite3') as connection:
        recent_rows = connection.execute('SELECT count() FROM trades WHERE time>=? AND time<?',
            (int(partition.start.timestamp() * 1000), int(partition.end.timestamp() * 1000))).fetchone()[0]
    assert recent_rows >= 17967, 'Historical repair rows cannot inflate the recent-capture threshold.'
    revision = spool.read_spooled_revision(root, partition)
    assert revision is not None and revision.complete
    assert revision.row_count == metadata['row_count'] >= recent_rows
    with sqlite3.connect(root / 'capture.sqlite3') as connection:
        candidates = connection.execute(
            'SELECT (time/60000)*60000,count() FROM trades GROUP BY time/60000 HAVING count()>=17967'
        ).fetchall()
    for timestamp, _ in candidates:
        minute = datetime.fromtimestamp(timestamp / 1000, UTC)
        candidate = BinancePerpProvisional().partition(minute.strftime('%Y-%m-%dT%H:%M:%SZ'))
        if (_instant(registration['start']) <= candidate.start
                and candidate.end <= _instant(registration['end'])):
            proof = spool.read_spooled_revision(root, candidate)
            assert proof is None or proof.row_count <= revision.row_count, (
                'Selected minute is not the largest fully proven qualifying minute.'
            )
    return revision


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


def test_recent_capture_matches_historical_and_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, metadata = _acceptance()
    _append(tmp_path, _recent_attempts(root, registered=True))
    _repair_recorded(tmp_path, _historical_attempts(root), monkeypatch)
    registration = json.loads((root / 'registration.json').read_text())
    revision = _busy_revision(tmp_path, metadata, registration)
    archive = _archive_rows(root)
    partition = BinancePerpProvisional().partition(metadata['partition_key'])
    start, end = int(partition.start.timestamp() * 1000), int(partition.end.timestamp() * 1000)
    history: dict[int, tuple[object, ...]] = {}
    for request in _historical_attempts(root):
        if request['purpose'] == 'parity' and not _failed(request):
            for raw in capture.parse_recent(_body(request)):
                row = spool.historical_row(raw)
                if start <= int(str(row[4])) < end:
                    key = int(str(row[0]))
                    assert key not in history or history[key] == row
                    history[key] = row
    rows = tuple(revision.rows())
    assert rows == archive == tuple(history[key] for key in sorted(history))


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


def test_registered_capture_rejects_omitted_attempts_and_partial_interval(tmp_path: Path) -> None:
    original = FIXTURES / 'exploratory-2026-09-28'
    provenance = json.loads((original / 'provenance.json').read_text())
    result = json.loads((original / 'result.json').read_text())
    # Add accounting derived from the genuine acquisition, without changing its bytes or interval.
    provenance['failures'] = result['failures']
    result['total_requested_weight'] = result['attempts'] * 5
    (tmp_path / 'registration.json').write_bytes((original / 'registration.json').read_bytes())
    (tmp_path / 'result.json').write_text(json.dumps(result))
    assert sum(1 for _ in _registered_attempts(tmp_path, _bundles(original, provenance), provenance)) == 1760
    omitted = (record for record in _bundles(original, provenance) if record['attempt'] != 12)
    with pytest.raises(AssertionError, match='Missing, repeated or reordered'):
        list(_registered_attempts(tmp_path, omitted, provenance))
    result['stop_reason'] = 'signal_interrupted'
    (tmp_path / 'result.json').write_text(json.dumps(result))
    with pytest.raises(AssertionError, match='partial registered interval'):
        list(_registered_attempts(tmp_path, _bundles(original, provenance), provenance))


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
    bridge = {**history, 'purpose': 'bridge', 'partition_key': key}
    assert _repair_recorded(tmp_path, iter([bridge]), monkeypatch) == (1, 0)
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


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _verify_resource_runs(
    root: Path, measurements: Mapping[str, object], metadata: Mapping[str, object],
    revision: Revision, *, attempts: int, failures: int, repair_attempts: int, repair_failures: int,
    recorded_seconds: float,
) -> None:
    provenance = json.loads((root / 'provenance.json').read_text())
    bundles = provenance.get('response_bundles') or [{
        'path': provenance['response_bundle'], 'sha256': provenance['sha256'],
        'attempts': provenance['request_count'],
    }]
    repository = Path(__file__).resolve().parents[2]
    sources = ('origo/workers/perp_capture.py', 'origo/sources/adapters/binance_perp_spool.py',
               'origo/sources/adapters/binance_daily.py')
    identity = {
        'registration_sha256': _sha256(root / 'registration.json'),
        'response_bundles': bundles,
        'historical_provenance_sha256': _sha256(root / 'historical.provenance.json'),
        'source_sha256': {name: _sha256(repository / name) for name in sources},
        'harness_sha256': _sha256(FIXTURES / 'replay_resources.py'),
    }
    parity_attempts = parity_failures = 0
    for record in _historical_attempts(root):
        if record['purpose'] == 'parity':
            parity_attempts += 1
            parity_failures += int(_failed(record))
    counters = {
        'recent_attempts': attempts, 'recent_failures': failures,
        'recent_weight_upper_bound': attempts * 5,
        'bridge_attempts': repair_attempts, 'bridge_failures': repair_failures,
        'bridge_weight_upper_bound': repair_attempts * 200,
        'parity_attempts': parity_attempts, 'parity_failures': parity_failures,
        'parity_weight_upper_bound': parity_attempts * 200,
    }
    assert parity_attempts > 0
    runs = measurements['runs']
    assert len(runs) == 2 and sorted(run['cadence_multiplier'] for run in runs) == [1, 2]
    for run in runs:
        assert run['origin'] == 'measured_capture_process'
        assert run['measurement_pass'] is True
        assert all(run['identity'][key] == value for key, value in identity.items())
        assert all(run[key] == value for key, value in counters.items())
        docker = run['docker']
        assert docker['exit_code'] == 0 and docker['oom_killed'] is False
        assert docker['nano_cpus'] == 1_000_000_000 and docker['network_mode'] == 'none'
        assert docker['memory_bytes'] == docker['memory_swap_bytes'] == 512 * 1024**2
        assert docker['image_id'].startswith('sha256:')
        assert run['cpu_limit'] == 1 and run['memory_limit_bytes'] == 512 * 1024**2
        assert 0 < run['max_rss_bytes'] <= 512 * 1024**2
        assert 0 < run['cgroup_memory_peak_bytes'] <= 512 * 1024**2
        assert 0 < run['peak_spool_bytes'] <= 16 * 1024**3
        assert 0 < run['peak_minute_payload_bytes'] <= 32 * 1024**2
        assert 0 < run['peak_status_bytes'] <= 16 * 1024
        for name in ('missing_recorded_repair_responses', 'unconsumed_bridge_responses',
                     'unresolved_breaks_after_repair', 'repair_queue_start', 'repair_queue_end'):
            assert run[name] == 0
        for name in ('replay_mismatches', 'unexpected_repair_errors', 'repair_thread_errors'):
            assert run[name] == []
        speed = run['cadence_multiplier']
        assert 0 <= run['max_poll_lateness_seconds'] <= 0.5 / speed
        assert abs(run['recorded_duration_seconds'] - recorded_seconds) <= 1
        assert 0 < run['wall_capture_seconds'] <= recorded_seconds / speed + 0.5 / speed
        selected = [minute for minute in run['minutes']
                    if minute['partition_key'] == metadata['partition_key']]
        assert len(selected) == 1 and selected[0]['complete'] is True
        assert selected[0]['rows'] == revision.row_count
        assert selected[0]['content_hash'] == revision.content_hash
        assert selected[0]['captured_rows'] >= 17967
        observed = root / run['directory'] / run['observation_bundle']
        assert observed.resolve().is_relative_to(root.resolve())
        assert _sha256(observed) == run['observation_sha256']
    assert all(measurements[key] == value for key, value in counters.items())
    assert measurements['max_rss_bytes'] == max(run['max_rss_bytes'] for run in runs)
    assert measurements['peak_minute_payload_bytes'] == max(run['peak_minute_payload_bytes'] for run in runs)
    assert measurements['registration_sha256'] == identity['registration_sha256']


def test_authentic_busy_capture_stays_within_resource_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, metadata = _acceptance()
    began = time.monotonic()
    attempts, failures = _append(tmp_path, _recent_attempts(root, registered=True))
    repair_attempts, repair_failures = _repair_recorded(tmp_path, _historical_attempts(root), monkeypatch)
    registration = json.loads((root / 'registration.json').read_text())
    revision = _busy_revision(tmp_path, metadata, registration)
    recorded_seconds = (_instant(registration['end']) - _instant(registration['start'])).total_seconds()
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
    _verify_resource_runs(
        root, measurements, metadata, revision, attempts=attempts, failures=failures,
        repair_attempts=repair_attempts, repair_failures=repair_failures,
        recorded_seconds=recorded_seconds,
    )


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
