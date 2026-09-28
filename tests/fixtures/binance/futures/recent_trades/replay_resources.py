"""Measure the real capture/spool code in isolated Docker; never manufacture provider rows."""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import importlib.metadata
import json
import os
import platform
import resource
import sqlite3
import subprocess
import threading
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

SOURCE_FILES = (
    'origo/workers/perp_capture.py', 'origo/sources/adapters/binance_perp_spool.py',
    'origo/sources/adapters/binance_daily.py',
)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError('Replay records require UTC timestamps.')
    return parsed


def bundles(root: Path, manifest: str = 'provenance.json') -> list[dict[str, object]]:
    provenance = json.loads((root / manifest).read_text())
    if 'response_bundles' in provenance:
        result = cast(list[dict[str, object]], provenance['response_bundles'])
    else:
        result = [{'path': provenance['response_bundle'], 'sha256': provenance['sha256'],
                   'attempts': provenance['request_count']}]
    for item in result:
        path = root / str(item['path'])
        if not path.resolve().is_relative_to(root.resolve()) or digest(path) != item['sha256']:
            raise ValueError('Response bundle path/hash differs from provenance.')
    return result


def recent(root: Path) -> Iterator[dict[str, object]]:
    previous: datetime | None = None
    for item in bundles(root):
        with gzip.open(root / str(item['path']), 'rt') as stream:
            for line in stream:
                record = cast(dict[str, object], json.loads(line))
                started = utc(record['captured_at'])
                if previous is not None and started < previous:
                    raise ValueError('Recorded attempts are not chronological.')
                previous = started
                if record['url'] != 'https://fapi.binance.com/fapi/v1/trades' or record['params'] != {'symbol': 'BTCUSDT', 'limit': 1000}:
                    raise ValueError('Replay accepts genuine BTCUSDT recent requests only.')
                if record.get('body_base64') is not None:
                    body(record)
                yield record


def body(record: Mapping[str, object]) -> bytes:
    payload = base64.b64decode(str(record['body_base64']), validate=True)
    if hashlib.sha256(payload).hexdigest() != record['body_sha256']:
        raise ValueError('A recorded response body hash differs.')
    return payload


def historical(root: Path) -> Iterator[dict[str, object]]:
    manifest = root / 'historical.provenance.json'
    if manifest.exists():
        for item in bundles(root, manifest.name):
            with gzip.open(root / str(item['path']), 'rt') as stream:
                for line in stream:
                    record = cast(dict[str, object], json.loads(line))
                    if record.get('body_base64') is not None:
                        body(record)
                    yield record
    crosscheck = root / 'historical_crosscheck.json'
    if crosscheck.exists():
        record = cast(dict[str, object], json.loads(crosscheck.read_text()))
        body(record)
        record['purpose'] = 'parity'
        yield record


def expected_error(record: Mapping[str, object]) -> str | None:
    if record.get('error_code') is not None:
        return str(record['error_code'])
    status = record.get('status')
    if status is None:
        return 'PROVIDER_TRANSPORT_FAILED'
    return None if 200 <= int(str(status)) < 300 else f'PROVIDER_HTTP_{status}'


def cgroup(name: str) -> str:
    return (Path('/sys/fs/cgroup') / name).read_text().strip()


def worker(root: Path, output: Path, speed: float, repo: Path) -> None:
    # Imports and fixture indexing stay inside the measured container process.
    import requests

    from origo.sources.adapters import binance_daily as transport
    from origo.sources.adapters import binance_perp_spool as spool
    from origo.sources.adapters.binance_perp_rest import BinancePerpProvisional
    from origo.sources.contracts import SourceError, failure_code
    from origo.workers import perp_capture as capture

    quota, period = cgroup('cpu.max').split()
    if quota == 'max' or int(quota) != int(period) or int(cgroup('memory.max')) != 512 * 1024**2:
        raise RuntimeError('Replay must run under an actual one-CPU/512-MiB cgroup.')
    output.mkdir(parents=True, exist_ok=True)
    state_root = Path('/spool')
    state_root.mkdir(exist_ok=True)
    os.environ['ORIGO_SOURCE_LOCK_DIR'] = str(state_root / 'locks')
    os.environ['BINANCE_API_KEY'] = 'offline-recorded-response-replay'
    os.environ.pop('ORIGO_WORKER_HEARTBEAT', None)
    setup_began = time.monotonic()
    identity = {'registration_sha256': digest(root / 'registration.json') if (root / 'registration.json').exists() else None,
        'response_bundles': bundles(root),
        'historical_provenance_sha256': digest(root / 'historical.provenance.json') if (root / 'historical.provenance.json').exists() else None,
        'source_sha256': {name: digest(repo / name) for name in SOURCE_FILES},
        'harness_sha256': digest(Path(__file__))}
    records = recent(root)
    first = next(records)
    first_at = utc(first['captured_at'])
    began = time.monotonic()
    def clock() -> datetime:
        return first_at + timedelta(seconds=(time.monotonic() - began) * speed)

    class ReplayTime:
        @staticmethod
        def time() -> float:
            return clock().timestamp()

        @staticmethod
        def sleep(seconds: float) -> None:
            time.sleep(seconds / speed)

    # The HTTP boundary is replaced; original persistence, pacing and circuit code still runs.
    transport.time = ReplayTime
    spool.now_utc = clock
    history_db = state_root / 'replay-history.sqlite3'
    history_weight = parity_attempts = parity_failures = corpus_bridge_attempts = 0
    with sqlite3.connect(history_db) as database:
        database.execute('CREATE TABLE responses(seq INTEGER PRIMARY KEY, request TEXT, record TEXT, used INTEGER DEFAULT 0)')
        database.execute('CREATE INDEX request_index ON responses(request,used,seq)')
        for record in historical(root):
            history_weight += 200
            if record.get('purpose') != 'bridge':
                parity_attempts += 1
                parity_failures += int(expected_error(record) is not None)
                continue
            corpus_bridge_attempts += 1
            params = cast(dict[str, object], record['params'])
            database.execute('INSERT INTO responses(request,record) VALUES (?,?)',
                             (json.dumps(params, sort_keys=True), json.dumps(record)))
        database.commit()

    active: dict[str, object] = {}
    sent_recent, sent_repair, missing_repair = 0, 0, 0
    bridge_attempts = bridge_failures = 0
    request_context = threading.local()
    replay_mismatches: list[dict[str, object]] = []
    repair_errors: list[dict[str, object]] = []
    unexpected_repair_errors: list[dict[str, object]] = []
    repair_thread_errors: list[str] = []
    done = threading.Event()
    queue_peak = 0
    queue_samples: list[dict[str, object]] = []

    def request(url: str, params: Mapping[str, str | int] | None,
                headers: Mapping[str, str] | None, egress_ip: str | None = None) -> requests.Response:
        nonlocal sent_recent, sent_repair
        record = cast(dict[str, object], request_context.record)
        if url == capture.RECENT_TRADES_URL:
            if params != {'symbol': 'BTCUSDT', 'limit': 1000} or egress_ip != capture.CAPTURE_EGRESS_IP:
                raise ValueError('Capture request identity changed.')
            sent_recent += 1
        elif url.endswith('/fapi/v1/historicalTrades'):
            if egress_ip != '37.27.112.144':
                raise ValueError('Repair role changed.')
            sent_repair += 1
        else:
            raise ValueError('Requests outside recorded endpoint identities are forbidden.')
        if record.get('completed_at') is not None:
            duration = max(0.0, (utc(record['completed_at']) - utc(record['captured_at'])).total_seconds())
            # Recorded duration surrounds get_response; do not count its limiter wait twice.
            already_elapsed = (time.monotonic() - float(request_context.started)) * speed
            time.sleep(max(0.0, duration - already_elapsed) / speed)
        if record.get('status') is None:
            raise SourceError(str(record.get('error_code', 'PROVIDER_TRANSPORT_FAILED')), 'Recorded provider attempt failed.')
        response = requests.Response()
        response.status_code = int(str(record['status']))
        response._content = body(record)
        response.headers.update(cast(dict[str, str], record.get('headers', record.get('response_headers', {}))))
        if record.get('used_weight_1m') is not None:
            response.headers['X-MBX-USED-WEIGHT-1M'] = str(record['used_weight_1m'])
        return response

    original_get_response = transport.get_response

    def replay_get_response(url: str, *, params: Mapping[str, str | int] | None = None,
                            headers: Mapping[str, str] | None = None, weight: int = 0,
                            egress_ip: str | None = None) -> transport.Response:
        nonlocal bridge_attempts, bridge_failures, missing_repair
        request_context.started = time.monotonic()
        request_context.expected = None
        is_bridge = url.endswith('/fapi/v1/historicalTrades')
        if is_bridge:
            bridge_attempts += 1
            with sqlite3.connect(history_db) as database:
                selected = database.execute('SELECT seq,record FROM responses WHERE request=? AND used=0 ORDER BY seq LIMIT 1',
                    (json.dumps(dict(params or {}), sort_keys=True),)).fetchone()
                if selected is None:
                    missing_repair += 1
                    bridge_failures += 1
                    raise SourceError('REPLAY_REPAIR_RESPONSE_MISSING', 'No exact genuine historical response exists for this repair request.')
                database.execute('UPDATE responses SET used=1 WHERE seq=?', (selected[0],))
                database.commit()
            record = cast(dict[str, object], json.loads(selected[1]))
        else:
            record = active
        if record['url'] != url or record['params'] != params:
            raise ValueError('Recorded request identity differs from replay.')
        request_context.record = record
        request_context.expected = expected_error(record)
        observed: str | None = None
        try:
            return original_get_response(url, params=params, headers=headers, weight=weight, egress_ip=egress_ip)
        except Exception as error:
            observed = failure_code(error)
            bridge_failures += int(is_bridge)
            raise
        finally:
            if is_bridge and observed != request_context.expected:
                replay_mismatches.append({'kind': 'bridge', 'attempt': bridge_attempts,
                                          'expected': request_context.expected, 'observed': observed})

    transport._request = request
    capture.get_response = replay_get_response
    spool.get_response = replay_get_response
    collector = capture.PerpCapture(state_root / 'capture', state_root / 'status' / 'perp_capture.status.json', clock=clock)

    def pending() -> int:
        commit = spool.capture_state(state_root / 'capture')
        return 0 if commit is None else commit.pending_gaps

    def repair_loop() -> None:
        nonlocal queue_peak
        tick = first_at.replace(second=0, microsecond=0) + timedelta(minutes=1)
        while not done.wait(max(0.0, (tick - clock()).total_seconds()) / speed):
            count = pending()
            queue_peak = max(queue_peak, count)
            queue_samples.append({'at': clock().isoformat(), 'pending_gaps': count})
            tick = clock() + timedelta(minutes=1)
            if count == 0:
                continue
            database_path = state_root / 'capture' / 'capture.sqlite3'
            with sqlite3.connect(database_path) as database:
                bounds = database.execute('SELECT min(first_ms),max(last_ms) FROM segments').fetchone()
            if bounds is None or bounds[0] is None:
                raise RuntimeError('A pending gap has no durable endpoints.')
            start = datetime.fromtimestamp(int(bounds[0]) / 1000, UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
            last = min(clock(), datetime.fromtimestamp(int(bounds[1]) / 1000, UTC)).replace(second=0, microsecond=0)
            while start < last:
                partition = BinancePerpProvisional().partition(start.strftime('%Y-%m-%dT%H:%M:%SZ'))
                if spool.classify_spooled_partition(state_root / 'capture', partition) == 'pending':
                    try:
                        spool.repair_spooled_gaps(state_root / 'capture', partition, egress_ip='37.27.112.144')
                    except (OSError, SourceError, ValueError) as error:
                        observation = {'at': clock().isoformat(), 'error_code': failure_code(error)}
                        repair_errors.append(observation)
                        if failure_code(error) != getattr(request_context, 'expected', None):
                            unexpected_repair_errors.append(observation)
                            done.set()
                    break
                start += timedelta(minutes=1)

    def checked_repair_loop() -> None:
        try:
            repair_loop()
        except Exception as error:
            repair_thread_errors.append(failure_code(error))
            done.set()
            raise

    setup_seconds = time.monotonic() - setup_began
    began = time.monotonic()
    repair_thread = threading.Thread(target=checked_repair_loop, name='recorded-repair', daemon=True)
    repair_thread.start()
    attempts = errors = advanced = repeats = 0
    peak_spool = peak_status = 0
    maximum_lateness = 0.0
    last_at = first_at
    ledger = output / 'observations.jsonl.gz'
    with gzip.open(ledger, 'wt') as observations:
        def deliver(record: dict[str, object]) -> None:
            nonlocal active, attempts, errors, advanced, repeats, peak_spool, peak_status, maximum_lateness, last_at, queue_peak
            scheduled = (utc(record['captured_at']) - first_at).total_seconds() / speed
            time.sleep(max(0.0, began + scheduled - time.monotonic()))
            lateness = max(0.0, time.monotonic() - began - scheduled)
            maximum_lateness = max(maximum_lateness, lateness)
            active = record
            attempts += 1
            expected = expected_error(record)
            observed_error: str | None = None
            try:
                commit = collector.step()
                advanced += int(commit.advanced)
                repeats += int(not commit.advanced)
                queue_peak = max(queue_peak, commit.pending_gaps)
                peak_spool = max(peak_spool, commit.spool_bytes)
            except (OSError, SourceError, ValueError) as error:
                observed_error = failure_code(error)
                errors += 1
                collector.publish_status(observed_error)
            if observed_error != expected:
                replay_mismatches.append({'kind': 'recent', 'attempt': attempts, 'expected': expected, 'observed': observed_error})
            last_at = utc(record.get('completed_at', record['captured_at']))
            peak_spool = max(peak_spool, spool.spool_bytes(state_root / 'capture'))
            peak_status = max(peak_status, collector.status.stat().st_size)
            observations.write(json.dumps({'attempt': attempts, 'recorded_at': record['captured_at'],
                'observed_at': clock().isoformat(), 'schedule_lateness_seconds': lateness,
                'error_code': observed_error, 'pending_gaps': pending(),
                'spool_bytes': spool.spool_bytes(state_root / 'capture'),
                'rss_high_water_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}) + '\n')
        deliver(first)
        for record in records:
            deliver(record)
    wall_capture_seconds = time.monotonic() - began
    queue_at_capture_end = pending()
    drain_started = time.monotonic()
    while pending() and not done.is_set() and (time.monotonic() - drain_started) * speed < 120:
        time.sleep(0.5 / speed)
    done.set()
    repair_thread.join(timeout=65 / speed)
    if repair_thread.is_alive():
        raise RuntimeError('Bounded repair did not finish after capture replay.')
    drain_seconds = time.monotonic() - drain_started
    unresolved = pending()
    with sqlite3.connect(history_db) as database:
        unconsumed_bridge = int(database.execute('SELECT count(*) FROM responses WHERE used=0').fetchone()[0])
    minute_metrics: list[dict[str, object]] = []
    capture_db = state_root / 'capture' / 'capture.sqlite3'
    if capture_db.exists():
        with sqlite3.connect(capture_db) as database:
            minute_bounds = database.execute('SELECT min(time),max(time) FROM trades').fetchone()
        if minute_bounds[0] is not None:
            start = datetime.fromtimestamp(int(minute_bounds[0]) / 1000, UTC).replace(second=0, microsecond=0)
            end = datetime.fromtimestamp(int(minute_bounds[1]) / 1000, UTC).replace(second=0, microsecond=0)
            while start < end:
                partition = BinancePerpProvisional().partition(start.strftime('%Y-%m-%dT%H:%M:%SZ'))
                revision = spool.read_spooled_revision(state_root / 'capture', partition)
                with sqlite3.connect(capture_db) as database:
                    measured_payload = database.execute('SELECT count(*),coalesce(sum(length(payload)),0) FROM trades WHERE time>=? AND time<?',
                        (int(start.timestamp() * 1000), int(partition.end.timestamp() * 1000))).fetchone()
                    captured_count, size = int(measured_payload[0]), int(measured_payload[1])
                    repair_path = state_root / 'capture' / 'repair.sqlite3'
                    if repair_path.exists():
                        database.execute('ATTACH DATABASE ? AS repair', (str(repair_path),))
                        size = int(database.execute('SELECT coalesce(sum(length(payload)),0) FROM (SELECT id,payload FROM trades WHERE time>=? AND time<? UNION SELECT id,payload FROM repair.trades WHERE time>=? AND time<?)',
                            (int(start.timestamp() * 1000), int(partition.end.timestamp() * 1000)) * 2).fetchone()[0])
                minute_metrics.append({'partition_key': partition.key, 'complete': revision is not None,
                    'rows': revision.row_count if revision is not None else None,
                    'content_hash': revision.content_hash if revision is not None else None,
                    'captured_rows': captured_count, 'minute_payload_bytes': size})
                start += timedelta(minutes=1)
    cpu_stats = {name: int(value) for name, value in (line.split() for line in cgroup('cpu.stat').splitlines())}
    report = {'schema_version': 1, 'origin': 'measured_capture_process', 'cadence_multiplier': speed,
        'recorded_start': first_at.isoformat(), 'recorded_end': last_at.isoformat(),
        'recorded_duration_seconds': (last_at - first_at).total_seconds(),
        'wall_seconds': time.monotonic() - began, 'wall_capture_seconds': wall_capture_seconds,
        'setup_seconds': setup_seconds, 'repair_drain_seconds': drain_seconds, 'repair_drain_limit_recorded_seconds': 120,
        'cpu_limit': int(quota) / int(period), 'memory_limit_bytes': int(cgroup('memory.max')),
        'cgroup_memory_peak_bytes': int(cgroup('memory.peak')),
        'max_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        'cpu_stats': cpu_stats, 'recent_attempts': attempts, 'recent_http_requests': sent_recent,
        'recent_failures': errors, 'recent_advancing_responses': advanced, 'recent_repeated_responses': repeats,
        'recent_weight_upper_bound': attempts * 5, 'recent_http_weight': sent_recent * 5,
        'bridge_attempts': bridge_attempts, 'bridge_failures': bridge_failures,
        'bridge_weight_upper_bound': bridge_attempts * 200, 'unconsumed_bridge_responses': unconsumed_bridge,
        'parity_attempts': parity_attempts, 'parity_failures': parity_failures, 'parity_weight_upper_bound': parity_attempts * 200,
        'operational_weight_upper_bound': attempts * 5 + bridge_attempts * 200,
        'total_evidence_weight_upper_bound': attempts * 5 + bridge_attempts * 200 + parity_attempts * 200,
        'corpus_bridge_attempts': corpus_bridge_attempts, 'repair_http_requests': sent_repair, 'repair_http_weight_upper_bound': sent_repair * 200,
        'missing_recorded_repair_responses': missing_repair, 'corpus_historical_evidence_weight': history_weight,
        'peak_spool_bytes': peak_spool, 'peak_status_bytes': peak_status,
        'peak_minute_payload_bytes': max((int(row['minute_payload_bytes']) for row in minute_metrics), default=0),
        'repair_queue_start': 0, 'repair_queue_peak': queue_peak, 'repair_queue_at_capture_end': queue_at_capture_end,
        'repair_queue_end': unresolved, 'unresolved_breaks_after_repair': unresolved,
        'max_poll_lateness_seconds': maximum_lateness, 'replay_mismatches': replay_mismatches,
        'repair_errors': repair_errors, 'unexpected_repair_errors': unexpected_repair_errors, 'repair_thread_errors': repair_thread_errors, 'queue_observations': queue_samples, 'minutes': minute_metrics,
        'observation_bundle': ledger.name, 'observation_sha256': digest(ledger),
        'python': platform.python_version(), 'dependencies': {name: importlib.metadata.version(name) for name in ('dagster', 'requests', 'polars', 'pyarrow')},
        'identity': identity,
        'measurement_pass': not (replay_mismatches or unexpected_repair_errors or repair_thread_errors or unconsumed_bridge or missing_repair or unresolved) and maximum_lateness <= 0.5 / speed and wall_capture_seconds <= (last_at - first_at).total_seconds() / speed + 0.5 / speed,
        'limitations': ['Offline replay; neither network latency nor production capacity is proven.',
            'Two-times mode compresses recorded timing and quota-clock time; it is a processing test only.',
            'Historical responses must match exact recorded request parameters; missing records remain failures.',
            'Canonical publication/ClickHouse are outside this capture/repair process measurement.']}
    (output / 'run.json').write_text(json.dumps(report, indent=2) + '\n')


def host(root: Path, output: Path, image: str) -> None:
    repo = Path(__file__).resolve().parents[5]
    relative = Path(__file__).resolve().relative_to(repo)
    root, output = root.resolve(), output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    image_id = subprocess.check_output(['docker', 'image', 'inspect', image, '--format', '{{.Id}}'], text=True).strip()
    runs: list[dict[str, object]] = []
    for speed, label in ((1, 'recorded'), (2, 'twice')):
        destination = output / label
        destination.mkdir(exist_ok=True)
        container = 'origo-perp-resource-' + uuid4().hex[:12]
        command = ['docker', 'create', '--name', container, '--cpus', '1', '--memory', '512m',
            '--memory-swap', '512m', '--network', 'none', '--read-only', '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:rw,size=32m',
            '--mount', 'type=volume,destination=/spool',
            '--mount', f'type=bind,source={repo},destination=/workspace,readonly',
            '--mount', f'type=bind,source={root},destination=/corpus,readonly',
            '--mount', f'type=bind,source={destination},destination=/evidence',
            '--env', 'PYTHONPATH=/workspace', '--env', 'PYTHONDONTWRITEBYTECODE=1',
            '--workdir', '/workspace', '--entrypoint', 'python', image_id,
            '/workspace/' + str(relative), '--container', '--corpus', '/corpus',
            '--output', '/evidence', '--speed', str(speed)]
        subprocess.run(command, check=True, capture_output=True, text=True)
        try:
            with (destination / 'container.log').open('w') as log:
                completed = subprocess.run(['docker', 'start', '--attach', container], stdout=log, stderr=subprocess.STDOUT, check=False)
            inspected = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
            metadata = {'exit_code': completed.returncode, 'oom_killed': inspected['State']['OOMKilled'],
                'image_id': inspected['Image'], 'network_mode': inspected['HostConfig']['NetworkMode'],
                'nano_cpus': inspected['HostConfig']['NanoCpus'], 'memory_bytes': inspected['HostConfig']['Memory'],
                'memory_swap_bytes': inspected['HostConfig']['MemorySwap']}
            (destination / 'docker.json').write_text(json.dumps(metadata, indent=2) + '\n')
            if completed.returncode != 0:
                raise RuntimeError(f'{label} replay failed; inspect {destination / "container.log"}')
            measured = cast(dict[str, object], json.loads((destination / 'run.json').read_text()))
            measured['docker'] = metadata
            measured['directory'] = label
            runs.append(measured)
            print(f'{label}: {measured["recent_attempts"]} attempts, RSS {measured["max_rss_bytes"]} bytes, pending {measured["repair_queue_end"]}', flush=True)
        finally:
            subprocess.run(['docker', 'rm', '--volumes', container], check=True, capture_output=True, text=True)
    sources = bundles(root)
    registration = root / 'registration.json'
    history = root / 'historical.jsonl.gz'
    report = {'schema_version': 1, 'origin': 'measured_capture_process',
        'purpose': 'Measured offline resource evidence; does not by itself qualify acquisition acceptance.',
        'qualifying_busy_minute': all(any(bool(minute['complete']) and int(minute['captured_rows']) >= 17967 for minute in cast(list[dict[str, object]], run['minutes'])) for run in runs),
        'registration_sha256': digest(registration) if registration.exists() else None,
        'recent_sha256': sources[0]['sha256'] if len(sources) == 1 else hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest(),
        'response_bundles': sources, 'historical_sha256': digest(history) if history.exists() else None,
        'cpu_limit': 1, 'max_rss_bytes': max(int(run['max_rss_bytes']) for run in runs),
        'peak_minute_payload_bytes': max(int(run['peak_minute_payload_bytes']) for run in runs),
        'unresolved_breaks_after_repair': max(int(run['unresolved_breaks_after_repair']) for run in runs),
        'repair_queue_start': 0, 'repair_queue_end': max(int(run['repair_queue_end']) for run in runs),
        'runs': runs}
    for field in ('recent_attempts', 'recent_failures', 'recent_weight_upper_bound', 'bridge_attempts',
                  'bridge_failures', 'bridge_weight_upper_bound', 'parity_attempts', 'parity_failures',
                  'parity_weight_upper_bound'):
        if runs[0][field] != runs[1][field]:
            raise RuntimeError(f'Recorded and twice-cadence runs disagree on {field}.')
        report[field] = runs[0][field]
    (output / 'resource-replay-report.json').write_text(json.dumps(report, indent=2) + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--image', default='trades-warehouse-dagster:latest')
    parser.add_argument('--container', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--speed', type=float, choices=(1, 2), default=1)
    args = parser.parse_args()
    if args.container:
        worker(args.corpus, args.output, args.speed, Path('/workspace'))
    else:
        host(args.corpus, args.output, args.image)


if __name__ == '__main__':
    main()
