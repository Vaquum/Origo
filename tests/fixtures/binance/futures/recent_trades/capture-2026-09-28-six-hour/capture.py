"""Retain every attempt in a preregistered, off-production six-hour sample."""
import base64
import gzip
import hashlib
import json
import os
import signal
import socket
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from origo.sources.adapters import binance_daily as daily

root = Path('tests/fixtures/binance/futures/recent_trades/capture-2026-09-28-six-hour')
root.mkdir(exist_ok=False)
locks = Path('/tmp/origo-s461-local-limiter')
locks.mkdir(exist_ok=True)
os.environ['ORIGO_SOURCE_LOCK_DIR'] = str(locks)
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
    connection.connect(('1.1.1.1', 53))
    ip = connection.getsockname()[0]
registered = datetime.now(UTC)
started = registered + timedelta(seconds=2)
end = started + timedelta(hours=6)
registration = {
    'schema_version': 2, 'origin': 'nonproduction local bound get_response; not production latency',
    'registered_at': registered.isoformat(), 'start': started.isoformat(), 'end': end.isoformat(),
    'endpoint': 'https://fapi.binance.com/fapi/v1/trades',
    'params': {'symbol': 'BTCUSDT', 'limit': 1000}, 'egress_ip': ip,
    'interval_seconds': 0.5, 'weight': 5, 'authentication': 'none',
    'selection': 'Every attempt for six hours; retain every five-minute chunk, including failures. Select largest fully proven single minute, report all interval counts and costs.',
    'qualifying_busy_minimum_rows': 17967, 'archive_available_after': '2026-09-29',
    'transport': 'unchanged binance_daily.get_response; bound fresh requests.Session per request',
    'git_revision': '0314adea688b0ce4f82ca27b25fd9380a5cc2f7e',
    'stop_conditions': ['registered interval complete', 'HTTP 401/403/451', '2 GiB compressed evidence', 'SIGTERM/SIGINT'],
    'capture_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
}
registration_path = root / 'registration.json'
with registration_path.open('x') as stream:
    json.dump(registration, stream, indent=2)
    stream.write('\n')
    stream.flush()
    os.fsync(stream.fileno())
(root / 'capture.py').write_bytes(Path(__file__).read_bytes())
original = daily._request
captured = []
def record_request(*args, **kwargs):
    response = original(*args, **kwargs)
    captured.append(response)
    return response
daily._request = record_request
stop_requested = False
def stop(signum, frame):
    global stop_requested
    stop_requested = True
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
print(json.dumps({'state': 'registered', **registration}), flush=True)
time.sleep(max(0, (started - datetime.now(UTC)).total_seconds()))
attempts = failures = total_bytes = 0
bundles = []
reason = 'interval_complete'
while datetime.now(UTC) < end and not stop_requested:
    chunk_path = root / f'responses-{len(bundles):03d}.jsonl.gz'
    chunk_end = min(end, datetime.now(UTC) + timedelta(minutes=5))
    chunk_count = 0
    with gzip.open(chunk_path, 'xb') as stream:
        while datetime.now(UTC) < chunk_end and not stop_requested:
            now = datetime.now(UTC)
            began = time.monotonic()
            captured.clear()
            record = {'attempt': attempts, 'captured_at': now.isoformat(), 'url': registration['endpoint'],
                      'params': registration['params'], 'egress_ip': ip, 'request_weight': 5, 'authentication': 'none'}
            try:
                response = daily.get_response(registration['endpoint'], params=registration['params'], weight=5, egress_ip=ip)
                record.update({'body_base64': base64.b64encode(response.body).decode(),
                               'body_sha256': hashlib.sha256(response.body).hexdigest(),
                               'status': response.status, 'response_headers': dict(response.headers)})
            except Exception as error:
                failures += 1
                record.update({'error_code': getattr(error, 'code', type(error).__name__), 'error': str(error)})
                if captured:
                    response = captured[-1]
                    record.update({'body_base64': base64.b64encode(response.content).decode(),
                                   'body_sha256': hashlib.sha256(response.content).hexdigest(),
                                   'status': response.status_code, 'response_headers': dict(response.headers)})
            record.update({'completed_at': datetime.now(UTC).isoformat(), 'elapsed_seconds': time.monotonic() - began})
            stream.write(json.dumps(record, separators=(',', ':')).encode() + b'\n')
            stream.flush()
            attempts += 1
            chunk_count += 1
            if record.get('status') in (401, 403, 451):
                reason = 'provider_denied_local_capture'
                stop_requested = True
            time.sleep(max(0, 0.5 - (time.monotonic() - began)))
    with chunk_path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    total_bytes += chunk_path.stat().st_size
    bundles.append({'path': chunk_path.name, 'sha256': digest, 'attempts': chunk_count})
    progress = {'state': 'recording', 'attempts': attempts, 'failures': failures,
                'response_bundles': bundles, 'compressed_bytes': total_bytes,
                'observed_at': datetime.now(UTC).isoformat(), 'registered_end': end.isoformat()}
    (root / 'progress.json').write_text(json.dumps(progress, indent=2) + '\n')
    print(json.dumps({key: value for key, value in progress.items() if key != 'response_bundles'}), flush=True)
    if total_bytes >= 2 * 1024**3:
        reason = 'compressed_evidence_limit'
        stop_requested = True
if stop_requested and reason == 'interval_complete':
    reason = 'signal_interrupted'
result = {'attempts': attempts, 'failures': failures, 'finished_at': datetime.now(UTC).isoformat(),
          'stop_reason': reason, 'response_bundles': bundles, 'total_requested_weight': attempts * 5}
(root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
provenance = {'schema_version': 2, 'origin': registration['origin'], 'request_count': attempts,
              'failures': failures, 'start': started.isoformat(), 'end': end.isoformat(),
              'response_bundles': bundles,
              'registration_sha256': hashlib.sha256(registration_path.read_bytes()).hexdigest()}
(root / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
print(json.dumps({'state': 'finished', **result}), flush=True)
