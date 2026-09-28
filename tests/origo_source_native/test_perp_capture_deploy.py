"""Execute the deployment/rollback control flow; live capture evidence is separate."""
from __future__ import annotations

import os
import subprocess
import textwrap
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_capture_service_has_only_its_owned_resources() -> None:
    for filename in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        compose = yaml.safe_load((ROOT / filename).read_text())
        capture = compose['services']['perp-capture']
        assert capture['command'] == 'python -m origo.workers.perp_capture'
        assert capture['network_mode'] == 'host'
        assert capture['user'] == '0:0'
        assert capture['cpus'] == 1 and capture['mem_limit'] == '512m'
        assert capture['read_only'] is True and capture['cap_drop'] == ['ALL']
        assert capture['security_opt'] == ['no-new-privileges:true']
        assert set(capture['volumes']) == {
            'perp-capture:/var/lib/origo-perp-capture',
            'source-locks:/opt/origo/locks', 'worker-heartbeats:/opt/origo/heartbeats',
        }
        assert set(capture['environment']) == {
            'PYTHONPATH', 'PYTHONDONTWRITEBYTECODE', 'ORIGO_PERP_CAPTURE_ROOT',
            'ORIGO_SOURCE_LOCK_DIR',
        }
        assert not {'depends_on', 'ports', 'env_file', 'secrets'} & capture.keys()
        repair = compose['services']['provisional-binance-perp-trades']
        assert 'perp-capture:/var/lib/origo-perp-capture' in repair['volumes']
        if filename == 'docker-compose.deploy.yml':
            assert repair['environment']['ORIGO_BINANCE_PERP_EGRESS_IPS'] == '37.27.112.144'
            assert capture['image'] == repair['image']


def test_capture_handover_refuses_unretired_or_misconfigured_repair(tmp_path: Path) -> None:
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    start = workflow.index('          # Capture owns .140')
    end = workflow.index('          # Save only worker identity', start)
    handover = textwrap.dedent(workflow[start:end])
    stub = r'''
set -euo pipefail
PROJECT_NAME=isolated-capture-test
function docker() {
  case "$*" in
    'ps -q --filter label=com.docker.compose.project=isolated-capture-test --filter label=com.docker.compose.service=provisional-binance-perp-trades')
      if [ -f "$CALLS" ] && [ "$CASE" = missing ]; then return 0; fi
      printf 'repair\n' ;;
    'stop repair')
      printf 'stop\n' >> "$CALLS"
      if [ "$CASE" = stop-failed ]; then return 1; fi ;;
    'compose -p isolated-capture-test -f docker-compose.deploy.yml up -d --no-deps --force-recreate provisional-binance-perp-trades')
      printf 'repair-only\n' >> "$CALLS"
      if [ "$CASE" = recreate-failed ]; then return 1; fi ;;
    'inspect --format {{range .Config.Env}}'*)
      if [ "$CASE" = inspect-failed ]; then return 1; fi
      if [ "$CASE" != dual-ip ]; then printf repair-only; fi ;;
    'compose -p isolated-capture-test -f docker-compose.deploy.yml up -d --no-deps perp-capture')
      if read -r swallowed; then return 5; fi
      printf 'capture\n' >> "$CALLS" ;;
    *) printf 'Unexpected Docker call: %s\n' "$*" >&2; return 3 ;;
  esac
}
'''
    for case in ('ok', 'stop-failed', 'recreate-failed', 'missing', 'inspect-failed', 'dual-ip'):
        calls = tmp_path / case
        result = subprocess.run(['bash', '-s'], input=stub + handover, text=True,
                                env={**os.environ, 'CASE': case, 'CALLS': str(calls)},
                                capture_output=True, check=False)
        observed = calls.read_text().splitlines()
        if case == 'ok':
            assert result.returncode == 0, result.stderr
            assert observed == ['stop', 'repair-only', 'capture']
        else:
            assert result.returncode != 0, case
            assert 'capture' not in observed


def test_rollback_preserves_spool_and_exclusive_ip_roles(tmp_path: Path) -> None:
    document = (ROOT / 'docs/operations/binance_perp_capture.md').read_text()
    rollback = document.split('```bash\n', 1)[1].split('```', 1)[0]
    spool = tmp_path / 'spool'
    spool.mkdir()
    checkpoint = spool / 'ownership-checkpoint'
    checkpoint.write_text('committed control state')
    stub = r'''
PROJECT_NAME=isolated-capture-test
function docker() {
  case "$*" in
    'ps -aq --filter label=com.docker.compose.project=isolated-capture-test --filter label=com.docker.compose.service=perp-capture')
      if [ ! -f "$REMOVED" ] || [ "$CASE" = orphan-remains ]; then printf 'capture-orphan\n'; fi ;;
    'stop capture-orphan')
      printf 'stop\n' >> "$CALLS"
      if [ "$CASE" = stop-failed ]; then return 1; fi ;;
    'rm capture-orphan')
      printf 'remove\n' >> "$CALLS"
      if [ "$CASE" = remove-failed ]; then return 1; fi
      touch "$REMOVED" ;;
    *) printf 'Unexpected Docker call: %s\n' "$*" >&2; return 3 ;;
  esac
}
'''
    for case in ('ok', 'stop-failed', 'remove-failed', 'orphan-remains'):
        calls, removed = tmp_path / f'{case}-calls', tmp_path / f'{case}-removed'
        result = subprocess.run(
            ['bash', '-s'], input=stub + rollback + '\nprintf "dual-ip\\n" >> "$CALLS"\n',
            text=True, capture_output=True, check=False,
            env={**os.environ, 'CASE': case, 'CALLS': str(calls), 'REMOVED': str(removed)},
        )
        observed = calls.read_text().splitlines()
        assert checkpoint.read_text() == 'committed control state'
        if case == 'ok':
            assert result.returncode == 0, result.stderr
            assert observed == ['stop', 'remove', 'dual-ip']
        else:
            assert result.returncode != 0, case
            assert 'dual-ip' not in observed


def test_capture_deploy_recovers_overlap_or_verified_bridge(tmp_path: Path) -> None:
    """Restart the actual collector process over authentic response overlap.

    The local transport replays original bodies without network or pacing; this is
    durable process-restart evidence, not measured Docker/provider rollout latency.
    """
    import base64
    import gzip
    import hashlib
    import json
    import sys
    from datetime import UTC, datetime, timedelta

    from origo.sources.adapters.binance_perp_spool import (
        capture_state, historical_row, read_spooled_revision,
    )
    from origo.sources.contracts import Partition

    fixture = ROOT / 'tests/fixtures/binance/futures/recent_trades/2026-09-21/responses.jsonl.gz'
    provenance = json.loads(fixture.with_name('provenance.json').read_text())
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == provenance['sha256']
    records = [json.loads(line) for line in gzip.decompress(fixture.read_bytes()).splitlines()]
    script = r'''
import base64, gzip, json, os, signal, sys
from pathlib import Path
from origo.sources.adapters.binance_daily import Response
from origo.workers import perp_capture as capture
rows = [json.loads(line) for line in gzip.decompress(Path(os.environ['FIXTURE']).read_bytes()).splitlines()]
selected = iter(rows[int(os.environ['FIRST']):int(os.environ['LAST'])])
remaining = int(os.environ['LAST']) - int(os.environ['FIRST'])
def transport(url, *, params, weight, egress_ip):
    global remaining
    record = next(selected)
    assert url == record['url'] and params == record['params']
    assert weight == 5 and egress_ip == '37.27.112.140'
    remaining -= 1
    if not remaining:
        os.kill(os.getpid(), signal.SIGTERM)
    return Response(base64.b64decode(record['body_base64']), {}, record['status'], egress_ip)
capture.get_response = transport
capture.POLL_INTERVAL_SECONDS = 0
sys.argv = ['perp_capture']
result = capture.main()
assert remaining == 0
print(os.getpid())
raise SystemExit(result)
'''
    spool = tmp_path / 'spool'
    env = {**os.environ, 'FIXTURE': str(fixture), 'ORIGO_PERP_CAPTURE_ROOT': str(spool),
           'ORIGO_HEARTBEAT_DIR': str(tmp_path / 'heartbeats')}
    identities: list[str] = []
    process_ids: list[str] = []
    for first, last in ((0, 80), (80, len(records))):
        result = subprocess.run([sys.executable, '-c', script], cwd=ROOT,
                                env={**env, 'FIRST': str(first), 'LAST': str(last)},
                                capture_output=True, text=True, timeout=90, check=False)
        assert result.returncode == 0, result.stderr
        process_ids.append(result.stdout.strip())
        commit = capture_state(spool)
        assert commit is not None and commit.pending_gaps == 0
        identities.append(commit.segment_id)
    assert process_ids[0] != process_ids[1]
    assert identities[0] == identities[1]
    start = datetime(2026, 9, 21, 15, 11, tzinfo=UTC)
    partition = Partition(start.strftime('%Y-%m-%dT%H:%M:%SZ'), start,
                          start + timedelta(minutes=1), True)
    revision = read_spooled_revision(spool, partition)
    assert revision is not None
    expected = {}
    for record in records:
        for row in json.loads(base64.b64decode(record['body_base64'])):
            if int(start.timestamp() * 1000) <= row['time'] < int(partition.end.timestamp() * 1000):
                expected[row['id']] = historical_row(row)
    assert tuple(revision.rows()) == tuple(expected[key] for key in sorted(expected))
    assert len(expected) == 9948
