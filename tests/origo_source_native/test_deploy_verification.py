from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import pytest
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.test_utils import create_run_for_test, instance_for_test

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.orchestration.verify_deploy import verify_deploy
from origo.sources.contracts import Client
from origo.workers.provisional import selected_specs
from origo.workers.receipts import ensure_monitoring_tables, record_receipt

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github' / 'workflows' / 'deploy_on_merge.yml'
JOB = 'maintain_operational_metadata_job'

# The deployment script's verification block, run against a stand-in `docker` that answers
# `compose config --services`, `compose ps -aq`, `inspect` and the verifier exec. The script
# arrives on stdin (`bash -s`) as it does on the host; `sleep` returns at once. Calls to
# `inspect` are numbered: call 1 is the baseline, calls 2..91 are samples 1..90. The verifier
# stays alive until call VERIFIER_UNTIL or until the script exits, so a slow maintenance run is
# a slow verifier.
HARNESS = r"""
set -euo pipefail
PROJECT_NAME=test
launched_run=run-1
trap 'touch "$COUNTER.exit"' EXIT
sleep() { :; }
docker() {
  if [ "$1" = compose ]; then
    case "$*" in
      *' config --services'*)
        [ -z "${FAIL_CONFIG:-}" ] || return 1
        [ -n "${EMPTY_CONFIG:-}" ] || printf '%s\n' dagster perp-capture vector ;;
      *' ps -aq'*)
        [ -z "${FAIL_PS:-}" ] || return 1
        for service in dagster perp-capture vector; do
          [ "$service" = "${MISSING:-}" ] || echo "$service"
        done ;;
      *' exec -T dagster python -m origo.orchestration.verify_deploy'*)
        echo "$*" >> "$LOG"
        limit=$((SECONDS + 20))
        while seen=$(cat "$COUNTER" 2>/dev/null); [ "${seen:-0}" -lt "${VERIFIER_UNTIL:-0}" ] \
          && [ "$SECONDS" -lt "$limit" ] && [ ! -e "$COUNTER.exit" ]; do
          /bin/sleep 0.01
        done
        return "${VERIFIER_STATUS:-0}" ;;
      *) return 99 ;;
    esac
  else
    [ -z "${FAIL_INSPECT:-}" ] || return 1
    shift 2
    call=$(( $(cat "$COUNTER" 2>/dev/null || echo 0) + 1 ))
    echo "$call" > "$COUNTER"
    for id in "$@"; do
      state=running; health=healthy; restarts=0
      [ "$id" != vector ] || health=none
      if [ "$id" = perp-capture ]; then
        case "${SCENARIO:-steady}" in
          rising) [ "$call" -lt 4 ] || restarts=1 ;;
          late_restart) [ "$call" -lt 45 ] || restarts=1 ;;
          restarting) [ "$call" -lt 4 ] || state=restarting ;;
          exited) [ "$call" -lt 4 ] || state=exited ;;
          starting) health=starting ;;
          unhealthy) health=unhealthy ;;
          recovers) [ "$call" -gt 36 ] || health=unhealthy ;;
          recovers_too_late) [ "$call" -gt 95 ] || health=unhealthy ;;
        esac
      fi
      echo "$id $state $health $restarts"
    done
  fi
}
"""


def _block() -> str:
    lines = WORKFLOW.read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "echo 'post-up: verification starting'")
    end = next(i for i in range(start, len(lines)) if lines[i] == '          REMOTE')
    return textwrap.dedent('\n'.join(lines[start:end])) + '\n'


def _run_block(tmp_path: Path, **environment: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    log = tmp_path / 'verifier.log'
    log.write_text('')
    (tmp_path / 'counter').unlink(missing_ok=True)
    (tmp_path / 'counter.exit').unlink(missing_ok=True)
    script = HARNESS + _block() + "echo 'MARKER: reached the end of the script'\n"
    result = subprocess.run(
        ['bash', '-s'],
        input=script,
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, 'LOG': str(log), 'COUNTER': str(tmp_path / 'counter'), **environment},
    )
    return result, log.read_text().splitlines()


def test_deploy_fails_on_a_crash_looping_unhealthy_or_missing_service(tmp_path: Path) -> None:
    text = WORKFLOW.read_text()
    assert len([line for line in text.splitlines() if 'origo.orchestration.verify_deploy' in line]) == 1
    exec_command = next(line for line in _block().splitlines() if 'verify_deploy' in line)
    assert '</dev/null' in exec_command and '--deadline-seconds 840' in exec_command

    result, calls = _run_block(tmp_path)
    assert result.returncode == 0, result.stderr
    assert 'MARKER' in result.stdout and 'post-up: deployment verified' in result.stdout
    assert len(calls) == 1
    arguments = calls[0].split()
    assert arguments[arguments.index('--run-id') + 1] == 'run-1'
    assert int(arguments[arguments.index('--since') + 1]) > 0
    assert arguments[arguments.index('--deadline-seconds') + 1] == '840'
    assert int((tmp_path / 'counter').read_text()) >= 31

    settled, _ = _run_block(tmp_path, SCENARIO='recovers')
    assert settled.returncode == 0, settled.stderr
    assert 'MARKER' in settled.stdout

    slow, _ = _run_block(tmp_path, VERIFIER_UNTIL='60')
    assert slow.returncode == 0, slow.stderr
    assert 'MARKER' in slow.stdout
    assert int((tmp_path / 'counter').read_text()) >= 60

    changed = 'container states changed during verification'
    not_healthy = 'containers not healthy'
    failing = {
        'rising restart count': ({'SCENARIO': 'rising'}, changed),
        'restart while the verifier still runs': ({'SCENARIO': 'late_restart', 'VERIFIER_UNTIL': '60'}, changed),
        'restarting': ({'SCENARIO': 'restarting'}, changed),
        'exited': ({'SCENARIO': 'exited'}, changed),
        'health starting after the last sample': ({'SCENARIO': 'starting'}, not_healthy),
        'unhealthy after the last sample': ({'SCENARIO': 'unhealthy'}, not_healthy),
        'healthy only after the last sample': ({'SCENARIO': 'recovers_too_late'}, not_healthy),
        'missing container': ({'MISSING': 'perp-capture'}, changed),
        'failing config': ({'FAIL_CONFIG': '1'}, ''),
        'empty config': ({'EMPTY_CONFIG': '1'}, ''),
        'failing ps': ({'FAIL_PS': '1'}, ''),
        'failing inspect': ({'FAIL_INSPECT': '1'}, ''),
        'failing verifier': ({'VERIFIER_STATUS': '1'}, ''),
    }
    for name, (environment, message) in failing.items():
        failed, _ = _run_block(tmp_path, **environment)
        assert failed.returncode != 0, name
        assert 'MARKER' not in failed.stdout, name
        assert 'deployment verified' not in failed.stdout, name
        assert message in failed.stderr, name
    assert 'perp-capture missing missing 0' in _run_block(tmp_path, MISSING='perp-capture')[0].stderr


@pytest.fixture
def instance(tmp_path: Path) -> Iterator[DagsterInstance]:
    with instance_for_test(temp_dir=str(tmp_path)) as value:
        yield value


def _client() -> Client:
    return make_clickhouse_client(get_clickhouse_settings())


def _receipt(
    client: Client, key: str, minute: datetime, *, series: str | None = None,
    status: Literal['OK', 'FAILED'] = 'OK',
) -> None:
    record_receipt(
        client, 'origo', feed='provisional', series=series or key, minute=minute, rows=1,
        sha256='', duration_ms=1, status=status,
    )


def _last_closed(since: datetime) -> datetime:
    return since.replace(second=0, microsecond=0) - timedelta(minutes=1)


class _Time:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_verifier_needs_the_launched_run_and_a_fresh_receipt_from_every_enabled_source(
    origo_test_env: dict[str, str], instance: DagsterInstance
) -> None:
    keys = tuple(spec.key for spec in selected_specs({}))
    assert len(keys) >= 4
    victim = keys[0]
    succeeded = create_run_for_test(instance, job_name=JOB, status=DagsterRunStatus.SUCCESS).run_id
    client = _client()
    try:
        ensure_monitoring_tables(client, 'origo')

        def verify(run_id: str, since: datetime, timer: _Time | None = None) -> list[str]:
            timer = timer or _Time()
            return verify_deploy(
                instance, client, 'origo', run_id=run_id, since=since, deadline_seconds=30,
                poll_seconds=10, clock=timer.clock, sleep=timer.sleep,
            )

        def fresh(since: datetime, *, skip: tuple[str, ...] = ()) -> None:
            for key in keys:
                if key not in skip:
                    _receipt(client, key, _last_closed(since))

        def reset() -> None:
            client.execute('TRUNCATE TABLE origo.worker_minute_log')

        since = datetime.now(UTC) - timedelta(seconds=30)
        fresh(since)
        assert verify(succeeded, since) == []

        unmet: dict[str, list[str]] = {}
        reset()
        fresh(since, skip=(victim,))
        unmet['no receipt'] = verify(succeeded, since)

        reset()
        fresh(since, skip=(victim,))
        _receipt(client, victim, _last_closed(since), status='FAILED')
        unmet['failed receipt'] = verify(succeeded, since)

        reset()
        fresh(since, skip=(victim,))
        _receipt(client, victim, _last_closed(since), series=f'{victim}:mount')
        unmet['mount publication only'] = verify(succeeded, since)

        reset()
        fresh(since, skip=(victim,))
        _receipt(client, victim, _last_closed(since) - timedelta(minutes=1))
        unmet['older minute'] = verify(succeeded, since)

        reset()
        _receipt(client, victim, _last_closed(datetime.now(UTC)))
        time.sleep(2.2)
        late = datetime.now(UTC)
        fresh(late, skip=(victim,))
        unmet['recorded before since'] = verify(succeeded, late)

        for name, conditions in unmet.items():
            assert len(conditions) == 1 and f'source {victim} ' in conditions[0], (name, conditions)
        assert not any(key in unmet['no receipt'][0] for key in keys if key != victim)

        reset()
        fresh(since)
        running = create_run_for_test(instance, job_name=JOB, status=DagsterRunStatus.STARTED).run_id
        timer = _Time()
        waiting = verify(running, since, timer)
        assert len(waiting) == 1 and running in waiting[0] and 'STARTED' in waiting[0]
        assert timer.sleeps and timer.now >= 30
        for status in (DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED):
            ended = create_run_for_test(instance, job_name=JOB, status=status).run_id
            timer = _Time()
            conditions = verify(ended, since, timer)
            assert len(conditions) == 1 and ended in conditions[0] and status.value in conditions[0]
            assert timer.sleeps == []
        timer = _Time()
        conditions = verify('no-such-run', since, timer)
        assert len(conditions) == 1 and 'no-such-run does not exist' in conditions[0]
        assert timer.sleeps == []
    finally:
        client.disconnect()


def test_verifier_process_exits_nonzero_and_names_each_unmet_condition(
    origo_test_env: dict[str, str], tmp_path: Path
) -> None:
    keys = tuple(spec.key for spec in selected_specs({}))
    with instance_for_test(temp_dir=str(tmp_path)) as instance:
        run_id = create_run_for_test(instance, job_name=JOB, status=DagsterRunStatus.SUCCESS).run_id
        since = datetime.now(UTC) - timedelta(seconds=30)
        client = _client()
        try:
            ensure_monitoring_tables(client, 'origo')
            for key in keys[:-1]:
                _receipt(client, key, _last_closed(since))

            def run() -> subprocess.CompletedProcess[str]:
                return subprocess.run(
                    [
                        sys.executable, '-m', 'origo.orchestration.verify_deploy', '--run-id', run_id,
                        '--since', str(int(since.timestamp())), '--deadline-seconds', '2',
                    ],
                    cwd=ROOT, capture_output=True, text=True, timeout=300,
                )

            failed = run()
            assert failed.returncode == 1, failed.stderr
            assert f'source {keys[-1]} has no OK minute receipt' in failed.stderr
            assert all(f'source {key} ' not in failed.stderr for key in keys[:-1])

            _receipt(client, keys[-1], _last_closed(since))
            verified = run()
            assert verified.returncode == 0, verified.stderr
            assert 'has no OK minute receipt' not in verified.stderr
            assert 'maintenance run' not in verified.stderr
        finally:
            client.disconnect()
