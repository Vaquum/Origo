from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from origo.sources.adapters import binance_daily
from origo.sources.adapters.book_spool import Market
from origo.workers.book_capture import AttemptBudget

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('filename', ['docker-compose.yml', 'docker-compose.deploy.yml'])
def test_capture_and_provisional_services_deploy_as_disjoint_units(filename: str) -> None:
    compose = yaml.safe_load((ROOT / filename).read_text())
    services = compose['services']
    for market in ('spot', 'perp'):
        capture = services[f'book-capture-{market}']
        worker = services[f'provisional-binance-{market}-book']
        assert capture['command'] == f'python -m origo.workers.book_capture --market {market}'
        assert worker['command'] == 'python -m origo.workers.provisional'
        assert worker['environment']['ORIGO_PROVISIONAL_SOURCE'] == f'binance_{market}_book'
        assert 'ORIGO_PROVISIONAL_SOURCE' not in capture['environment']
        assert capture['read_only'] and capture['cap_drop'] == ['ALL']
        assert capture['cpus'] == 1 and capture['mem_limit'] == '512m'
        assert 'book-spool:/var/lib/origo-book-spool' in worker['volumes']
        assert 'book-spool:/var/lib/origo-book-spool' in capture['volumes']
    assert services['book-capture-spot']['ports'] == ['127.0.0.1:8088:8088']
    assert 'ports' not in services['book-capture-perp']
    for job_service in ('dagster', 'dagit'):
        assert 'book-spool:/var/lib/origo-book-spool' in services[job_service]['volumes']
    assert 'book-spool' in compose['volumes']
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert workflow.count('ORIGO_BOOK_TOP20_TOKEN: ${{ secrets.ORIGO_BOOK_TOP20_TOKEN }}') == 2
    assert '"ORIGO_BOOK_TOP20_TOKEN": os.environ["ORIGO_BOOK_TOP20_TOKEN"]' in workflow
    for market in ('spot', 'perp'):
        assert (
            f'book-capture-{market} provisional-binance' in workflow
            or f'book-capture-{market}' in workflow
        )
        assert f'provisional-binance-{market}-book' in workflow


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_capture_services_share_writable_limiter_volume_and_preserve_cooldown_on_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
) -> None:
    for filename in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        services = yaml.safe_load((ROOT / filename).read_text())['services']
        for name in (f'book-capture-{market}', f'provisional-binance-{market}-book', 'dagster'):
            assert 'source-locks:/opt/origo/locks' in services[name]['volumes']
        assert (
            services[f'book-capture-{market}']['environment']['ORIGO_SOURCE_LOCK_DIR']
            == '/opt/origo/locks'
        )
    # Persist the existing host-family circuit exactly where both Compose callers mount it.
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    family = 'api_binance_com' if market == 'spot' else 'fapi_binance_com'
    ledger = tmp_path / f'binance_rest_budget.{family}.state'
    ledger.write_text('9999999999 9999999999')
    calls: list[str] = []

    def forbidden(*args: object, **kwargs: object) -> object:
        calls.append('outbound')
        pytest.fail('Persisted provider circuit must prevent transport.')

    monkeypatch.setattr(binance_daily, '_request', forbidden)
    first = AttemptBudget(tmp_path, market)
    with pytest.raises(binance_daily.SourceError) as blocked:
        first.seed()
    assert blocked.value.code == 'PROVIDER_RATE_CIRCUIT'
    replacement = AttemptBudget(tmp_path, market)
    with pytest.raises(binance_daily.SourceError):
        replacement.seed()
    assert replacement.evidence()['seed_total'] == 2
    assert not calls and ledger.read_text() == '9999999999 9999999999'


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_provider_pause_does_not_block_capture_process_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market,
) -> None:
    from datetime import UTC, datetime, timedelta
    from origo.workers import book_capture
    from origo.workers.runtime import heartbeat_path, touch_heartbeat

    def unavailable(*args: object, **kwargs: object) -> binance_daily.Response:
        raise binance_daily.SourceError('PROVIDER_RATE_CIRCUIT', 'Controlled provider cooldown.')

    monkeypatch.setattr(book_capture, 'get_response', unavailable)
    budget = AttemptBudget(tmp_path / 'locks', market)
    sampler = book_capture.BookSampler(tmp_path / 'spool', market)
    for _ in range(3):
        with pytest.raises(binance_daily.SourceError, match='Controlled provider cooldown'):
            budget.seed()
    with pytest.raises(binance_daily.SourceError) as limited:
        budget.seed()
    assert limited.value.code == 'BOOK_ATTEMPT_LIMIT'
    directory = tmp_path / 'heartbeats'
    touch_heartbeat(heartbeat_path(directory, f'book_capture_{market}'))
    book_capture.publish_status(sampler, budget, book_capture.status_path(directory, market), limited.value.code)
    now = datetime.now(UTC)
    assert book_capture.check_capture(directory, market, now) == 0
    assert sampler.book is None
    with pytest.raises(binance_daily.SourceError) as unavailable_book:
        sampler.top20(now)
    assert unavailable_book.value.code == 'BOOK_UNAVAILABLE'
    assert book_capture.check_capture(directory, market, now + timedelta(seconds=181)) == 1


def test_vendor_credential_reaches_native_jobs() -> None:
    for filename in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        services = yaml.safe_load((ROOT / filename).read_text())['services']
        for name, service in services.items():
            environment = service.get('environment', [])
            values = list(environment) if isinstance(environment, dict) else environment
            entries = [entry for entry in values if entry.startswith('CRYPTOHFTDATA_API_KEY')]
            if name in ('dagster', 'dagit'):
                expected = ('CRYPTOHFTDATA_API_KEY=${CRYPTOHFTDATA_API_KEY:-}'
                            if filename == 'docker-compose.yml' else
                            'CRYPTOHFTDATA_API_KEY=${CRYPTOHFTDATA_API_KEY:?CRYPTOHFTDATA_API_KEY must be set}')
                assert entries == [expected]
            else:
                assert not entries
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert workflow.count('CRYPTOHFTDATA_API_KEY: ${{ secrets.CRYPTOHFTDATA_API_KEY }}') == 2
    assert '"CRYPTOHFTDATA_API_KEY": os.environ["CRYPTOHFTDATA_API_KEY"]' in workflow
    assert 'umask 077' in workflow


def test_native_jobs_only_rollout_expires_with_version(tmp_path: Path) -> None:
    import json
    import os
    import subprocess
    import sys
    import textwrap
    import tomllib

    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    config = tomllib.loads((ROOT / 'pyproject.toml').read_text())
    pinned = config.get('tool', {}).get('origo', {}).get('release', {}).get(
        'native_jobs_only_version', config['project']['version'])
    major, minor, patch = map(int, pinned.split('.'))
    next_version = f'{major}.{minor}.{patch + 1}'
    mode_code = workflow.split('          python3 - <<\'PYTHON\' >> "$GITHUB_ENV"\n', 1)[1].split('          PYTHON', 1)[0]
    mode_code = textwrap.dedent(mode_code)
    start = workflow.index('          retained_workers=()')
    end = workflow.index("          echo 'post-up: ClickHouse smoke test starting'", start)
    script = textwrap.dedent(workflow[start:end])
    import shlex
    import signal

    readiness_line = next(line for line in script.splitlines() if 'exec -T dagster python -c ' in line)
    readiness = shlex.split(readiness_line.split('exec -T dagster python -c ', 1)[1].split(' </dev/null', 1)[0])[0]
    driver = tmp_path / 'readiness.py'
    driver.write_text("""import sys
from pathlib import Path
read_bytes = Path.read_bytes
observed = Path(sys.argv[2]).read_bytes()
def proc_bytes(self):
    assert str(self) == '/proc/1/cmdline'
    return observed
Path.read_bytes = proc_bytes
exec(sys.argv[1])
""")
    ready_argv = tmp_path / 'ready-argv'
    for arguments, ready in (
        ([sys.executable, '-m', 'origo.orchestration.daemon', 'run'], True),
        ([str(Path(sys.executable).with_name('dagster-daemon')), 'run'], False),
        (['/bin/sh', '-ec', 'sleep 30; exec python -m origo.orchestration.daemon run'], False),
    ):
        process = subprocess.Popen(arguments, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   start_new_session=True)
        try:
            proc_args = Path(f'/proc/{process.pid}/cmdline')
            if proc_args.exists():
                observed_args = proc_args.read_bytes()
            else:
                # macOS exposes these controlled process arguments through ps.
                command = subprocess.run(['ps', '-ww', '-p', str(process.pid), '-o', 'command='],
                                         capture_output=True, text=True, check=True, timeout=10)
                observed_args = b'\0'.join(arg.encode() for arg in shlex.split(command.stdout)) + b'\0'
            assert observed_args
            ready_argv.write_bytes(observed_args)
            result = subprocess.run([sys.executable, str(driver), readiness, str(ready_argv)],
                                    capture_output=True, text=True, timeout=10)
            assert (result.returncode == 0) == ready, (observed_args, result.stderr)
            if ready:
                accepted_argv = ready_argv.read_bytes()
        finally:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
    ready_argv.write_bytes(accepted_argv)
    deploy = tmp_path / 'deploy'
    deploy.mkdir()
    (deploy / 'prepare_binance_egress.sh').write_text('echo setup >> "$CALLS"\n')
    stub = r'''
set -euo pipefail
PROJECT_NAME=test
function docker() {
  printf '%s\n' "$*" >> "$CALLS"
  case "$*" in
    *' run --rm --no-deps -T --entrypoint python dagster -')
      cat >/dev/null
      [ "$CASE" != preflight-failed ] ;;
    *' run --rm --no-deps -T --entrypoint python provisional-binance-perp-trades '*) return 0 ;;
    *' ps -q book-capture-spot '*) printf 'live-capture\n' ;;
    *' ps -q dagster dagit provisional-worker') printf 'old-daemon\nold-ui\nold-spot\n' ;;
    *' ps -q dagster dagit') printf 'old-daemon\nold-ui\n' ;;
    'ps -q --filter '*service=provisional-binance-perp-trades) return 0 ;;
    'ps -q --filter '*service=perp-repair) printf 'repair\n' ;;
    *'inspect --format {{range .Config.Env}}'*) printf repair-only ;;
    'inspect --format {{.Id}} {{.Config.Hostname}} '*) printf '%s host-%s\n' "${@: -1}" "${@: -1}" ;;
    'inspect --format {{.Id}} '*) printf '%s\n' "${@: -1}" ;;
    *' stop dagster dagit') [ "$CASE" != stop-failed ] ;;
    *' up -d '*) touch "$REPLACED" ;;
    'ps -aq --no-trunc') printf 'old-daemon\nold-ui\nold-spot\nnew-daemon\nnew-ui\nlive-capture\n' ;;
    'inspect --format {{.State.Running}} live-capture') printf true ;;
    'inspect --format {{.State.Running}} '*)
      if [ "$CASE" = unretired ]; then printf true; else printf false; fi ;;
    *' exec -T dagster python -c '*) "$PYTHON" "$READINESS_DRIVER" "${@: -1}" "$READY_ARGV" ;;
    *' exec -T dagster python -m origo.orchestration.recovery '*)
      [[ "$*" == *'--retired-worker host-old-daemon'* ]] && [[ "$*" == *'--retired-worker host-old-ui'* ]] ;;
    *' exec -T dagster python -m origo.sources.rollout') return 0 ;;
    *) printf 'Unexpected Docker call: %s\n' "$*" >&2; return 2 ;;
  esac
}
'''
    for version, case in ((pinned, 'ok'), (pinned, 'ok-again'), (next_version, 'full'),
                          (pinned, 'preflight-failed'), (pinned, 'stop-failed'), (pinned, 'unretired')):
        (tmp_path / 'pyproject.toml').write_text(
            f'[project]\nversion={json.dumps(version)}\n[tool.origo.release]\nnative_jobs_only_version={json.dumps(pinned)}\n')
        mode = subprocess.run([sys.executable, '-c', mode_code], cwd=tmp_path,
                              capture_output=True, text=True, check=True).stdout.strip().split('=')[1]
        assert (mode == 'true') == (version == pinned)
        calls = tmp_path / f'calls-{case}'
        replaced = tmp_path / f'replaced-{case}'
        result = subprocess.run(['bash', '-s'], input=stub + script, cwd=tmp_path,
                                env={**os.environ, 'CASE': case, 'CALLS': str(calls),
                                     'REPLACED': str(replaced), 'ORIGO_NATIVE_JOBS_ONLY': mode,
                                     'PYTHON': sys.executable, 'READINESS_DRIVER': str(driver),
                                     'READY_ARGV': str(ready_argv)},
                                text=True, capture_output=True)
        observed = calls.read_text()
        if case in ('preflight-failed', 'stop-failed'):
            assert result.returncode != 0 and not replaced.exists()
        elif case == 'unretired':
            assert result.returncode != 0 and 'origo.sources.rollout' not in observed
        else:
            assert result.returncode == 0, result.stderr
            assert '--legacy-before' in observed
            assert 'origo.sources.rollout' in observed
            if mode == 'true':
                assert '--force-recreate dagster dagit monitor' in observed
                assert observed.index('stop dagster dagit') < observed.index('--force-recreate dagster dagit monitor')
                assert '--force-recreate dagster dagit provisional-worker' not in observed
                assert 'up -d --no-deps perp-capture' not in observed and 'setup' not in observed
                assert 'inspect --format {{.State.Running}} live-capture' in observed
            else:
                assert '--force-recreate dagster dagit provisional-worker' in observed
                assert 'up -d --no-deps perp-capture' in observed and 'setup' in observed
    assert 'backfill_active(instance, spec)' in script
    assert 'groups - SourceStore(client, settings.database, spec).enabled_groups()' in script
