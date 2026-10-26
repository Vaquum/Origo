from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml
from dagster import DagsterInstance


from .test_dagster_metadata_maintenance import metadata_instance as metadata_instance

ROOT = Path(__file__).resolve().parents[2]


def test_book_pause_stops_only_retired_admission_and_requires_quiescence(
    metadata_instance: DagsterInstance,
) -> None:
    """Metadata controls reject queued old work without stopping unrelated work or losing cursors."""
    from dagster import AssetKey, DagsterRunStatus
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
    from dagster._core.remote_origin import (
        ManagedGrpcPythonEnvCodeLocationOrigin,
        RemoteInstigatorOrigin,
        RemoteRepositoryOrigin,
    )
    from dagster._core.scheduler.instigation import (
        InstigatorState,
        InstigatorStatus,
        InstigatorType,
        SensorInstigatorData,
    )
    from dagster._core.test_utils import create_run_for_test
    from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

    from origo.sources.prepare import retire_book_admission

    instance = metadata_instance
    repository = RemoteRepositoryOrigin(
        ManagedGrpcPythonEnvCodeLocationOrigin(
            LoadableTargetOrigin(module_name='origo.definitions'), location_name='origo'),
        '__repository__',
    )
    retired = ('publish_btc_briefing_feed_sensor', 'publish_btc_briefing_history_sensor',
               'binance_spot_book_reconciliation_sensor', 'binance_perp_book_failure_sensor')
    for name in (*retired, 'binance_spot_trades_reconciliation_sensor'):
        instance.add_instigator_state(InstigatorState(
            RemoteInstigatorOrigin(repository, name), InstigatorType.SENSOR,
            InstigatorStatus.RUNNING, SensorInstigatorData(cursor='preserved-cursor'),
        ))
    with pytest.raises(RuntimeError, match='admission remains enabled'):
        retire_book_admission(instance, check=True)
    retire_book_admission(instance)
    for state in instance.all_instigator_state():
        assert state.status == (InstigatorStatus.STOPPED if state.name in retired else InstigatorStatus.RUNNING)
        assert state.sensor_instigator_data.cursor == 'preserved-cursor'
    unrelated = create_run_for_test(instance, job_name='refresh_binance_spot_trades_canonical_source_job')
    for job in ('audit_binance_spot_book_source_job', 'binance_spot_depth20_1m_job',
                'binance_spot_depth200_1m_job', 'publish_btc_briefing_job',
                'build_depth_snapshot_store_arrow_job'):
        run = create_run_for_test(instance, job_name=job)
        with pytest.raises(RuntimeError, match='book work remains active'):
            retire_book_admission(instance)
        instance.report_run_canceled(run)
        retire_book_admission(instance)
    backfill = PartitionBackfill(
        backfill_id='retired-book-admission-control', status=BulkActionStatus.REQUESTED,
        from_failure=False, tags={}, backfill_timestamp=0.0,
        asset_selection=[AssetKey('build_binance_perp_book_canonical_revision_origo')],
    )
    instance.add_backfill(backfill)
    with pytest.raises(RuntimeError, match='book backfill remains active'):
        retire_book_admission(instance)
    instance.update_backfill(backfill.with_status(BulkActionStatus.CANCELED))
    retire_book_admission(instance)
    retire_book_admission(instance, check=True)
    assert instance.get_run_by_id(unrelated.run_id).status == DagsterRunStatus.NOT_STARTED




@pytest.mark.parametrize('filename', ['docker-compose.yml', 'docker-compose.deploy.yml'])
def test_only_render_book_workers_are_deployed_with_exact_inputs(filename: str) -> None:
    compose = yaml.safe_load((ROOT / filename).read_text())
    services = compose['services']
    for market in ('spot', 'perp'):
        render = services[f'provisional-binance-{market}-top20']
        assert render['command'] == 'python -m origo.workers.provisional'
        assert render['environment']['ORIGO_PROVISIONAL_SOURCE'] == f'binance_{market}_top20_v1'
        assert render['environment']['POLARS_MAX_THREADS'] == '1'
        assert render['cpus'] == 1 and render['mem_limit'] == '2g'
        assert 'book-spool:/var/lib/origo-book-spool' in render['volumes']
        assert {'source-locks:/opt/origo/locks', 'source-publications:/opt/origo/shadow',
                'worker-heartbeats:/opt/origo/heartbeats'} <= set(render['volumes'])
        assert any(volume.endswith(':/opt/arrow') for volume in render['volumes'])
        assert not any(key.startswith(('BINANCE_', 'HUGGINGFACE_')) for key in render['environment'])
        assert set(render['depends_on']) == {'clickhouse'}
        assert render['healthcheck']['test'] == ['CMD', 'python', '-m', 'origo.workers.provisional', '--check']
    # Render the actual Compose boundary using isolated configuration values, without starting services.
    variables = set(re.findall(r'\$\{([A-Z_][A-Z0-9_]*)', (ROOT / filename).read_text()))
    environment = {**os.environ, **{key: 'isolated' for key in variables},
                   'APP_IMAGE': 'origo:test', 'CLICKHOUSE_IMAGE': 'origo-clickhouse:test'}
    rendered = json.loads(subprocess.check_output(
        ['docker', 'compose', '-f', str(ROOT / filename), 'config', '--format', 'json'],
        env=environment, text=True,
    ))['services']
    for market in ('spot', 'perp'):
        assert set(rendered[f'provisional-binance-{market}-top20']['environment']) == {
            'PYTHONPATH', 'DAGSTER_WEBSERVER_URL', 'CLICKHOUSE_PASSWORD', 'ORIGO_PROVISIONAL_SOURCE',
            'ORIGO_RENDER_BOOK_A_TOKEN', 'ORIGO_RENDER_BOOK_B_TOKEN', 'POLARS_MAX_THREADS',
        }
    for replica in ('A', 'B'):
        key = f'ORIGO_RENDER_BOOK_{replica}_TOKEN'
        assert key in rendered['monitor']['environment']
        assert all(key not in rendered[name]['environment'] for name in ('dagster', 'dagit'))
    assert not {'depth-worker', 'book-capture-spot', 'book-capture-perp',
                'provisional-binance-spot-book', 'provisional-binance-perp-book'} & services.keys()
    for job_service in ('dagster', 'dagit'):
        assert 'book-spool:/var/lib/origo-book-spool' in services[job_service]['volumes']
    assert 'book-spool' in compose['volumes']
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert 'ORIGO_BOOK_TOP20_TOKEN' not in workflow
    for replica in ('A', 'B'):
        token = f'ORIGO_RENDER_BOOK_{replica}_TOKEN'
        assert workflow.count(f'{token}: ${{{{ secrets.{token} }}}}') == 2
        assert f'"{token}": os.environ["{token}"]' in workflow






def test_retired_hourly_vendor_credential_is_absent() -> None:
    """Deployment and importable source code cannot restore the retired provider."""
    for filename in ('docker-compose.yml', 'docker-compose.deploy.yml',
                     '.github/workflows/deploy_on_merge.yml'):
        assert 'cryptohftdata' not in (ROOT / filename).read_text().lower()
    for path in (ROOT / 'origo').rglob('*.py'):
        assert 'cryptohftdata' not in path.read_text().lower(), path
    for filename in ('origo/sources/adapters/book_vendor.py',
                     'origo/sources/adapters/book_columnar.py',
                     'origo/sources/adapters/book_replay.cpp',
                     'tools/benchmark_book_replay.py'):
        assert not (ROOT / filename).exists()


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
    start = workflow.index('          # Before any container is replaced')
    end = workflow.index("          echo 'post-up: ClickHouse smoke test starting'", start)
    script = textwrap.dedent(workflow[start:end])
    import shlex
    import signal
    import time

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
    daemon_home = tmp_path / 'daemon-home'
    daemon_home.mkdir()
    (daemon_home / 'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    (tmp_path / 'workspace.yaml').write_text('load_from: []\n')
    daemon_environment = {**os.environ, 'DAGSTER_HOME': str(daemon_home),
                          'PYTHONPATH': str(ROOT) + os.pathsep + os.environ.get('PYTHONPATH', '')}
    for arguments, ready in (
        ([sys.executable, '-m', 'origo.orchestration.daemon', 'run'], True),
        ([str(Path(sys.executable).with_name('dagster-daemon')), 'run'], False),
        (['/bin/sh', '-ec', 'sleep 30; exec python -m origo.orchestration.daemon run'], False),
    ):
        process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   cwd=tmp_path, env=daemon_environment, start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    output, _ = process.communicate(timeout=10)
                    raise AssertionError(f'Daemon control exited before argv observation: {arguments}: {output!r}')
                proc_args = Path(f'/proc/{process.pid}/cmdline')
                if proc_args.exists():
                    observed_args = proc_args.read_bytes()
                else:
                    # macOS exposes these controlled process arguments through ps.
                    command = subprocess.run(['ps', '-ww', '-p', str(process.pid), '-o', 'command='],
                                             capture_output=True, text=True, check=True, timeout=10)
                    observed_args = b'\0'.join(arg.encode() for arg in shlex.split(command.stdout)) + b'\0'
                if observed_args:
                    break
                assert time.monotonic() < deadline, 'Process argv did not become readable'
                time.sleep(0.01)
            ready_argv.write_bytes(observed_args)
            result = subprocess.run([sys.executable, str(driver), readiness, str(ready_argv)],
                                    capture_output=True, text=True, timeout=10)
            assert (result.returncode == 0) == ready, (observed_args, result.stderr)
            if ready:
                accepted_argv = ready_argv.read_bytes()
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=10)
    ready_argv.write_bytes(accepted_argv)
    deploy = tmp_path / 'deploy'
    deploy.mkdir()
    (deploy / 'prepare_binance_egress.sh').write_text('echo setup >> "$CALLS"\n')
    stub = r'''
set -euo pipefail
PROJECT_NAME=test
function python3() {
  [ "$1" = deploy/render_top20_deploy.py ]
  printf '%s\n' "$*" >> "$CALLS"
  [ "$CASE" != preservation-failed ]
}
function docker() {
  printf '%s\n' "$*" >> "$CALLS"
  case "$*" in
    *' run --rm --no-deps -T --entrypoint python dagster -m origo.sources.compatibility') return 0 ;;
    *' run --rm --no-deps -T --entrypoint python dagster -')
      cat >/dev/null
      [ "$CASE" != preflight-failed ] ;;
    *' run --rm --no-deps -T --entrypoint python provisional-binance-perp-trades '*) return 0 ;;
    *' ps -q provisional-binance-perp-trades '*) printf 'live-capture\n' ;;
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
                          (pinned, 'preflight-failed'), (pinned, 'preservation-failed'),
                          (pinned, 'stop-failed'), (pinned, 'unretired')):
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
                                     'DEPLOY_DIR': str(tmp_path),
                                     'PYTHON': sys.executable, 'READINESS_DRIVER': str(driver),
                                     'READY_ARGV': str(ready_argv)},
                                text=True, capture_output=True)
        observed = calls.read_text()
        if case in ('preflight-failed', 'preservation-failed', 'stop-failed'):
            assert result.returncode != 0 and not replaced.exists()
        elif case == 'unretired':
            assert result.returncode != 0 and 'origo.sources.rollout' not in observed
        else:
            assert result.returncode == 0, result.stderr
            assert '--legacy-before' in observed
            assert 'origo.sources.rollout' in observed
            if mode == 'true':
                assert '--force-recreate dagster dagit monitor law' in observed
                assert observed.index('render_top20_deploy.py check') < observed.index('stop dagster dagit')
                assert observed.index('stop dagster dagit') < observed.index('--force-recreate dagster dagit monitor')
                assert '--force-recreate dagster dagit provisional-worker' not in observed
                assert 'up -d --no-deps perp-capture' not in observed and 'setup' not in observed
                assert 'inspect --format {{.State.Running}} live-capture' in observed
            else:
                assert '--force-recreate dagster dagit provisional-worker' in observed
                assert 'up -d --no-deps perp-capture' in observed and 'setup' in observed
    assert 'backfill_active(instance, spec)' in script
    assert 'groups - SourceStore(client, settings.database, spec).enabled_groups()' in script
    assert workflow.index('python3 - capture') < workflow.index('scp docker-compose.deploy.yml')


def test_book_file_retirement_preserves_shared_stores_and_refuses_old_tables(
    origo_test_env: dict[str, str], metadata_instance: DagsterInstance,
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The actual cleanup body rejects retained old tables and only removes its fixed paths."""
    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.sources.binance_top20 import BINANCE_SPOT_TOP20_SPEC, BINANCE_PERP_TOP20_SPEC
    from origo.sources.storage import SourceStore

    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        for spec in (BINANCE_SPOT_TOP20_SPEC, BINANCE_PERP_TOP20_SPEC):
            SourceStore(client, 'origo', spec).setup(anchor=spec.partitions.start)
        from contextlib import nullcontext

        monkeypatch.setattr(DagsterInstance, 'get', lambda: nullcontext(metadata_instance))
        script = (ROOT / 'deploy/retire_legacy_book_files.sh').read_text()
        body = script.split("<<'PYTHON'\n", 1)[1].split('\nPYTHON', 1)[0]
        for deployed, local in (('/var/lib/origo-book-spool', 'spool'),
                                ('/opt/arrow', 'arrow'), ('/opt/origo/heartbeats', 'heartbeats')):
            body = body.replace(deployed, str(tmp_path / local))
        removed = [tmp_path / path for path in (
            'spool/spot', 'spool/perp', 'arrow/depth20_snapshots', 'arrow/depth200_snapshots')]
        preserved = [tmp_path / path for path in (
            'spool/render-top20-v1', 'arrow/binance_spot_top20_v1', 'arrow/bars',
            'heartbeats/provisional_binance_spot_top20_v1')]
        # Explicit filesystem control markers; they make no market-data claim.
        for directory in (*removed, *preserved):
            directory.mkdir(parents=True)
            (directory / 'cleanup-control.txt').write_text('preserve exact bytes')
        client.execute('CREATE TABLE origo.binance_spot_depth20_snapshots (control UInt8) ENGINE=Memory')
        with pytest.raises(AssertionError):
            exec(compile(body, str(ROOT / 'deploy/retire_legacy_book_files.sh'), 'exec'), {})
        assert all(path.exists() for path in (*removed, *preserved))
        client.execute('DROP TABLE origo.binance_spot_depth20_snapshots')
        for _ in range(2):
            exec(compile(body, str(ROOT / 'deploy/retire_legacy_book_files.sh'), 'exec'), {})
            assert not any(path.exists() for path in removed)
            assert all((path / 'cleanup-control.txt').read_text() == 'preserve exact bytes'
                       for path in preserved)
    finally:
        client.disconnect()
