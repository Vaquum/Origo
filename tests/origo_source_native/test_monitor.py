from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, cast

import pytest
import yaml
from dagster import Failure

from origo.alerts.email import AlertSettings, send_alert
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.definitions import MONITOR_CHECK_NAMES, defs, origo_monitor_checks
from origo.workers.dagster_reader import DagsterReader
from origo.workers.monitor import DELIVERY_LAG_SECONDS, CollectorProbe, Monitor
from origo.workers.receipts import ensure_monitoring_tables, record_receipt
from origo.workers.report import Reporter
from origo.workers.runtime import heartbeat_path, touch_heartbeat

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / 'tests/fixtures/dagster/graphql'
NOW = datetime(2026, 9, 17, 14, 30, tzinfo=UTC)


class _Recorder(ThreadingHTTPServer):
    """A local Dagit, collector and Resend stand-in that records every request."""

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, _Handler)
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.graphql: dict[str, object] = {
            path.stem: json.loads(path.read_text()) for path in FIXTURES.glob('*.json')
        }
        self.history_rows = True
        self.history_malformed = False
        self.history_calls: list[str] = []
        self.email_status = 200


class _Handler(BaseHTTPRequestHandler):
    def _respond(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode() if not isinstance(payload, bytes) else payload
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        server = cast(_Recorder, self.server)
        length = int(self.headers.get('Content-Length', '0'))
        body: dict[str, Any] = json.loads(self.rfile.read(length) or b'{}')
        if self.path == '/graphql':
            document = server.graphql.get(str(body.get('operationName')))
            if document is None:
                self._respond(400, {'errors': [{'message': 'unknown operation'}]})
                return
            self._respond(200, document)
            return
        server.posts.append((self.path, {**body, '_authorization': self.headers.get('Authorization', '')}))
        if self.path == '/emails':
            self._respond(server.email_status, {'id': f'email-{len(server.posts)}'})
            return
        if self.path.startswith('/report_asset_'):
            self._respond(200, {})
            return
        self._respond(404, {'error': self.path})

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        server = cast(_Recorder, self.server)
        if self.path.startswith('/history'):
            server.history_calls.append(self.path)
            if server.history_malformed:
                self.wfile.write(b'garbage without a status line\r\n')
                self.close_connection = True
                return
            self._respond(200, b'{"ts": 1}\n' if server.history_rows else b'')
            return
        self._respond(404, {'error': self.path})

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - http.server API
        return None


@pytest.fixture()
def recorder() -> Iterator[_Recorder]:
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    server = _Recorder(('127.0.0.1', port))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _url(server: _Recorder) -> str:
    return f'http://127.0.0.1:{server.server_address[1]}'


class _EmptyClient:
    """A ClickHouse stand-in for tests that do not need receipts or the container log."""

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[tuple[object, ...]]:
        return []

    def disconnect(self) -> None:
        return None


class _FailingClient(_EmptyClient):
    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[tuple[object, ...]]:
        raise RuntimeError('ClickHouse is down')


def _settings(server: _Recorder, **overrides: object) -> AlertSettings:
    values: dict[str, Any] = {
        'resend_api_key': 'test-key',
        'resend_api_url': _url(server) + '/emails',
        'email_from': 'origo@example.test',
        'email_to': ('operator@example.test',),
        'cooldown_seconds': 21600,
        'queue_threshold': 200,
        'digest_hour_utc': 3,
    }
    values.update(overrides)
    return AlertSettings(**values)


def _monitor(
    server: _Recorder,
    tmp_path: Path,
    *,
    client: object | None = None,
    dagster_url: str | None = None,
    probes: tuple[CollectorProbe, ...] = (),
    settings: AlertSettings | None = None,
) -> Monitor:
    return Monitor(
        dagster=DagsterReader(dagster_url or _url(server), timeout_seconds=2.0),
        client=cast(Any, client or _EmptyClient()),
        database='origo',
        heartbeat_dir=tmp_path / 'heartbeats',
        probes=probes,
        settings=settings if settings is not None else _settings(server),
        reporter=Reporter(_url(server), timeout_seconds=2.0),
        cursor_path=tmp_path / 'monitor.cursor.json',
    )


def _emails(server: _Recorder) -> list[dict[str, Any]]:
    return [body for path, body in server.posts if path == '/emails']


def _check_posts(server: _Recorder) -> list[dict[str, Any]]:
    return [body for path, body in server.posts if path == '/report_asset_check/origo_monitor']


def _failure_keys() -> set[str]:
    runs = json.loads((FIXTURES / 'Failures.json').read_text())['data']['runsOrError']['results']
    keys: set[str] = set()
    for run in runs:
        partition = next((t['value'] for t in run['tags'] if t['key'] == 'dagster/partition'), '')
        keys.add(f'run_failure:{run["jobName"]}:{partition}')
    return keys


def test_monitor_reports_run_failures_once_and_suppresses_repeats_within_cooldown(
    recorder: _Recorder, tmp_path: Path
) -> None:
    monitor = _monitor(recorder, tmp_path)
    first = monitor.tick(NOW)
    expected = _failure_keys()
    assert expected and set(first.failed) >= expected
    emails = _emails(recorder)
    assert len(emails) == 1
    for key in expected:
        assert key in emails[0]['text']
    assert all(post['passed'] is False for post in _check_posts(recorder) if post['check_name'] == 'queue_bounded')
    second = monitor.tick(NOW + timedelta(minutes=1))
    assert set(second.failed) >= expected
    assert len(_emails(recorder)) == 1
    assert len(_check_posts(recorder)) == 10
    later = monitor.tick(NOW + timedelta(hours=7))
    assert set(later.failed) >= expected
    assert len(_emails(recorder)) == 2


def test_monitor_reports_failed_checks_and_queue_backlog(recorder: _Recorder, tmp_path: Path) -> None:
    health = cast(dict[str, Any], recorder.graphql['Health'])
    health['data']['queued']['count'] = 501
    recorder.graphql['Failures'] = {'data': {'runsOrError': {'__typename': 'Runs', 'results': []}}}
    # The execution's own timestamp is its run's start: an hour ago, as for a check evaluated
    # inside a queued or long run. The evaluation was stored inside this tick's window.
    recorder.graphql['CheckExecutions'] = {
        'data': {
            'assetCheckExecutions': [
                {
                    'status': 'FAILED',
                    'timestamp': (NOW - timedelta(hours=1)).timestamp(),
                    'evaluation': {'timestamp': NOW.timestamp()},
                }
            ]
        }
    }
    checks = cast(dict[str, Any], recorder.graphql['Checks'])
    declared = {
        f'check_failed:{"/".join(node["assetKey"]["path"])}:{check["name"]}'
        for node in checks['data']['assetNodes']
        for check in node['assetChecksOrError'].get('checks', [])
    }
    assert declared
    monitor = _monitor(recorder, tmp_path)
    outcome = monitor.tick(NOW)
    assert 'queue_backlog' in outcome.failed
    assert declared <= set(outcome.failed)
    # The same evaluation is behind the cursor on the next tick, so it is not found again.
    again = monitor.tick(NOW + timedelta(minutes=1))
    assert 'queue_backlog' in again.failed
    assert not declared & set(again.failed)

    health['data']['queued']['count'] = 3
    recorder.graphql['CheckExecutions'] = {
        'data': {
            'assetCheckExecutions': [
                {
                    'status': 'SUCCEEDED',
                    'timestamp': NOW.timestamp(),
                    'evaluation': {'timestamp': NOW.timestamp()},
                }
            ]
        }
    }
    quiet = _monitor(recorder, tmp_path / 'quiet').tick(NOW)
    assert quiet.failed == ()
    assert len(_emails(recorder)) == 1


def test_dagit_unreachable_is_itself_a_finding(recorder: _Recorder, tmp_path: Path) -> None:
    stale = heartbeat_path(tmp_path / 'heartbeats', 'depth')
    touch_heartbeat(stale)
    old = NOW.timestamp() - 600
    os.utime(stale, (old, old))
    outcome = _monitor(recorder, tmp_path, dagster_url='http://127.0.0.1:1').tick(NOW)
    assert 'dagster_unreachable' in outcome.failed
    # The other sources are still evaluated in the same tick and every check is written.
    assert 'heartbeat_stale:depth' in outcome.failed
    assert {post['check_name'] for post in _check_posts(recorder)} == set(MONITOR_CHECK_NAMES)
    assert 'dagster_unreachable' in _emails(recorder)[0]['text']
    # The failure cursor did not move while Dagit was down: the same cursor file, a
    # reachable Dagit, and every fixture failure is reported.
    recovered = _monitor(recorder, tmp_path).tick(NOW + timedelta(minutes=1))
    assert 'dagster_unreachable' not in recovered.failed
    assert _failure_keys() <= set(recovered.failed)


def test_monitor_flags_stale_heartbeats_and_failed_receipts(
    recorder: _Recorder, tmp_path: Path, origo_test_env: dict[str, str]
) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        ensure_monitoring_tables(client, 'origo')
        record_receipt(
            client, 'origo', feed='depth', series='depth20_snapshots',
            minute=datetime(2026, 9, 17, 14, 28, tzinfo=UTC), rows=0, sha256='', duration_ms=12,
            status='FAILED', error_code='PROVIDER_HTTP_503', error='collector answered 503',
        )
        record_receipt(
            client, 'origo', feed='depth', series='depth200_snapshots',
            minute=datetime(2026, 9, 17, 14, 28, tzinfo=UTC), rows=40, sha256='ab', duration_ms=9,
            status='OK',
        )
        fresh = heartbeat_path(tmp_path / 'heartbeats', 'provisional')
        touch_heartbeat(fresh)
        stale = heartbeat_path(tmp_path / 'heartbeats', 'depth')
        touch_heartbeat(stale)
        old = time.time() - 181
        os.utime(stale, (old, old))
        tick_time = datetime.now(UTC) + timedelta(seconds=DELIVERY_LAG_SECONDS + 30)
        outcome = _monitor(recorder, tmp_path, client=client).tick(tick_time)
        assert 'heartbeat_stale:depth' in outcome.failed
        assert 'heartbeat_stale:provisional' not in outcome.failed
        assert 'receipt_failed:depth:depth20_snapshots' in outcome.failed
        assert 'receipt_failed:depth:depth200_snapshots' not in outcome.failed
        alive = [post for post in _check_posts(recorder) if post['check_name'] == 'workers_alive']
        assert alive and alive[0]['passed'] is False
        assert 'PROVIDER_HTTP_503' in _emails(recorder)[0]['text']
    finally:
        client.disconnect()


def test_monitor_distinguishes_collector_outage_from_worker_silence(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('DEPTH20_PROBE_URL', _url(recorder))
    monkeypatch.setenv('DEPTH20_PROBE_TOKEN', 'probe-token')
    probes = (CollectorProbe('depth20', 'DEPTH20_PROBE_URL', 'DEPTH20_PROBE_TOKEN'),)
    recorder.history_rows = False
    outcome = _monitor(recorder, tmp_path, probes=probes).tick(NOW)
    assert 'collector_silent:depth20' in outcome.failed
    assert not any(key.startswith('heartbeat_stale') for key in outcome.failed)
    minute = int((NOW - timedelta(minutes=1)).timestamp())
    assert recorder.history_calls == [f'/history?from={minute}&to={minute}']
    serving = [post for post in _check_posts(recorder) if post['check_name'] == 'collectors_serving']
    assert serving[0]['passed'] is False

    recorder.history_rows = True
    healthy = _monitor(recorder, tmp_path / 'healthy', probes=probes).tick(NOW)
    assert not any(key.startswith('collector_silent') for key in healthy.failed)


def test_monitor_alerts_on_error_rows_in_container_log(
    recorder: _Recorder, tmp_path: Path, origo_test_env: dict[str, str]
) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        ensure_monitoring_tables(client, 'origo')
        stamp = (datetime.now(UTC) - timedelta(minutes=2)).replace(tzinfo=None)
        client.execute(
            'INSERT INTO origo.container_log VALUES',
            [
                (stamp, 'dagster', 'origo-dagster-1', 'stderr', 'ERROR', 'Traceback (most recent call last)'),
                (stamp, 'dagster', 'origo-dagster-1', 'stderr', 'ERROR', 'RuntimeError: boom'),
                (stamp, 'vector', 'origo-vector-1', 'stdout', 'INFO', 'Vector has started.'),
                (stamp, 'clickhouse', 'origo-clickhouse-1', 'stderr', 'ERROR', '<Error> merge failed'),
            ],
        )
        outcome = _monitor(recorder, tmp_path, client=client).tick(datetime.now(UTC))
        assert {key for key in outcome.failed if key.startswith('error_logs:')} == {
            'error_logs:clickhouse',
            'error_logs:dagster',
        }
        logs = [post for post in _check_posts(recorder) if post['check_name'] == 'no_error_logs']
        assert logs[0]['passed'] is False and logs[0]['metadata']['findings'] == 2
        assert 'RuntimeError: boom' in _emails(recorder)[0]['text']
    finally:
        client.disconnect()


def test_monitor_writes_checks_to_dagit_before_sending_one_email(
    recorder: _Recorder, tmp_path: Path
) -> None:
    outcome = _monitor(recorder, tmp_path).tick(NOW)
    assert outcome.failed
    paths = [path for path, _ in recorder.posts]
    assert paths.count('/emails') == 1
    assert paths[:5] == ['/report_asset_check/origo_monitor'] * 5
    assert paths.index('/emails') > 4
    checks = _check_posts(recorder)
    assert sorted(post['check_name'] for post in checks) == sorted(MONITOR_CHECK_NAMES)
    assert all(post['metadata']['evaluated_at'] == NOW.isoformat() for post in checks)
    email = _emails(recorder)[0]
    assert email['_authorization'] == 'Bearer test-key'
    assert email['to'] == ['operator@example.test']
    assert 'Dagit check evaluations: written.' in email['text']
    for key in outcome.failed:
        assert key in email['text']

    # A Dagit that refuses the write is reported in the same e-mail.
    second = _monitor(recorder, tmp_path / 'refused', dagster_url=_url(recorder))
    second.reporter = Reporter('http://127.0.0.1:1', timeout_seconds=0.5)
    second.tick(NOW)
    assert 'Dagit check evaluations: FAILED to write.' in _emails(recorder)[-1]['text']


def test_daily_digest_is_sent_once_at_the_configured_hour(recorder: _Recorder, tmp_path: Path) -> None:
    recorder.graphql['Failures'] = {'data': {'runsOrError': {'__typename': 'Runs', 'results': []}}}
    monitor = _monitor(recorder, tmp_path, settings=_settings(recorder, digest_hour_utc=NOW.hour))
    monitor.tick(NOW)
    monitor.tick(NOW + timedelta(minutes=1))
    digests = [email for email in _emails(recorder) if 'daily digest' in email['subject']]
    assert len(digests) == 1
    assert digests[0]['subject'] == f'Origo daily digest {NOW.date().isoformat()}'
    assert 'ticks: 1' in digests[0]['text']
    monitor.tick(NOW + timedelta(days=1))
    assert len([email for email in _emails(recorder) if 'daily digest' in email['subject']]) == 2
    quiet = _monitor(recorder, tmp_path / 'quiet', settings=_settings(recorder, digest_hour_utc=(NOW.hour + 1) % 24))
    quiet.tick(NOW)
    assert len([email for email in _emails(recorder) if 'daily digest' in email['subject']]) == 2


def test_send_alert_posts_to_resend(recorder: _Recorder) -> None:
    settings = _settings(recorder)
    send_alert(settings, 'Origo alert: 1 new finding', 'body line\n')
    (path, body), = [(path, body) for path, body in recorder.posts if path == '/emails']
    assert body['_authorization'] == 'Bearer test-key'
    assert (body['from'], body['to'], body['subject'], body['text']) == (
        'origo@example.test',
        ['operator@example.test'],
        'Origo alert: 1 new finding',
        'body line\n',
    )
    recorder.email_status = 500
    with pytest.raises(RuntimeError, match='HTTP 500'):
        send_alert(settings, 'again', 'body')
    with pytest.raises(RuntimeError, match='Alert delivery failed'):
        send_alert(_settings(recorder, resend_api_url='http://127.0.0.1:1/emails'), 'down', 'body')


def test_monitor_checks_are_declared_in_definitions() -> None:
    keys = sorted(key.to_user_string() for checks in defs.asset_checks for key in checks.check_keys)
    assert keys == [f'origo_monitor:{name}' for name in MONITOR_CHECK_NAMES]
    assert sorted(MONITOR_CHECK_NAMES) == [
        'collectors_serving', 'dagster_reachable', 'no_error_logs', 'queue_bounded', 'workers_alive',
    ]
    with pytest.raises(Failure, match='evaluated by the monitor worker'):
        list(origo_monitor_checks.op.compute_fn.decorated_fn())


def test_monitoring_doc_states_the_model_and_investigation_order() -> None:
    text = (REPO_ROOT / 'docs/Developer/Monitoring.md').read_text()
    for heading in (
        '## One truth, one pane, one detector',
        '## Where each fact lives',
        '## Investigation order',
        '## Alerts and the daily digest',
        '## Rules that must not change',
    ):
        assert heading in text
    order = [text.index(step) for step in ('Dagit first', 'ClickHouse second', 'Docker third', 'collectors last')]
    assert order == sorted(order)
    assert 'docs/Developer/Monitoring.md' in (REPO_ROOT / 'AGENTS.md').read_text()


def test_settings_and_deployment_wiring_are_complete() -> None:
    assert AlertSettings.from_environment({}) is None
    with pytest.raises(RuntimeError, match='ORIGO_ALERT_EMAIL_TO'):
        AlertSettings.from_environment({'ORIGO_ALERT_RESEND_API_KEY': 'k', 'ORIGO_ALERT_EMAIL_FROM': 'a@b'})
    settings = AlertSettings.from_environment(
        {
            'ORIGO_ALERT_RESEND_API_KEY': 'k',
            'ORIGO_ALERT_EMAIL_FROM': 'onboarding@resend.dev',
            'ORIGO_ALERT_EMAIL_TO': 'one@example.test, two@example.test',
            'ORIGO_ALERT_COOLDOWN_SECONDS': '600',
        }
    )
    assert settings == AlertSettings('k', 'https://api.resend.com/emails', 'onboarding@resend.dev', ('one@example.test', 'two@example.test'), 600, 200, 7)
    for name in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        compose = yaml.safe_load((REPO_ROOT / name).read_text())
        for service in ('monitor', 'vector'):
            assert service in compose['services'], name
            assert compose['services'][service]['restart'] == 'unless-stopped'
        monitor = compose['services']['monitor']
        assert monitor['command'] == 'python -m origo.workers.monitor'
        assert monitor['healthcheck']['test'] == ['CMD', 'python', '-m', 'origo.workers.monitor', '--check']
        assert 'worker-heartbeats:/opt/origo/heartbeats' in monitor['volumes']
        assert '/var/run/docker.sock:/var/run/docker.sock:ro' in compose['services']['vector']['volumes']
        assert './deploy/vector.yaml:/etc/vector/vector.yaml:ro' in compose['services']['vector']['volumes']
        assert 'worker-heartbeats' in compose['volumes']
    deploy = yaml.safe_load((REPO_ROOT / 'docker-compose.deploy.yml').read_text())
    environment = deploy['services']['monitor']['environment']
    assert sum(entry.startswith('ORIGO_ALERT_') for entry in environment) == 6
    assert 'ORIGO_ALERT_RESEND_API_KEY=${RESEND_API_KEY:?RESEND_API_KEY is required}' in environment
    vector = yaml.safe_load((REPO_ROOT / 'deploy/vector.yaml').read_text())
    assert vector['sinks']['clickhouse']['table'] == 'container_log'
    assert vector['sinks']['clickhouse']['database'] == 'origo'
    assert vector['sources']['containers']['type'] == 'docker_logs'
    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert 'scp deploy/vector.yaml' in workflow
    assert 'test -n "$RESEND_API_KEY"' in workflow and 'test -n "$ORIGO_ALERT_EMAIL_TO"' in workflow
    assert 'up -d --wait --wait-timeout 600 clickhouse dagster dagit monitor vector' in workflow


def test_a_failing_detector_is_a_finding_and_the_tick_still_reports(
    recorder: _Recorder, tmp_path: Path
) -> None:
    monitor = _monitor(recorder, tmp_path, client=_FailingClient())
    outcome = monitor.tick(NOW)
    assert {'detector_failed:workers', 'detector_failed:logs'} <= set(outcome.failed)
    assert _failure_keys() <= set(outcome.failed)
    checks = _check_posts(recorder)
    assert sorted(post['check_name'] for post in checks) == sorted(MONITOR_CHECK_NAMES)
    assert {post['check_name']: post['passed'] for post in checks}['workers_alive'] is False
    assert {post['check_name']: post['passed'] for post in checks}['no_error_logs'] is False
    email = _emails(recorder)[0]
    assert 'detector_failed:workers' in email['text'] and 'ClickHouse is down' in email['text']
    cursor = json.loads((tmp_path / 'monitor.cursor.json').read_text())
    start = (NOW - timedelta(minutes=Monitor.lookback_minutes)).isoformat()
    # The detectors that could not read leave their cursors where they were; the Dagster
    # detector completed and advanced.
    assert cursor['receipts_after'] == start and cursor['logs_after'] == start
    assert cursor['failures_after'] == NOW.timestamp()
    healthy = _monitor(recorder, tmp_path).tick(NOW + timedelta(minutes=1))
    assert not any(key.startswith('detector_failed') for key in healthy.failed)
    cursor = json.loads((tmp_path / 'monitor.cursor.json').read_text())
    window_end = NOW + timedelta(minutes=1) - timedelta(seconds=DELIVERY_LAG_SECONDS)
    assert cursor['receipts_after'] == window_end.isoformat()
    assert cursor['logs_after'] == window_end.isoformat()


def test_late_deliveries_and_malformed_collector_responses_are_still_reported(
    recorder: _Recorder, tmp_path: Path, origo_test_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        ensure_monitoring_tables(client, 'origo')
        first = datetime.now(UTC)
        monitor = _monitor(recorder, tmp_path, client=client)
        assert not any(key.startswith(('error_logs', 'receipt_failed')) for key in monitor.tick(first).failed)
        # Stamped before the first tick, delivered after it: the lagged window still reads them.
        late = (first - timedelta(seconds=10)).replace(tzinfo=None)
        client.execute(
            'INSERT INTO origo.container_log VALUES',
            [(late, 'dagster', 'origo-dagster-1', 'stderr', 'ERROR', 'late Traceback')],
        )
        client.execute(
            'INSERT INTO origo.worker_minute_log VALUES',
            [('depth', 'depth20_snapshots', late.replace(second=0, microsecond=0), 0, '', 5, 'FAILED', 'LATE', 'late receipt', 'host', late)],
        )
        second = monitor.tick(first + timedelta(seconds=2 * DELIVERY_LAG_SECONDS))
        assert 'error_logs:dagster' in second.failed
        assert 'receipt_failed:depth:depth20_snapshots' in second.failed
    finally:
        client.disconnect()

    monkeypatch.setenv('DEPTH20_PROBE_URL', _url(recorder))
    monkeypatch.setenv('DEPTH20_PROBE_TOKEN', 'probe-token')
    recorder.history_malformed = True
    probes = (CollectorProbe('depth20', 'DEPTH20_PROBE_URL', 'DEPTH20_PROBE_TOKEN'),)
    outcome = _monitor(recorder, tmp_path / 'malformed', probes=probes).tick(NOW)
    assert 'collector_silent:depth20' in outcome.failed
    assert not any(key.startswith('detector_failed') for key in outcome.failed)
    serving = [post for post in _check_posts(recorder) if post['check_name'] == 'collectors_serving']
    assert serving[-1]['passed'] is False and serving[-1]['metadata']['findings'] == 1
