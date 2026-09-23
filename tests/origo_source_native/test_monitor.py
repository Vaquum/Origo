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
from typing import IO, Any, cast

import pytest
import yaml
from dagster import Failure

from origo.alerts.email import AlertSettings, send_alert
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.definitions import MONITOR_CHECK_NAMES, defs, origo_monitor_checks
from origo.law import LawReport, evaluate
from origo.law_catalog import build_catalog
from origo.sources.registry import SOURCE_REGISTRY
from origo.workers.dagster_reader import DagsterReader
from origo.workers.monitor import (
    DELIVERY_LAG_SECONDS,
    CollectorProbe,
    Cursor,
    Finding,
    LawTape,
    Monitor,
    held_law_keys,
)
from origo.workers.receipts import ensure_monitoring_tables, record_receipt
from origo.workers.report import Reporter
from origo.workers.runtime import heartbeat_path, touch_heartbeat

from .test_law import LawCase
from .test_law import law_case as law_case

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
        server.posts.append(
            (
                self.path,
                {
                    **body,
                    '_authorization': self.headers.get('Authorization', ''),
                    '_user_agent': self.headers.get('User-Agent', ''),
                },
            )
        )
        if self.path == '/emails':
            self._respond(server.email_status, {'id': f'email-{len(server.posts)}'})
            return
        if self.path.startswith('/report_asset_'):
            self._respond(200, {})
            return
        self._respond(404, {'error': self.path})

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        server = cast(_Recorder, self.server)
        if self.path == '/healthz':
            self._respond(200, b'ok\n')
            return
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
    publication_root: Path | None = None,
) -> Monitor:
    if publication_root is None:
        publication_root = tmp_path / 'shadow'
        publication_root.mkdir(parents=True, exist_ok=True)
    for spec in SOURCE_REGISTRY:
        if spec.provisional is not None:
            heartbeat = heartbeat_path(tmp_path / 'heartbeats', f'provisional_{spec.key}')
            if not heartbeat.exists():
                touch_heartbeat(heartbeat)
    return Monitor(
        dagster=DagsterReader(dagster_url or _url(server), timeout_seconds=2.0),
        client=cast(Any, client or _EmptyClient()),
        database='origo',
        law_client=_EmptyClient(),
        law_root=tmp_path / 'law',
        heartbeat_dir=tmp_path / 'heartbeats',
        probes=probes,
        settings=settings if settings is not None else _settings(server),
        reporter=Reporter(_url(server), timeout_seconds=2.0),
        cursor_path=tmp_path / 'monitor.cursor.json',
        publication_root=publication_root,
    )


def _emails(server: _Recorder) -> list[dict[str, Any]]:
    return [body for path, body in server.posts if path == '/emails']


def _check_posts(server: _Recorder) -> list[dict[str, Any]]:
    return [body for path, body in server.posts if path == '/report_asset_check/origo_monitor']


def _failure_keys() -> set[str]:
    runs = json.loads((FIXTURES / 'Failures.json').read_text())['data']['runsOrError']['results']
    return {f'run_failure:{run["jobName"]}' for run in runs}


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
    # A job failing on successive partitions is one key, so the cooldown covers the whole
    # outage; the partitions are in the detail.
    briefing = [
        line
        for line in emails[0]['text'].splitlines()
        if line.startswith('- run_failure:publish_btc_briefing_feed_job:')
    ]
    assert len(briefing) == 1
    assert '2 runs of publish_btc_briefing_feed_job failed' in briefing[0]
    assert 'partition 2021-11-28' in briefing[0] and 'partition 2018-01-16' in briefing[0]
    assert len([key for key in first.failed if key.startswith('run_failure:')]) == len(expected)
    assert all(post['passed'] is False for post in _check_posts(recorder) if post['check_name'] == 'queue_bounded')
    second = monitor.tick(NOW + timedelta(minutes=1))
    assert set(second.failed) >= expected
    assert len(_emails(recorder)) == 1
    assert len(_check_posts(recorder)) == 14
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
    assert quiet.failed and all(key.startswith('law:') for key in quiet.failed)
    assert len(_emails(recorder)) == 2


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
        fresh = heartbeat_path(tmp_path / 'heartbeats', 'provisional_binance_spot_trades')
        touch_heartbeat(fresh)
        stale = heartbeat_path(tmp_path / 'heartbeats', 'depth')
        touch_heartbeat(stale)
        old = time.time() - 181
        os.utime(stale, (old, old))
        tick_time = datetime.now(UTC) + timedelta(seconds=DELIVERY_LAG_SECONDS + 30)
        outcome = _monitor(recorder, tmp_path, client=client).tick(tick_time)
        assert 'heartbeat_stale:depth' in outcome.failed
        assert 'heartbeat_stale:provisional_binance_spot_trades' not in outcome.failed
        assert 'receipt_failed:depth:depth20_snapshots' in outcome.failed
        assert 'receipt_failed:depth:depth200_snapshots' not in outcome.failed
        alive = [post for post in _check_posts(recorder) if post['check_name'] == 'workers_alive']
        assert alive and alive[0]['passed'] is False
        assert 'PROVIDER_HTTP_503' in _emails(recorder)[0]['text']
    finally:
        client.disconnect()


def test_monitor_requires_each_source_heartbeat_and_ignores_retired_shared_worker(
    recorder: _Recorder, tmp_path: Path
) -> None:
    monitor = _monitor(recorder, tmp_path)
    directory = tmp_path / 'heartbeats'
    missing = 'provisional_binance_perp_trades'
    stale = 'provisional_binance_spot_trades'
    heartbeat_path(directory, missing).unlink()
    retired = heartbeat_path(directory, 'provisional')
    touch_heartbeat(retired)
    old = NOW.timestamp() - 181
    for path in (retired, heartbeat_path(directory, stale)):
        os.utime(path, (old, old))
    outcome = monitor.tick(NOW)
    assert {key for key in outcome.failed if key.startswith('heartbeat_stale:')} == {
        f'heartbeat_stale:{missing}', f'heartbeat_stale:{stale}'
    }
    assert len(monitor._heartbeats()) == 4


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
        if not (key.startswith(('law:R1:', 'law:C1:', 'law:D1:')) and ':FAIL:' in key):
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
    # Resend's edge rejects the default urllib agent with 403 (Cloudflare error 1010).
    assert body['_user_agent'] == 'origo-monitor'
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
        'collectors_serving',
        'dagster_reachable',
        'data_current',
        'no_error_logs',
        'publication_current',
        'queue_bounded',
        'workers_alive',
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
        assert 'source-publications:/opt/origo/shadow:ro' in monitor['volumes']
        assert '/var/run/docker.sock:/var/run/docker.sock:ro' in compose['services']['vector']['volumes']
        assert './deploy/vector.yaml:/etc/vector/vector.yaml:ro' in compose['services']['vector']['volumes']
        # Without this, Vector 0.58 keeps ${CLICKHOUSE_PASSWORD} literal and the sink gets 401.
        assert 'VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION=true' in (
            compose['services']['vector']['environment']
        ), name
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


class _SpanClient(_EmptyClient):
    """A ClickHouse stand-in serving one state-span row per listed source."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[tuple[object, ...]]:
        if 'source_active_partitions' in query:
            return self._rows
        return []


class _HoldReader(DagsterReader):
    """A Dagster stand-in with a fixed publication hold."""

    def __init__(self, owned: bool) -> None:
        super().__init__('http://dagit.invalid')
        self._owned = owned

    def backfill_owns_publication(self, source_key: str) -> bool:
        return self._owned


def _manifest(root: Path, source: str, consumer: str, end: datetime) -> None:
    path = root / source / consumer / 'latest.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'active_through': end.isoformat(), 'kind': consumer}))


def _perp_span(current: datetime, span: timedelta = timedelta(days=30)) -> list[tuple[object, ...]]:
    old = current - span
    return [('binance_perp_trades', old, current, old, current, 100)]


def test_publication_stale_mount_is_a_finding(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / 'shadow'
    _manifest(root, 'binance_perp_trades', 'mount', NOW - timedelta(hours=4))
    _manifest(root, 'binance_perp_trades', 'huggingface', NOW)
    monitor = _monitor(recorder, tmp_path, client=_SpanClient(_perp_span(NOW)), publication_root=root)
    monkeypatch.setattr(monitor, 'dagster', _HoldReader(False))
    findings = monitor._publication_findings()
    assert [finding.key for finding in findings] == ['publication_stale:binance_perp_trades:mount']
    assert findings[0].check == 'publication_current'
    assert (NOW - timedelta(hours=4)).isoformat() in findings[0].detail
    assert NOW.isoformat() in findings[0].detail


def test_publication_stale_huggingface_is_a_finding(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / 'shadow'
    _manifest(root, 'binance_perp_trades', 'mount', NOW)
    _manifest(root, 'binance_perp_trades', 'huggingface', NOW - timedelta(hours=30))
    monitor = _monitor(recorder, tmp_path, client=_SpanClient(_perp_span(NOW)), publication_root=root)
    monkeypatch.setattr(monitor, 'dagster', _HoldReader(False))
    findings = monitor._publication_findings()
    assert [finding.key for finding in findings] == [
        'publication_stale:binance_perp_trades:huggingface'
    ]
    assert findings[0].check == 'publication_current'


def test_publication_current_consumers_are_quiet(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / 'shadow'
    _manifest(root, 'binance_perp_trades', 'mount', NOW)
    _manifest(root, 'binance_perp_trades', 'huggingface', NOW)
    monitor = _monitor(recorder, tmp_path, client=_SpanClient(_perp_span(NOW)), publication_root=root)
    monkeypatch.setattr(monitor, 'dagster', _HoldReader(False))
    assert monitor._publication_findings() == []
    # A held publication is legitimate, however far the state advanced.
    _manifest(root, 'binance_perp_trades', 'mount', NOW - timedelta(hours=4))
    _manifest(root, 'binance_perp_trades', 'huggingface', NOW - timedelta(hours=30))
    monkeypatch.setattr(monitor, 'dagster', _HoldReader(True))
    assert monitor._publication_findings() == []
    # A source that never published stays quiet while its state is younger than grace.
    empty = tmp_path / 'empty'
    empty.mkdir()
    fresh = _monitor(
        recorder,
        tmp_path / 'fresh',
        client=_SpanClient(_perp_span(NOW, timedelta(minutes=10))),
        publication_root=empty,
    )
    monkeypatch.setattr(fresh, 'dagster', _HoldReader(False))
    assert fresh._publication_findings() == []


@pytest.mark.parametrize('payload', ['{not json', '[]', 'null', '"just a string"'])
def test_publication_unreadable_manifest_is_a_finding_and_skips_only_that_consumer(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: str
) -> None:
    root = tmp_path / 'shadow'
    broken = root / 'binance_perp_trades' / 'mount' / 'latest.json'
    broken.parent.mkdir(parents=True, exist_ok=True)
    broken.write_text(payload)
    _manifest(root, 'binance_perp_trades', 'huggingface', NOW - timedelta(hours=30))
    monitor = _monitor(recorder, tmp_path, client=_SpanClient(_perp_span(NOW)), publication_root=root)
    monkeypatch.setattr(monitor, 'dagster', _HoldReader(False))
    findings = monitor._publication_findings()
    assert [finding.key for finding in findings] == [
        'publication_manifest_unreadable:binance_perp_trades:mount',
        'publication_stale:binance_perp_trades:huggingface',
    ]
    assert all(finding.check == 'publication_current' for finding in findings)


def test_publication_missing_root_fails_loud(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monitor = _monitor(
        recorder, tmp_path, client=_SpanClient(_perp_span(NOW)), publication_root=tmp_path / 'missing'
    )
    monkeypatch.setattr(monitor, 'dagster', _HoldReader(False))
    with pytest.raises(RuntimeError, match='is not mounted'):
        monitor._publication_findings()


def test_publication_unknown_hold_fails_loud(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from origo.workers.dagster_reader import DagsterUnreachable

    class _DownReader(DagsterReader):
        def backfill_owns_publication(self, source_key: str) -> bool:
            raise DagsterUnreachable('Backfills: HTTP 502')

    root = tmp_path / 'shadow'
    root.mkdir()
    monitor = _monitor(recorder, tmp_path, client=_SpanClient(_perp_span(NOW)), publication_root=root)
    monkeypatch.setattr(monitor, 'dagster', _DownReader('http://dagit.invalid'))
    with pytest.raises(DagsterUnreachable, match='HTTP 502'):
        monitor._publication_findings()


def test_monitor_flags_runs_queued_past_the_stuck_threshold(recorder: _Recorder, tmp_path: Path) -> None:
    recorder.graphql['StuckRuns'] = {
        'data': {
            'runsOrError': {
                '__typename': 'Runs',
                'results': [
                    {
                        'runId': '0c2f2b81-e1cc-431d-86d1-17aacbf05d75',
                        'jobName': 'maintain_operational_metadata_job',
                        'creationTime': (NOW - timedelta(days=3)).timestamp(),
                    }
                ],
            }
        }
    }
    monitor = _monitor(recorder, tmp_path)
    outcome = monitor.tick(NOW)
    assert 'queue_stuck:maintain_operational_metadata_job' in outcome.failed
    assert all(
        post['passed'] is False for post in _check_posts(recorder) if post['check_name'] == 'queue_bounded'
    )


def _protocol_report(now: datetime) -> LawReport:
    # Alert/tape protocol only: empty-store UNKNOWN evidence, not invented market rows.
    report = evaluate(_EmptyClient(), 'origo', now)
    catalog = build_catalog('')
    report['catalog_version'] = catalog['version']
    return report


def test_law_tape_precedes_unheld_dagit_and_held_mail(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monitor = _monitor(recorder, tmp_path)
    original = monitor.reporter.check
    checked: list[str] = []

    def check(asset: str, name: str, *, passed: bool, metadata: Mapping[str, object]) -> bool:
        assert (tmp_path / 'law' / NOW.strftime('samples-%Y-%m-%d.jsonl')).exists()
        checked.append(name)
        return original(asset, name, passed=passed, metadata=metadata)

    monkeypatch.setattr(monitor.reporter, 'check', check)
    monitor.tick(NOW)
    assert checked == list(MONITOR_CHECK_NAMES)
    report = json.loads((tmp_path / 'law' / NOW.strftime('samples-%Y-%m-%d.jsonl')).read_text())
    assert report['status'] in ('FAIL', 'UNKNOWN')
    assert len(report['gates']) < len(monitor.catalog['gates'])
    assert all(event['reason'] != 'not_observed' for event in report['gates'])
    assert len(json.dumps(report, separators=(',', ':')).encode()) < 64 * 1024
    assert _check_posts(recorder)[checked.index('data_current')]['passed'] is False
    assert 'law:' in _emails(recorder)[0]['text']


def test_data_current_hold_uses_consecutive_slots_and_cooldown(tmp_path: Path) -> None:
    cursor = Cursor.load(tmp_path / 'cursor.json', NOW, 15)
    finding = Finding('law:R1:binance_perp_trades:FAIL:reader_stale', 'data_current', 'lag', 'lag')
    for minute in range(5):
        held = held_law_keys(cursor, [finding], (NOW + timedelta(minutes=minute)).isoformat())
        assert (finding.key in held) == (minute < 4)
        again = held_law_keys(cursor, [finding], (NOW + timedelta(minutes=minute)).isoformat())
        assert again == held
    cursor.save(tmp_path / 'cursor.json')
    restarted = Cursor.load(tmp_path / 'cursor.json', NOW, 15)
    assert not held_law_keys(restarted, [finding], (NOW + timedelta(minutes=5)).isoformat())
    assert held_law_keys(restarted, [finding], (NOW + timedelta(minutes=7)).isoformat())
    held_law_keys(restarted, [], (NOW + timedelta(minutes=8)).isoformat())
    assert held_law_keys(restarted, [finding], (NOW + timedelta(minutes=9)).isoformat())
    immediate = Finding('law:C2:binance_perp_trades:FAIL:calendar_gap', 'data_current', 'gap', 'gap')
    assert not held_law_keys(restarted, [immediate], NOW.isoformat())


def test_tape_restart_duplicate_and_partial_write_preserve_evidence(
    recorder: _Recorder, tmp_path: Path,
) -> None:
    first = _monitor(recorder, tmp_path)
    first.tick(NOW)
    segment = tmp_path / 'law' / NOW.strftime('samples-%Y-%m-%d.jsonl')
    original = segment.read_bytes()
    _monitor(recorder, tmp_path).tick(NOW)
    assert segment.read_bytes() == original
    with segment.open('ab') as stream:
        stream.write(b'{"sampling_slot":')
    tape = LawTape(tmp_path / 'law')
    report = _protocol_report(NOW + timedelta(minutes=1))
    tape.append(report, build_catalog(''))
    lines = segment.read_bytes().splitlines()
    assert len(lines) == 3 and lines[1] == b'{"sampling_slot": [incomplete]'
    assert json.loads(lines[2])['sampling_slot'] == report['sampling_slot']
    assert LawTape(tmp_path / 'law').latest(NOW)['sampling_slot'] == report['sampling_slot']



def test_pre_catalog_component_proofs_remain_visible_without_historical_pass(
    recorder: _Recorder, tmp_path: Path, law_case: LawCase,
) -> None:
    law_case.minute(0)
    monitor = _monitor(recorder, tmp_path)
    monitor.law_client = law_case.client
    monitor.deployed_sha = 'ca888afed9bc1681ceebe4c9cfd0502538e2a2d2'
    now = datetime.now(UTC)
    findings = monitor._law_findings(now, [])
    report = monitor.pending_report
    assert report is not None
    proofs = [dict(item) for item in report['projections'] if item['reason'] == 'validated_activation']
    assert proofs
    old_ids = {item['evidence_id'] for item in proofs}
    before = [event for event in report['gates'] if event['evidence_id'] in old_ids]
    assert before and all(event['outcome'] == 'PASS' for event in before)
    assert monitor._commit_law(now, findings) == []
    saved = json.loads((monitor.law_tape.root / now.strftime('samples-%Y-%m-%d.jsonl')).read_text())
    assert [item for item in saved['projections'] if item['evidence_id'] in old_ids] == proofs
    assert all(event['outcome'] == 'NOT_EVALUATED' and event['evidence_id'] == ''
               and event['reason'] == 'historical_definition_unavailable' for event in before)
    events = [json.loads(line) for path in monitor.law_tape.root.glob('gate-events-*.jsonl')
              for line in path.read_text().splitlines()]
    assert not any(event['evidence_id'] in old_ids for event in events)
    # Actual validation after the catalog exists remains attributable to its definition.
    law_case.minute(1)
    later = now + timedelta(minutes=1)
    findings = monitor._law_findings(later, [])
    current = monitor.pending_report
    assert current is not None
    new_ids = {item['evidence_id'] for item in current['projections']
               if item['reason'] == 'validated_activation'} - old_ids
    assert new_ids
    assert monitor._commit_law(later, findings) == []
    events = [json.loads(line) for path in monitor.law_tape.root.glob('gate-events-*.jsonl')
              for line in path.read_text().splitlines()]
    assert all(any(event['evidence_id'] == identity and event['outcome'] == 'PASS'
                   for event in events) for identity in new_ids)
    assert not any(event['evidence_id'] in old_ids for event in events)


def test_law_tape_failure_keeps_other_checks_and_delivery_running(
    recorder: _Recorder, tmp_path: Path,
) -> None:
    monitor = _monitor(recorder, tmp_path)
    monitor.law_tape.root.write_text('not a directory')
    outcome = monitor.tick(NOW)
    assert 'detector_failed:law' in outcome.failed
    assert set(post['check_name'] for post in _check_posts(recorder)) == set(MONITOR_CHECK_NAMES)
    assert 'detector_failed:law' in _emails(recorder)[0]['text']
    assert _failure_keys() <= set(outcome.failed)


def test_law_page_probe_is_bounded_and_uses_existing_alert_path(
    recorder: _Recorder, tmp_path: Path,
) -> None:
    monitor = _monitor(recorder, tmp_path)
    monitor.page_url = _url(recorder) + '/healthz'
    assert monitor._page_findings() == []
    monitor.page_url = _url(recorder) + '/history'
    assert monitor._page_findings()[0].key == 'law_page_unreachable'
    monitor.page_url = 'http://127.0.0.1:1/healthz'
    started = time.monotonic()
    outcome = monitor.tick(NOW)
    assert time.monotonic() - started < 5
    assert 'law_page_unreachable' in outcome.failed
    assert 'law_page_unreachable' in _emails(recorder)[0]['text']
    report = monitor.law_tape.last
    assert report is not None
    event = next(event for event in report['gates'] if event['gate_id'] == 'monitor.data_current')
    assert event['outcome'] == 'FAIL'
    assert event['evidence']['finding_count'] == sum(item.startswith(('law:', 'detector_failed:law')) or item == 'law_page_unreachable' for item in outcome.failed)


def test_historical_gate_import_is_bounded_resumable_and_attributable(
    recorder: _Recorder, tmp_path: Path, origo_test_env: dict[str, str],
) -> None:
    from origo.sources.contracts import SourceError
    from origo.sources.lifecycle import SourceRuntime
    from origo.sources.locking import source_lock
    from origo.sources.storage import SourceStore
    from origo.workers.monitor import LawClient

    client = make_clickhouse_client(get_clickhouse_settings())
    spec = SOURCE_REGISTRY[0]
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'worker:law-history')
    try:
        runtime.setup()
        start = datetime.now(UTC)
        monitor = _monitor(recorder, tmp_path, client=client)
        monitor.law_client = LawClient(get_clickhouse_settings())
        monitor.catalog = build_catalog('')
        monitor.law_tape.append(_protocol_report(start), monitor.catalog)
        with source_lock(runtime.lock_root, spec.key, 'consumer_mount'):
            with pytest.raises(SourceError):
                runtime.publish('mount', str(tmp_path / 'mount'))
        observed = datetime.now(UTC) + timedelta(seconds=DELIVERY_LAG_SECONDS)
        cursor = Cursor.load(tmp_path / 'cursor.json', observed, 15)
        begun = time.monotonic()
        assert monitor._history_findings(cursor, observed) == []
        assert time.monotonic() - begun < 5
        segment = next(monitor.law_tape.root.glob('gate-events-*'))
        events = [json.loads(line) for line in segment.read_text().splitlines()]
        assert len(events) == 1
        assert events[0]['gate_id'] == 'locks.contention.source'
        assert events[0]['outcome'] == 'EXPECTED_WAIT'
        assert datetime.fromisoformat(events[0]['evaluated_at']) < observed
        cursor.save(tmp_path / 'cursor.json')
        restarted = Cursor.load(tmp_path / 'cursor.json', observed, 15)
        original = segment.read_bytes()
        monitor.law_tape = LawTape(monitor.law_tape.root)
        assert monitor._history_findings(restarted, observed) == []
        assert segment.read_bytes() == original
        assert restarted.law_history == cursor.law_history
        # A later minute sample does not erase an actual imported event or renew its time.
        from origo.workers.law_page import TapeCache, _object

        findings = monitor._law_findings(observed, [])
        assert monitor._commit_law(observed, findings) == []
        cache = TapeCache(monitor.law_tape.root)
        cache.refresh_latest(observed)
        cache.advance(observed, budget_seconds=2)
        recorded = _object(cache.current(observed)['last_gate_events'])
        assert recorded['locks.contention.source'] == events[0]
        assert not any(event['gate_id'] == 'locks.contention.source'
                       for event in monitor.law_tape.last['gates'])
    finally:
        client.disconnect()


def test_segment_retention_preserves_30_days_and_closeout_evidence(tmp_path: Path) -> None:
    tape = LawTape(tmp_path / 'law')
    catalog = build_catalog('')
    old = NOW - timedelta(days=31)
    report = _protocol_report(old)
    tape.append(report, catalog)
    old_path = tape.root / old.strftime('samples-%Y-%m-%d.jsonl')
    # A selected evidence copy is outside rotating day segments; contents remain original.
    selected = tape.root / 'closeout' / old_path.name
    selected.parent.mkdir()
    selected.write_bytes(old_path.read_bytes())
    for days in range(30, 0, -1):
        stamp = NOW - timedelta(days=days)
        tape._append(tape.root / stamp.strftime('samples-%Y-%m-%d.jsonl'), _protocol_report(stamp))
    tape.append(_protocol_report(NOW), catalog)
    assert not old_path.exists()
    assert len(list(tape.root.glob('samples-*'))) == 31
    assert json.loads(selected.read_text())['sampling_slot'] == report['sampling_slot']
    assert (tape.root / f'catalog-{catalog["version"]}.json').exists()


def test_complete_json_without_newline_is_not_a_committed_observation(tmp_path: Path) -> None:
    tape = LawTape(tmp_path)
    report = _protocol_report(NOW)
    segment = tmp_path / NOW.strftime('samples-%Y-%m-%d.jsonl')
    segment.write_text(json.dumps(report))
    assert tape.latest(NOW) is None and tape.corrupt_tail
    tape.append(_protocol_report(NOW + timedelta(minutes=1)), build_catalog(''))
    assert len(segment.read_text().splitlines()) == 2
    with pytest.raises(ValueError):
        json.loads(segment.read_text().splitlines()[0])


def test_gate_event_index_resumes_without_rescanning_days(
    recorder: _Recorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monitor = _monitor(recorder, tmp_path)
    monitor.tick(NOW)
    report = monitor.law_tape.last
    assert report is not None
    events = [event for event in report['gates'] if event['evidence_id']]
    restarted = LawTape(monitor.law_tape.root)
    with pytest.raises(TimeoutError):
        restarted.append_events(events, deadline=time.monotonic() - 1)
    restarted.append_events(events)
    saved = {path.name: path.read_bytes() for path in restarted.root.glob('gate-events-*')}
    original = Path.open
    reads: list[str] = []

    def opened(path: Path, mode: str = 'r', *args: object, **kwargs: object) -> IO[bytes] | IO[str]:
        if path.name.startswith('gate-events-') and mode == 'rb':
            reads.append(path.name)
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', opened)
    restarted.append_events(events)
    assert reads == []
    assert saved == {path.name: path.read_bytes() for path in restarted.root.glob('gate-events-*')}
