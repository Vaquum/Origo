"""Acceptance uses untouched captures; scheduling and transport faults are injected locally."""
from __future__ import annotations

import ast

import base64
import copy
import hashlib
import html
import json
import resource
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

import pytest
from playwright.sync_api import sync_playwright

from origo import observatory
from origo.alerts import email as transport
from origo.alerts import summary as delivery
from origo.alerts.email import AlertSettings, DeliveryError
from origo.alerts.summary import (
    BODY_BYTES,
    add_loss_interval,
    attempt_delivery,
    dashboard_link,
    plan_notification,
    render_email,
    validate_pending,
)
from origo.observatory import (
    LAW_COPY,
    LAW_NAMES,
    THEME,
    Document,
    Json,
    Measurement,
    NotificationObservation,
    ObservationFrame,
    OperatorSummary,
    build_summary,
    lifecycle_transitions,
    measurement_trend,
    repeat_text,
)
from origo.workers import law_page as page
from origo.workers import monitor as monitor_module
from origo.workers.monitor import Cursor, Finding, LawTape, Monitor, held_law_keys, law_findings
from origo.workers.report import Reporter
from origo.law import LawReport
from origo.law_catalog import LawCatalog, Scalar, build_catalog

ROOT = Path(__file__).resolve().parents[2]
CAPTURES = ROOT / 'tests/fixtures/law/alerts'


def _document(path: Path) -> Document:
    result: object = json.loads(path.read_bytes())
    assert isinstance(result, dict), path
    return cast(Document, result)


def _events(name: str) -> list[Document]:
    return sorted(page._objects(_document(CAPTURES / f'{name}.json')['events']),
                  key=lambda event: str(event['evaluated_at']))


def _recorded_frames(name: str) -> list[ObservationFrame]:
    """Replay original reason-bearing keys through the production hold; no historical delivery claim."""
    result: list[ObservationFrame] = []
    cursor = Cursor(0, '', '', {}, '', 0, 0)
    for event in _events(name):
        family, scope = str(event['gate_id']).removeprefix('law.').split(':', 1)
        observed_at = str(event['evaluated_at'])
        slot = datetime.fromisoformat(observed_at).replace(second=0, microsecond=0).isoformat()
        status = cast(Literal['FAIL', 'UNKNOWN', 'PASS', 'EXPECTED_WAIT'], event['outcome'])
        key = f'law:{family}:{scope}:{status}:{event["reason"]}'
        findings = [Finding(key, 'data_current', str(event['gate_id']), str(event['reason']))] if status in ('FAIL', 'UNKNOWN') else []
        held = held_law_keys(cursor, findings, slot)
        evidence = page._object(event['evidence'])
        threshold = evidence.get('budget_seconds')
        measurements: list[Measurement] = [
            {'name': key, 'value': value, 'unit': 'seconds' if key.endswith('seconds') else 'count',
             'threshold': threshold if key == 'age_seconds' and isinstance(threshold, (int, float)) else None,
             'observed_at': observed_at, 'definition_version': str(event['definition_version'])}
            for key, value in evidence.items() if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        observation: NotificationObservation = {
            'group_id': f'law:{family}:{scope}', 'check': f'law.{family}', 'scope': scope,
            'kind': 'condition', 'status': status, 'definition_version': str(event['definition_version']),
            'detector_keys': [key] if findings else [], 'eligible': bool(findings) and key not in held,
            'read_window_key': None, 'measurements': measurements,
            'evidence_refs': [str(event['evidence_id'])], 'complete': True,
        }
        result.append({'sampling_slot': slot, 'catalog_version': str(event['catalog_version']),
                       'law_sample_ref': None, 'law_observed': True, 'checks_complete': {'data_current': True},
                       'read_windows': {}, 'observations': [observation], 'omitted_groups': 0, 'complete': True})
    return result


def _now() -> datetime:
    return datetime.fromisoformat(str(_document(CAPTURES / 'current.json')['checked_at'])) + timedelta(seconds=1)


def _summary(frames: list[ObservationFrame] | None = None, now: datetime | None = None) -> OperatorSummary:
    return build_summary(_document(CAPTURES / 'current.json'), _document(CAPTURES / 'catalog.json'),
                         frames if frames is not None else _recorded_frames('r1-perp-20260928'), now=now or _now())


def _through(frames: list[ObservationFrame], time_of_day: str) -> list[ObservationFrame]:
    return [frame for frame in frames if frame['sampling_slot'][11:16] <= time_of_day]


def _settings(**changes: object) -> AlertSettings:
    settings = AlertSettings('local-transport-credential', 'https://api.resend.com/emails',
                             'monitor@example.com', ('operator@example.com',), 21600, 200, 7,
                             'http://37.27.112.167:8484/law')
    return replace(settings, **changes)


def _cursor(path: Path, now: datetime) -> Cursor:
    return Cursor.load(path, now, 60)


@contextmanager
def _serving(tmp_path: Path) -> Iterator[tuple[str, page.TapeCache, OperatorSummary]]:
    current, catalog = (_document(CAPTURES / name) for name in ('current.json', 'catalog.json'))
    report = page._object(current['last_report'])
    tape = tmp_path / 'law'
    tape.mkdir()
    (tape / f"catalog-{catalog['version']}.json").write_bytes((CAPTURES / 'catalog.json').read_bytes())
    (tape / f"samples-{str(report['sampling_slot'])[:10]}.jsonl").write_text(json.dumps(report, separators=(',', ':')) + '\n')
    summary = _summary()
    (tape / 'operator-summary.json').write_text(json.dumps(summary, ensure_ascii=False, separators=(',', ':')))
    cache = page.TapeCache(tape)
    cache.refresh_latest(_now())
    while cache.loading:
        cache.advance(_now(), budget_seconds=1)
    server = page.LawServer(('127.0.0.1', 0), cache, _now)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', cache, summary
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_dashboard_and_email_share_recorded_summary(tmp_path: Path) -> None:
    manifest = _document(CAPTURES / 'manifest.json')
    shas: set[str] = set()
    for entry in page._objects(manifest['files']):
        body = (ROOT / str(entry['path'])).read_bytes()
        assert hashlib.sha256(body).hexdigest() == entry['sha256'], entry['path']
        assert len(body) == entry['bytes']
        shas.update(str(sha) for sha in cast(list[Json], entry['deployed_shas']))
    for sha in shas:
        assert len(sha) == 40
        assert subprocess.check_output(['git', 'cat-file', '-t', sha], cwd=ROOT, text=True).strip() == 'commit'
    imported = subprocess.check_output([sys.executable, '-c',
        "import sys, origo.observatory, origo.workers.law_page; print(','.join(sorted(set(sys.modules) & {'origo.law','dagster','clickhouse_driver','numpy','pyarrow','polars'})))"], cwd=ROOT, text=True)
    assert imported.strip() == '', imported
    with _serving(tmp_path) as (_, cache, summary):
        assert cache.current(_now())['operator_summary'] == cast(Document, summary)
        content = render_email(summary, dashboard_url=None, dagit_written=True)
        expected = [*LAW_NAMES.values(), 'Published outputs', 'Worker heartbeats', 'Queued runs',
                    'Collectors serving', 'Error lines · observed / 60m', 'Clear data-law observations']
        assert [card['label'] for card in summary['cards']] == expected
        for card in summary['cards']:
            assert card['label'] in content.text and html.escape(card['label']) in content.html
            assert card['value'] in content.text and card['badge'] in content.text
        for sentence in LAW_COPY.values():
            assert sentence in content.text and html.escape(sentence) in content.html
        for position, family in enumerate(LAW_NAMES):
            assert summary['cards'][position]['id'] == family
        assert summary['cards'][0]['value'] == '4 / 4'
        assert summary['cards'][3]['value'] == '2,880 / 2,880'
        assert summary['cards'][4]['value'] == summary['cards'][5]['value'] == '1 / 1'
        stale = cache.current(datetime.fromisoformat(str(summary['observed_at'])) + timedelta(seconds=120))
        stale_summary = page._object(stale['operator_summary'])
        assert stale_summary['status'] == 'UNKNOWN'
        assert all(card['status'] == 'UNKNOWN' for card in page._objects(stale_summary['cards']))
        (cache.root / 'operator-summary.json').unlink()
        unavailable = page._object(cache.current(_now())['operator_summary'])
        assert unavailable['status'] == 'UNKNOWN'
        assert all(card['status'] == 'UNKNOWN' for card in page._objects(unavailable['cards']))


def test_html_email_matches_dashboard_and_text(tmp_path: Path) -> None:
    with _serving(tmp_path) as (url, _, summary), sync_playwright() as browser_runtime:
        browser = browser_runtime.chromium.launch()
        try:
            for width, height in ((1366, 900), (390, 844)):
                tab = browser.new_page(viewport={'width': width, 'height': height})
                tab.goto(url + '/law')
                tab.locator('[data-overview="R1"]').wait_for()
                for card in summary['cards']:
                    node = tab.locator(f'[data-overview="{card["id"]}"]')
                    assert card['label'] in node.inner_text()
                    assert node.locator('.overview-value').inner_text() == card['value']
                    assert node.locator('.badge').inner_text() == card['badge']
                tab.screenshot(path=str(tmp_path / f'dashboard-local-preview-{width}.png'), full_page=True)
                content = render_email(summary, dashboard_url='http://37.27.112.167:8484/law', dagit_written=True)
                assert max(len(content.html.encode()), len(content.text.encode())) <= BODY_BYTES
                assert '<script' not in content.html and '<img' not in content.html
                tab.set_content(content.html)
                assert tab.locator('body').evaluate('(node) => node.scrollWidth <= innerWidth')
                assert tab.locator('script, img').count() == 0
                assert tab.locator('body').evaluate('(node) => getComputedStyle(node).color') == 'rgb(23, 43, 54)'
                for card in summary['cards']:
                    assert card['label'] in tab.inner_text('body')
                tab.screenshot(path=str(tmp_path / f'email-local-preview-{width}.png'), full_page=True)
                tab.close()
        finally:
            browser.close()
    hostile = copy.deepcopy(_summary())
    hostile['delivery_status'] = '<script>alert("renderer escaping")</script>'
    content = render_email(hostile, dashboard_url=None, dagit_written=False)
    assert '<script>' not in content.html and '&lt;script&gt;' in content.html
    assert hostile['delivery_status'] in content.text
    assert 'Dashboard link not configured.' in content.text
    assert 'Dagit check write did not succeed' in content.text
    assert 'Omitted recorded groups:' in content.text
    for key in ('ink', 'paper', 'green'):
        assert THEME[key] in content.html
    hostile['delivery_status'] *= 10000
    with pytest.raises(ValueError, match='64 KiB'):
        render_email(hostile, dashboard_url=None, dagit_written=True)


def _assert_missing_current_measurement_has_no_trend() -> None:
    frames = copy.deepcopy(_through(_recorded_frames('r1-perp-20260928'), '02:00'))
    retained_measurements = frames[-2]['observations'][0]['measurements']
    # Inject an omitted sidecar group, preserving every retained source measurement.
    frames[-1]['observations'] = []
    frames[-1]['omitted_groups'] = 1
    frames[-1]['complete'] = False
    incident = _summary(frames, datetime.fromisoformat(frames[-1]['sampling_slot']))['incidents'][0]
    assert incident['measurements'] == retained_measurements
    assert incident['verification'] == 'unverified'
    assert incident['trend']['value'] is None, 'An omitted current group cannot supply the current hourly endpoint.'
    assert incident['trend']['unavailable_reason'] == 'current_observation_missing'
    assert incident['trend']['current_at'] is None


def test_repeat_trends_preserve_evidence_coverage() -> None:
    frames = _recorded_frames('r1-perp-20260928')
    selected = _through(frames, '02:00')
    now = datetime.fromisoformat(selected[-1]['sampling_slot']) + timedelta(seconds=30)
    summary = _summary(selected, now)
    incident = summary['incidents'][0]
    failing = sum(frame['observations'][0]['status'] == 'FAIL' for frame in selected)
    assert incident['observations'] == failing
    assert f'repeated {failing - 1} times' in repeat_text(incident)
    duplicate = _summary([*selected, *selected], now)['incidents'][0]
    assert duplicate['observations'] == failing
    current, previous = selected[-1]['observations'][0], selected[-61]['observations'][0]
    latest_age = next(item['value'] for item in current['measurements'] if item['name'] == 'age_seconds')
    previous_age = next(item['value'] for item in previous['measurements'] if item['name'] == 'age_seconds')
    assert isinstance(latest_age, (int, float)) and isinstance(previous_age, (int, float))
    assert incident['trend']['value'] == pytest.approx(latest_age - previous_age)
    assert 'point-to-point' in incident['trend']['description']
    assert 'recovery' not in incident['trend']['description'].lower()
    missing_comparison = [frame for frame in selected if frame is not selected[-61]]
    unverified = _summary(missing_comparison, now)['incidents'][0]
    assert unverified['trend']['value'] is None
    assert not unverified['coverage']['complete']
    assert unverified['observations'] == failing - 1
    assert _summary(selected[30:], now)['incidents'][0]['coverage']['reason'] != 'complete_episode'
    assert 'baseline unavailable' in repeat_text(_summary(selected[30:], now)['incidents'][0])
    incompatible = copy.deepcopy(previous)
    incompatible['definition_version'] = 'incompatible-test-definition'
    assert measurement_trend(current, selected[-1]['sampling_slot'], incompatible,
                             selected[-61]['sampling_slot'])['value'] is None
    assert _summary(frames)['cards'][10]['value'] == '33'
    _assert_missing_current_measurement_has_no_trend()


def _assert_resumed_failure_notifies_at_eligibility() -> None:
    frames = _recorded_frames('r1-perp-20260924')
    for first_slot in ('00:00', '09:31'):
        selected = [frame for frame in frames if first_slot <= frame['sampling_slot'][11:16] <= '09:37']
        transitions = lifecycle_transitions(selected, now=datetime.fromisoformat(selected[-1]['sampling_slot']))
        resumed = [event for event in transitions if '09:32' <= event['sampling_slot'][11:16] <= '09:37']
        assert [(event['sampling_slot'][11:16], event['description']) for event in resumed] == [
            ('09:36', 'Ongoing · Verification restored')
        ], 'Restored failure must notify once, on the fifth same-key recorded FAIL.'
        assert resumed[0]['lifecycle'] == 'ongoing'
        held = _through(selected, '09:35')
        incident = _summary(held, datetime.fromisoformat(held[-1]['sampling_slot']))['incidents'][0]
        assert incident['status'] == 'FAIL' and incident['verification'] == 'verified'
    # The initial lifecycle is injected protocol state; all following FAIL values,
    # reason-bearing keys, original times and hold decisions are the genuine replay.
    initial = next(frame for frame in frames if frame['sampling_slot'][11:16] == '09:31')
    episode = observatory._Episode(initial['observations'][0], active=True, had_failure=True,
                                   verification='expected_wait', lifecycle='expected_wait',
                                   status='EXPECTED_WAIT', previous_slot=initial['sampling_slot'])
    for frame in frames:
        if '09:32' <= frame['sampling_slot'][11:16] <= '09:37':
            observatory._condition(episode, frame['observations'][0], frame)
            if not frame['observations'][0]['eligible']:
                assert not episode.transitions
    assert [(event['sampling_slot'][11:16], event['description']) for event in episode.transitions] == [
        ('09:36', 'Ongoing · Expected wait ended')
    ]
    assert episode.lifecycle == 'ongoing' and episode.had_failure


def _quiet_event_protocol_frames() -> list[ObservationFrame]:
    """Exercise the event protocol using recorded aggregate quiet reads, not invented service events."""
    frames: list[ObservationFrame] = []
    for name in ('current-0608.json', 'current-0611.json', 'current.json'):
        report = page._object(_document(CAPTURES / name)['last_report'])
        gate = next(event for event in page._objects(report['gates']) if event['gate_id'] == 'monitor.no_error_logs')
        evidence = page._object(gate['evidence'])
        count = evidence['error_lines']
        assert isinstance(count, int) and count == 0 and gate['outcome'] == 'PASS'
        observation: NotificationObservation = {
            'group_id': str(gate['gate_id']), 'check': 'no_error_logs', 'scope': '',
            'kind': 'event_stream', 'status': 'PASS', 'definition_version': str(gate['definition_version']),
            'detector_keys': [], 'eligible': False, 'read_window_key': 'error_lines',
            'measurements': [{'name': 'event_count', 'value': count, 'unit': 'error lines', 'threshold': None,
                              'observed_at': str(gate['evaluated_at']), 'definition_version': str(gate['definition_version'])}],
            'evidence_refs': [str(gate['evidence_id'])], 'complete': True,
        }
        frames.append({'sampling_slot': str(report['sampling_slot']), 'catalog_version': str(report['catalog_version']),
                       'law_sample_ref': None, 'law_observed': True, 'checks_complete': {'no_error_logs': True},
                       'read_windows': {'error_lines': {'window_start': str(evidence['window_start']),
                                                      'window_end': str(evidence['window_end']), 'count': count,
                                                      'counts_limited': evidence['counts_limited'] is True, 'complete': True}},
                       'observations': [observation] if not frames else [], 'omitted_groups': 0, 'complete': True})
    # Later event frames omit the group after a successful quiet aggregate read.
    # Coverage failure only: the captured count and all source values stay intact.
    frames[0]['checks_complete']['no_error_logs'] = False
    frames[0]['read_windows']['error_lines']['complete'] = False
    frames[0]['complete'] = False
    return frames


def _assert_quiet_event_verification_transition() -> None:
    frames = _quiet_event_protocol_frames()
    now = datetime.fromisoformat(frames[-1]['sampling_slot'])
    transitions = lifecycle_transitions(frames, now=now)
    assert [(event['sampling_slot'], event['description']) for event in transitions] == [
        (frames[0]['sampling_slot'], 'New · Unverified'),
        (frames[1]['sampling_slot'], 'Verification restored · No further events observed'),
    ], 'A successful quiet read must notify restored verification exactly once.'
    assert all(event['lifecycle'] != 'recovered' for event in transitions)


def _assert_quiet_event_does_not_promise_recovery() -> None:
    frames = _quiet_event_protocol_frames()
    for count in (2, 3):
        selected = frames[:count]
        incident = _summary(selected, datetime.fromisoformat(selected[-1]['sampling_slot']))['incidents'][0]
        assert incident['kind'] == 'event_stream' and incident['verification'] == 'verified'
        assert incident['lifecycle'] == 'historical_events' and incident['status'] == 'PASS'
        assert incident.get('recovery_pending') is False, 'Quiet event reads do not confirm condition recovery.'
        assert not any('Recovery pending' in line or 'second consecutive pass' in line
                       for line in incident.get('display_lines', []))


def test_lifecycle_preserves_transient_and_unverified_incidents() -> None:
    _assert_resumed_failure_notifies_at_eligibility()
    _assert_quiet_event_verification_transition()
    _assert_quiet_event_does_not_promise_recovery()
    frames = _recorded_frames('r1-perp-20260928')
    def incident_at(clock: str) -> Document:
        selected = _through(frames, clock)
        now = datetime.fromisoformat(selected[-1]['sampling_slot']) + timedelta(seconds=30)
        return cast(Document, _summary(selected, now)['incidents'][0])
    assert incident_at('00:29')['had_eligible_failure'] is False
    assert incident_at('00:30')['had_eligible_failure'] is True
    assert incident_at('02:54')['recovery_pending'] is True
    assert incident_at('02:55')['lifecycle'] == 'ongoing'
    assert incident_at('02:57')['lifecycle'] == 'recovered'
    assert incident_at('05:31')['lifecycle'] == 'reopened'
    unknown_only = [frame for frame in frames if '04:26' <= frame['sampling_slot'][11:16] <= '04:28']
    unknown_result = _summary(unknown_only, datetime.fromisoformat(unknown_only[-1]['sampling_slot']))['incidents'][0]
    assert unknown_result['lifecycle'] == 'recovered'
    assert unknown_result['had_eligible_failure'] is False
    assert 'Verification restored' in ' '.join(unknown_result.get('transitions', []))
    historical = _recorded_frames('r1-perp-20260924')
    unknown = _through(historical, '09:31')
    unresolved = _summary(unknown, datetime.fromisoformat(unknown[-1]['sampling_slot']))['incidents'][0]
    assert unresolved['verification'] == 'unverified' and unresolved['had_eligible_failure']
    restored = _through(historical, '09:36')
    verified = _summary(restored, datetime.fromisoformat(restored[-1]['sampling_slot']))['incidents'][0]
    assert verified['lifecycle'] == 'ongoing' and verified['verification'] == 'verified'
    c1 = _recorded_frames('c1-perp-20260928')
    wait = _through(c1, '04:27')
    assert _summary(wait, datetime.fromisoformat(wait[-1]['sampling_slot']))['incidents'][0]['lifecycle'] == 'expected_wait'
    assert _summary(_through(c1, '08:06'), datetime.fromisoformat(c1[486]['sampling_slot']))['incidents'][0].get('recovery_pending')
    assert _summary(_through(c1, '08:07'), datetime.fromisoformat(c1[487]['sampling_slot']))['incidents'][0]['lifecycle'] == 'recovered'
    m1 = _recorded_frames('m1-spot-20260924')
    assert m1[0]['observations'][0]['status'] == 'FAIL' and not m1[0]['observations'][0]['eligible']
    assert not lifecycle_transitions(m1, now=datetime.fromisoformat(m1[-1]['sampling_slot']))
    m2 = _recorded_frames('m2-spot-20260925')
    assert m2[0]['observations'][0]['eligible'] is True
    transitions = lifecycle_transitions(m2, now=datetime.fromisoformat(m2[-1]['sampling_slot']))
    assert any(event['description'] == 'New' for event in transitions)
    assert any(event['description'] == 'Recovered' for event in transitions)
    eligible = _through(frames, '00:31')
    before, after = (lifecycle_transitions(items, now=datetime.fromisoformat(items[-1]['sampling_slot'])) for items in (eligible[:-1], eligible))
    assert before == after, 'Repeated eligible failure must not create a state transition.'
    # Persist the actual replayed transition at its original recorded slot. Removing
    # earlier history must not relabel an ongoing incident as a fresh notification.
    recorded = copy.deepcopy(frames)
    all_transitions = lifecycle_transitions(recorded, now=datetime.fromisoformat(recorded[-1]['sampling_slot']))
    for frame in recorded:
        frame['notification_transitions'] = [event for event in all_transitions if event['sampling_slot'] == frame['sampling_slot']]
    for first in (31, 180, 267, 330, 590):
        retained = recorded[first:]
        transitions = lifecycle_transitions(retained, now=datetime.fromisoformat(retained[-1]['sampling_slot']))
        assert transitions == [event for event in all_transitions if event['sampling_slot'] >= retained[0]['sampling_slot']]
    ongoing = [frame for frame in recorded if '00:31' <= frame['sampling_slot'][11:16] <= '02:00']
    assert not lifecycle_transitions(ongoing, now=datetime.fromisoformat(ongoing[-1]['sampling_slot']))
    fresh = next(copy.deepcopy(frame) for frame in frames if frame['sampling_slot'][11:16] == '02:01')
    assert not lifecycle_transitions([*ongoing, fresh], now=datetime.fromisoformat(fresh['sampling_slot']))


def _assert_digest_loss_uses_evidence_slots(tmp_path: Path) -> None:
    # Advance delivery clocks only; retain the genuine summary and source timestamps.
    summary, now, settings = _summary(), _now(), _settings()
    path = tmp_path / 'acknowledged-digest.json'
    cursor = _cursor(path, now)
    assert plan_notification(cursor, summary, settings, now, True, transitions=False)
    def accepted(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
        assert payload and idempotency_key
        return 'local-digest-acknowledgement'
    assert attempt_delivery(cursor, settings, now, lambda: cursor.save(path),
                            completion_clock=lambda: now, sender=accepted) == 'acknowledged'
    assert cursor.notified_through == summary['sampling_slot']
    assert cursor.notified_through is not None
    watermark = datetime.fromisoformat(cursor.notified_through)
    assert watermark.second == watermark.microsecond == 0
    for seconds in (0, 5.25, 59.999999):
        next_day = watermark + timedelta(days=1, seconds=seconds)
        restored = _cursor(path, next_day)
        # The monitor prunes before preparation, which invokes the same pruning again.
        delivery.prune_unsent(restored, next_day)
        assert restored.lost_intervals == [], 'A later second in the same minute is not lost evidence.'
        assert restored.expired_through is None
        assert plan_notification(restored, summary, settings, next_day, True, transitions=False)
        pending = restored.pending_notification
        assert pending is not None and pending.get('disclosed_intervals') == []
        payload = cast(dict[str, str], json.loads(base64.b64decode(pending['payload_base64'])))
        assert payload['subject'] == f'Origo daily digest {next_day.date().isoformat()}'
        assert 'exceeded the 24-hour replay horizon' not in payload['text'] + payload['html']
    # A whole unsent minute beyond the retained boundary still creates a disclosure.
    late = watermark + timedelta(days=1, minutes=1, seconds=5)
    overdue = _cursor(path, late)
    cutoff = watermark + timedelta(minutes=1)
    delivery.prune_unsent(overdue, late)
    assert overdue.notified_through == watermark.isoformat()
    assert overdue.expired_through == cutoff.isoformat()
    assert overdue.lost_intervals == [{'start': watermark.isoformat(), 'end': cutoff.isoformat(),
                                     'reason': 'Unsent evidence exceeded the 24-hour replay horizon.'}]
    delivery.prune_unsent(overdue, late + timedelta(seconds=20))
    assert len(overdue.lost_intervals) == 1 and overdue.expired_through == cutoff.isoformat()
    assert plan_notification(overdue, summary, settings, late, True, transitions=False)
    pending = overdue.pending_notification
    assert pending is not None and pending.get('disclosed_intervals') == overdue.lost_intervals
    payload = cast(dict[str, str], json.loads(base64.b64decode(pending['payload_base64'])))
    for body in (payload['text'], payload['html']):
        assert 'Unsent evidence exceeded the 24-hour replay horizon.' in body
        assert cutoff.isoformat() in body
    late_path = tmp_path / 'late-digest.json'
    assert attempt_delivery(overdue, settings, late, lambda: overdue.save(late_path),
                            completion_clock=lambda: late, sender=accepted) == 'acknowledged'
    assert overdue.lost_intervals == []
    delivery.prune_unsent(overdue, late + timedelta(seconds=20))
    assert overdue.lost_intervals == [], 'Acknowledged loss must not recur within the same evidence slot.'
    # A slow tick crosses a minute while preparing the same genuine evidence snapshot.
    snapshot_end = summary['coverage']['window_end']
    assert snapshot_end is not None
    observed = datetime.fromisoformat(snapshot_end)
    horizon = observed - timedelta(days=1)
    for older_minutes, monitor_pruned in ((0, True), (1, True), (1, False)):
        crossed = _cursor(tmp_path / f'crossed-minute-{older_minutes}-{monitor_pruned}.json', now)
        crossed.notified_through = (horizon - timedelta(minutes=older_minutes)).isoformat()
        expected_loss = [{'start': crossed.notified_through, 'end': horizon.isoformat(),
                          'reason': 'Unsent evidence exceeded the 24-hour replay horizon.'}] if older_minutes else []
        if monitor_pruned:
            delivery.prune_unsent(crossed, observed + timedelta(seconds=59))
            assert crossed.lost_intervals == expected_loss
        dispatch_time = observed + timedelta(seconds=65)
        assert plan_notification(crossed, summary, settings, dispatch_time, True, transitions=False)
        assert crossed.lost_intervals == expected_loss, 'Preparation latency cannot expire evidence still in the snapshot.'
        pending = crossed.pending_notification
        assert pending is not None and pending.get('disclosed_intervals') == expected_loss
        assert pending['prepared_at'] == dispatch_time.timestamp()
        assert pending['evidence_end'] == summary['sampling_slot']


def test_all_notifications_obey_global_hourly_budget(tmp_path: Path) -> None:
    _assert_digest_loss_uses_evidence_slots(tmp_path)
    # Clock inputs exercise scheduling only; the captured operator observations remain unchanged.
    summary, start = _summary(), _now().replace(hour=6, minute=40, second=0, microsecond=0)
    for cooldown in (0, 21600):
        path = tmp_path / f'quota-{cooldown}.json'
        cursor, settings = _cursor(path, start), _settings(cooldown_seconds=cooldown)
        dispatches: list[tuple[float, str]] = []
        clock = start
        def sender(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
            assert path.exists(), 'Intent and attempt reservation must precede POST.'
            saved = _document(path)
            assert page._object(saved['pending_notification'])['idempotency_key'] == idempotency_key
            dispatches.append((clock.timestamp(), hashlib.sha256(payload).hexdigest()))
            return f'local-ack-{len(dispatches)}'
        for minute in range(24 * 60):
            clock = start + timedelta(minutes=minute)
            plan_notification(cursor, summary, settings, clock, True, transitions=True)
            attempt_delivery(cursor, settings, clock, lambda: cursor.save(path),
                             completion_clock=lambda: clock, sender=sender)
        assert 1 < len(dispatches) <= 24
        assert all(right[0] - left[0] >= 3600 for left, right in zip(dispatches, dispatches[1:]))
        assert cursor.last_digest_date == start.date().isoformat(), "Tomorrow's digest is not due before 07:00 UTC."
    path = tmp_path / 'quiet.json'
    quiet = _cursor(path, start)
    quiet.last_digest_date = start.date().isoformat()
    assert not plan_notification(quiet, summary, _settings(), start, True, transitions=False)
    quiet.last_digest_date = (start - timedelta(days=3)).date().isoformat()
    at_fifteen = start.replace(hour=15, minute=0)
    assert plan_notification(quiet, summary, _settings(), at_fifteen, True, transitions=False)
    pending = quiet.pending_notification
    assert pending is not None
    payload = cast(dict[str, str], json.loads(base64.b64decode(pending['payload_base64'])))
    assert payload['subject'] == f'Origo daily digest {start.date().isoformat()}'
    assert 'lifetime' not in payload['text'].lower()


class _Response:
    def __init__(self, status: int, body: bytes, retry_after: str | None = None) -> None:
        self.status, self.body = status, body
        self.headers = {'Retry-After': retry_after} if retry_after else {}
    def __enter__(self) -> _Response:
        return self
    def __exit__(self, *_args: object) -> bool:
        return False
    def read(self, limit: int) -> bytes:
        return self.body[:limit]


def test_retry_restart_and_storage_faults_preserve_delivery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    summary, now = _summary(), _now()
    settings, path = _settings(), tmp_path / 'cursor.json'
    cursor = _cursor(path, now)
    assert plan_notification(cursor, summary, settings, now, False, transitions=True)
    original = copy.deepcopy(cursor.pending_notification)
    assert original is not None
    calls: list[tuple[bytes, str, str]] = []
    def sender(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
        saved = _document(path)
        assert page._object(saved['pending_notification'])['attempt_started_at'] is not None
        calls.append((payload, api_url, idempotency_key))
        return 'local-provider-acceptance'
    persist_count = 0
    def fail_after_acceptance() -> None:
        nonlocal persist_count
        persist_count += 1
        if persist_count == 2:
            raise OSError('injected atomic-save failure after provider acceptance')
        cursor.save(path)
    with pytest.raises(OSError, match='after provider acceptance'):
        attempt_delivery(cursor, settings, now, fail_after_acceptance, completion_clock=lambda: now, sender=sender)
    restarted = _cursor(path, now + timedelta(minutes=1))
    assert restarted.notified_through is None
    assert restarted.pending_notification is not None
    assert restarted.pending_notification['payload_sha256'] == original['payload_sha256']
    changed = _settings(email_from='changed@example.com', email_to=('other@example.com',), resend_api_url='https://other.example.com/emails')
    retry_time = now + timedelta(minutes=1)
    assert attempt_delivery(restarted, changed, retry_time, lambda: restarted.save(path),
                            completion_clock=lambda: retry_time, sender=sender) == 'acknowledged'
    assert calls[0] == calls[1]
    assert restarted.pending_notification is None and restarted.last_delivery is not None
    assert restarted.last_delivery['acknowledgement_id'] == 'local-provider-acceptance'
    assert restarted.notified_through == summary['sampling_slot']
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.stat().st_size <= 256 * 1024
    restored = _cursor(path, retry_time)
    assert restored.last_delivery == restarted.last_delivery
    assert restored.next_distinct_at == restarted.next_distinct_at
    blocked_path = tmp_path / 'no-durable-cursor.json'
    blocked = _cursor(blocked_path, now)
    assert plan_notification(blocked, summary, settings, now, True, transitions=True)
    def cannot_persist() -> None:
        raise OSError('injected private persistence fault')
    before = len(calls)
    with pytest.raises(OSError):
        attempt_delivery(blocked, settings, now, cannot_persist, sender=sender)
    assert len(calls) == before
    corrupted = copy.deepcopy(original)
    corrupted['payload_sha256'] = '0' * 64
    with pytest.raises(ValueError):
        validate_pending(corrupted)
    for status, body, disposition in ((200, b'{}', 'uncertain'), (429, b'{}', 'retryable'),
         (409, b'{"name":"concurrent_idempotent_requests"}', 'retryable'),
         (409, b'{"name":"invalid_idempotent_request"}', 'invariant'),
         (400, b'{}', 'rejected'), (503, b'{}', 'uncertain')):
        def fake_urlopen(request: urllib.request.Request, timeout: float) -> _Response:
            assert request.data == calls[0][0]
            assert request.get_header('User-agent') == 'origo-monitor'
            assert request.get_header('Idempotency-key') == calls[0][2]
            assert timeout == 10
            return _Response(status, body, '120' if status == 429 else None)
        monkeypatch.setattr(transport.urllib.request, 'urlopen', fake_urlopen)
        with pytest.raises(DeliveryError) as caught:
            transport.send_alert(calls[0][0], api_key='test', api_url=settings.resend_api_url, idempotency_key=calls[0][2])
        assert caught.value.disposition == disposition
        if status == 429:
            assert caught.value.retry_after == 120
    expired = _cursor(tmp_path / 'expiry.json', now)
    assert plan_notification(expired, summary, settings, now, True, transitions=True)
    assert attempt_delivery(expired, None, now + timedelta(hours=1), lambda: expired.save(tmp_path / 'expiry.json')) == 'expired'
    assert expired.notified_through is None and expired.lost_intervals
    # Legacy dates/timestamps are migrated once; no restart extends the one-time reservation.
    for tag, sent, digest in (
        ('recent', {'legacy': now.timestamp() - 30}, ''),
        ('digest', {}, now.date().isoformat()),
        ('future', {'legacy': now.timestamp() + 86400}, ''),
    ):
        legacy_path = tmp_path / f'legacy-{tag}.json'
        legacy_path.write_text(json.dumps({'failures_after': now.timestamp(), 'receipts_after': now.isoformat(),
            'logs_after': now.isoformat(), 'sent': sent, 'last_digest_date': digest, 'ticks': 5, 'findings': 2}))
        migrated = _cursor(legacy_path, now)
        assert now.timestamp() + 3600 <= migrated.next_distinct_at <= now.timestamp() + 3610
        if tag == 'future':
            assert any('future' in interval['reason'] for interval in migrated.lost_intervals)
        migrated.save(legacy_path)
        assert _cursor(legacy_path, now + timedelta(minutes=10)).next_distinct_at == migrated.next_distinct_at
    retry_path = tmp_path / 'retry-after.json'
    retry_cursor = _cursor(retry_path, now)
    assert plan_notification(retry_cursor, summary, settings, now, True, transitions=True)
    def throttled(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
        raise DeliveryError('injected429', disposition='retryable', status=429, retry_after=120)
    assert attempt_delivery(retry_cursor, settings, now, lambda: retry_cursor.save(retry_path),
                            completion_clock=lambda: now, sender=throttled) == 'retryable'
    after_restart = _cursor(retry_path, now + timedelta(minutes=1))
    assert after_restart.pending_notification is not None
    assert after_restart.pending_notification['next_attempt_at'] == now.timestamp() + 120
    assert after_restart.pending_notification['expires_at'] == now.timestamp() + 3600
    assert attempt_delivery(after_restart, settings, now + timedelta(seconds=119),
                            lambda: after_restart.save(retry_path), sender=throttled) == 'waiting'
    for index in range(40):
        start = now + timedelta(days=index)
        add_loss_interval(expired, start.isoformat(), (start + timedelta(minutes=1)).isoformat(), f'local-storage-fault-{index}')
    assert len(expired.lost_intervals) <= 32



class _ReplayReporter(Reporter):
    def __init__(self, calls: list[str]) -> None:
        super().__init__('http://local-transport.invalid')
        self.calls = calls
        self.receipts: list[Mapping[str, object]] = []

    def _post(self, path: str, payload: Mapping[str, object]) -> bool:
        self.calls.append('dagit')
        self.receipts.append(payload)
        return True


def _monitor_with_recorded_reads(root: Path, monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> Monitor:
    """Replay captured read results; inject only storage/transport behavior, with no production probes."""
    report = page._object(_document(CAPTURES / 'current.json')['last_report'])
    monitor = object.__new__(Monitor)
    monitor.catalog = cast(LawCatalog, _document(CAPTURES / 'catalog.json'))
    monitor.law_tape = LawTape(root / 'law')
    monitor.law_tape.root.mkdir(parents=True)
    monitor.notification_history = monitor_module._NotificationHistory(monitor.law_tape.root)
    monitor.notification_clock = None
    monitor.cursor_path = root / 'private' / 'monitor.cursor.json'
    monitor.settings = _settings()
    monitor.reporter = _ReplayReporter(calls)
    monitor.queue_threshold = 200
    monitor.law_known_since = datetime(2026, 9, 1, tzinfo=UTC)
    evidence = {str(event['gate_id']).removeprefix('monitor.'): cast(dict[str, Scalar], event['evidence'])
                for event in page._objects(report['gates']) if str(event['gate_id']).startswith('monitor.')}
    def dagster(_cursor: Cursor, _end: datetime) -> tuple[list[Finding], bool]:
        monitor.tick_evidence['dagster_reachable'] = evidence['dagster_reachable'].copy()
        monitor.tick_evidence['queue_bounded'] = evidence['queue_bounded'].copy()
        monitor.condition_states['dagster_unreachable'] = ('dagster_reachable', '', True)
        monitor.condition_states['queue_backlog'] = ('queue_bounded', '', True)
        return [], True
    def read(check: str) -> list[Finding]:
        monitor.tick_evidence[check] = evidence[check].copy()
        return []
    def law(_now: datetime, _findings: Sequence[Finding]) -> list[Finding]:
        monitor.pending_report = cast(LawReport, copy.deepcopy(report))
        return law_findings(monitor.pending_report)
    monkeypatch.setattr(monitor, '_dagster_findings', dagster)
    def workers(_cursor: Cursor, _end: datetime) -> list[Finding]:
        return read('workers_alive')
    def collectors(_minute: datetime) -> list[Finding]:
        return read('collectors_serving')
    def logs(_cursor: Cursor, _end: datetime) -> list[Finding]:
        return read('no_error_logs')
    def no_findings(*_args: object) -> list[Finding]:
        return []
    monkeypatch.setattr(monitor, '_worker_findings', workers)
    monkeypatch.setattr(monitor, '_collector_findings', collectors)
    monkeypatch.setattr(monitor, '_log_findings', logs)
    monkeypatch.setattr(monitor, '_publication_findings', lambda: read('publication_current'))
    monkeypatch.setattr(monitor, '_law_findings', law)
    monkeypatch.setattr(monitor, '_history_findings', no_findings)
    monkeypatch.setattr(monitor, '_page_findings', no_findings)
    return monitor


def _assert_monitor_failure_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = _now()
    for fault in ('mail', 'public', 'private', 'corrupt', 'disabled', 'law-read'):
        with monkeypatch.context() as patch:
            calls: list[str] = []
            monitor = _monitor_with_recorded_reads(tmp_path / fault, patch, calls)
            def sender(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
                assert calls[:7] == ['dagit'] * 7, 'New email must follow all seven Dagit write attempts.'
                calls.append('post')
                if fault == 'mail':
                    raise DeliveryError('injected local timeout', disposition='uncertain')
                if fault == 'public':
                    decoded = cast(dict[str, str], json.loads(payload))
                    assert 'incomplete' in decoded['text'].lower()
                return 'local-verification-ack'
            patch.setattr(delivery, 'send_alert', sender)
            original_pending: object = None
            if fault == 'public':
                def public_write_fails(*_args: object) -> None:
                    raise OSError('injected public sidecar disk fault')
                patch.setattr(monitor.notification_history, 'append', public_write_fails)
            elif fault == 'private':
                original_save = Cursor.save
                def private_save_fails(cursor: Cursor, path: Path) -> None:
                    def bad_fsync(_fd: int) -> None:
                        raise OSError('injected private fsync fault')
                    with patch.context() as inner:
                        inner.setattr(monitor_module.os, 'fsync', bad_fsync)
                        original_save(cursor, path)
                patch.setattr(Cursor, 'save', private_save_fails)
            elif fault == 'corrupt':
                cursor = _cursor(monitor.cursor_path, now)
                assert plan_notification(cursor, _summary(), _settings(), now, True, transitions=True)
                cursor.save(monitor.cursor_path)
                damaged = _document(monitor.cursor_path)
                pending = page._object(damaged['pending_notification'])
                pending['payload_sha256'] = '0' * 64
                original_pending = copy.deepcopy(pending)
                monitor.cursor_path.write_text(json.dumps(damaged))
            elif fault == 'disabled':
                monitor.settings = None
                cursor = _cursor(monitor.cursor_path, now)
                cursor.notification_started_at = (now - timedelta(hours=25)).isoformat()
                cursor.save(monitor.cursor_path)
            elif fault == 'law-read':
                def law_read_fails(*_args: object) -> list[Finding]:
                    raise OSError('injected law evidence read fault')
                patch.setattr(monitor, '_law_findings', law_read_fails)
            monitor.tick(now)
            if fault == 'private':
                assert 'post' not in calls and not monitor.cursor_path.exists()
                assert any('notification_fault' in cast(dict[str, object], item.get('metadata', {})) for item in cast(_ReplayReporter, monitor.reporter).receipts)
            else:
                saved = _cursor(monitor.cursor_path, now)
                assert saved.failures_after == now.timestamp()
                assert saved.receipts_after == (now - timedelta(seconds=monitor_module.DELIVERY_LAG_SECONDS)).isoformat()
                assert saved.logs_after == saved.receipts_after
                if fault == 'mail':
                    assert saved.pending_notification is not None and saved.notified_through is None
                elif fault == 'corrupt':
                    assert 'post' not in calls and saved.notification_fault
                    assert _document(monitor.cursor_path)['pending_notification'] == original_pending
                elif fault == 'disabled':
                    assert 'post' not in calls and saved.notified_through is None
                    assert saved.expired_through is not None and saved.lost_intervals
                else:
                    assert calls.count('post') == 1
                if fault == 'law-read':
                    assert not list(monitor.law_tape.root.glob('samples-*'))
                    frame = monitor.notification_history.observations()[-1]
                    assert not frame['law_observed'] and frame['law_sample_ref'] is None
                    assert frame['read_windows']['error_lines']['count'] == 0



def _measure_monitor_replay(root: Path) -> Document:
    script = r'''
import base64
import copy
import json
import resource
import sys
import runpy
import time
import zlib
from datetime import datetime, timedelta
from pathlib import Path
import pytest
test_helpers = runpy.run_path(sys.argv[1])
_monitor_with_recorded_reads = test_helpers['_monitor_with_recorded_reads']
_now, _document, _recorded_frames = (test_helpers[key] for key in ('_now', '_document', '_recorded_frames'))
CAPTURES = test_helpers['CAPTURES']
from origo.observatory import build_summary, lifecycle_transitions
from origo.workers.monitor import _NotificationHistory

with pytest.MonkeyPatch.context() as patch:
    directory = sys.argv[2]
    monitor = _monitor_with_recorded_reads(Path(directory), patch, [])
    monitor.settings = None
    now = _now()
    monitor.tick(now)
    frame = monitor.notification_history.observations()[-1]
    original_measurements = [row['measurements'] for row in frame['observations']]
    replay = _NotificationHistory(Path(directory) / 'load-protocol-only')
    for minute in range(1440):
        slot = (now.replace(second=0, microsecond=0) - timedelta(minutes=1439-minute)).isoformat()
        envelope = {**frame, 'sampling_slot': slot}
        raw = json.dumps({'frame': base64.b64encode(zlib.compress(json.dumps(envelope, separators=(',', ':')).encode())).decode(), 'brief': None}, separators=(',', ':')).encode()
        replay.frames[slot] = raw
    index_bytes = sys.getsizeof(replay.frames) + sum(sys.getsizeof(key) + sys.getsizeof(raw) for key, raw in replay.frames.items())
    started = time.monotonic()
    frames = replay.observations()
    decoded_seconds = time.monotonic()-started
    assert [row['measurements'] for row in frames[0]['observations']] == original_measurements
    current, catalog = (_document(CAPTURES/name) for name in ('current.json', 'catalog.json'))
    started = time.monotonic()
    summary = build_summary(current, catalog, frames, now=now)
    summary_seconds = time.monotonic()-started
    started = time.monotonic()
    lifecycle_transitions(frames, now=now)
    reducer_seconds = time.monotonic()-started
    actual = _recorded_frames('r1-perp-20260928')
    started = time.monotonic()
    build_summary(current, catalog, actual, now=now)
    actual_seconds = time.monotonic()-started
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 if sys.platform.startswith('linux') else 1024*1024)
    result={'load_kind':'1440 protocol envelopes reuse one actual captured all-law/operational frame; source measurement values/times unchanged','groups_per_frame':len(frame['observations']),'measurements_per_frame':sum(len(row['measurements']) for row in frame['observations']),'encoded_frame_bytes':max(map(len,replay.frames.values())),'frames':len(frames),'retained_index_bytes':index_bytes,'observations_seconds':decoded_seconds,'build_summary_seconds':summary_seconds,'lifecycle_reducer_seconds':reducer_seconds,'once_each_total_seconds':decoded_seconds+summary_seconds+reducer_seconds,'genuine_r1_frames':len(actual),'genuine_r1_summary_seconds':actual_seconds,'peak_rss_mib':rss}
    print(json.dumps(result,indent=2))
'''
    measured = subprocess.run([sys.executable, '-c', script, str(Path(__file__)), str(root)],
                              cwd=ROOT, check=True, capture_output=True, text=True)
    result = cast(Document, json.loads(measured.stdout))
    (root / 'monitor-replay-metrics.json').write_text(measured.stdout)
    return result

def test_monitor_pipeline_preserves_law_and_resource_contracts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from .test_law_page import _measure_memory
    _assert_monitor_failure_paths(tmp_path / 'failure-paths', monkeypatch)
    measured_monitor = _measure_monitor_replay(tmp_path / 'monitor-benchmark')
    assert measured_monitor['frames'] == 1440 and measured_monitor['groups_per_frame'] == 18
    assert float(str(measured_monitor['retained_index_bytes'])) <= 16 * 1024 * 1024
    assert float(str(measured_monitor['once_each_total_seconds'])) < 1
    assert float(str(measured_monitor['peak_rss_mib'])) < 1024
    assert measured_monitor['genuine_r1_frames'] == 711
    assert float(str(measured_monitor['genuine_r1_summary_seconds'])) < 1
    baseline = _document(ROOT / 'tests/fixtures/law/overview-baseline-58b45de.json')
    report = page._object(baseline['report'])
    assert len(json.dumps(report, separators=(',', ':')).encode()) == baseline['compact_report_bytes'] == 126790
    assert page.MAX_SAMPLE_BYTES == 96 * 1024 * 1024 and page.MAX_RECORD == 1024 * 1024
    original_catalog = _document(CAPTURES / 'catalog.json')
    # The captures predate a main-branch law change. Their hashes/provenance remain
    # historical evidence; this PR's invariance oracle is its current main base.
    base_sha = subprocess.run(['git', 'merge-base', 'HEAD', 'origin/main'], cwd=ROOT,
                              check=True, capture_output=True, text=True).stdout.strip()
    for relative in ('origo/law.py',):
        base_source = subprocess.run(['git', 'show', f'{base_sha}:{relative}'], cwd=ROOT,
                                     check=True, capture_output=True).stdout
        assert (ROOT / relative).read_bytes() == base_source, relative
    # New source acquisition descriptors may extend the catalog; core operational
    # law declarations remain the same as main, independently of formatting/line shifts.
    catalog_base = subprocess.run(['git', 'show', f'{base_sha}:origo/law_catalog.py'], cwd=ROOT,
                                  check=True, capture_output=True).stdout
    before_laws = next(node for node in ast.parse(catalog_base).body
                       if isinstance(node, ast.FunctionDef) and node.name == '_operational_gates')
    after_laws = next(node for node in ast.parse((ROOT / 'origo/law_catalog.py').read_bytes()).body
                      if isinstance(node, ast.FunctionDef) and node.name == '_operational_gates')
    assert ast.dump(before_laws, include_attributes=False) == ast.dump(after_laws, include_attributes=False)
    rebuilt = build_catalog(base_sha)
    original_ids = {str(gate['id']) for gate in page._objects(original_catalog['gates']) if str(gate['id']).startswith('law.')}
    rebuilt_ids = {gate['id'] for gate in rebuilt['gates'] if gate['id'].startswith('law.')}
    assert rebuilt_ids == original_ids and len(original_ids) == 17
    with _serving(tmp_path) as (url, cache, summary):
        with urllib.request.urlopen(url + '/law.json', timeout=5) as response:
            assert len(response.read()) <= 160 * 1024, 'Actual current wire must include summary within160KiB.'
        # The established load benchmark changes protocol envelopes, never recorded source values.
        measured = _measure_memory(cache.root)
        assert measured['samples'] == 43201 and measured['events'] == 2505600
        assert measured['limited'] is False
        assert float(str(measured['rss_mib'])) < 256
        assert float(str(measured['current_seconds'])) < 1
        assert len(json.dumps(summary, ensure_ascii=False, separators=(',', ':')).encode()) <= 16 * 1024
        before = cache.current(_now())['consecutive_clear_slots']
        (cache.root / 'notification-observations-2026-09-28.jsonl').write_text('unreadable notification sidecar\n')
        cache.advance(_now(), budget_seconds=1)
        assert cache.current(_now())['consecutive_clear_slots'] == before
        assert cache.current(_now())['history_limited'] is False
        assert not any(path.name.startswith('notification-') for path in cache._paths(_now()))
        warmed = time.monotonic()
        cache.current(_now())
        assert time.monotonic() - warmed < 1
    # Missing-law capture still retains independently recorded operational reads.
    current = _document(CAPTURES / 'current.json')
    report = page._object(current['last_report'])
    monitor = object.__new__(Monitor)
    monitor.catalog = cast(LawCatalog, _document(CAPTURES / 'catalog.json'))
    monitor.notification_history = monitor_module._NotificationHistory(tmp_path / 'sidecars')
    monitor.event_counts = {}
    monitor.publication_policy = {}
    monitor.condition_states = {'queue_backlog': ('queue_bounded', '', True)}
    monitor.tick_evidence = {
        str(event['gate_id']).removeprefix('monitor.'): cast(dict[str, Scalar], event['evidence'])
        for event in page._objects(report['gates']) if str(event['gate_id']).startswith('monitor.')
    }
    reads = {check: True for check in monitor.tick_evidence}
    reads['data_current'] = False
    cursor = _cursor(tmp_path / 'adapter.cursor.json', _now())
    frame = monitor._observation_frame(None, [], set(), cursor, reads, _now())
    assert frame['law_sample_ref'] is None and frame['law_observed'] is False
    assert frame['read_windows']['error_lines']['window_end'] == '2026-09-28T11:49:21+00:00'
    assert frame['read_windows']['error_lines']['count'] == 0
    monitor.notification_history.append(frame, None, _now())
    segment = next(monitor.notification_history.root.glob('notification-observations-*'))
    assert segment.stat().st_size <= 8193
    assert not list(monitor.notification_history.root.glob('samples-*'))
    replay = monitor_module._NotificationHistory(monitor.notification_history.root)
    replay.refresh(_now())
    assert replay.observations() == [frame]
    assert len(replay.frames) == 1
    replay.refresh(_now())
    assert len(replay.frames) == 1, 'Incremental replay must not duplicate slots.'
    retained = monitor.notification_history.root / 'closeout'
    retained.mkdir()
    retained_file = retained / 'notification-observations-2026-01-01.jsonl'
    retained_file.write_bytes(segment.read_bytes())
    old = monitor.notification_history.root / 'notification-observations-2026-01-01.jsonl'
    old.write_bytes(segment.read_bytes())
    replay.refresh(_now())
    assert not old.exists() and retained_file.exists()
    # Protocol overload duplicates an existing recorded row; it asserts byte admission,
    # never a claim that these duplicate envelopes are additional source incidents.
    overloaded = copy.deepcopy(frame)
    overloaded['observations'] *= 100
    bounded = monitor_module._NotificationHistory(tmp_path / 'bounded-sidecar')
    bounded.append(overloaded, None, _now())
    bounded_line = next(bounded.root.glob('notification-observations-*')).read_bytes()
    assert len(bounded_line) <= 8193 and len(overloaded['observations']) <= 64
    assert overloaded['omitted_groups'] and not overloaded['complete']
    raw_decoded, _ = bounded.decode(bounded_line)
    assert raw_decoded == overloaded
    retained_history = monitor_module._NotificationHistory(tmp_path / 'retained-history')
    for recorded in _recorded_frames('r1-perp-20260928'):
        retained_history.append(recorded, None, _now())
    retained_segment = next(retained_history.root.glob('notification-observations-*'))
    original_lines = retained_segment.read_bytes()
    # Repeat the original protocol bytes to exceed replay admission without inventing observations.
    repeats = 1 + (2 * 1024 * 1024 // len(original_lines))
    retained_segment.write_bytes(original_lines * repeats)
    bounded_replay = monitor_module._NotificationHistory(retained_history.root)
    started = time.monotonic()
    bounded_replay.refresh(_now())
    assert time.monotonic() - started <= 1
    assert retained_segment.stat().st_size - bounded_replay.starts[retained_segment] <= 1024 * 1024
    assert bounded_replay.loading
    assert sum(len(raw) for raw in bounded_replay.frames.values()) <= 16 * 1024 * 1024
    rss_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 if sys.platform.startswith('linux') else 1024 * 1024)
    assert rss_mib < 1024
    # A due immutable retry may use a committed slot without repeating any detector.
    monitor.cursor_path = tmp_path / 'resume.cursor.json'
    monitor.notification_clock = None
    monitor.settings = _settings()
    monitor.law_tape = LawTape(tmp_path / 'same-minute-law')
    monitor.law_tape.last = cast(LawReport, report)
    reserved = _cursor(monitor.cursor_path, _now())
    prior = _now() - timedelta(minutes=1)
    assert plan_notification(reserved, _summary(), monitor.settings, prior, True, transitions=True)
    def uncertain(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
        raise DeliveryError('injected local timeout', disposition='uncertain')
    assert attempt_delivery(reserved, monitor.settings, prior, lambda: reserved.save(monitor.cursor_path),
                            completion_clock=lambda: prior, sender=uncertain) == 'uncertain'
    monitor.notification_clock = (prior.timestamp(), time.monotonic() - 60)
    posts: list[bytes] = []
    def accept(payload: bytes, *, api_key: str, api_url: str, idempotency_key: str) -> str:
        posts.append(payload)
        return 'same-minute-local-acceptance'
    monkeypatch.setattr(delivery, 'send_alert', accept)
    calls: list[str] = []
    def forbidden_probe(*_args: object, **_kwargs: object) -> None:
        calls.append('probe')
        raise AssertionError('A committed-minute retry cannot probe or write Dagit again.')
    for name in ('_dagster_findings', '_worker_findings', '_collector_findings', '_log_findings', '_law_findings', '_page_findings'):
        monkeypatch.setattr(monitor, name, forbidden_probe)
    outcome = monitor.tick(_now())
    assert outcome.processed == () and outcome.failed == () and len(posts) == 1 and calls == []
    before = monitor.cursor_path.read_bytes()
    monitor.tick(_now())
    assert monitor.cursor_path.read_bytes() == before and len(posts) == 1 and calls == []
    assert subprocess.run(['git', 'diff', '--quiet', 'origin/main', '--', 'origo/law.py'], cwd=ROOT).returncode == 0


def test_public_dashboard_url_is_deployed_and_validated(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    subprocess.run([sys.executable, '-m', 'pytest', '-q',
        'tests/tools/test_deploy_compose_bind.py::test_dashboard_url_is_optional_in_both_monitor_compose_paths',
        'tests/tools/test_deploy_workflow.py::test_optional_dashboard_url_reaches_generated_deploy_environment'],
        cwd=ROOT, check=True, capture_output=True, text=True)
    def no_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError('URL configuration must not issue DNS or network probes.')
    monkeypatch.setattr(transport.urllib.request, 'urlopen', no_network)
    for value in ('http://37.27.112.167:8484/law', 'https://10.0.0.10/law', 'http://100.64.0.1/law', 'https://ops.example.com/law/', 'https://[fd00::1]/law'):
        assert transport.validate_dashboard_url(value) is not None
    for value in ('http://law/law', 'https://localhost/law', 'http://local.localhost/law', 'http://127.0.0.1/law', 'http://169.254.0.1/law', 'http://224.0.0.1/law', 'http://0.0.0.0/law', 'http://[::]/law', 'http://user:password@ops.example.com/law', 'https://ops.example.com/other', 'https://ops.example.com/law?x=1', 'https://ops.example.com/law#gate'):
        with pytest.raises(ValueError):
            transport.validate_dashboard_url(value)
    assert AlertSettings.from_environment({'ORIGO_ALERT_DASHBOARD_URL': 'https://ops.example.com/law'}) is None
    with pytest.raises(RuntimeError, match='incomplete'):
        AlertSettings.from_environment({'ORIGO_ALERT_EMAIL_TO': 'operator@example.com'})
    environment = {'ORIGO_ALERT_RESEND_API_KEY': 'test', 'ORIGO_ALERT_EMAIL_FROM': 'sender@example.com', 'ORIGO_ALERT_EMAIL_TO': 'operator@example.com'}
    assert (settings := AlertSettings.from_environment(environment)) is not None
    assert settings.public_dashboard_url is None
    invalid = AlertSettings.from_environment({**environment, 'ORIGO_ALERT_DASHBOARD_URL': 'http://law/law'})
    assert invalid is not None and invalid.public_dashboard_url is None and invalid.dashboard_url_fault
    assert 'Links omitted' in caplog.text
    target: dict[str, object] = {'view': 'laws', 'gate': 'law.R1:binance_perp_trades', 'source': 'source with spaces', 'family': 'R1', 'ignored': 'never transmitted'}
    link = dashboard_link('https://ops.example.com/law', target)
    assert link is not None
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(link).query)
    assert query['gate'] == ['law.R1:binance_perp_trades'] and query['source'] == ['source with spaces']
    assert 'ignored' not in query and not urllib.parse.urlsplit(link).fragment
