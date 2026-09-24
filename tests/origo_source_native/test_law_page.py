from __future__ import annotations

import copy
import json
import re
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from playwright.sync_api import Page, Route, sync_playwright

from origo import law
from origo.law_catalog import build_catalog, gate_evaluation
from origo.workers import law_page as page

from .test_law import law_case as law_case
from .test_law import real_law_report as real_law_report
from .test_monitor import _monitor, _Recorder
from .test_monitor import recorder as recorder

SHA = 'ca888afed9bc1681ceebe4c9cfd0502538e2a2d2'
BASELINE = Path(__file__).parents[1] / 'fixtures/law/overview-baseline-58b45de.json'


def _write(path: Path, records: list[page.Document]) -> None:
    path.write_text(''.join(json.dumps(record) + '\n' for record in records))


@pytest.fixture()
def tape(tmp_path: Path, real_law_report: law.LawReport) -> tuple[Path, page.Document, datetime]:
    catalog = build_catalog(SHA)
    report = copy.deepcopy(real_law_report)
    report['catalog_version'], report['deployed_sha'] = catalog['version'], SHA
    for feed in report['feeds']:
        for name, predicate in feed['predicates'].items():
            descriptor = next((gate for gate in catalog['gates'] if gate['id'] == f"law.{name}:{feed['source_key']}"), None)
            if descriptor:
                status = predicate['status']
                report['gates'].append(gate_evaluation(descriptor,
                    evidence_id=f"{report['sampling_slot']}:{feed['source_key']}:{name}",
                    evaluated_at=report['evaluation_start'],
                    outcome='EXPECTED_WAIT' if status == 'NOT_DUE' else status,
                    evidence=predicate['evidence'], reason=predicate['reason']))
    inventory_gate = next(gate for gate in catalog['gates'] if gate['id'] == 'law.inventory')
    report['gates'].append(gate_evaluation(inventory_gate, evidence_id=report['sampling_slot'] + ':inventory', evaluated_at=report['evaluation_start'], outcome='PASS', evidence={'live': len(report['inventory'])}, reason='all_live_sources_evaluated'))
    for event in report['gates']:
        event['catalog_version'] = catalog['version']
    # The association comes from the real catalog, not an invented failing gate.
    for observation in report['projections']:
        source = observation['id'].split(':')[0]
        observation['gate_ids'] = [gate['id'] for gate in catalog['gates'] if source in gate['scope'] and gate['id'].startswith('law.')]
    root = tmp_path / 'law'
    root.mkdir()
    (root / f"catalog-{catalog['version']}.json").write_text(json.dumps(catalog))
    document = cast(page.Document, json.loads(json.dumps(report)))
    day = report['sampling_slot'][:10]
    _write(root / f'samples-{day}.jsonl', [document])
    _write(root / f'gate-events-{day}.jsonl', page._objects(document['gates']))
    return root, document, datetime.fromisoformat(report['evaluation_end']) + timedelta(seconds=1)


@pytest.fixture()
def serving(tape: tuple[Path, page.Document, datetime]) -> Iterator[tuple[str, page.TapeCache, page.Document, datetime]]:
    root, report, now = tape
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    cache.advance(now, budget_seconds=2)
    server = page.LawServer(('127.0.0.1', 0), cache, lambda: now)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', cache, report, now
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture()
def health_server() -> Iterator[int]:
    server = page.HTTPServer(('127.0.0.1', 0), page.HealthHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _get(url: str) -> page.Document:
    with urllib.request.urlopen(url, timeout=3) as response:
        return page._decode(response.read())


def test_http_routes_read_tape_without_database_or_credentials(
    serving: tuple[str, page.TapeCache, page.Document, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    url, cache, original, _ = serving
    before = {path.name: path.read_bytes() for path in cache.root.iterdir()}
    for name in ('CLICKHOUSE_PASSWORD', 'BINANCE_API_KEY', 'RESEND_API_KEY', 'HF_TOKEN'):
        monkeypatch.delenv(name, raising=False)
    response = _get(url + '/law.json')
    assert response['last_report'] == original
    assert response['status'] == original['status']
    with urllib.request.urlopen(url + '/law') as html:
        assert html.status == 200
        assert b'Content-Type' not in html.read()  # HTML shell, no credentials or embedded query.
    for _ in range(3):
        assert _get(url + '/law.json')['last_report'] == original
    assert before == {path.name: path.read_bytes() for path in cache.root.iterdir()}
    result = subprocess.run([sys.executable, '-c',
        'import sys; import origo.workers.law_page; '
        'assert not any(k.startswith(("clickhouse", "origo.sources", "origo.law")) for k in sys.modules)'],
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_missing_malformed_or_stale_report_is_unknown(tape: tuple[Path, page.Document, datetime]) -> None:
    root, original, now = tape
    cache = page.TapeCache(root)
    assert cache.current(now)['status'] == original['status']
    assert cache.current(now + timedelta(seconds=121))['status'] == 'UNKNOWN'
    path = next(root.glob('samples-*'))
    with path.open('a') as stream:
        stream.write('{')
    assert cache.current(now)['status'] == original['status']  # Unfinished append is not a report.
    with path.open('a') as stream:
        stream.write('\n')
    assert cache.current(now)['status'] == 'UNKNOWN'
    assert cache.current(now)['reason'] == 'malformed_report'
    for mutation in ('inventory', 'schema_version'):
        record = copy.deepcopy(original)
        record[mutation] = [] if mutation == 'inventory' else 999
        _write(path, [record])
        assert cache.current(now)['status'] == 'UNKNOWN'
    for mutation in ('future_end', 'wrong_slot', 'missing_inventory_feed', 'unsupported_live_unknown'):
        record = copy.deepcopy(original)
        if mutation == 'future_end':
            record['evaluation_end'] = (now + timedelta(days=1)).isoformat()
        elif mutation == 'wrong_slot':
            record['sampling_slot'] = (page._instant(original['sampling_slot']) - timedelta(minutes=1)).isoformat()
        elif mutation == 'missing_inventory_feed':
            record['feeds'] = page._objects(record['feeds'])[1:]
        else:
            # Corrupt protocol envelopes use genuine UNKNOWN predicate evidence; no market rows change.
            unknown = next(predicate for feed in page._objects(original['feeds'])
                for predicate in page._objects(list(page._object(feed['predicates']).values()))
                if predicate['status'] == 'UNKNOWN')
            # A falsely green floor must not conceal the additional UNKNOWN inventory member.
            for feed in page._objects(record['feeds']):
                for predicate in page._object(feed['predicates']).values():
                    page._object(predicate)['status'] = 'PASS'
            record['inventory'] = [*cast(list[page.Json], record['inventory']), 'unsupported_live']
            record['feeds'] = [*page._objects(record['feeds']),
                {'source_key': 'unsupported_live', 'predicates': {'inventory': unknown}}]
            record['status'] = 'PASS'
        _write(path, [record])
        assert cache.current(now)['status'] == 'UNKNOWN'
        assert cache.current(now)['reason'] == 'malformed_report'
    path.unlink()
    assert cache.current(now)['reason'] == 'missing_report'


def test_recovery_and_clear_duration_require_consecutive_observations(tape: tuple[Path, page.Document, datetime]) -> None:
    root, original, now = tape
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    cache.advance(now, budget_seconds=2)
    current = cache.current(now)
    assert current['consecutive_clear_slots'] == 0  # Genuine incomplete system cannot claim 72h.
    assert all(value is None for value in page._object(current['delay_change_1h_seconds']).values())
    brief = page._sample_brief(original)
    previous = now.replace(second=0, microsecond=0) - timedelta(hours=1)
    cache.samples[previous.isoformat()] = {**brief, 'slot': previous.isoformat()}
    delta = page._object(cache.current(now)['delay_change_1h_seconds'])
    assert delta['binance_spot_trades'] == 0  # Same captured observation; no invented market metric.
    # Clock vectors exercise protocol continuity only, never alter market rows or claim production health.
    terminal = page._instant(original['sampling_slot'])
    for index in range(4321):
        stamp = terminal - timedelta(minutes=4320 - index)
        cache.samples[stamp.isoformat()] = {**brief, 'slot': stamp.isoformat(), 'start': stamp.isoformat(), 'status': 'PASS'}
    valid = page.consecutive_window(list(cache.samples.values()), terminal.isoformat())
    assert len(valid) == 4321
    assert (page._instant(valid[0]['start']) - page._instant(valid[-1]['start'])).total_seconds() == 72 * 3600
    assert cache.current(now)['consecutive_clear_slots'] == 0  # Current genuine FAIL still overrides protocol vectors.
    missing = terminal - timedelta(minutes=1)
    del cache.samples[missing.isoformat()]
    assert len(page.consecutive_window(list(cache.samples.values()), terminal.isoformat())) == 1
    cache.samples[missing.isoformat()] = {**brief, 'slot': missing.isoformat(), 'status': 'UNKNOWN'}
    assert len(page.consecutive_window(list(cache.samples.values()), terminal.isoformat())) == 1
    cache.samples[missing.isoformat()] = {**brief, 'slot': missing.isoformat(), 'status': 'PASS', 'policy': 'changed'}
    assert len(page.consecutive_window(list(cache.samples.values()), terminal.isoformat())) == 1


def test_page_reads_incrementally_and_health_ignores_data_verdict(
    serving: tuple[str, page.TapeCache, page.Document, datetime], health_server: int,
) -> None:
    url, cache, report, now = serving
    before = cache.bytes_read
    for _ in range(3):
        assert _get(url + '/law.json')['status'] == report['status']
    assert cache.bytes_read == before
    port = str(health_server)
    check = subprocess.run([sys.executable, '-m', 'origo.workers.law_page', '--check', '--health-port', port], capture_output=True)
    assert check.returncode == 0
    for path in cache.root.glob('samples-*'):
        path.unlink()
    assert cache.current(now)['status'] == 'UNKNOWN'
    assert page.main(['--check', '--health-port', port]) == 0
    assert page.main(['--check', '--health-port', '1']) == 1


def test_gate_history_api_is_catalog_only_paginated_and_gap_honest(tape: tuple[Path, page.Document, datetime]) -> None:
    root, report, now = tape
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    original = page._objects(report['gates'])[0]
    gate = str(original['gate_id'])
    event_path = next(root.glob('gate-events-*'))
    # Duplicate protocol deliveries refer to the same real evaluated predicate.
    _write(event_path, [original, original])
    cache.advance(now, budget_seconds=2)
    query = {'gate_id': [gate], 'from': [(now - timedelta(days=2)).isoformat()], 'to': [now.isoformat()]}
    result = cache.history(query, now)
    assert result['events'] == [original]
    days = page._objects(result['days'])
    assert any(day['not_observed'] is True for day in days)
    assert sum(int(str(day['evaluation_count'])) for day in days) == 1
    assert page._objects(result['definitions'])[0]['definition_version'] == original['definition_version']
    observed_minute = page._instant(original['evaluated_at']).replace(second=0, microsecond=0)
    outside = cache.history({**query, 'from': [(observed_minute - timedelta(minutes=1)).isoformat()],
        'to': [observed_minute.isoformat()]}, now)
    assert outside['events'] == []
    assert sum(int(str(day['observed_slots'])) for day in page._objects(outside['days'])) == 0
    inside = cache.history({**query, 'from': [observed_minute.isoformat()],
        'to': [now.isoformat()]}, now)
    assert sum(int(str(day['observed_slots'])) for day in page._objects(inside['days'])) == 1
    assert sum(int(str(day['expected_slots'])) for day in page._objects(inside['days'])) == 1
    for changes in ({'gate_id': ['../../etc/passwd']}, {'sql': ['SELECT 1']}, {'cursor': ['invented']},
                    {'from': [(now - timedelta(days=31)).isoformat()]}):
        with pytest.raises(ValueError):
            cache.history({**query, **changes}, now)
    # Genuine predicate payload, repeated transport envelopes only; these IDs are test events, not market data.
    records = [{**original, 'evidence_id': f'protocol-delivery-{index}'} for index in range(1001)]
    _write(event_path, [*records, records[0]])
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    cache.advance(now, budget_seconds=2)
    result = cache.history(query, now)
    assert len(page._objects(result['events'])) <= 1000 and result['next_cursor']
    received = page._objects(result['events'])
    while result['next_cursor']:
        result = cache.history({**query, 'cursor': [str(result['next_cursor'])]}, now)
        received.extend(page._objects(result['events']))
    assert len(received) == 1001
    assert len({str(event['evidence_id']) for event in received}) == 1001


def test_browser_sources_gates_recovery_and_accessible_drilldown(
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, cache, _, now = serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport={'width': 1440, 'height': 1050}, has_touch=True)
        tab = context.new_page()
        errors: list[str] = []
        tab.on('pageerror', lambda error: errors.append(str(error)))
        started = time.monotonic()
        tab.goto(url + '/law?view=sources')
        tab.locator('.source').first.wait_for()
        assert time.monotonic() - started < 2
        catalog = page._object(cache.current(now)['catalog'])
        sources = page._objects(catalog['sources'])
        assert tab.locator('.source').count() == len(sources)
        assert tab.locator('.node').count() == sum(len(page._objects(source['projections'])) for source in sources)
        assert tab.locator('table').count() == 0
        target = 'binance_spot_trades:raw_latest'
        tab.locator(f'[data-projection="{target}"]').click()
        assert 'projection=' in tab.url
        assert tab.locator('#detail').is_visible()
        assert 'Active build / component proof' in tab.locator('#detail').inner_text()
        started = time.monotonic()
        tab.locator('#detail [data-gate]').first.click()
        tab.locator('.gate').first.wait_for()
        assert time.monotonic() - started < 0.3
        assert f'projection={target.replace(":", "%3A")}' in tab.url
        assert tab.locator('.gate .info').count() == tab.locator('.gate').count()
        tab.locator('#gate-search').fill('Reader age')
        assert tab.locator('.gate').count() == 2
        tab.locator('#gate-search').fill('')
        info = tab.locator('.gate .info').first
        info.focus()
        tooltip = tab.locator('.gate .tooltip').first
        assert tooltip.is_visible()
        assert 'governs' in tooltip.inner_text().lower() and 'fails or waits when' in tooltip.inner_text().lower()
        href = tooltip.locator('a').get_attribute('href')
        assert href and href.startswith(f'https://github.com/Vaquum/Origo/blob/{SHA}/')
        info.tap()
        assert info.get_attribute('aria-expanded') == 'true'
        assert tab.locator('.heatmap').first.locator('button').count() == 30
        tab.screenshot(path='/tmp/origo-law-gates.png', full_page=True)
        tab.locator('[data-view="recovery"]').click()
        assert 'projection=' in tab.url and 'source=' in tab.url
        assert tab.locator('.chart-card').count() == 1
        assert 'Core-law consecutive clear window' not in tab.locator('#message').inner_text()
        tab.screenshot(path='/tmp/origo-law-recovery.png', full_page=True)
        tab.set_viewport_size({'width': 390, 'height': 844})
        tab.locator('[data-view="sources"]').click()
        assert tab.evaluate('document.documentElement.scrollWidth <= innerWidth')
        tab.locator('[data-close]').click()
        tab.screenshot(path='/tmp/origo-law-mobile.png', full_page=True)
        tab.set_viewport_size({'width': 1440, 'height': 1050})
        tab.screenshot(path='/tmp/origo-law-desktop.png', full_page=True)
        assert tab.locator('.node .CURRENT').count() > 0
        tab.route('**/law.json', lambda route: route.abort('failed'))
        tab.evaluate('refresh()')
        assert 'unknown' in tab.locator('#health').inner_text()
        assert tab.locator('.node .CURRENT').count() == 0
        assert tab.locator('.node .UNKNOWN').count() == tab.locator('.node').count()
        assert 'observation service unavailable' in tab.locator('#message').inner_text().lower()
        assert not errors
        browser.close()


def test_slow_headers_have_absolute_deadline_and_bounded_capacity(
    serving: tuple[str, page.TapeCache, page.Document, datetime], health_server: int,
) -> None:
    url, _, _, _ = serving
    address = ('127.0.0.1', int(url.rsplit(':', 1)[1]))
    clients: list[socket.socket] = []
    closed = threading.Event()
    stop = threading.Event()
    finished: list[float] = []

    def trickle(client: socket.socket) -> None:
        while not stop.wait(0.05):
            try:
                client.sendall(b'x')
            except OSError:
                finished.append(time.monotonic())
                closed.set()
                break

    workers: list[threading.Thread] = []
    started = time.monotonic()
    try:
        first = socket.create_connection(address, timeout=1)
        first.sendall(b'GET /law HTTP/1.1\r\nHost: localhost\r\nX-Slow: ')
        clients.append(first)
        worker = threading.Thread(target=trickle, args=(first,))
        worker.start()
        workers.append(worker)
        # A live trickling client cannot hold up unrelated health or current-data GETs.
        assert _get(url + '/law.json')['catalog_key']
        with urllib.request.urlopen(url + '/law', timeout=1) as response:
            assert response.status == 200
        for _ in range(page.REQUEST_SLOTS - 1):
            client = socket.create_connection(address, timeout=1)
            client.sendall(b'GET /law HTTP/1.1\r\nHost: localhost\r\nX-Slow: ')
            clients.append(client)
            worker = threading.Thread(target=trickle, args=(client,))
            worker.start()
            workers.append(worker)
        extra = socket.create_connection(address, timeout=1)
        clients.append(extra)
        try:
            assert extra.recv(1) == b''
        except ConnectionResetError:
            assert time.monotonic() - started < page.REQUEST_TIMEOUT
        with urllib.request.urlopen(f'http://127.0.0.1:{health_server}/healthz', timeout=1) as response:
            assert response.read() == b'ok\n'
        assert page.main(['--check', '--health-port', str(health_server)]) == 0
        assert closed.wait(page.REQUEST_TIMEOUT + 1)
        for worker in workers:
            worker.join(timeout=page.REQUEST_TIMEOUT + 1)
        assert len(finished) == page.REQUEST_SLOTS
        assert max(finished) - started < page.REQUEST_TIMEOUT + 1
        with urllib.request.urlopen(url + '/law', timeout=1) as response:
            assert response.status == 200
    finally:
        stop.set()
        for client in clients:
            client.close()
        for worker in workers:
            worker.join(timeout=1)


def test_history_cache_limits_preserve_current_report(
    tape: tuple[Path, page.Document, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, report, now = tape
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    original = page._objects(report['gates'])[0]
    monkeypatch.setattr(page, 'MAX_SAMPLE_BYTES', 1)
    monkeypatch.setattr(page, 'MAX_EVENT_IDENTITIES', 1)
    cache._sample(report)
    cache._event(original)
    cache._event({**original, 'evidence_id': 'repeated-protocol-envelope'})
    assert cache.samples == {} and cache.event_count == 1
    assert cache.current(now)['history_limited'] is True
    assert cache.current(now)['last_report'] == report
    assert cache._definitions(str(original['gate_id']))


def _measure_memory(root: Path) -> page.Document:
    script = r"""
import json, resource, sys, time
from datetime import datetime, timedelta
from pathlib import Path
from origo.workers.law_page import TapeCache
root = Path(sys.argv[1])
report = json.loads(next(root.glob('samples-*')).read_text().splitlines()[-1])
now = datetime.fromisoformat(report['evaluation_end']) + timedelta(seconds=1)
cache = TapeCache(root)
cache.refresh_latest(now)
for n in range(43201):
    stamp = (now - timedelta(minutes=n)).replace(second=0, microsecond=0).isoformat()
    cache._sample({**report, 'sampling_slot': stamp, 'evaluation_start': stamp})
# Normal production volume: 58 gate envelopes per minute. Original evidence is unchanged.
events = [event for event in report['gates'] if event.get('evidence_id')]
for n in range(43200):
    stamp = (now - timedelta(minutes=43200-n)).isoformat()
    for index in range(58):
        cache._event({**events[index % len(events)], 'evaluated_at': stamp,
            'evidence_id': f'protocol:{n}:{index}'}, offset=(n*58+index)*1024)
started = time.monotonic()
current = cache.current(now)
rss = (next(int(line.split()[1]) for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmHWM:')) / 1024
    if sys.platform.startswith('linux') else resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1048576)
print(json.dumps({'rss_mib': rss, 'samples': len(cache.samples), 'events': cache.event_count, 'index_bytes': sum(len(value) for value in cache.index.values()),
    'current_seconds': time.monotonic()-started, 'limited': current['history_limited'],
    'operations_slots': len(cache.operations_samples), 'operations_bytes': cache.operations_bytes, 'sample_bytes': cache.sample_bytes}))
"""
    measured = subprocess.run([sys.executable, '-c', script, str(root)], check=True, capture_output=True, text=True)
    return page._decode(measured.stdout.encode())


def test_thirty_day_replay_cache_fits_page_memory_budget(tape: tuple[Path, page.Document, datetime]) -> None:
    result = _measure_memory(tape[0])
    assert result['samples'] == 43201 and result['events'] == 2_505_600
    assert result['index_bytes'] == 2_505_600 * 16
    assert float(str(result['rss_mib'])) < 256 and float(str(result['current_seconds'])) < 1
    assert result['limited'] is False
    Path('/tmp/origo-law-cache-memory.json').write_text(json.dumps(result, indent=2))


def test_browser_recovery_pagination_and_history_lru(
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, cache, report, now = serving
    gate = 'law.R1:binance_spot_trades'
    original = next(item for item in page._objects(report['gates']) if item['gate_id'] == gate)
    with next(cache.root.glob('gate-events-*')).open('a') as stream:
        stream.write(''.join(json.dumps({**original, 'evidence_id': f'protocol-delivery-{index}'})+'\n' for index in range(1000)))
    cache.advance(now, budget_seconds=2)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url + '/law?view=recovery&source=binance_spot_trades')
        more = tab.locator('[data-history-more]')
        more.wait_for()
        assert 'Incomplete chart' in tab.locator('.chart-card').inner_text()
        more.click()
        more.wait_for(state='hidden')
        assert tab.evaluate(f'getHistory(historyKey("{gate}")).events.length') == 1001
        tab.evaluate('''async () => {
            const end = data.checked_at;
            for (let hours=1; hours<=12; hours++) {
                state.from=new Date(Date.parse(end)-hours*3600000).toISOString();state.to=end;
                await loadHistory('law.R1:binance_spot_trades');
            }
        }''')
        assert tab.evaluate('histories.size') == 8
        assert tab.evaluate("getHistory(historyKey('law.R1:binance_spot_trades')).events.length") == 1000
        browser.close()


def test_gate_index_latest_first_two_pages_and_partial_startup(tape: tuple[Path, page.Document, datetime]) -> None:
    root, report, now = tape
    gate = 'law.R1:binance_spot_trades'
    original = next(event for event in page._objects(report['gates']) if event['gate_id'] == gate)
    others = [event for event in page._objects(report['gates']) if event['gate_id'] != gate]
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(days=1)
    # One genuine selected-gate observation and 57 unrelated evidence envelopes per minute.
    with ExitStack() as stack:
        streams = {day: stack.enter_context((root / f'gate-events-{day}.jsonl').open('w'))
            for day in (start.date().isoformat(), end.date().isoformat())}
        for minute in range(1440):
            stamp = start + timedelta(minutes=minute, seconds=5)
            for index in range(58):
                event = original if index == 0 else others[(index-1) % len(others)]
                streams[stamp.date().isoformat()].write(json.dumps({**event,
                    'evaluated_at': stamp.isoformat(), 'evidence_id': f'protocol:{minute}:{index}'})+'\n')
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    query = {'gate_id': [gate], 'from': [start.isoformat()], 'to': [end.isoformat()]}
    partial = cache.history(query, now)
    assert partial['events'] == [] and partial['loading'] is True
    while cache.loading:
        cache.advance(now, budget_seconds=1)
    first = cache.history(query, now)
    events = page._objects(first['events'])
    assert len(events) == 1000 and first['loading'] is False
    assert events[0]['evaluated_at'] == (end-timedelta(seconds=55)).isoformat()
    second = cache.history({**query, 'cursor': [str(first['next_cursor'])]}, now)
    assert len(page._objects(second['events'])) == 440 and second['next_cursor'] is None
    assert len({str(event['evidence_id']) for event in events+page._objects(second['events'])}) == 1440
    # A late original-time import and a repeated delivery do not shift or duplicate the cursor.
    late = {**original, 'evaluated_at': (start+timedelta(minutes=10, seconds=5)).isoformat(),
        'evidence_id': 'late-protocol-envelope'}
    with (root / f'gate-events-{start.date().isoformat()}.jsonl').open('a') as stream:
        stream.write(json.dumps(late)+'\n'+json.dumps(late)+'\n')
    cache.advance(now, budget_seconds=1)
    resumed = cache.history({**query, 'cursor': [str(first['next_cursor'])]}, now)
    assert len(page._objects(resumed['events'])) == 441
    assert sum(event['evidence_id'] == late['evidence_id'] for event in page._objects(resumed['events'])) == 1
    restarted = page.TapeCache(root)
    restarted.refresh_latest(now)
    while restarted.loading:
        restarted.advance(now, budget_seconds=1)
    assert restarted.history(query, now)['events'] == events


def test_browser_age_expires_during_held_fetch(
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, cache, _, now = serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.clock.install()
        # A genuine report at 119 seconds old is still within its contractual freshness window.
        tab.route('**/law.json', lambda route: route.fulfill(json=cache.current(now+timedelta(seconds=118))))
        tab.goto(url+'/law?view=sources')
        tab.locator('.node .CURRENT').first.wait_for()
        tab.unroute('**/law.json')
        held: list[object] = []
        tab.route('**/law.json', lambda route: held.append(route))
        tab.evaluate('void refresh()')
        tab.evaluate('void refresh()')
        tab.clock.run_for(2200)
        assert len(held) == 1
        assert tab.evaluate('refreshing') is True
        assert tab.evaluate('data.reason') == 'stale_report'
        assert tab.locator('.node .CURRENT').count() == 0
        tab.clock.run_for(4000)
        assert tab.evaluate('refreshing') is False
        browser.close()


def test_oversized_records_advance_without_accepting_fragments(tape: tuple[Path, page.Document, datetime]) -> None:
    root, report, now = tape
    event = page._objects(report['gates'])[0]
    path = next(root.glob('gate-events-*'))
    path.write_bytes(b'x' * (page.READ_BUDGET * 2 + 7) + b'\n' + json.dumps(event).encode() + b'\n')
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    for _ in range(5):
        before = cache.positions.get(path.name, 0)
        cache.advance(now, budget_seconds=2)
        assert cache.positions[path.name] > before or not cache.loading
        if not cache.loading:
            break
    assert not cache.loading and cache.history_limited
    assert cache.event_count == 1 and cache.positions[path.name] == path.stat().st_size
    assert cache.current(now)['status'] == report['status']


def test_core_definition_changes_break_streak_but_deployment_metadata_does_not(
    tape: tuple[Path, page.Document, datetime],
) -> None:
    _, report, _ = tape
    original = page._sample_brief(report)
    assert original['policy']
    moved = copy.deepcopy(report)
    moved['deployed_sha'] = '0' * 40
    moved['catalog_version'] = 'different-deployment'
    for event in page._objects(moved['gates']):
        event['deployed_sha'] = '0' * 40
    assert page._sample_brief(moved)['policy'] == original['policy']
    core = next(event for event in page._objects(moved['gates']) if str(event['gate_id']).startswith('law.'))
    core['definition_version'] = 'predicate-change'
    assert page._sample_brief(moved)['policy'] != original['policy']
    core['definition_version'] = None
    assert page._sample_brief(moved)['policy'] is None
    moved['gates'] = []
    assert page._sample_brief(moved)['policy'] is None
    assert not page.consecutive_window([{**original, 'status': 'PASS', 'policy': None}], str(original['slot']))


def test_current_wire_is_small_and_complete_overview_is_separate(
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, cache, _, now = serving
    current = _get(url+'/law.json')
    assert 'catalog' not in current and 'gate_days' not in current
    catalog = _get(url+'/law/catalog.json')
    overview = _get(url+'/law/gates.json')
    assert set(page._object(overview['gates'])) == {str(gate['id']) for gate in page._objects(catalog['gates'])}
    assert len(cast(list[page.Json], overview['days'])) == 30
    sizes = {route: len(json.dumps(_get(url+route)).encode()) for route in ('/law.json', '/law/catalog.json', '/law/gates.json')}
    assert sizes['/law.json'] < 100_000 and sizes['/law/gates.json'] < 100_000
    request = urllib.request.Request(url+'/law.json', headers={'Accept-Encoding': 'gzip'})
    with urllib.request.urlopen(request) as response:
        assert response.headers['Content-Encoding'] == 'gzip'
        sizes['compressed_current'] = len(response.read())
    Path('/tmp/origo-law-wire-size.json').write_text(json.dumps(sizes))
    assert cache.current(now)['catalog']


def test_slow_response_and_disconnected_clients_leave_service_available(
    tape: tuple[Path, page.Document, datetime], capsys: pytest.CaptureFixture[str],
) -> None:
    root, _, now = tape
    cache = page.TapeCache(root)
    cache.refresh_latest(now)

    class SmallSendBuffer(page.LawServer):
        def get_request(self) -> tuple[socket.socket, tuple[str, int]]:
            connection, address = super().get_request()
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
            return connection, address

    server = SmallSendBuffer(('127.0.0.1', 0), cache, lambda: now)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    address = ('127.0.0.1', server.server_port)
    try:
        client = socket.socket()
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        client.settimeout(10)
        client.connect(address)
        with client:
            client.sendall(b'GET /law/catalog.json HTTP/1.0\r\nHost: localhost\r\n\r\n')
            time.sleep(page.REQUEST_TIMEOUT + 0.3)
            with client.makefile('rb') as stream:
                assert b'200' in stream.readline()
                headers: dict[bytes, bytes] = {}
                while (line := stream.readline()) != b'\r\n':
                    key, value = line.split(b':', 1)
                    headers[key.lower()] = value.strip()
                body = stream.read()
            assert len(body) == int(headers[b'content-length'])
            assert page._decode(body)['gates']
        for _ in range(5):
            with socket.create_connection(address, timeout=2) as reset:
                reset.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                reset.sendall(b'GET /law/catalog.json HTTP/1.0\r\nHost: localhost\r\n\r\n')
            time.sleep(0.03)
        assert _get(f'http://127.0.0.1:{server.server_port}/law.json')['catalog_key']
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert 'Traceback' not in capsys.readouterr().err


def test_many_deployments_keep_bounded_catalogs_and_original_event_code(
    tape: tuple[Path, page.Document, datetime],
) -> None:
    root, report, now = tape
    original_path = next(root.glob('catalog-*'))
    template = original_path.read_text()
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    event = page._objects(report['gates'])[0]
    # Deployment envelopes replay identical genuine definitions; no source evidence is invented.
    for index in range(420):
        sha = f'{index + 1:040x}'
        record = page._decode(template.replace(SHA, sha).encode())
        version = f'deployment-envelope-{index}'
        record['version'] = version
        (root / f'catalog-{version}.json').write_text(json.dumps(record))
        cache._load_catalog(version)
    assert len(cache.catalogs) == 2 and len(cache.catalog_versions) == 421
    assert cache.catalog_bytes < page.MAX_CATALOG_BYTES and not cache.history_limited
    assert all(len(versions) == 1 for versions in cache.definitions.values())
    cache.advance(now, budget_seconds=2)
    query = {'gate_id': [str(event['gate_id'])], 'from': [(now-timedelta(hours=1)).isoformat()], 'to': [now.isoformat()]}
    result = cache.history(query, now)
    definition = page._objects(result['definitions'])[0]
    assert page._object(definition['code'])['deployed_sha'] == SHA
    assert SHA in str(page._object(definition['code'])['url'])
    legacy = {key: value for key, value in event.items() if key != 'deployed_sha'}
    legacy['evidence_id'] = 'legacy-delivery-of-original-evidence'
    _write(next(root.glob('gate-events-*')), [legacy])
    restarted = page.TapeCache(root)
    restarted.refresh_latest(now)
    restarted.advance(now, budget_seconds=2)
    legacy_result = restarted.history(query, now)
    assert legacy_result['events'] == [legacy] and legacy_result['definitions'] == []
    Path('/tmp/origo-law-catalog-memory.json').write_text(json.dumps({'snapshots': len(cache.catalog_versions), 'resident_full': len(cache.catalogs), 'accounted_bytes': cache.catalog_bytes}))



def test_same_sha_configuration_regimes_preserve_exact_original_definitions(
    tape: tuple[Path, page.Document, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, report, now = tape
    records: list[page.Document] = []
    versions: list[str] = []
    # These are real catalog builds with changed operator configuration, not invented queue measurements.
    for threshold in (200, 201):
        monkeypatch.setenv('ORIGO_ALERT_QUEUE_THRESHOLD', str(threshold))
        catalog = build_catalog(SHA)
        versions.append(catalog['version'])
        descriptor = next(gate for gate in catalog['gates'] if gate['id'] == 'monitor.queue_bounded')
        event = gate_evaluation(descriptor, evidence_id=f'configuration-envelope:{catalog["version"]}',
            evaluated_at=str(report['evaluation_start']), outcome='UNKNOWN', evidence={}, reason='not_observed')
        event['catalog_version'] = catalog['version']
        records.append(cast(page.Document, json.loads(json.dumps(event))))
        (root / f'catalog-{catalog["version"]}.json').write_text(json.dumps(catalog))
    assert versions[0] != versions[1]
    _write(next(root.glob('gate-events-*')), records)
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    for version in versions:
        cache._load_catalog(version)
    cache.advance(now, budget_seconds=2)
    query = {'gate_id': ['monitor.queue_bounded'], 'from': [(now-timedelta(hours=1)).isoformat()], 'to': [now.isoformat()]}
    result = cache.history(query, now)
    definitions = {str(item['catalog_version']): item for item in page._objects(result['definitions'])}
    assert set(definitions) == set(versions)
    for version, threshold in zip(versions, (200, 201), strict=True):
        assert page._object(definitions[version]['thresholds'])['queue_threshold'] == threshold
        code = page._object(definitions[version]['code'])
        assert code['deployed_sha'] == SHA and f'/blob/{SHA}/' in str(code['url'])
    # A SHA-only legacy event is ambiguous here; never guess the last-loaded configuration.
    _write(next(root.glob('gate-events-*')), [{key: value for key, value in records[0].items() if key != 'catalog_version'}])
    restarted = page.TapeCache(root)
    restarted.refresh_latest(now)
    for version in reversed(versions):
        restarted._load_catalog(version)
    restarted.advance(now, budget_seconds=2)
    assert restarted.history(query, now)['definitions'] == []
    # Explicit unknown catalog metadata must not silently fall back to the SHA either.
    _write(next(root.glob('gate-events-*')), [{**records[0], 'catalog_version': '../unknown'}])
    restarted = page.TapeCache(root)
    restarted.refresh_latest(now)
    restarted.advance(now, budget_seconds=2)
    assert restarted.history(query, now)['definitions'] == []



def test_recovery_delta_retains_direction_without_changing_duration(
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, _, report, _ = serving
    feed = next(item for item in page._objects(report['feeds']) if item['source_key'] == 'binance_spot_trades')
    magnitude = page._number(page._object(page._object(page._object(feed['predicates'])['R1'])['evidence'])['age_seconds'])
    assert magnitude is not None and magnitude > 0
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law?view=recovery&source=binance_spot_trades')
        tab.locator('.chart-card').wait_for()
        normal = tab.evaluate('duration', magnitude)
        # Signed formatting vectors use a captured age magnitude, not claimed historical market deltas.
        for value, expected in ((-magnitude, f'-{normal} · less lag'), (magnitude, f'+{normal} · more lag'), (0, '0s · unchanged'), (None, 'No exact observation one hour earlier')):
            tab.evaluate("value=>{data.delay_change_1h_seconds.binance_spot_trades=value;renderContent()}", value)
            assert tab.locator('.recovery-metrics>div').nth(1).locator('strong').inner_text() == expected
        assert tab.evaluate('duration', magnitude) == normal
        browser.close()


def test_catalog_report_interleaving_never_displays_mixed_definitions(
    serving: tuple[str, page.TapeCache, page.Document, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    url, cache, report, now = serving
    old_version = str(report['catalog_version'])
    old_overview = cache.overview(now)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law?view=gates')
        tab.locator('.day').first.wait_for()
        tab.wait_for_function('() => ![...requests.keys()].some(key => key.startsWith("days|"))')
        # A genuine configured catalog changes between the two HTTP reads. Source evidence is unchanged.
        old_catalog = page._object(cache.current(now)['catalog'])
        old_gate = next(gate for gate in page._objects(old_catalog['gates']) if gate['id'] == 'monitor.queue_bounded')
        threshold = int(str(page._object(old_gate['thresholds'])['queue_threshold'])) + 1
        monkeypatch.setenv('ORIGO_ALERT_QUEUE_THRESHOLD', str(threshold))
        new_catalog = build_catalog(SHA)
        (cache.root / f'catalog-{new_catalog["version"]}.json').write_text(json.dumps(new_catalog))
        cache._load_catalog(new_catalog['version'])
        next_report = {**report, 'catalog_version': new_catalog['version']}

        def arrive_before_catalog_response() -> None:
            _write(next(cache.root.glob('samples-*')), [next_report])
            cache.refresh_latest(now)

        tab.route('**/law/catalog.json', lambda route: (arrive_before_catalog_response(), route.continue_()))
        tab.evaluate('refresh()')
        tab.wait_for_function('() => ![...requests.keys()].some(key => key.startsWith("days|"))')
        assert tab.evaluate('data.status') == 'UNKNOWN'
        assert tab.evaluate('data.last_report.catalog_version') == old_version
        assert tab.evaluate('data.catalog.version') == old_version
        assert tab.evaluate('cachedCatalog.version') == old_version
        assert tab.evaluate("descriptor('monitor.queue_bounded').thresholds.queue_threshold") == threshold - 1
        tab.unroute('**/law/catalog.json')
        # A delayed old overview must not repopulate the heatmap after adopting the new report/catalog pair.
        tab.route('**/law/gates.json', lambda route: route.fulfill(json=old_overview))
        tab.evaluate('refresh()')
        tab.wait_for_function('() => ![...requests.keys()].some(key => key.startsWith("days|"))')
        assert tab.evaluate('data.last_report.catalog_version') == new_catalog['version']
        assert tab.evaluate('data.catalog.version') == new_catalog['version']
        assert tab.evaluate('data.gate_days === undefined') is True
        assert tab.evaluate("descriptor('monitor.queue_bounded').thresholds.queue_threshold") == threshold
        tab.unroute('**/law/gates.json')
        tab.evaluate('loadOverview()')
        assert tab.locator('.day').count() == len(new_catalog['law_gate_ids']) * 30
        assert tab.evaluate('data.status') == report['status']
        browser.close()



@pytest.mark.parametrize('corruption', ['invalid_json', 'invalid_timestamp', 'indexed_record', 'missing_identity', 'missing_slot', 'duplicate_sample',
    *[f'identity:{field}:{kind}' for field in ('gate_id', 'definition_version', 'evidence_id') for kind in ('null', 'list', 'numeric')]])
def test_corrupt_history_records_remain_visibly_incomplete(
    tape: tuple[Path, page.Document, datetime], corruption: str,
) -> None:
    root, report, now = tape
    original = page._objects(report['gates'])[0]
    path = next(root.glob('gate-events-*'))
    good = json.dumps(original).encode() + b'\n'
    broken = good[:-2] + b'!\n' if corruption == 'invalid_json' else json.dumps({**original, 'evaluated_at': 'damaged-timestamp'}).encode() + b'\n'
    if corruption == 'missing_identity':
        broken = json.dumps({key: value for key, value in original.items() if key != 'evidence_id'}).encode() + b'\n'
    if corruption.startswith('identity:'):
        _, field, kind = corruption.split(':')
        invalid: page.Json = {'null': None, 'list': [original[field]], 'numeric': 1}[kind]
        broken = json.dumps({**original, field: invalid}).encode() + b'\n'
    if corruption in ('missing_slot', 'duplicate_sample'):
        path = next(root.glob('samples-*'))
        good = json.dumps(report).encode() + b'\n'
        broken = json.dumps({key: value for key, value in report.items() if key != 'sampling_slot'}).encode() + b'\n' if corruption == 'missing_slot' else good
    path.write_bytes(good if corruption == 'indexed_record' else broken + good)
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    cache.advance(now, budget_seconds=2)
    assert cache.positions[path.name] == path.stat().st_size and not cache.loading
    if corruption == 'indexed_record':
        path.write_bytes(b'!' + good[1:])  # Damage a previously indexed real event without changing byte positions.
    query = {'gate_id': [str(original['gate_id'])], 'from': [(now-timedelta(hours=1)).isoformat()], 'to': [now.isoformat()]}
    result = cache.history(query, now)
    assert result['limited'] is True and cache.current(now)['history_limited'] is True
    assert cache.current(now)['last_report'] == report
    assert result['events'] == ([] if corruption == 'indexed_record' else [original])


def test_paginated_history_retains_each_original_catalog_definition(
    serving: tuple[str, page.TapeCache, page.Document, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    url, cache, report, now = serving
    monkeypatch.setattr(page, 'PAGE_SIZE', 10)
    catalogs = []
    events: list[page.Document] = []
    # Actual configured catalogs; UNKNOWN events claim no unobserved reader measurement.
    for index, threshold in enumerate((200, 201)):
        monkeypatch.setattr(law, 'R1_SPOT_BUDGET_SECONDS', threshold)
        catalog = build_catalog(SHA)
        catalogs.append(catalog)
        (cache.root / f'catalog-{catalog["version"]}.json').write_text(json.dumps(catalog))
        cache._load_catalog(catalog['version'])
        descriptor = next(gate for gate in catalog['gates'] if gate['id'] == 'law.R1:binance_spot_trades')
        stamp = page._instant(report['evaluation_start']) - timedelta(seconds=30 if index == 0 else 0)
        event = gate_evaluation(descriptor, evidence_id=f'configuration-envelope:{index}',
            evaluated_at=stamp.isoformat(), outcome='UNKNOWN', evidence={}, reason='not_observed', catalog_version=catalog['version'])
        events.append(cast(page.Document, json.loads(json.dumps(event))))
    with next(cache.root.glob('gate-events-*')).open('a') as stream:
        stream.write(json.dumps(events[0])+'\n')
        for index in range(page.PAGE_SIZE):
            stream.write(json.dumps({**events[1], 'evidence_id': f'configuration-delivery:{index}'})+'\n')
    cache.advance(now, budget_seconds=2)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law?view=gates&gate=law.R1:binance_spot_trades')
        more = tab.locator('[data-more]')
        more.wait_for()
        assert tab.evaluate("getHistory(historyKey('law.R1:binance_spot_trades')).definitions.length") == 1
        more.click()
        more.wait_for(state='hidden')
        loaded = tab.evaluate("getHistory(historyKey('law.R1:binance_spot_trades'))")
        assert len(loaded['events']) == page.PAGE_SIZE + 2
        assert {item['catalog_version'] for item in loaded['definitions']} == {str(report['catalog_version']), *(catalog['version'] for catalog in catalogs)}
        for position, threshold in ((0, 201), (-1, 200)):
            item = tab.locator('#detail .event').nth(position)
            item.locator('summary').first.click()
            assert f'budget seconds: {threshold}' in item.inner_text()
            assert f'/blob/{SHA}/' in str(item.locator('details a').get_attribute('href'))
        # Definition retention follows retained events, including the browser's existing 5,000-event cap.
        pruned = tab.evaluate("()=>{const h=getHistory(historyKey('law.R1:binance_spot_trades'));return mergeDefinitions([h.events[0]],h.definitions)}")
        assert len(pruned) == 1 and pruned[0]['catalog_version'] == catalogs[1]['version']
        # Replay an actual first response with metadata-budget omission, followed by a complete older page.
        def omit_first_definitions(route: Route) -> None:
            response = route.fetch()
            limited = cast(page.Document, response.json())
            limited.update(definitions=[], definitions_limited=True, limited=True)
            route.fulfill(json=limited)
        tab.route('**/law/history.json?*', omit_first_definitions)
        tab.evaluate("async()=>{histories.clear();await loadHistory('law.R1:binance_spot_trades')}")
        tab.unroute('**/law/history.json?*')
        more.click()
        more.wait_for(state='hidden')
        retained = tab.evaluate("getHistory(historyKey('law.R1:binance_spot_trades'))")
        assert retained['definitions_limited'] is True and retained['limited'] is True
        assert len(retained['events']) == page.PAGE_SIZE + 2 and len(retained['definitions']) == 2
        assert 'lookup budget reached' in tab.locator('#detail').inner_text()
        tab.evaluate("async()=>{histories.clear();await loadHistory('law.R1:binance_spot_trades')}")
        assert tab.evaluate("getHistory(historyKey('law.R1:binance_spot_trades')).limited") is False
        browser.close()



def test_monitor_verdict_history_uses_only_committed_valid_samples(
    tape: tuple[Path, page.Document, datetime],
) -> None:
    from origo.workers.monitor import law_findings

    root, report, now = tape
    catalog = build_catalog(SHA)
    descriptor = next(gate for gate in catalog['gates'] if gate['id'] == page.OWN_GATE)
    findings = law_findings(cast(law.LawReport, report))
    assert findings  # The captured fixture truthfully has incomplete source coverage.
    verdict = gate_evaluation(descriptor, evidence_id=f'{page.OWN_GATE}:{report["sampling_slot"]}',
        evaluated_at=str(report['evaluation_start']), outcome='FAIL', evidence={'finding_count': len(findings)},
        reason='finding_present', catalog_version=catalog['version'])
    event = cast(page.Document, json.loads(json.dumps(verdict)))
    committed = {**report, 'gates': [*page._objects(report['gates']), event]}
    sample_path = next(root.glob('samples-*'))
    _write(sample_path, [committed])
    # Inject the previously possible orphan PASS; this protocol corruption must never become evidence.
    _write(next(root.glob('gate-events-*')), [{**event, 'outcome': 'PASS'}])
    query = {'gate_id': [page.OWN_GATE], 'from': [(now-timedelta(hours=1)).isoformat()], 'to': [now.isoformat()]}
    for _ in range(2):  # Restart must choose the committed sample again, independent of file scan order.
        cache = page.TapeCache(root)
        cache.refresh_latest(now)
        assert cache.history(query, now)['loading'] is True
        cache.advance(now, budget_seconds=2)
        history = cache.history(query, now)
        assert history['events'] == [event] and history['loading'] is False
        assert sum(int(str(day['fail_count'])) for day in page._objects(history['days'])) == 1
        assert sum(int(str(day['pass_count'])) for day in page._objects(history['days'])) == 0
        assert page._objects(history['definitions'])[0]['catalog_version'] == catalog['version']
    with sample_path.open('ab') as stream:
        stream.write(json.dumps(committed).encode()[:-1])
    assert cache.history(query, now)['loading'] is True  # Sample tail controls loading, not the event-stream tail.
    invalid = {**committed, 'schema_version': 999, 'gates': [{**event, 'outcome': 'PASS'}]}
    _write(sample_path, [invalid])
    restarted = page.TapeCache(root)
    restarted.refresh_latest(now)
    restarted.advance(now, budget_seconds=2)
    assert restarted.history(query, now)['events'] == []
    assert restarted.history(query, now)['limited'] is True
    assert restarted.current(now)['status'] == 'UNKNOWN'



def test_empty_not_evaluated_identity_keeps_current_report_available(
    tape: tuple[Path, page.Document, datetime],
) -> None:
    root, report, now = tape
    catalog = build_catalog(SHA)
    descriptor = next(gate for gate in catalog['gates'] if gate['id'].startswith('source.component_integrity.'))
    placeholder = gate_evaluation(descriptor, evidence_id='', evaluated_at=str(report['evaluation_start']),
        outcome='NOT_EVALUATED', evidence={}, reason='historical_definition_unavailable', catalog_version=catalog['version'])
    recorded = {**report, 'gates': [*page._objects(report['gates']), cast(page.Document, json.loads(json.dumps(placeholder)))]}
    _write(next(root.glob('samples-*')), [recorded])
    cache = page.TapeCache(root)
    current = cache.current(now)
    assert current['status'] == report['status'] and current['reason'] == ''
    assert descriptor['id'] not in page._object(current['last_gate_events'])



@pytest.mark.parametrize('budget', ['bytes', 'time'])
def test_definition_lookup_budget_is_visible_request_local_and_retryable(
    tape: tuple[Path, page.Document, datetime], monkeypatch: pytest.MonkeyPatch, budget: str,
) -> None:
    root, report, now = tape
    original = page._objects(report['gates'])[0]
    records: list[page.Document] = []
    versions: list[str] = []
    for index in range(14):
        # Genuine registry definitions and captured event evidence; replay only deployment/time envelopes.
        sha = f'{index+1:040x}'
        catalog = build_catalog(sha)
        versions.append(catalog['version'])
        (root / f'catalog-{catalog["version"]}.json').write_text(json.dumps(catalog))
        records.append({**original, 'deployed_sha': sha, 'catalog_version': catalog['version'],
            'evidence_id': f'catalog-budget-envelope:{index}',
            'evaluated_at': (page._instant(original['evaluated_at'])-timedelta(seconds=index)).isoformat()})
    _write(next(root.glob('gate-events-*')), records)
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    for version in versions:
        cache._load_catalog(version)
    cache.advance(now, budget_seconds=2)
    query = {'gate_id': [str(original['gate_id'])], 'from': [(now-timedelta(hours=1)).isoformat()], 'to': [now.isoformat()]}
    reads: list[int] = []
    original_read = Path.read_bytes

    def measured_read(path: Path) -> bytes:
        raw = original_read(path)
        if path.name.startswith('catalog-'):
            reads.append(len(raw))
            if budget == 'time':
                time.sleep(0.06)  # One delayed filesystem read exhausts the unchanged 50 ms lookup budget.
        return raw

    with monkeypatch.context() as scoped:
        scoped.setattr(Path, 'read_bytes', measured_read)
        if budget == 'bytes':
            scoped.setattr(page, 'time', SimpleNamespace(monotonic=lambda: 0.0))  # Isolate byte admission from wall-clock scheduling.
        result = cache.history(query, now)
    assert result['definitions_limited'] is True and result['limited'] is True
    assert len(page._objects(result['events'])) == len(records)
    assert 0 < len(page._objects(result['definitions'])) < len(records)
    assert len(page._objects(result['definitions'])) == len(reads)
    assert sum(reads) <= page.READ_BUDGET
    assert len(reads) == 1 if budget == 'time' else sum(reads) + (root / f'catalog-{versions[len(reads)]}.json').stat().st_size > page.READ_BUDGET
    assert cache.history_limited is False and cache.current(now)['history_limited'] is False
    returned_versions = {definition['catalog_version'] for definition in page._objects(result['definitions'])}
    omitted = next(record for record in records if record['catalog_version'] not in returned_versions)
    stamp = page._instant(omitted['evaluated_at'])
    retried = cache.history({**query, 'from': [(stamp-timedelta(microseconds=1)).isoformat()],
        'to': [(stamp+timedelta(microseconds=1)).isoformat()]}, now)
    assert retried['events'] == [omitted]
    assert retried['limited'] is False and retried['definitions_limited'] is False
    definition = page._objects(retried['definitions'])[0]
    assert definition['catalog_version'] == omitted['catalog_version']
    assert page._object(definition['code'])['deployed_sha'] == omitted['deployed_sha']


@pytest.fixture()
def production_tape(tmp_path: Path) -> tuple[Path, page.Document, datetime]:
    baseline = page._decode(BASELINE.read_bytes())
    report = copy.deepcopy(page._object(baseline['report']))
    catalog = build_catalog(str(baseline['deployed_sha']))
    report['catalog_version'] = catalog['version']
    root = tmp_path / 'captured-law'
    root.mkdir()
    (root / f"catalog-{catalog['version']}.json").write_text(json.dumps(catalog))
    day = str(report['sampling_slot'])[:10]
    _write(root / f'samples-{day}.jsonl', [report])
    _write(root / f'gate-events-{day}.jsonl', [event for event in page._objects(report['gates']) if event.get('evidence_id')])
    return root, report, page._instant(report['evaluation_end']) + timedelta(seconds=1)


@pytest.fixture()
def production_serving(production_tape: tuple[Path, page.Document, datetime]) -> Iterator[tuple[str, page.TapeCache, page.Document, datetime]]:
    root, report, now = production_tape
    cache = page.TapeCache(root)
    cache.refresh_latest(now)
    while cache.loading:
        cache.advance(now, budget_seconds=1)
    server = page.LawServer(('127.0.0.1', 0), cache, lambda: now)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', cache, report, now
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture()
def operational_report(recorder: _Recorder, tmp_path: Path) -> page.Document:
    monitor = _monitor(recorder, tmp_path / 'measured-monitor')
    monitor.tick(datetime.now(UTC))
    assert monitor.law_tape.last is not None
    return page._decode(json.dumps(monitor.law_tape.last).encode())


def _figure(tab: Page, key: str) -> str:
    return tab.locator(f'[data-overview="{key}"]').inner_text()


def test_served_page_layout_contract() -> None:
    tabs = re.findall(r'<button[^>]*role="tab"[^>]*>', page.PAGE)
    assert [re.search(r'data-view="([^"]+)"', tag).group(1) for tag in tabs] == ['overview', 'sources', 'laws', 'recovery']
    assert 'aria-selected="true"' in tabs[0]
    assert not any(text in page.PAGE for text in ('Source to reader · live evidence', 'Know where your data stands.',
        'Follow every declared stage from its source to its published output.'))


def test_named_laws_preserve_original_definitions_and_clear_window() -> None:
    baseline = page._decode(BASELINE.read_bytes())
    catalog = build_catalog(str(baseline['deployed_sha']))
    original = page._objects(baseline['law_descriptors'])
    current = [gate for gate in catalog['gates'] if gate['id'].startswith('law.')]
    assert len(current) == 17
    original_ids = {str(gate['id']) for gate in original}
    assert {gate['id'] for gate in current} - original_ids == {
        'law.M1:binance_spot_trades', 'law.M2:binance_spot_trades'}
    # The accepted legacy law contract stays unchanged. Its implementation hash
    # advances with the additive proof evaluator; old evidence retains its version.
    current_semantics = [{**{key: value for key, value in gate.items() if key != 'definition_version'},
                          'code': {'path': gate['code']['path']}} for gate in current if gate['id'] in original_ids]
    original_semantics = [{**{key: value for key, value in gate.items() if key != 'definition_version'},
                           'code': {'path': page._object(gate['code'])['path']}} for gate in original]
    assert sorted(current_semantics, key=lambda gate: str(gate['id'])) == sorted(original_semantics, key=lambda gate: str(gate['id']))
    report = page._object(baseline['report'])
    before = page._sample_brief(report)
    assert before['policy'] == baseline['expected_policy'] == '5647749677518a069becefee8dbdafb774d940f23575986b7d2b04c08314da5e'
    decorated = copy.deepcopy(report)
    # Additive protocol fields are not new measurements and must not redefine the data laws.
    for event in page._objects(decorated['gates']):
        if str(event['gate_id']).startswith('monitor.'):
            page._object(event['evidence']).update(queued_runs=None, workers_fresh=None, error_lines=None)
    for observation in page._objects(decorated['projections']):
        if ':consumer:' in str(observation['id']):
            observation['publication_policy'] = {'reason': 'unknown', 'lag_seconds': None, 'grace_seconds': None,
                'state_through': None, 'published_through': None}
    after = page._sample_brief(decorated)
    assert before == after
    end = page._instant(report['sampling_slot'])
    vectors = [{**before, 'slot': (end-timedelta(minutes=4320-index)).isoformat(),
        'start': (end-timedelta(minutes=4320-index)).isoformat()} for index in range(4321)]
    assert len(page.consecutive_window(vectors, end.isoformat())) == 4321
    vectors[-1] = {**after, 'slot': end.isoformat(), 'start': end.isoformat()}
    assert len(page.consecutive_window(vectors, end.isoformat())) == 4321


def test_overview_production_shape_stays_within_resource_budgets(
    production_tape: tuple[Path, page.Document, datetime], operational_report: page.Document,
) -> None:
    root, report, _ = production_tape
    baseline = page._decode(BASELINE.read_bytes())
    raw = page._object(baseline['report'])
    assert len(json.dumps(raw, separators=(',', ':')).encode()) == baseline['compact_report_bytes'] == 126790
    assert len(page._objects(report['feeds'])) == 6 and len(page._objects(report['projections'])) == 54
    additions = {str(event['gate_id']): event['evidence'] for event in page._objects(operational_report['gates']) if str(event['gate_id']).startswith('monitor.')}
    decorated = copy.deepcopy(report)
    for event in page._objects(decorated['gates']):
        if event['gate_id'] in additions:
            event['evidence'] = additions[str(event['gate_id'])]
    for observation in page._objects(decorated['projections']):
        if ':consumer:' in str(observation['id']):
            observation['publication_policy'] = {'reason': 'unknown', 'lag_seconds': None, 'grace_seconds': None, 'state_through': None, 'published_through': None}
    size = len(json.dumps(decorated, separators=(',', ':')).encode())
    assert size-len(json.dumps(report, separators=(',', ':')).encode()) <= 8*1024 and size <= 160*1024
    _write(next(root.glob('samples-*')), [decorated])
    assert page.MAX_RECORD == 1024 * 1024 and page.MAX_SAMPLE_BYTES == 96 * 1024 * 1024
    result = _measure_memory(root)
    assert result['samples'] == 43201 and result['events'] == 2_505_600
    assert result['index_bytes'] == 2_505_600 * 16 and result['limited'] is False
    assert float(str(result['rss_mib'])) < 256 and float(str(result['current_seconds'])) < 1
    assert int(str(result['operations_slots'])) <= 1441 and int(str(result['operations_bytes'])) <= 1024*1024
    Path('/tmp/origo-overview-production-memory.json').write_text(json.dumps({**result, 'report_bytes': size}, indent=2))


def test_overview_totals_match_catalog_and_observed_evidence(
    production_serving: tuple[str, page.TapeCache, page.Document, datetime], operational_report: page.Document, tmp_path: Path,
) -> None:
    url, _, report, _ = production_serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law')
        tab.locator('.overview-card').first.wait_for()
        assert tab.locator('.overview-card').count() == 6
        for key, numerator, denominator in (('R1', 4, 4), ('C1', 2, 4), ('C2', 4, 4), ('D1', 2880, 2880)):
            text = _figure(tab, key).replace(',', '')
            assert re.search(rf'{numerator}\s*/\s*{denominator}', text), text
        assert 'not due' in _figure(tab, 'C1').lower()
        output_count = len([item for item in page._objects(report['projections']) if ':consumer:' in str(item['id'])])
        assert tab.locator('[data-overview="outputs"] .overview-value').inner_text() == f'Unknown / {output_count}'
        assert 'unknown' in _figure(tab, 'queue').lower() or 'not recorded' in _figure(tab, 'queue').lower()
        assert 'Monitor checks' in tab.locator('#monitor-band').inner_text()
        for key in ('R1', 'C1', 'C2', 'D1', 'M1', 'M2', 'outputs', 'workers', 'queue', 'collectors', 'errors', 'clear'):
            tab.locator(f'[data-overview="{key}"]').click()
            assert tab.locator('#detail').is_visible()
            assert tab.locator('#detail').inner_text().strip()
            assert '127.0.0.1:4000' not in tab.locator('#detail').inner_text()
            tab.locator('[data-close]').click()
        assert len(page._objects(report['feeds'])) == 6
        browser.close()

    evidence = {str(event['gate_id']).removeprefix('monitor.'): page._object(event['evidence'])
        for event in page._objects(operational_report['gates']) if str(event['gate_id']).startswith('monitor.')}
    now = page._instant(operational_report['evaluation_end']) + timedelta(seconds=1)
    cache = page.TapeCache(tmp_path / 'measured-monitor' / 'law')
    cache.refresh_latest(now)
    while cache.loading:
        cache.advance(now, budget_seconds=1)
    server = page.LawServer(('127.0.0.1', 0), cache, lambda: now)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            tab = browser.new_page()
            tab.goto(f'http://127.0.0.1:{server.server_port}/law')
            tab.locator('.overview-card').first.wait_for()
            for key, gate, numerator, denominator in (
                ('workers', 'workers_alive', 'workers_fresh', 'workers_expected'),
                ('collectors', 'collectors_serving', 'collectors_serving', 'collectors_expected'),
            ):
                value = evidence[gate]
                assert tab.locator(f'[data-overview="{key}"] .overview-value').inner_text() == f"{value[numerator]} / {value[denominator]}"
            assert evidence['dagster_reachable']['reachable'] is True
            assert tab.locator('[data-overview="queue"] .overview-value').inner_text() == str(evidence['queue_bounded']['queued_runs'])
            operations = page._object(cache.current(now)['operations_history'])
            for key, metric, gate in (('workers', 'failed_receipts', 'workers_alive'), ('errors', 'error_lines', 'no_error_logs')):
                values = page._object(page._object(operations[metric])['60m'])
                assert values['count'] == evidence[gate][metric]
                assert f"≥ {values['count']}" in _figure(tab, key)
                assert str(values['observed_slots']) in _figure(tab, key)
            for key, gates in (('workers', ('workers_alive',)), ('queue', ('queue_bounded', 'dagster_reachable')),
                ('collectors', ('collectors_serving',)), ('errors', ('no_error_logs',))):
                tab.locator(f'[data-overview="{key}"]').click()
                for index, gate in enumerate(gates):
                    fields = tab.locator('#detail .evidence-row').nth(index).locator('dl').evaluate(
                        "dl=>Object.fromEntries([...dl.querySelectorAll('dt')].map(dt=>[dt.textContent,dt.nextElementSibling.textContent]))")
                    expected = {name.replace('_', ' '): str(value).lower() if isinstance(value, bool) else str(value)
                        for name, value in evidence[gate].items() if value is not None and not isinstance(value, (dict, list))}
                    assert fields == expected
                metric = {'workers': 'failed_receipts', 'errors': 'error_lines'}.get(key)
                if metric:
                    for horizon in ('60m', '24h'):
                        values = page._object(page._object(operations[metric])[horizon])
                        assert f"{horizon} · ≥ {values['count']} observed" in tab.locator('#detail').inner_text()
                        assert str(values['window_end']) in tab.locator('#detail').inner_text()
                tab.locator('[data-close]').click()
            outputs = [item for item in page._objects(operational_report['projections']) if ':consumer:' in str(item['id'])]
            assert outputs and all('publication_policy' in item for item in outputs)
            for remove_policy in (False, True):
                if remove_policy:
                    tab.evaluate("()=>{for(const p of data.last_report.projections)delete p.publication_policy;render()}")
                assert tab.locator('[data-overview="outputs"] .overview-value').inner_text() == f'Unknown / {len(outputs)}'
                tab.locator('[data-overview="outputs"]').click()
                assert tab.locator('#detail .evidence-row').count() == len(outputs)
                assert tab.locator('#detail .evidence-row .UNKNOWN').count() == len(outputs)
                tab.locator('[data-close]').click()
            browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_overview_layout_and_navigation_at_desktop_and_mobile_sizes(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, _, _ = production_serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page(viewport={'width': 1366, 'height': 900}, has_touch=True)
        tab.goto(url+'/law')
        tab.locator('.overview-card').first.wait_for()
        assert tab.locator('[role="tab"]').all_text_contents() == ['Overview', 'Sources', 'Laws', 'Recovery']
        for item in tab.locator('.overview-card,.overview-secondary,#monitor-band,#inventory-band').all():
            box = item.bounding_box()
            assert box is not None and box['width'] > 0 and box['height'] > 0
            assert 0 <= box['x'] and 0 <= box['y'] and box['x']+box['width'] <= 1366 and box['y']+box['height'] <= 900, box
        tab.locator('[data-overview="R1"]').focus()
        tab.keyboard.press('Enter')
        assert tab.locator('#detail').is_visible()
        tab.locator('[data-close]').click()
        assert tab.locator('#detail').is_hidden()
        tab.locator('[data-overview="R1"]').click()
        tab.locator('[data-view="laws"]').click()
        tab.locator('.gate [data-gate="law.R1:binance_spot_trades"]').click()
        tab.locator('[data-close]').click()
        assert tab.evaluate('state.view') == 'laws' and tab.locator('#detail').is_hidden()
        tab.locator('.gate [data-gate="law.R1:binance_spot_trades"]').click()
        tab.go_back()
        tab.wait_for_function('() => !state.gate')
        tab.locator('.gate [data-gate="law.C1:binance_spot_trades"]').click()
        tab.go_back()
        tab.wait_for_function('() => !state.gate')
        tab.go_forward()
        tab.wait_for_function("() => state.gate === 'law.C1:binance_spot_trades'")
        tab.locator('[data-close]').click()
        assert tab.evaluate('state.view') == 'laws' and tab.locator('#detail').is_hidden()
        assert tab.evaluate('state.gate || state.overview || null') is None
        tab.locator('[data-view="sources"]').click()
        tab.locator('.source').first.wait_for()
        tab.evaluate('window.scrollTo(0,250)')
        position = tab.evaluate('scrollY')
        target = tab.locator('.source-title').nth(2)
        box = target.bounding_box()
        assert box is not None and 0 <= box['y'] < 900
        target.click()
        tab.go_back()
        tab.wait_for_function('() => !state.source')
        tab.wait_for_function('value => Math.abs(scrollY-value)<3', arg=position)
        tab.set_viewport_size({'width': 390, 'height': 844})
        for view in ('overview', 'sources', 'laws', 'recovery'):
            tab.locator(f'[data-view="{view}"]').click()
            assert tab.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert 'READER RECOVERY' not in tab.locator('body').inner_text()
        tab.screenshot(path='/tmp/origo-overview-mobile.png', full_page=True)
        tab.set_viewport_size({'width': 1366, 'height': 900})
        tab.locator('[data-view="overview"]').click()
        tab.screenshot(path='/tmp/origo-overview-desktop.png', full_page=True)
        browser.close()


def test_named_laws_filter_history_and_legacy_links(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, _, _ = production_serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law?view=gates&family=law.R1')
        tab.locator('.gate').first.wait_for()
        assert tab.locator('.gate').count() == 4 and tab.locator('#gate-family').input_value() == 'R1'
        assert tab.evaluate('state.view') == 'laws'
        assert tab.locator('.law-group').get_attribute('data-law-family') == 'R1'
        tab.locator('#gate-family').select_option('C1')
        tab.go_back()
        tab.wait_for_function("() => state.family === 'R1'")
        assert tab.locator('.gate').count() == 4 and tab.locator('.law-group').get_attribute('data-law-family') == 'R1'
        tab.go_forward()
        tab.wait_for_function("() => state.family === 'C1'")
        assert tab.locator('.gate').count() == 4 and tab.locator('.law-group').get_attribute('data-law-family') == 'C1'
        tab.goto(url+'/law?view=gates')
        tab.locator('.gate').first.wait_for()
        assert tab.locator('.law-group').count() == 6 and tab.locator('.gate').count() == 16
        assert tab.evaluate("state.view") == 'laws'
        assert all(str(gate).startswith('law.') for gate in tab.locator('.gate [data-gate]').evaluate_all('(items)=>items.map(x=>x.dataset.gate)'))
        # Catalog envelope corruption cannot add an obligation or remove a required denominator.
        tab.evaluate("()=>{const extra=structuredClone(descriptor('law.R1:binance_spot_trades'));extra.id+=':outside-inventory';data.catalog.gates.push(extra);render()}")
        assert tab.locator('.gate').count() == 16
        tab.evaluate("()=>{data.catalog.gates=data.catalog.gates.filter(g=>g.id!=='law.R1:binance_spot_trades');data.last_report.feeds=data.last_report.feeds.filter(f=>f.source_key!=='binance_spot_trades');render()}")
        assert tab.locator('.gate').count() == 16
        assert 'unknown' in tab.locator('.gate').filter(has=tab.locator('[data-gate="law.R1:binance_spot_trades"]')).inner_text().lower()
        tab.reload()
        tab.locator('.gate').first.wait_for()
        # The overview response re-renders every card; focus only after it lands.
        tab.wait_for_function('() => ![...requests.keys()].some(key => key.startsWith("days|"))')
        info = tab.locator('.gate .info').first
        info.focus()
        tooltip = tab.locator('.gate .tooltip').first
        assert tooltip.is_visible() and 'threshold' in tooltip.inner_text().lower()
        assert '/blob/58b45deee0602e7524c2efcba4e532174ad40902/' in str(tooltip.locator('a').get_attribute('href'))
        tab.locator('[data-law-id="law.D1:binance_spot_depth200_1m"] .day').last.click()
        tab.locator('#detail .event').first.wait_for()
        tab.locator('#detail .event details').first.locator('summary').click()
        assert 'Definition at evaluation' in tab.locator('#detail').inner_text()
        tab.goto(url+'/law?view=gates&gate=monitor.queue_bounded')
        tab.wait_for_function('() => data !== null')
        assert 'runtime' in tab.locator('#detail').inner_text().lower() or 'runtime' in tab.locator('#message').inner_text().lower()
        assert tab.locator('.gate').count() <= 16
        browser.close()


def test_depth_recovery_uses_real_continuity_evidence(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, _, _ = production_serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        for source in ('binance_spot_depth20_1m', 'binance_spot_depth200_1m'):
            tab.goto(url+'/law?view=recovery&source='+source)
            card = tab.locator('.chart-card')
            card.wait_for()
            tab.wait_for_function('() => document.querySelector(".chart-card svg") !== null')
            text = card.inner_text().lower()
            assert '1,440' in text or '1440' in text
            assert '0' in text and 'missing' in text
            assert not any(word in text for word in ('reader delay', 'daily archive', 'older history', 'unknown'))
            assert card.locator('svg text').filter(has_text=re.compile(r'^0$')).count() >= 1
            tab.locator('[data-view="sources"]').click()
            selected = tab.locator(f'[data-source-card="{source}"]')
            assert '1,440' in selected.inner_text() or '1440' in selected.inner_text()
            assert 'reader delay' not in selected.inner_text().lower()
        tab.evaluate("()=>{delete data.last_report.feeds.find(f=>f.source_key===state.source).predicates.D1;select({view:'recovery'})}")
        assert 'unknown' in tab.locator('.chart-card').inner_text().lower()
        browser.close()


def test_recovery_comparison_states_and_signed_change(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, report, _ = production_serving
    feed = next(item for item in page._objects(report['feeds']) if item['source_key'] == 'binance_spot_trades')
    magnitude = page._number(page._object(page._object(page._object(feed['predicates'])['R1'])['evidence'])['age_seconds'])
    assert magnitude is not None and magnitude > 0
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law?view=recovery&source=binance_spot_trades')
        tab.locator('.chart-card').wait_for()
        assert 'one hour' in tab.locator('.chart-card').inner_text().lower()
        assert any(word in tab.locator('.chart-card').inner_text().lower() for word in ('no exact observation one hour earlier', 'baseline unavailable')), tab.locator('.chart-card').inner_text()
        normal = tab.evaluate('duration', magnitude)
        for value, expected in ((-magnitude, f'-{normal} · less lag'), (magnitude, f'+{normal} · more lag'), (0, '0s · unchanged')):
            tab.evaluate("value=>{data.delay_change_1h_seconds.binance_spot_trades=value;renderContent()}", value)
            assert expected in tab.locator('.chart-card').inner_text()
        for flag in ('history_loading', 'history_limited'):
            tab.evaluate("flag=>{data.delay_change_1h_seconds.binance_spot_trades=null;data.history_loading=false;data.history_limited=false;data[flag]=true;renderContent()}", flag)
            assert ('loading' if flag.endswith('loading') else 'limited') in tab.locator('.chart-card').inner_text().lower()
        browser.close()


def test_wait_and_publication_presentations_preserve_raw_evidence(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, report, _ = production_serving
    feed = next(item for item in page._objects(report['feeds']) if item['source_key'] == 'binance_perp_trades')
    c1 = page._object(page._object(feed['predicates'])['C1'])
    evidence = page._object(c1['evidence'])
    assert c1['status'] == 'NOT_DUE'
    deadline = page._instant(evidence['deadline']).strftime('%Y-%m-%d %H:%M')
    target = 'binance_perp_trades:consumer:mount'
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url+'/law?view=sources')
        tab.locator('.source').first.wait_for()
        tab.locator('[data-projection="binance_perp_trades:raw"]').click()
        text = tab.locator('#detail').inner_text()
        assert str(evidence['day']) in text and deadline in text
        tab.locator('[data-close]').click()
        # This protocol test applies policy states to an unchanged captured artifact; it makes no new production claim.
        tab.evaluate("id=>{const p=data.last_report.projections.find(p=>p.id===id);window.savedOutput=structuredClone(p);p.status='STALE';p.reason='publication_state_changed';p.publication_policy={reason:'within_budget',lag_seconds:0,grace_seconds:10800,state_through:p.data_through,published_through:p.data_through};render()}", target)
        node = tab.locator(f'[data-projection="{target}"]')
        node.click()
        assert 'pending' in tab.locator('#detail').inner_text().lower()
        tab.locator('[data-close]').click()
        assert tab.evaluate("data.last_report.projections.find(p=>p.id==='binance_perp_trades:consumer:mount').status") == 'STALE'
        source = tab.locator('[data-source-card="binance_perp_trades"]')
        assert source.locator('.source-head .FAIL').count() == 0
        for state, word in (('unknown', 'unknown'), ('stale', 'failed'), ('backfill_active', 'backfill')):
            tab.evaluate("reason=>{data.last_report.projections.find(p=>p.id==='binance_perp_trades:consumer:mount').publication_policy.reason=reason;render()}", state)
            node.click()
            assert word in tab.locator('#detail').inner_text().lower()
            tab.locator('[data-close]').click()
        tab.evaluate("()=>{const p=data.last_report.projections.find(p=>p.id==='binance_perp_trades:consumer:mount');p.status=savedOutput.status;p.reason=savedOutput.reason;render()}")
        assert 'current' in node.inner_text().lower()
        tab.locator('[data-view="overview"]').click()
        assert tab.locator('[data-overview="outputs"] .overview-value').inner_text() == '1 / 10'
        assert '9 unknown' in _figure(tab, 'outputs')
        tab.locator('[data-view="sources"]').click()
        tab.evaluate("()=>{const p=data.last_report.projections.find(p=>p.id==='binance_perp_trades:consumer:mount');p.status='FAILED';p.reason='artifact_missing';render()}")
        assert node.locator('.FAIL,.FAILED').count() == 1
        tab.locator('[data-view="overview"]').click()
        assert tab.locator('[data-overview="outputs"] .overview-value').inner_text() == '0 / 10'
        assert '9 unknown' in _figure(tab, 'outputs')
        tab.locator('[data-view="sources"]').click()
        tab.evaluate("()=>{const p=data.last_report.projections.find(p=>p.id==='binance_perp_trades:consumer:mount');p.status='STALE';p.reason='publication_state_changed';p.publication_policy.reason='within_budget';const f=data.last_report.feeds.find(f=>f.source_key==='binance_perp_trades');f.predicates.R1.status='FAIL';render()}")
        assert source.locator('.source-head .FAIL').count() == 1
        tab.evaluate("()=>{data.last_report.feeds.find(f=>f.source_key==='binance_perp_trades').predicates.R1.status='UNKNOWN';render()}")
        assert source.locator('.source-head .UNKNOWN').count() == 1
        tab.evaluate("()=>{delete data.last_report.projections.find(p=>p.id==='binance_perp_trades:consumer:mount').publication_policy;render()}")
        assert 'unknown' in node.inner_text().lower()
        browser.close()


def test_overview_unknowns_override_stale_or_mismatched_numbers(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, _, _ = production_serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.clock.install()
        requests: list[str] = []
        tab.on('request', lambda request: requests.append(request.url))
        tab.goto(url+'/law')
        tab.locator('.overview-card').first.wait_for()
        assert 'Age of this sample' in tab.locator('body').inner_text()
        assert 'not recorded' in _figure(tab, 'queue').lower() or 'unknown' in _figure(tab, 'queue').lower()
        tab.evaluate("()=>{window.originalStages=structuredClone(data.last_report.projections);for(const p of data.last_report.projections){p.status='UNKNOWN';p.reason='not_observed'}render()}")
        assert tab.locator('[data-overview="outputs"] .overview-value').inner_text() == 'Unknown / 10'
        assert tab.locator('#metrics .metric strong').nth(1).inner_text() == 'Unknown / 56'
        tab.evaluate("()=>{const known=originalStages.find(p=>!p.id.includes(':consumer:')&&p.status==='CURRENT');Object.assign(data.last_report.projections.find(p=>p.id===known.id),known);render()}")
        assert tab.locator('#metrics .metric strong').nth(1).inner_text() == '1 / 56'
        assert '55 unknown' in tab.locator('#metrics .metric').nth(1).inner_text().lower()
        tab.evaluate("()=>{data.last_report.projections=originalStages;render()}")
        before = len(requests)
        age = tab.locator('#sample-age').inner_text()
        tab.clock.run_for(2000)
        assert tab.locator('#sample-age').inner_text() != age and len(requests) == before
        tab.route('**/law.json', lambda route: None)
        tab.clock.fast_forward(121000)
        assert tab.locator('.overview-card .PASS,.overview-card .CURRENT').count() == 0
        assert 'unknown' in tab.locator('#health').inner_text().lower()
        for key in ('R1', 'C1', 'C2', 'D1'):
            assert 'unknown' in _figure(tab, key).lower() or 'unavailable' in _figure(tab, key).lower()
        browser.close()


def test_history_concurrency_deadlines_and_current_refresh(production_serving: tuple[str, page.TapeCache, page.Document, datetime]) -> None:
    url, _, _, _ = production_serving
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.add_init_script('''{
          const original = window.fetch;window.historyRequests={active:0,peak:0,aborted:0,started:0,current:0};
          window.fetch=(url,options={})=>{
            const history=String(url).includes('/law/history.json')||String(url).includes('/law/gates.json');
            if(!history){if(String(url).endsWith('/law.json'))historyRequests.current++;return original(url,options)}
            historyRequests.active++;historyRequests.started++;historyRequests.peak=Math.max(historyRequests.peak,historyRequests.active);
            options.signal?.addEventListener('abort',()=>historyRequests.aborted++,{once:true});
            return original(url,options).finally(()=>historyRequests.active--);
          };
        }''')
        tab.goto(url+'/law')
        tab.locator('.overview-card').first.wait_for()
        assert tab.evaluate('historyRequests.started') == 0
        tab.route('**/law/history.json?*', lambda route: None)
        tab.route('**/law/gates.json', lambda route: None)
        tab.locator('[data-view="recovery"]').click()
        tab.wait_for_function('() => historyRequests.active===2')
        assert tab.evaluate('historyRequests.peak') == 2
        current = tab.evaluate('historyRequests.current')
        tab.evaluate('refresh()')
        assert tab.evaluate('historyRequests.current') == current + 1
        tab.wait_for_function('() => historyRequests.aborted>0', timeout=6500)
        tab.locator('[data-view="laws"]').click()
        tab.locator('[data-view="overview"]').click()
        tab.wait_for_function('() => historyRequests.active===0', timeout=2000)
        assert tab.evaluate('historyRequests.peak') <= 2
        assert tab.locator('.overview-card').count() == 6
        assert tab.evaluate('histories.size') <= 8
        browser.close()


def test_operational_history_totals_preserve_intervals_and_limits(
    production_tape: tuple[Path, page.Document, datetime], operational_report: page.Document,
    production_serving: tuple[str, page.TapeCache, page.Document, datetime], monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, original, now = production_tape
    terminal = page._instant(original['sampling_slot']).replace(second=0, microsecond=0)
    measured = {str(event['gate_id']): page._object(event['evidence']) for event in page._objects(operational_report['gates'])}
    # Protocol clock/cap vectors replay actual local-monitor reads, never claim production history.
    def record(end: datetime, *, start: datetime | None = None, cap: bool = False) -> page.Document:
        result = copy.deepcopy(original)
        result.update(sampling_slot=end.isoformat(), evaluation_start=end.isoformat(), evaluation_end=end.isoformat())
        for event in page._objects(result['gates']):
            metric = {'monitor.no_error_logs': 'error_lines', 'monitor.workers_alive': 'failed_receipts'}.get(str(event['gate_id']))
            if metric:
                assert measured[str(event['gate_id'])][metric] == 0
                event['evidence'] = {**measured[str(event['gate_id'])], metric: 1000 if cap else measured[str(event['gate_id'])][metric],
                    'window_start': (start or end-timedelta(minutes=1)).isoformat(), 'window_end': end.isoformat(), 'counts_limited': cap}
        return result

    def totals(records: list[page.Document], *, loading: bool = False, limited: bool = False) -> tuple[page.TapeCache, page.Document]:
        _write(next(root.glob('samples-*')), [records[-1]])
        cache = page.TapeCache(root)
        cache.refresh_latest(now)
        for item in records:
            cache._sample(item)
        cache.loading, cache.operations_limited = loading, limited
        cache._refresh_operations()
        return cache, page._object(page._object(cache.current(now)['operations_history'])['error_lines'])

    complete = [record(terminal-timedelta(minutes=59-index)) for index in range(60)]
    _, horizons = totals(complete)
    hour = page._object(horizons['60m'])
    assert hour['count'] == 0 and hour['complete'] is True and hour['covered_seconds'] == 3600
    assert hour['observed_slots'] == hour['expected_slots'] == 60
    assert page._object(horizons['24h'])['complete'] is False
    previous_hour = [record(terminal-timedelta(minutes=60-index)) for index in range(60)]
    for fault in ('missing_metrics', 'read_failed'):
        latest = record(terminal)
        for event in page._objects(latest['gates']):
            if event['gate_id'] in ('monitor.no_error_logs', 'monitor.workers_alive'):
                event.update(evidence={} if fault == 'missing_metrics' else {'read_ok': False}, outcome='UNKNOWN', reason=fault)
        failed_cache, values = totals([*previous_hour, latest])
        for metric in ('error_lines', 'failed_receipts'):
            failed = page._object(page._object(page._object(failed_cache.current(now)['operations_history'])[metric])['60m'])
            assert failed['window_end'] == (terminal-timedelta(minutes=1)).isoformat()
            assert failed['count'] == 0 and failed['observed_slots'] == 60 and failed['covered_seconds'] == 3600
            assert failed['complete'] is False, fault
        failed_cache.refresh_latest(now)
        failed_cache._refresh_operations()
        failed = page._object(page._object(page._object(failed_cache.current(now)['operations_history'])['error_lines'])['60m'])
        assert failed['complete'] is False and failed['window_end'] == (terminal-timedelta(minutes=1)).isoformat()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            tab = browser.new_page()
            tab.route('**/law.json', lambda route: route.fulfill(json=failed_cache.current(now)))
            tab.goto(production_serving[0]+'/law')
            tab.locator('.overview-card').first.wait_for()
            assert '≥ 0' in _figure(tab, 'errors')
            tab.locator('[data-overview="errors"]').click()
            text = tab.locator('#detail').inner_text().lower()
            assert 'covered' in text and 'window end' in text
            assert str(failed['window_end']).lower() in text
            browser.close()
    for records, reason in ((complete[1:], 'missing'), ([*complete, complete[-1]], 'duplicate'),
        ([record(terminal-timedelta(minutes=59), start=terminal-timedelta(minutes=61)), *complete[1:]], 'boundary'),
        ([*complete[:-1], record(terminal, start=terminal-timedelta(minutes=2))], 'overlap'),
        ([*complete[:-1], record(terminal, cap=True)], 'cap')):
        _, values = totals(records)
        row = page._object(values['60m'])
        assert row['complete'] is False, reason
        assert row['count'] is not None, reason
        assert float(str(row['covered_seconds'])) <= 3600
        if reason == 'duplicate':
            assert row['observed_slots'] == 59 and row['covered_seconds'] == 59*60
    for duplicate_records in ([complete[-1], complete[-1]], [record(terminal, cap=True), complete[-1]]):
        duplicate_cache, duplicate_values = totals(duplicate_records)
        row = page._object(duplicate_values['60m'])
        assert row['count'] is None and row['observed_slots'] == row['covered_seconds'] == 0
        duplicate_cache.refresh_latest(now)
        duplicate_cache._refresh_operations()
        refreshed = page._object(page._object(page._object(duplicate_cache.current(now)['operations_history'])['error_lines'])['60m'])
        assert refreshed['count'] is None and refreshed['observed_slots'] == refreshed['covered_seconds'] == 0
    exact_cap = record(terminal, cap=True)
    fractional = record(terminal)
    for event in page._objects(exact_cap['gates']):
        if event['gate_id'] == 'monitor.no_error_logs':
            page._object(event['evidence'])['counts_limited'] = False
    for event in page._objects(fractional['gates']):
        if event['gate_id'] == 'monitor.no_error_logs':
            page._object(event['evidence'])['window_start'] = (terminal-timedelta(minutes=1)+timedelta(microseconds=1)).isoformat()
    _, cap_result = totals([*complete[:-1], exact_cap])
    assert page._object(cap_result['60m'])['limited'] is True
    assert page._object(cap_result['60m'])['count'] == 1000
    _, fractional_result = totals([*complete[:-1], fractional])
    assert page._object(fractional_result['60m'])['complete'] is False
    assert page._object(fractional_result['60m'])['covered_seconds'] == 59*60
    for flag in ('loading', 'limited'):
        _, values = totals(complete, **{flag: True})
        assert page._object(values['60m'])['complete'] is False
    _, absent = totals([original])
    assert page._object(absent['60m'])['count'] is None
    cache, full = totals([record(terminal-timedelta(minutes=1499-index)) for index in range(1500)])
    assert len(cache.operations_samples) <= 1441 and cache.operations_bytes <= 1024 * 1024
    assert page._object(full['24h'])['count'] == 0 and page._object(full['24h'])['complete'] is True
    cache._event({})  # Unrelated malformed gate history must not poison complete recent intervals.
    assert cache.history_limited is True
    cache._refresh_operations()
    for metric in ('error_lines', 'failed_receipts'):
        for horizon in ('60m', '24h'):
            row = page._object(page._object(cache.operations_history[metric])[horizon])
            assert row['complete'] is True and row['limited'] is False
    with monkeypatch.context() as limits:
        limits.setattr(page, 'MAX_OPERATIONS_BYTES', 0)
        cache._remember_operations(record(terminal+timedelta(minutes=1)))
    cache._refresh_operations()
    for metric in ('error_lines', 'failed_receipts'):
        for horizon in ('60m', '24h'):
            row = page._object(page._object(cache.operations_history[metric])[horizon])
            assert row['complete'] is False and row['limited'] is True and row['count'] is None
    assert page.MAX_SAMPLE_BYTES == 96 * 1024 * 1024


def test_market_state_laws_and_projection_nodes_render_from_tape(
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, _, report, _ = serving
    feed = next(item for item in page._objects(report['feeds']) if item['source_key'] == 'binance_spot_trades')
    predicates = page._object(feed['predicates'])
    assert page._object(predicates['M1'])['status'] == 'PASS'
    assert page._object(predicates['M2'])['status'] == 'FAIL'
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        tab = browser.new_page()
        tab.goto(url + '/law?view=sources')
        cube = tab.locator('[data-projection="binance_spot_trades:market_state"]')
        assert 'failed' in cube.inner_text().lower()
        latest = tab.locator('[data-projection="binance_spot_trades:market_state_latest"]')
        assert 'current' in latest.inner_text().lower()
        tab.screenshot(path='/tmp/origo-market-state-law-sources.png', full_page=True)
        tab.goto(url + '/law?view=laws&family=M2')
        tab.wait_for_selector('[data-law-id="law.M2:binance_spot_trades"]')
        assert tab.locator('.gate').count() == 1
        assert 'fail' in tab.locator('.gate').inner_text().lower()
        tab.locator('.gate [data-gate="law.M2:binance_spot_trades"]').click()
        assert '2021-01-01' in tab.locator('#detail').inner_text()
        tab.goto(url + '/law')
        tab.wait_for_selector('[data-overview="M1"]')
        assert 'pass' in tab.locator('[data-overview="M1"]').inner_text().lower()
        assert 'fail' in tab.locator('[data-overview="M2"]').inner_text().lower()
        tab.screenshot(path='/tmp/origo-market-state-law-overview.png', full_page=True)
        browser.close()


def test_market_state_unobserved_members_prevent_a_clear_window(
    production_tape: tuple[Path, page.Document, datetime],
) -> None:
    root, original, now = production_tape
    assert original['status'] == 'PASS'
    cache = page.TapeCache(root)
    current = cache.current(now)
    assert current['status'] == 'UNKNOWN'
    assert current['consecutive_clear_slots'] == 0
    required = page._object(current['catalog'])['law_gate_ids']
    assert isinstance(required, list) and 'law.M1:binance_spot_trades' in required
    assert 'law.M2:binance_spot_trades' in required
    assert current['last_report'] == original
