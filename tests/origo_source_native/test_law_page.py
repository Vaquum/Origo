from __future__ import annotations

import copy
import json
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from playwright.sync_api import sync_playwright

from origo import law
from origo.law_catalog import build_catalog, gate_evaluation
from origo.workers import law_page as page

from .test_law import law_case as law_case
from .test_law import real_law_report as real_law_report

SHA = 'ca888afed9bc1681ceebe4c9cfd0502538e2a2d2'


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
    serving: tuple[str, page.TapeCache, page.Document, datetime],
) -> None:
    url, cache, report, now = serving
    before = cache.bytes_read
    for _ in range(3):
        assert _get(url + '/law.json')['status'] == report['status']
    assert cache.bytes_read == before
    port = url.rsplit(':', 1)[1]
    check = subprocess.run([sys.executable, '-m', 'origo.workers.law_page', '--check', '--port', port], capture_output=True)
    assert check.returncode == 0
    for path in cache.root.glob('samples-*'):
        path.unlink()
    assert cache.current(now)['status'] == 'UNKNOWN'
    assert page.main(['--check', '--port', port]) == 0
    assert page.main(['--check', '--port', '1']) == 1


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
        tab.goto(url + '/law')
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
        assert tab.locator('.gate').count() == 1
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
        assert 'Core-law consecutive clear window' in tab.locator('#message').inner_text()
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
    serving: tuple[str, page.TapeCache, page.Document, datetime],
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
        assert _get(url + '/law.json')['catalog']
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


def test_thirty_day_replay_cache_fits_page_memory_budget(tape: tuple[Path, page.Document, datetime]) -> None:
    root, _, _ = tape
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
events = report['gates']
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
    'current_seconds': time.monotonic()-started, 'limited': current['history_limited']}))
"""
    measured = subprocess.run([sys.executable, '-c', script, str(root)], check=True, capture_output=True, text=True)
    result = json.loads(measured.stdout)
    assert result['samples'] == 43201 and result['events'] == 2_505_600
    assert result['index_bytes'] == 2_505_600 * 16
    assert result['rss_mib'] < 256 and result['current_seconds'] < 1
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
        tab.goto(url+'/law')
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
