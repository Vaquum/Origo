from __future__ import annotations

import copy
import json
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from playwright.sync_api import sync_playwright

from origo import law
from origo.law_catalog import build_catalog, gate_evaluation
from origo.workers import law_page as page

from .test_law import law_case as law_case, real_law_report as real_law_report

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
        stream.write('{"corrupt":true}\n')
    assert cache.current(now)['status'] == 'UNKNOWN'
    assert cache.current(now)['reason'] == 'malformed_report'
    for mutation in ('inventory', 'schema_version'):
        record = copy.deepcopy(original)
        record[mutation] = [] if mutation == 'inventory' else 999
        _write(path, [record])
        assert cache.current(now)['status'] == 'UNKNOWN'
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
    for changes in ({'gate_id': ['../../etc/passwd']}, {'sql': ['SELECT 1']}, {'cursor': ['invented']},
                    {'from': [(now - timedelta(days=31)).isoformat()]}):
        with pytest.raises(ValueError):
            cache.history({**query, **changes}, now)
    # Genuine predicate payload, repeated transport envelopes only; these IDs are test events, not market data.
    records = [{**original, 'evidence_id': f'protocol-delivery-{index}'} for index in range(1001)]
    _write(event_path, records + [records[0]])
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
        tab.locator('[data-view="recovery"]').click()
        assert 'projection=' in tab.url and 'source=' in tab.url
        assert tab.locator('.chart-card').count() == 1
        assert 'Core-law consecutive clear window' in tab.locator('#message').inner_text()
        tab.set_viewport_size({'width': 390, 'height': 844})
        tab.locator('[data-view="sources"]').click()
        assert tab.evaluate('document.documentElement.scrollWidth <= innerWidth')
        tab.locator('[data-close]').click()
        tab.screenshot(path='/tmp/origo-law-mobile.png', full_page=True)
        tab.set_viewport_size({'width': 1440, 'height': 1050})
        tab.screenshot(path='/tmp/origo-law-desktop.png', full_page=True)
        assert not errors
        browser.close()
