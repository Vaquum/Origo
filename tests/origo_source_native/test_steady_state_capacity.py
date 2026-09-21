from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.binance_daily import Response
from origo.sources.contracts import Partition, ProvisionalAdapter, Revision, Row
from origo.sources.lifecycle import SourceRuntime
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore, StorageError
from origo.workers.dagster_reader import DagsterReader
from origo.workers.provisional import ProvisionalFeed
from origo.workers.receipts import ensure_monitoring_tables
from origo.workers.report import Reporter

from .helpers import ORIGO_DATABASE
from .test_provisional_worker import _Dagster, _Reporter


@dataclass(frozen=True)
class _RecordedMinute:
    original: ProvisionalAdapter
    minute: Partition

    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        # Restrict the replay calendar to the authentic minute in this source's corpus.
        # No market timestamps/rows change, and fetch/build/publish remain the real path.
        if any(item.start <= self.minute.start < item.end for item in covered):
            return ()
        return (self.minute,)

    def partition(self, key: str) -> Partition:
        return self.original.partition(key)

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        return self.original.fetch(partition, previous_evidence)


def test_frontier_failure_does_not_starve_other_sources(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'source-files'))
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    declarations = (
        ('spot', 'trades', 'binance_spot_rest'),
        ('futures', 'trades', 'binance_perp_rest'),
        ('spot', 'aggtrades', 'binance_spot_agg_rest'),
        ('futures', 'aggtrades', 'binance_perp_agg_rest'),
    )
    specs = []
    minutes: dict[str, Partition] = {}
    pending: dict[str, list[dict[str, object]]] = {}
    root = Path(__file__).resolve().parents[1] / 'fixtures/binance'
    for spec, (market, family, module_name) in zip(SOURCE_REGISTRY, declarations, strict=True):
        fixture = root / market / 'rest' / family
        provenance = json.loads((fixture / 'provenance.json').read_text())
        calls = cast(list[dict[str, object]], list(provenance['requests']))
        pending[spec.key] = calls
        module = importlib.import_module('origo.sources.adapters.' + module_name)
        assert spec.provisional is not None
        minute = spec.provisional.partition(
            datetime.fromisoformat(provenance['minute_start']).strftime('%Y-%m-%dT%H:%M:%SZ')
        )
        minutes[spec.key] = minute

        def replay(
            url: str,
            *,
            params: dict[str, str | int],
            headers: dict[str, str],
            weight: int,
            calls: list[dict[str, object]] = calls,
            fixture: Path = fixture,
            required: bool = family == 'trades' and market == 'futures',
        ) -> Response:
            assert calls, 'Unexpected request beyond captured fixture'
            request = calls.pop(0)
            assert url == request['url'] and params == request['params']
            assert headers == ({'X-MBX-APIKEY': '0' * 64} if required else {})
            body = (fixture / str(request['file'])).read_bytes()
            assert hashlib.sha256(body).hexdigest() == request['sha256']
            return Response(body, {}, int(str(request.get('status', 200))))

        monkeypatch.setattr(module, 'get_response', replay)
        specs.append(replace(spec, provisional=_RecordedMinute(spec.provisional, minute)))
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        for spec in specs:
            store = SourceStore(client, ORIGO_DATABASE, spec)
            SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4())).setup(
                anchor=minutes[spec.key].start
            )
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        original = SourceStore.execute
        broken = specs[0].key

        def failed_lookup(
            self: SourceStore,
            query: str,
            params: object | None = None,
            settings: Mapping[str, object] | None = None,
        ) -> list[Row]:
            if self.spec.key == broken and 'groupUniqArray' in query:
                raise StorageError('Injected source-local Code 241 / JoiningTransform')
            return original(self, query, params, settings)

        monkeypatch.setattr(SourceStore, 'execute', failed_lookup)
        reporter = _Reporter()
        feed = ProvisionalFeed(
            specs,
            publication_root=tmp_path / 'source-files',
            reporter=cast(Reporter, reporter),
            dagster=cast(DagsterReader, _Dagster()),
            host='isolated-s439',
        )
        tick = max(partition.end for partition in minutes.values()) + timedelta(seconds=5)
        outcome = feed.tick(tick)
        assert outcome.failed == (f'{broken}:tick',)
        for spec in specs[1:]:
            assert f'{spec.key}:{minutes[spec.key].key}' in outcome.processed
            assert f'{spec.key}:mount' in outcome.processed
            assert pending[spec.key] == []
            manifest_path = tmp_path / 'source-files' / spec.key / 'mount' / 'latest.json'
            manifest = json.loads(manifest_path.read_text())
            assert datetime.fromisoformat(manifest['active_through']) == minutes[spec.key].end
            files = manifest['files']
            assert len({entry['series'] for entry in files}) == 12
            assert all(Path(entry['path']).is_file() for entry in files)
            assert all(
                hashlib.sha256(Path(entry['path']).read_bytes()).hexdigest() == entry['sha256']
                for entry in files
            )
        assert not any(
            key == f'{broken}_provisional_feed' for key, _, _ in reporter.materializations
        )
        latest = client.execute(
            f'SELECT argMax(event_type, event_time) FROM {ORIGO_DATABASE}.source_failure_log '
            "WHERE source_key=%(source)s AND operation='worker_tick' GROUP BY failure_key",
            {'source': broken},
        )
        assert latest == [('FAILED',)]
        # Restore the first source's read and exercise its real retry/build/publication.
        monkeypatch.setattr(SourceStore, 'execute', original)
        feed.specs = (specs[0],)
        recovered = feed.tick(tick)
        assert recovered.failed == ()
        assert f'{broken}:{minutes[broken].key}' in recovered.processed
        assert f'{broken}:mount' in recovered.processed
        assert pending[broken] == []
        latest = client.execute(
            f'SELECT argMax(event_type, event_time) FROM {ORIGO_DATABASE}.source_failure_log '
            "WHERE source_key=%(source)s AND operation='worker_tick' GROUP BY failure_key",
            {'source': broken},
        )
        assert latest == [('RECOVERED',)]
    finally:
        client.disconnect()


def test_exact_provider_shape_budget_and_source_fairness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real recorded payloads through the shared limiter; slow I/O cannot hold its lock."""
    import base64
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    import requests as http
    from origo.sources.adapters import binance_daily as daily
    from .test_trade_capture import recordings
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'budget'))
    monkeypatch.delenv('ORIGO_WORKER_HEARTBEAT', raising=False)
    recent = recordings()[0]
    root = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures/rest/trades'
    manifest = json.loads((root / 'provenance.json').read_text())
    historic = next(row for row in manifest['requests'] if row['url'].endswith('historicalTrades'))
    historical_body = (root / historic['file']).read_bytes()
    assert hashlib.sha256(historical_body).hexdigest() == historic['sha256']
    recent_body = base64.b64decode(recent['body_base64'])
    assert hashlib.sha256(recent_body).hexdigest() == recent['body_sha256']
    entered = threading.Event()
    release = threading.Event()
    observed = []
    def transport(url: str, params: dict, headers: dict) -> http.Response:
        observed.append((url, dict(params), dict(headers)))
        response = http.Response()
        response.status_code = 200
        response.headers['X-MBX-USED-WEIGHT-1M'] = '205'
        if url.endswith('historicalTrades'):
            assert params == historic['params'] and params['limit'] == 500
            assert headers == {'X-MBX-APIKEY': 'isolated-test-key'}
            response._content = historical_body
            entered.set()
            assert release.wait(4), 'Test release must not leave a transport running'
        else:
            assert url == recent['url'] and params == recent['params']
            assert headers == {} and params['limit'] == 1000
            response._content = recent_body
        return response
    monkeypatch.setattr(daily, '_request', transport)
    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(daily.get_response, historic['url'], params=historic['params'],
                           headers={'X-MBX-APIKEY': 'isolated-test-key'}, weight=200)
        assert entered.wait(3)
        started = time.monotonic()
        fast = pool.submit(daily.get_response, recent['url'], params=recent['params'],
                           headers={}, weight=5, lane='live')
        try:
            result = fast.result(timeout=2)
            assert time.monotonic() - started < 2
            assert result.body == recent_body and result.cost.weight == 5
        finally:
            release.set()
        assert slow.result(timeout=2).body == historical_body
    assert len(observed) == 2
    # Host aliases share one reservation file; a separate market keeps its own budget.
    assert daily._budget_host('https://api1.binance.com/api/v3/trades') == daily._budget_host(
        'https://api.binance.com/api/v3/trades')
    assert daily._budget_host(recent['url']) != daily._budget_host('https://api.binance.com')


def test_useful_capacity_counts_real_work_and_conserves_backlog(tmp_path: Path) -> None:
    from origo.sources.adapters.binance_perp_rest import historical_row
    from origo.steady_state.trade_spool import TradeSpool, SealedMinute
    from .test_trade_capture import recordings, replay
    records = recordings()
    spool = TradeSpool.create(tmp_path / 'work.sqlite', historical_row)
    try:
        outcomes = replay(spool, records)
        start = datetime.fromisoformat(records[0]['captured_at'])
        end = datetime.fromisoformat(records[-1]['completed_at']) + timedelta(seconds=1)
        work = spool.useful_work(start, end)
        sealed = [minute for outcome in outcomes for minute in outcome.sealed]
        actual = [spool.sealed_minute(minute) for minute in sealed]
        assert all(isinstance(minute, SealedMinute) for minute in actual)
        assert work.sealed_minutes == 2
        assert work.sealed_rows == sum(len(minute.rows) for minute in actual)
        assert work.requests == len(records) and work.request_weight == 5 * len(records)
        assert work.stored_rows == sum(outcome.new_rows for outcome in outcomes)
        assert work.stored_rows > work.sealed_rows
        duplicate = replay(spool, [records[-1]])[0]
        assert duplicate.new_rows == 0 and duplicate.sealed == ()
        after = spool.useful_work(start, end)
        assert after.sealed_minutes == work.sealed_minutes
        assert after.sealed_rows == work.sealed_rows
        assert after.stored_rows == work.stored_rows
        assert after.requests == work.requests + 1
        assert after.request_weight == work.request_weight + 5
        # One acknowledged, verified minute releases only its own retained raw rows.
        first = actual[0]
        released = spool.acknowledge(sealed[0], content_hash=first.content_hash,
                                     generation='verified-fixture-generation', now=end)
        assert released.hash_matched and released.rows_released > 0
        second = spool.sealed_minute(sealed[1])
        assert isinstance(second, SealedMinute) and second.rows == actual[1].rows
        assert spool.health(end)['unacknowledged_sealed_minutes'] == 1
    finally:
        spool.close()
