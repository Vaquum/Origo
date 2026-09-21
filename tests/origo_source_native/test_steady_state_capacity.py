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
