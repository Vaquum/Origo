from __future__ import annotations

import hashlib
import json
import time
import tracemalloc
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from clickhouse_driver import Client as NativeClient
from clickhouse_driver.errors import ServerException

from origo.sources.contracts import Partition, Row
from origo.sources.storage import SourceStore
from origo.steady_state.coverage import (
    COVERAGE_QUERY_SETTINGS,
    coverage_from_intervals,
    read_coverage,
)

from .helpers import ORIGO_DATABASE
from .steady_state_helpers import ROOT, metadata_rows, restore_metadata
from .test_provisional_worker import _Dagster, _feed, _Reporter


def test_frontiers_use_complete_coverage_not_newest_timestamp() -> None:
    # Remove a real canonical day; later real canonical days must not jump the hole.
    rows = metadata_rows('source_activation_log')
    selected = [
        row for row in rows if row['source_key'] == 'binance_spot_trades' and not row['provisional']
    ]
    selected.sort(key=lambda row: str(row['partition_start']))
    intervals = tuple(
        Partition(
            str(row['partition_key']),
            datetime.fromisoformat(str(row['partition_start'])).replace(tzinfo=UTC),
            datetime.fromisoformat(str(row['partition_end'])).replace(tzinfo=UTC),
        )
        for row in selected
    )
    anchor = intervals[0].start
    now = datetime(2026, 9, 21, 13, tzinfo=UTC)
    missing = intervals[1]
    result = coverage_from_intervals(anchor, intervals[:1] + intervals[2:], now)
    assert result.contiguous_end == missing.start
    assert result.canonical_end == missing.start
    assert result.oldest_missing == missing.start
    assert result.newest_end > missing.end
    assert result.missing_minutes >= 24 * 60
    # Coverage can only increase when the missing accepted interval is restored.
    complete = coverage_from_intervals(anchor, intervals, now)
    assert complete.contiguous_end == intervals[-1].end
    assert complete.missing_minutes == result.missing_minutes - 24 * 60


class _MeasuredClient:
    def __init__(self, client: NativeClient) -> None:
        self.client = client
        self.query_ids: list[str] = []

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        query_id = 's439_' + uuid4().hex
        self.query_ids.append(query_id)
        return self.client.execute(
            query,
            params,
            settings={**(settings or {}), 'log_queries': 1, 'log_queries_min_query_duration_ms': 0},
            query_id=query_id,
        )

    def disconnect(self) -> None:
        self.client.disconnect()


def test_frontier_lookup_is_bounded_on_production_metadata(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = origo_test_env
    assert settings['CLICKHOUSE_HOST'] == '127.0.0.1'
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    client = NativeClient(
        host=settings['CLICKHOUSE_HOST'],
        port=int(settings['CLICKHOUSE_PORT']),
        user=settings['CLICKHOUSE_USER'],
        password=settings['CLICKHOUSE_PASSWORD'],
    )
    try:
        version = client.execute('SELECT version()')[0][0]
        assert version.startswith('25.3.'), 'Replay must use the production ClickHouse family'
        stores = restore_metadata(client, ORIGO_DATABASE, tmp_path / 'locks')
        count = client.execute(f'SELECT count() FROM {ORIGO_DATABASE}.source_active_partitions')[0][
            0
        ]
        assert count >= 25380
        old = (
            f'SELECT max(partition_end) FROM {ORIGO_DATABASE}.source_current_partitions '
            "WHERE source_key='binance_spot_trades'"
        )
        # Replay the original view from the pinned pre-fix code, not a candidate view.
        candidate_view = client.execute(f'SHOW CREATE TABLE {ORIGO_DATABASE}.source_current_partitions')[0][0]
        entry = json.loads((ROOT / 'provenance.json').read_text())['legacy_view']
        legacy_view = (ROOT / entry['file']).read_bytes()
        assert hashlib.sha256(legacy_view).hexdigest() == entry['sha256']
        client.execute(legacy_view.decode())
        with pytest.raises(ServerException) as raised:
            client.execute(old, settings=COVERAGE_QUERY_SETTINGS)
        assert raised.value.code == 241, 'Retain the actual pre-fix memory failure, not any failure'
        client.execute(candidate_view.replace('CREATE VIEW', 'CREATE OR REPLACE VIEW', 1))
        # Actual SQL readers also get bounded metadata selection, not only the worker.
        assert client.execute(old, settings=COVERAGE_QUERY_SETTINGS)[0][0] is not None
        report: list[dict[str, object]] = []
        now = datetime(2026, 9, 21, 13, tzinfo=UTC)
        for original in stores:
            measured = _MeasuredClient(client)
            store = SourceStore(measured, ORIGO_DATABASE, original.spec)
            tracemalloc.start()
            start = time.monotonic()
            coverage = read_coverage(store, now)
            elapsed = time.monotonic() - start
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            assert elapsed < 5
            assert len(measured.query_ids) == 2
            assert peak < 64 * 1024 * 1024
            assert not coverage.incomplete_partitions
            feed = _feed(original.spec, tmp_path, _Dagster(), _Reporter())
            assert feed._frontier_gap_key(store, original.spec, now, coverage=coverage) == (
                coverage.contiguous_end.strftime('%Y-%m-%dT%H:%M:%SZ')
            )
            # Independently merge the captured activation intervals, not the SQL view.
            intervals = sorted(coverage.intervals, key=lambda item: item.start)
            frontier = coverage.anchor
            for interval in intervals:
                if interval.start > frontier:
                    break
                frontier = max(frontier, interval.end)
            assert frontier == coverage.contiguous_end < coverage.newest_end
            client.execute('SYSTEM FLUSH LOGS')
            metrics = client.execute(
                'SELECT query_id, memory_usage, read_rows, read_bytes FROM system.query_log '
                "WHERE query_id IN %(ids)s AND type='QueryFinish'",
                {'ids': tuple(measured.query_ids)},
            )
            assert len(metrics) == 2
            assert max(row[1] for row in metrics) <= 512 * 1024 * 1024
            report.append(
                {
                    'source': original.spec.key,
                    'elapsed_seconds': elapsed,
                    'incremental_python_peak_bytes': peak,
                    'query_metrics': metrics,
                    'frontier': frontier.isoformat(),
                }
            )
        (tmp_path / 'r440_measurements.json').write_text(json.dumps(report, indent=2))
        print('R440_MEASUREMENTS=' + json.dumps(report))
    finally:
        tracemalloc.stop()
        client.disconnect()


def test_inventory_and_wall_clock_prevent_false_green(tmp_path: Path) -> None:
    from datetime import timedelta
    from origo.steady_state.policy import load_inventory, registry_discrepancies
    from origo.sources.registry import SOURCE_REGISTRY
    from .steady_state_evidence_cases import START, evaluator, evidence_writer, sample

    inventory = load_inventory()
    assert registry_discrepancies(inventory, SOURCE_REGISTRY) == ()
    assert registry_discrepancies(inventory, SOURCE_REGISTRY[:-1])
    rows = metadata_rows('source_activation_log')
    canonical = [row for row in rows if row['source_key'] == 'binance_spot_trades' and not row['provisional']]
    anchor = min(datetime.fromisoformat(str(row['partition_start'])).replace(tzinfo=UTC) for row in canonical)
    end = max(datetime.fromisoformat(str(row['partition_end'])).replace(tzinfo=UTC) for row in canonical)
    writer, identity = evidence_writer(tmp_path)
    entry = sample(START, identity)
    # The source and consumer are equally frozen. Relative lag is zero; wall age is not.
    entry['sources'] = {'binance_spot_trades': {
        'status': 'observed', 'anchor': anchor.isoformat(), 'due': START.isoformat(),
        'canonical_end': end.isoformat(), 'prefix_end': end.isoformat(),
        'contiguous_end': START.isoformat(),  # An incorrect supplied summary is not the oracle.
        'tail_start': (START - timedelta(hours=2)).isoformat(), 'tail_intervals': [],
        'incomplete_partition_count': 0,
    }}
    entry['consumers'] = {'binance_spot_trades:mount': {
        'status': 'observed', 'manifest': {'exists': True, 'active_through': end.isoformat()},
        'series': {},
    }}
    writer.append_sample(entry)
    checked = evaluator(writer)
    from origo.steady_state.verification import EvidenceError
    with pytest.raises(EvidenceError, match='not reproducible'):
        checked.evaluate_ss01()
    # Honest but stale coverage must also fail, even when both endpoints agree.
    sources = entry['sources']
    assert isinstance(sources, dict)
    sources['binance_spot_trades']['contiguous_end'] = end.isoformat()
    honest, honest_identity = evidence_writer(tmp_path / 'honest')
    assert honest_identity == identity
    honest.append_sample(entry)
    checked = evaluator(honest)
    checked.evaluate_ss01()
    checked.evaluate_ss02()
    assert any(item.entity == 'binance_spot_trades' and item.verdict == 'FAIL' for item in checked.results)
    assert any(item.verdict == 'UNKNOWN' and 'binance_perp_trades' in item.entity for item in checked.results)
    assert not all(item.verdict == 'PASS' for item in checked.results if item.metric_id == 'SS-02')
