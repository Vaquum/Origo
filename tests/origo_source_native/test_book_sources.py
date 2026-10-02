from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from dagster import AssetKey, DagsterInstance, Definitions

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.book_spool import Market, payload_rows, read_payload, sealed_minutes
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import (
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceError,
    SourceReadPolicy,
)
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS, BOOK_HASH_CHUNK_ROWS
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore

from .test_book_capture import replay

SPECS = (BINANCE_SPOT_BOOK_SPEC, BINANCE_PERP_BOOK_SPEC)


@pytest.fixture(params=SPECS, ids=['spot', 'perp'])
def book_runtime(
    request: pytest.FixtureRequest,
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[SourceRuntime]:
    declared = cast(RevisionedSourceSpec, request.param)
    market: Market = 'spot' if declared.key == 'binance_spot_book' else 'perp'
    sampler = replay(tmp_path / 'spool', market)
    assert sampler.last_seal is not None
    first = sampler.last_seal - timedelta(minutes=1)
    # The recorded minute is historical test input; only the partition calendar differs.
    spec = replace(
        declared,
        partitions=PartitionPolicy((first - timedelta(days=1)).date()),
        orchestration=replace(declared.orchestration, retry_count=0),
    )
    monkeypatch.setenv('ORIGO_BOOK_SPOOL_ROOT', str(tmp_path / 'spool'))
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        yield runtime
    finally:
        client.disconnect()


def _market(runtime: SourceRuntime) -> Market:
    return 'spot' if runtime.spec.key == 'binance_spot_book' else 'perp'


def _keys(runtime: SourceRuntime) -> tuple[str, ...]:
    root = runtime.lock_root.parent / 'spool'
    minutes = sealed_minutes(
        root, _market(runtime), datetime(2026, 10, 2, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC)
    )
    return tuple(
        datetime.fromisoformat(minute['minute_start']).strftime('%Y-%m-%dT%H:%MZ')
        for minute in minutes
    )


def test_book_sources_register_as_canary_beside_the_four_trade_sources() -> None:
    assert len(SOURCE_REGISTRY) == 6
    assert (
        len([spec for spec in SOURCE_REGISTRY if spec.key.endswith(('trades', 'aggtrades'))]) == 4
    )
    for spec in SPECS:
        assert spec in SOURCE_REGISTRY and spec.rollout_stage == RolloutStage.CANARY
        assert spec.read_policy == SourceReadPolicy.AVAILABLE
        assert {component.key for component in spec.components} == {
            *BOOK_COMPONENT_KEYS,
            *(key + '_latest' for key in BOOK_COMPONENT_KEYS),
        }
        assert spec.consumers == spec.aliases == spec.retired_tables == spec.retired_rows == ()
        bundle = build_source_bundle(spec)
        repository = Definitions(
            assets=bundle.assets,
            jobs=bundle.jobs,
            schedules=bundle.schedules,
            sensors=bundle.sensors,
        ).get_repository_def()
        assert repository.has_job(f'backfill_{spec.key}_source_job')
        assert not repository.has_schedule_def(f'{spec.key}_provisional_schedule')
        assert repository.asset_graph.get(AssetKey(f'{spec.key}_provisional_feed'))
        assert all(schedule.default_status.value == 'RUNNING' for schedule in bundle.schedules)
        assert all(sensor.default_status.value == 'RUNNING' for sensor in bundle.sensors)


def test_sealed_minutes_build_both_depths_and_minute_rows_idempotently(
    book_runtime: SourceRuntime,
) -> None:
    runtime = book_runtime
    key = _keys(runtime)[-1]
    record = runtime.build(key, provisional=True)
    assert record.generation == 1
    assert {key for key, _ in record.component_hashes} == {
        key + '_latest' for key in BOOK_COMPONENT_KEYS
    }
    assert runtime.build(key, provisional=True) == record
    snapshot = runtime.store.snapshot()
    assert len(snapshot.records) == 1
    root = runtime.lock_root.parent / 'spool'
    seal = sealed_minutes(root, _market(runtime), record.partition.start, record.partition.end)[0]
    samples = tuple(payload_rows(read_payload(root, seal), seal))
    for depth, expected in ((20, 600), (200, 60)):
        component = f'depth{depth}'
        rows = runtime.store.rows(component + '_latest', snapshot)
        assert len(rows) == expected
        assert rows[0][2:] == next(row[3:] for row in samples if row[0] == depth)
        minute = runtime.store.rows(component + '_1m_latest', snapshot)
        assert len(minute) == 1
        latest = rows[-1]
        bids = cast(list[tuple[float, float]], latest[3])
        asks = cast(list[tuple[float, float]], latest[4])
        mid = (bids[0][0] + asks[0][0]) / 2
        bn, an = sum(p * q for p, q in bids), sum(p * q for p, q in asks)
        assert minute[0][1] == latest[1]
        assert minute[0][2:] == pytest.approx(
            (mid, (asks[0][0] - bids[0][0]) / mid * 10000, bn, an, (bn - an) / (bn + an))
        )
    assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]


def test_incomplete_local_day_fails_visibly_without_activating_partial_data(
    book_runtime: SourceRuntime,
) -> None:
    runtime = book_runtime
    minute = runtime.build(_keys(runtime)[-1], provisional=True)
    with pytest.raises(SourceError) as failed:
        runtime.build(minute.partition.start.date().isoformat())
    assert failed.value.code == 'BOOK_MINUTES_MISSING'
    assert runtime.store.records(canonical_only=True) == ()
    assert runtime.store.record(minute.partition) == minute
    assert runtime.store.execute(
        "SELECT error_code, blocking_scope FROM origo.source_failure_log WHERE event_type='FAILED'"
    ) == [('BOOK_MINUTES_MISSING', 'PARTITION')]


def test_available_policy_reads_past_a_gap_and_contiguous_default_is_unchanged(
    book_runtime: SourceRuntime,
) -> None:
    runtime = book_runtime
    keys = _keys(runtime)
    assert len(keys) >= 2
    first, last = (
        runtime.build(keys[0], provisional=True),
        runtime.build(keys[-1], provisional=True),
    )
    assert runtime.store.records() == (first, last)
    assert runtime.store.execute('SELECT count() FROM origo.source_current_partitions') == [(2,)]
    # The same real minutes under a contiguous source stop at its immutable earlier anchor.
    contiguous = replace(runtime.spec, read_policy=SourceReadPolicy.CONTIGUOUS)
    clipped = SourceStore(runtime.store.client, 'origo', contiguous)
    assert clipped.records() == ()
    assert all(spec.read_policy == SourceReadPolicy.CONTIGUOUS for spec in SOURCE_REGISTRY[:4])


def test_wide_book_components_hash_in_bounded_chunks_and_trade_hashes_are_unchanged(
    book_runtime: SourceRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.columnar import HASH_CHUNK_ROWS
    from origo.sources.contracts import Row

    runtime = book_runtime
    assert HASH_CHUNK_ROWS == 1048576
    assert all(
        component.hash_chunk_rows is None
        for spec in SOURCE_REGISTRY[:4]
        for component in spec.components
    )
    assert all(
        component.hash_chunk_rows == BOOK_HASH_CHUNK_ROWS
        for component in runtime.spec.components
        if component.key.startswith(('depth20_', 'depth200_')) is False
    )
    # Lower the actual profile's chunk bound to exercise multiple pages on unaltered real rows.
    bounded_spec = replace(
        runtime.spec,
        components=tuple(
            replace(component, hash_chunk_rows=64) if component.hash_chunk_rows else component
            for component in runtime.spec.components
        ),
    )
    runtime = SourceRuntime(
        bounded_spec,
        SourceStore(runtime.store.client, 'origo', bounded_spec),
        runtime.lock_root,
        str(uuid4()),
    )
    queries: list[str] = []
    execute = runtime.store.client.execute

    def observed(
        query: str, params: object | None = None, settings: dict[str, object] | None = None
    ) -> list[Row]:
        queries.append(query)
        return execute(query, params, settings=settings)

    monkeypatch.setattr(runtime.store.client, 'execute', observed)
    record = runtime.build(_keys(runtime)[-1], provisional=True)
    assert len(record.component_hashes) == 4
    assert any('LIMIT 64' in query for query in queries)
    assert not any('LIMIT 1048576' in query and 'bids' in query for query in queries)
    runtime.store.execute('SYSTEM FLUSH LOGS')
    peaks = runtime.store.execute(
        "SELECT max(memory_usage) FROM system.query_log WHERE type='QueryFinish' AND query LIKE '%LIMIT 64%' AND event_time > now() - INTERVAL 10 MINUTE"
    )
    assert 0 < int(str(peaks[0][0])) < 256 * 1024**2


def test_native_backfill_failure_and_retry_preserve_verified_minutes(
    book_runtime: SourceRuntime,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources import capacity

    class Volume:
        identity = 'fixture-volume'
        path = tmp_path

        def sample(self) -> tuple[int, int, int, int]:
            return (10**12, 9 * 10**11, 10**8, 9 * 10**7)

    # Only local capacity metadata is controlled; native jobs read the real book spool.
    def volumes(runtime: SourceRuntime) -> tuple[Volume, ...]:
        return (Volume(),)

    monkeypatch.setattr(capacity, '_volumes', volumes)
    runtime = book_runtime
    minute = runtime.build(_keys(runtime)[-1], provisional=True)
    bundle = build_source_bundle(runtime.spec)
    repository = Definitions(
        assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors, schedules=bundle.schedules
    ).get_repository_def()
    job = repository.get_job(f'backfill_{runtime.spec.key}_source_job')
    day = runtime.spec.partitions.first_day.isoformat()
    (tmp_path / 'dagster').mkdir()
    with DagsterInstance.local_temp(str(tmp_path / 'dagster')) as instance:
        for _ in range(2):
            failed = job.execute_in_process(
                instance=instance, partition_key=day, raise_on_error=False
            )
            assert not failed.success
            assert any('BOOK_MINUTES_MISSING' in str(event) for event in failed.all_events)
            assert runtime.store.record(minute.partition) == minute
            assert runtime.store.records(canonical_only=True) == ()
            assert (
                instance.get_materialized_partitions(
                    AssetKey(f'build_{runtime.spec.key}_canonical_revision_origo')
                )
                == set()
            )
    assert runtime.build(minute.partition.key, provisional=True) == minute


def test_real_depth200_hash_page_stays_within_memory_budget(
    origo_test_env: dict[str, str],
) -> None:
    from origo.sources.columnar import BoundedClient, binary_hash
    from origo.sources.contracts import Row
    from origo.sources.profiles.book import book_components
    from origo.workers.book_capture import DiffBook

    from .test_book_capture import recorded_events, seed_payload

    client = BoundedClient(make_clickhouse_client(get_clickhouse_settings()))
    component = next(c for c in book_components() if c.key == 'depth200')
    table = 'origo.book_hash_measurement'
    try:
        client.execute('CREATE DATABASE IF NOT EXISTS origo')
        columns = ', '.join(f'{c.name} {c.sql_type}' for c in component.columns)
        client.execute(f'CREATE TABLE {table} ({columns}) ENGINE=MergeTree ORDER BY datetime')
        book = DiffBook('perp', seed_payload('perp'))
        rows: list[Row] = []
        for _, event in recorded_events('perp'):
            if book.apply(event):
                bids, asks = book.top(200)
                # Benchmark original event states at original times, without publishing grid rows.
                rows.append(
                    (
                        datetime.fromtimestamp(event.event_ms / 1000, UTC),
                        event.event_ms,
                        book.last,
                        [(float(p), float(q)) for p, q in bids],
                        [(float(p), float(q)) for p, q in asks],
                    )
                )
        assert len(rows) > BOOK_HASH_CHUNK_ROWS
        client.execute(f'INSERT INTO {table} VALUES', rows)
        assert binary_hash(client, component, table).startswith('v2:')
        client.execute('SYSTEM FLUSH LOGS')
        peaks = client.execute(
            f'SELECT max(memory_usage), count() FROM system.query_log '
            f"WHERE type='QueryFinish' AND position(query,'FROM {table}')>0 "
            f"AND position(query,'LIMIT {BOOK_HASH_CHUNK_ROWS}')>0"
        )
        assert int(str(peaks[0][1])) >= 2
        assert 0 < int(str(peaks[0][0])) < 256 * 1024**2
    finally:
        client.disconnect()
