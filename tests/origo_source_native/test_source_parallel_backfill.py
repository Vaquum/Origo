from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from dagster import AssetKey, Definitions

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.assets.daily_trades_to_origo import _parse_trade_rows
from origo.sources.adapters import binance_daily
from origo.sources.adapters.binance_columnar import spot_table
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import Partition, Revision, SourceError, StateRecord
from origo.sources.lifecycle import SourceRuntime, _partition_lock
from origo.sources.locking import partition_work, source_lock
from origo.sources.profiles.spot_parity import _reference_table
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import ARCHIVES, archive_response


def test_native_partition_runs_and_publication_dependencies() -> None:
    bundle = build_source_bundle(BINANCE_SPOT_TRADES_SPEC)
    graph = Definitions(assets=bundle.assets).resolve_asset_graph()
    key = AssetKey('build_binance_spot_trades_canonical_revision_origo')
    assert graph.get(key).backfill_policy.max_partitions_per_run == 1
    job = next(job for job in bundle.jobs if job.name.startswith('backfill_'))
    consumers = {
        AssetKey(f'publish_binance_spot_trades_{c.key}') for c in BINANCE_SPOT_TRADES_SPEC.consumers
    }
    for consumer in consumers:
        assert graph.get(consumer).parent_keys == {key}
    assert (
        graph.get(AssetKey('reconcile_binance_spot_trades_source_origo')).parent_keys == consumers
    )
    assert len(job.asset_layer.executable_asset_keys) == 5
    assert job.get_run_config_for_partition_key('2020-01-01') == {}


@pytest.mark.parametrize('day', ['2017-08-17', '2020-01-01', '2024-12-31', '2025-01-01'])
def test_native_parsers_match_frozen_legacy_rows(day: str) -> None:
    import zipfile

    import polars as pl

    path = ARCHIVES / f'BTCUSDT-trades-{day}.csv'
    if path.exists():
        body = path.read_bytes()
    else:
        with zipfile.ZipFile(ARCHIVES / f'BTCUSDT-trades-{day}.zip') as archive:
            body = archive.read(f'BTCUSDT-trades-{day}.csv')
    partition = binance_daily.BinanceSpotDaily().partition(day)
    expected = _parse_trade_rows(body)
    for table in (spot_table(body, partition), _reference_table(body)):
        actual = pl.from_arrow(table).rows()
        assert len(actual) == len(expected)
        assert [tuple(row[:-1]) for row in actual] == [tuple(row[:-1]) for row in expected]
        assert [row[-1].replace(tzinfo=None) for row in actual] == [
            row[-1].replace(tzinfo=None) for row in expected
        ]


def test_concurrent_days_keep_generation_and_parity_isolated(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = BINANCE_SPOT_TRADES_SPEC
    client = make_clickhouse_client(get_clickhouse_settings())
    root = tmp_path / 'locks'
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), root, 'setup')
    runtime.setup()
    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    barrier = Barrier(2, timeout=20)
    fetch = binance_daily.BinanceSpotDaily.fetch

    def overlap(self: binance_daily.BinanceSpotDaily, partition: Partition) -> Revision:
        barrier.wait()
        return fetch(self, partition)

    monkeypatch.setattr(binance_daily.BinanceSpotDaily, 'fetch', overlap)

    def run(day: str) -> tuple[StateRecord, dict[str, object]]:
        connection = make_clickhouse_client(get_clickhouse_settings())
        worker = SourceRuntime(spec, SourceStore(connection, 'origo', spec), root, day)
        try:
            record = worker.build(day)
            verified, proof = worker.verify(day)
            assert record == verified
            return record, proof
        finally:
            connection.disconnect()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, ['2017-08-17', '2020-01-01']))
        assert len({record.build_id for record, _ in results}) == 2
        assert {proof['raw']['row_count'] for _, proof in results} == {3427, 194010}
        assert runtime.store.canonical_verified()
        assert (
            runtime.store.execute(
                "SELECT name FROM system.databases WHERE startsWith(name,'origo_source_parity_binance_spot_trades')"
            )
            == []
        )
        lock = _partition_lock(spec.canonical.partition('2020-01-01'))
        with partition_work(root, spec.key, lock):
            with pytest.raises(SourceError, match='already held'):
                with partition_work(root, spec.key, lock):
                    pytest.fail('Same partition admitted two writers')
            with pytest.raises(SourceError, match='already held'):
                with source_lock(root, spec.key, 'heavy'):
                    pytest.fail('Maintenance admitted while a builder holds its shared fence')
    finally:
        client.disconnect()


def test_previous_hash_generations_remain_verifiable(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace

    from origo.sources.hashing import content_hash
    from origo.sources.storage import ordered_component_rows

    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    spec = BINANCE_SPOT_TRADES_SPEC
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'compatibility'
    )
    try:
        runtime.setup()
        record = runtime.build('2017-08-17')
        hashes = []
        for component in runtime.store.components(record.partition):
            digest = content_hash(
                ordered_component_rows(
                    runtime.store.execute, component, runtime.store.component_table(component.key)
                ),
                schema_version=1,
            )
            hashes.append((component.key, digest))
            runtime.store.execute(
                'ALTER TABLE origo.source_component_log UPDATE content_hash=%(hash)s WHERE build_id=%(build)s AND component=%(component)s SETTINGS mutations_sync=2',
                {'hash': digest, 'build': record.build_id, 'component': component.key},
            )
        previous = replace(record, component_hashes=tuple(hashes), generation=record.generation + 1)
        runtime.store.insert_activation(previous, 'pre-upgrade-proof')
        runtime._validate_retained(previous)
        assert runtime.build('2017-08-17') == previous
        verified, checks = runtime.verify('2017-08-17')
        assert verified == previous and len(checks) == 7
    finally:
        client.disconnect()


@pytest.mark.parametrize('day', ['2020-01-01', '2025-01-01'])
def test_chunk_hash_matches_independent_rowbinary_encoding(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, day: str
) -> None:
    import hashlib
    import json
    import struct
    import zipfile
    from datetime import UTC, datetime

    from origo.sources import columnar
    from origo.sources.columnar import binary_hash

    chunk_rows = 65536 if day == '2020-01-01' else 1024
    monkeypatch.setattr(columnar, 'HASH_CHUNK_ROWS', chunk_rows)
    csv = ARCHIVES / f'BTCUSDT-trades-{day}.csv'
    if csv.exists():
        rows = _parse_trade_rows(csv.read_bytes())
    else:
        with zipfile.ZipFile(ARCHIVES / f'BTCUSDT-trades-{day}.zip') as archive:
            rows = _parse_trade_rows(archive.read(f'BTCUSDT-trades-{day}.csv'))
    spec = BINANCE_SPOT_TRADES_SPEC
    component = spec.components[0]
    header = json.dumps([(c.name, c.sql_type) for c in component.columns], separators=(',', ':'))
    expected = hashlib.sha256(b'origo-source-rowbinary-chunks-v2\n' + header.encode() + b'\n')
    expected.update(spec.schema_version.to_bytes(8, 'big'))
    expected.update(chunk_rows.to_bytes(8, 'big'))
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for chunk, offset in enumerate(range(0, len(rows), chunk_rows)):
        selected = rows[offset : offset + chunk_rows]
        block = bytearray()
        for row in selected:
            stamp = row[-1].replace(tzinfo=UTC) - epoch
            micros = (stamp.days * 86400 + stamp.seconds) * 1000000 + stamp.microseconds
            block.extend(struct.pack('<QdddQBBq', *row[:-1], micros))
        expected.update(chunk.to_bytes(8, 'big'))
        expected.update(len(selected).to_bytes(8, 'big'))
        expected.update(hashlib.sha256(block).digest())
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'hash')
    try:
        runtime.setup()
        table = 'origo.real_hash_rows'
        columns = ', '.join(f'{c.name} {c.sql_type}' for c in component.columns)
        client.execute(
            f'CREATE TABLE {table} ({columns}) ENGINE=MergeTree ORDER BY (datetime, trade_id)'
        )
        client.execute(f'INSERT INTO {table} VALUES', rows)
        assert len(rows) > chunk_rows * 2
        count, digest = runtime.store.validate_component(
            component, table, spec.canonical.partition(day)
        )
        assert count == len(rows)
        assert digest == 'v2:' + expected.hexdigest()
        # Different read blocks must preserve ordering and chunk boundaries.
        client.execute('SET max_block_size=8192')
        assert binary_hash(runtime.store.client, component, table) == 'v2:' + expected.hexdigest()
        assert (
            binary_hash(runtime.store.client, component, table, schema_version=2)
            != 'v2:' + expected.hexdigest()
        )
    finally:
        client.disconnect()
