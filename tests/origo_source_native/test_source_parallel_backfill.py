from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from dagster import AssetKey, Definitions

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily
from origo.sources.adapters.binance_columnar import spot_table
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import Partition, Revision, SourceError, StateRecord
from origo.sources.lifecycle import SourceRuntime, _partition_lock
from origo.sources.locking import partition_work, source_lock
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import ARCHIVES, archive_response


def _parse_trade_rows(body: bytes) -> list[tuple[object, ...]]:
    """An independent parse of a spot archive into the row shape the digest packs."""
    import csv
    from datetime import UTC, datetime, timedelta

    rows: list[tuple[object, ...]] = []
    for fields in csv.reader(body.decode().splitlines()):
        stamp = int(fields[4])
        micros = stamp * 1000 if len(fields[4]) == 13 else stamp
        instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)
        rows.append(
            (
                int(fields[0]),
                float(fields[1]),
                float(fields[2]),
                float(fields[3]),
                stamp,
                fields[5].lower() == 'true',
                fields[6].lower() == 'true',
                instant.replace(tzinfo=None),
            )
        )
    return rows


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
    assert len(job.asset_layer.executable_asset_keys) == 2 + len(BINANCE_SPOT_TRADES_SPEC.consumers)
    assert job.get_run_config_for_partition_key('2020-01-01') == {}


def test_concurrent_days_keep_generations_isolated_and_repair_coexists(
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

    def run(day: str) -> tuple[StateRecord, int]:
        connection = make_clickhouse_client(get_clickhouse_settings())
        worker = SourceRuntime(spec, SourceStore(connection, 'origo', spec), root, day)
        try:
            record = worker.build(day)
            assert worker.reconcile(day) == record
            rows = worker.store.execute(
                "SELECT row_count FROM origo.source_component_log WHERE build_id=%(build)s AND component='raw'",
                {'build': record.build_id},
            )
            return record, int(str(rows[0][0]))
        finally:
            connection.disconnect()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, ['2017-08-17', '2020-01-01']))
        assert len({record.build_id for record, _ in results}) == 2
        assert {rows for _, rows in results} == {3427, 194010}
        assert runtime.store.canonical_ready()
        lock = _partition_lock(spec.canonical.partition('2020-01-01'))
        with partition_work(root, spec.key, lock):
            # Repairing another day shares the maintenance fence with an active builder.
            assert runtime.repair('2017-08-17') == results[0][0]
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
        assert runtime.reconcile('2017-08-17') == previous
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
    from origo.sources.columnar import binary_hash, insert_arrow

    chunk_rows = 65536 if day == '2020-01-01' else 1024
    monkeypatch.setattr(columnar, 'HASH_CHUNK_ROWS', chunk_rows)
    monkeypatch.setattr(columnar, 'INSERT_BATCH_ROWS', chunk_rows)
    csv = ARCHIVES / f'BTCUSDT-trades-{day}.csv'
    if csv.exists():
        body = csv.read_bytes()
    else:
        with zipfile.ZipFile(ARCHIVES / f'BTCUSDT-trades-{day}.zip') as archive:
            body = archive.read(f'BTCUSDT-trades-{day}.csv')
    rows = _parse_trade_rows(body)
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
        insert_arrow(table, spot_table(body, spec.canonical.partition(day)))
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


def test_copied_content_is_checked_before_activation(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from uuid import UUID

    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    spec = BINANCE_SPOT_TRADES_SPEC
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'copy')
    original = SourceRuntime._build_components

    def damage_copy(
        self: SourceRuntime, partition: Partition, revision: Revision, build_id: UUID, expected: int
    ) -> StateRecord:
        result = original(self, partition, revision, build_id, expected)
        table = self.store.component_table('raw')
        first = self.store.execute(f'SELECT min(trade_id) FROM {table}')[0][0]
        self.store.execute(
            f'ALTER TABLE {table} DELETE WHERE trade_id=%(first)s SETTINGS mutations_sync=2',
            {'first': first},
        )
        return result

    try:
        runtime.setup()
        monkeypatch.setattr(SourceRuntime, '_build_components', damage_copy)
        with pytest.raises(SourceError, match='Retained raw content'):
            runtime.build('2017-08-17')
        assert runtime.store.records(canonical_only=True) == ()
        assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(0,)]
    finally:
        client.disconnect()


def test_interrupted_bulk_batch_retries_without_exposing_partial_rows(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from collections.abc import Iterator
    from contextlib import contextmanager

    import numpy as np

    from origo.sources import columnar

    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    monkeypatch.setattr(columnar, 'INSERT_BATCH_ROWS', 65536)
    spec = BINANCE_SPOT_TRADES_SPEC
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'batch')
    real_client = columnar.native_column_client
    committed: list[int] = []

    @contextmanager
    def interrupted_client() -> Iterator[columnar.NativeColumnClient]:
        with real_client() as transport:

            class Interrupted:
                def execute(self, query: str, data: list[object], *, columnar: bool) -> int:
                    prefix: list[object] = []
                    for column in data:
                        assert isinstance(column, np.ndarray)
                        prefix.append(column[:65536])
                    committed.append(transport.execute(query, prefix, columnar=columnar))
                    raise OSError('Connection lost before second bulk batch')

                def disconnect(self) -> None:
                    transport.disconnect()

            yield Interrupted()

    try:
        runtime.setup()
        with monkeypatch.context() as patch:
            patch.setattr(columnar, 'native_column_client', interrupted_client)
            with pytest.raises(OSError, match='second bulk batch'):
                runtime.build('2020-01-01')
        assert committed == [65536]
        assert runtime.store.records(canonical_only=True) == ()
        assert runtime.store.execute(
            f'SELECT count() FROM {runtime.store.component_table("raw")}'
        ) == [(0,)]
        record = runtime.build('2020-01-01')
        assert runtime.reconcile('2020-01-01') == record
        assert runtime.store.execute(
            "SELECT row_count FROM origo.source_component_log WHERE build_id=%(build)s AND component='raw'",
            {'build': record.build_id},
        ) == [(194010,)]
        assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]
    finally:
        client.disconnect()
