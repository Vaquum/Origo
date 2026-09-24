from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta, tzinfo
from pathlib import Path
from threading import Event
from uuid import uuid4

import pytest
from dagster import AssetKey, DagsterInstance, Definitions, RunRequest, build_sensor_context

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import capacity, dagit, rollout
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import (
    Partition,
    PartitionPolicy,
    Revision,
    Row,
    SourceError,
    StateRecord,
)
from origo.sources.hashing import content_hash
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.profiles.market_state import CUBE_START
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import ARCHIVES

_DAY = '2025-01-01'
_MINUTE = '2025-01-01T00:00:00Z'
_ASSET = 'build_binance_spot_trades_canonical_revision_origo'


def _capture(day: str) -> tuple[str, tuple[Row, ...]]:
    body = (ARCHIVES / f'BTCUSDT-trades-{day}.csv').read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    provenance = json.loads((ARCHIVES / f'BTCUSDT-trades-{day}.provenance.json').read_text())
    assert digest == provenance['selected_sha256']
    partition = daily.BinanceSpotDaily().partition(day)
    return digest, tuple(daily.spot_csv_rows(body, partition))


@dataclass
class CapturedArchive:
    fetched: int = 0

    def candidate(self, now: datetime) -> Partition:
        return daily.BinanceSpotDaily().candidate(now)

    def partition(self, key: str) -> Partition:
        return daily.BinanceSpotDaily().partition(key)

    def discover(self, partition: Partition) -> str:
        return _capture(partition.key)[0]

    def fetch(self, partition: Partition) -> Revision:
        self.fetched += 1
        digest, rows = _capture(partition.key)
        return Revision(digest, content_hash(rows, schema_version=1), '{}', len(rows), lambda: rows)

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        assert revision.key == self.discover(partition)


@dataclass
class CapturedMinutes:
    fetched: int = 0

    def partition(self, key: str) -> Partition:
        start = datetime.fromisoformat(key)
        return Partition(key, start, start + timedelta(minutes=1), True)

    def candidates(
        self, now: datetime, anchor: datetime, covered: tuple[Partition, ...]
    ) -> tuple[Partition, ...]:
        candidate = self.partition(_MINUTE)
        return (candidate,) if candidate.end <= now and candidate not in covered else ()

    def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
        # Minute fragments contain unmodified rows from the checksum-proven archive capture.
        self.fetched += 1
        _, archive = _capture(partition.start.date().isoformat())
        rows = tuple(
            row for row in archive
            if isinstance(row[-1], datetime) and partition.start <= row[-1] < partition.end
        )
        assert rows
        digest = content_hash(rows, schema_version=1)
        return Revision(digest, digest, '{}', len(rows), lambda: rows)


@dataclass(frozen=True)
class CubeRuntime:
    runtime: SourceRuntime
    archive: CapturedArchive
    minutes: CapturedMinutes


@pytest.fixture
def registered_cube(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[CubeRuntime]:
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    archive, minutes = CapturedArchive(), CapturedMinutes()
    spec = replace(
        BINANCE_SPOT_TRADES_SPEC,
        canonical=archive,
        partitions=PartitionPolicy(date(2025, 1, 1)),
        provisional=minutes,
        orchestration=replace(BINANCE_SPOT_TRADES_SPEC.orchestration, retry_count=0),
    )
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4()))
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(runtime.lock_root))
    try:
        runtime.setup(anchor=datetime(2025, 1, 1, tzinfo=UTC))
        yield CubeRuntime(runtime, archive, minutes)
    finally:
        client.disconnect()


def _physical(runtime: SourceRuntime, record: StateRecord) -> dict[str, list[Row]]:
    return {
        component.key: runtime.store.execute(
            f'SELECT * FROM {runtime.store.component_table(component.key)} '
            f'WHERE build_id=%(build)s ORDER BY {", ".join(component.primary_key)}',
            {'build': record.build_id},
        )
        for component in runtime.store.accepted_components(record)
    }


def _cube_count(runtime: SourceRuntime) -> int:
    value = runtime.store.execute(
        'SELECT count() FROM origo.binance_spot_trades_market_state_current'
    )[0][0]
    assert isinstance(value, int)
    return value


def _assert_upgrade(runtime: SourceRuntime, before: StateRecord, after: StateRecord) -> None:
    assert after.build_id == before.build_id
    assert after.revision == before.revision
    assert after.generation == before.generation + 1
    assert dict(before.component_hashes).items() <= dict(after.component_hashes).items()
    added = 'market_state_latest' if before.partition.provisional else 'market_state'
    assert set(dict(after.component_hashes)) - set(dict(before.component_hashes)) == {added}
    assert runtime.store.missing_components(after) == ()
    assert runtime.store.execute(
        f'SELECT sum(toUInt64(trade_count)), sum(toUInt64(taker_buy_trade_count)) '
        f'FROM {runtime.store.component_table(added)} WHERE build_id=%(build)s',
        {'build': after.build_id},
    ) == runtime.store.execute(
        f'SELECT count(), countIf(is_buyer_maker=0) FROM '
        f'{runtime.store.component_table("raw_latest" if before.partition.provisional else "raw")} '
        'WHERE build_id=%(build)s',
        {'build': after.build_id},
    )


def test_market_state_upgrade_preserves_existing_products(registered_cube: CubeRuntime) -> None:
    runtime = registered_cube.runtime
    assert runtime.store.enabled_groups() == frozenset()
    before = runtime.build(_DAY)
    original = _physical(runtime, before)
    snapshot = runtime.store.snapshot()
    assert len(before.component_hashes) == 7
    assert _cube_count(runtime) == 0
    runtime.enable_components('market_state')
    assert [item.key for item in runtime.store.missing_components(before)] == ['market_state']
    assert runtime.store.canonical_ready()
    after = runtime.upgrade_components(_DAY)
    _assert_upgrade(runtime, before, after)
    expanded = _physical(runtime, after)
    assert {key: expanded[key] for key in original} == original
    assert registered_cube.archive.fetched == 1
    assert _cube_count(runtime) > 0
    assert runtime.store.rows('market_state', snapshot) == []
    assert runtime.store.rows('market_state', runtime.store.snapshot())
    assert runtime.upgrade_components(_DAY) == after
    assert runtime.build(_DAY) == after
    assert runtime.repair(_DAY) == after
    assert registered_cube.archive.fetched == 1
    assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(2,)]
    assert runtime.store.execute('SELECT count() FROM origo.source_build_log') == [(1,)]


@pytest.mark.parametrize('interruption', ['after_copy', 'before_activation'])
def test_market_state_upgrade_resumes_without_duplicate_rows(
    registered_cube: CubeRuntime, monkeypatch: pytest.MonkeyPatch, interruption: str
) -> None:
    runtime = registered_cube.runtime
    before = runtime.build(_DAY)
    original = _physical(runtime, before)
    runtime.enable_components('market_state')
    execute = runtime.store.execute
    interrupted = False

    def fail_once(
        query: str, params: object | None = None, *, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        nonlocal interrupted
        copying = query.startswith('INSERT INTO origo.binance_spot_trades_market_state_revisions')
        activating = query.startswith('INSERT INTO origo.source_activation_log')
        if interruption == 'before_activation' and activating and not interrupted:
            interrupted = True
            raise OSError('Injected interruption after receipt, before activation')
        result = execute(query, params, settings=settings)
        if interruption == 'after_copy' and copying and not interrupted:
            interrupted = True
            raise OSError('Injected lost acknowledgement after projection rows')
        return result

    with monkeypatch.context() as patch:
        patch.setattr(runtime.store, 'execute', fail_once)
        with pytest.raises(SourceError, match='Additive projection upgrade failed'):
            runtime.upgrade_components(_DAY)
    assert interrupted
    assert runtime.store.record(before.partition) == before
    assert _physical(runtime, before) == original
    assert _cube_count(runtime) == 0
    assert runtime.store.rows('market_state', runtime.store.snapshot()) == []
    assert runtime.store.execute(
        "SELECT DISTINCT blocking_scope FROM origo.source_failure_log WHERE event_type='FAILED'"
    ) == [('NONE',)]
    assert runtime.store.canonical_ready()
    after = runtime.upgrade_components(_DAY)
    _assert_upgrade(runtime, before, after)
    assert runtime.upgrade_components(_DAY) == after
    assert runtime.store.execute(
        'SELECT count(), uniqExact(tuple(time_index, price_index)) '
        'FROM origo.binance_spot_trades_market_state_revisions'
    ) == [(_cube_count(runtime), _cube_count(runtime))]
    assert runtime.store.execute(
        "SELECT count() FROM origo.source_component_log WHERE component='market_state'"
    ) == [(1,)]
    assert registered_cube.archive.fetched == 1


def test_market_state_registration_applies_from_2021(registered_cube: CubeRuntime) -> None:
    runtime = registered_cube.runtime
    assert len(runtime.spec.components) == 12
    assert BINANCE_SPOT_TRADES_SPEC.partitions.first_day.isoformat() == '2017-08-17'
    cube = [item for item in runtime.spec.components if item.activation_group == 'market_state']
    assert {item.key for item in cube} == {'market_state', 'market_state_latest'}
    assert all(item.start_at == CUBE_START for item in cube)
    runtime.enable_components('market_state')
    before_cutoff = runtime.build('2017-08-17')
    assert len(before_cutoff.component_hashes) == 7
    assert runtime.store.missing_components(before_cutoff) == ()
    assert runtime.upgrade_components('2017-08-17') == before_cutoff
    assert {item.key for item in runtime.store.known_components(before_cutoff.partition)} == set(
        dict(before_cutoff.component_hashes)
    )
    at_cutoff = runtime.spec.canonical.partition('2021-01-01')
    assert 'market_state' in {item.key for item in runtime.store.components(at_cutoff)}
    after_cutoff = runtime.build(_DAY)
    assert len(after_cutoff.component_hashes) == 8
    assert _cube_count(runtime) > 0
    with pytest.raises(SourceError, match='inventory'):
        runtime.store.accepted_components(
            replace(before_cutoff, component_hashes=(*before_cutoff.component_hashes, ('market_state', 'bad')))
        )


@pytest.mark.parametrize('operation', ['replacement', 'rollback'])
def test_market_state_provisional_replacement_and_rollback(
    registered_cube: CubeRuntime, operation: str
) -> None:
    runtime = registered_cube.runtime
    if operation == 'rollback':
        legacy = runtime.build(_DAY)
        runtime.enable_components('market_state')
        upgraded = runtime.upgrade_components(_DAY)
        cells = runtime.store.rows('market_state', runtime.store.snapshot())
        assert cells
        rolled_back = runtime.rollback(legacy, operator='test', reason='Verify legacy footprint selection')
        assert rolled_back.component_hashes == legacy.component_hashes
        assert _cube_count(runtime) == 0
        assert runtime.store.rows('market_state', runtime.store.snapshot()) == []
        restored = runtime.upgrade_components(_DAY)
        _assert_upgrade(runtime, rolled_back, restored)
        assert restored.component_hashes == upgraded.component_hashes
        assert runtime.store.rows('market_state', runtime.store.snapshot()) == cells
        assert runtime.store.execute(
            "SELECT count() FROM origo.source_component_log WHERE component='market_state'"
        ) == [(1,)]
        assert registered_cube.archive.fetched == 1
        return
    provisional = runtime.build(_MINUTE, provisional=True)
    assert len(provisional.component_hashes) == 3
    runtime.enable_components('market_state')
    upgraded = runtime.build(_MINUTE, provisional=True)
    _assert_upgrade(runtime, provisional, upgraded)
    assert registered_cube.minutes.fetched == 1
    provisional_cube = runtime.store.rows('market_state_latest', runtime.store.snapshot())
    assert provisional_cube and _cube_count(runtime) == len(provisional_cube)
    rolled_back = runtime.rollback(provisional, operator='test', reason='Verify legacy minute selection')
    assert rolled_back.component_hashes == provisional.component_hashes
    assert _cube_count(runtime) == 0
    assert runtime.store.rows('market_state_latest', runtime.store.snapshot()) == []
    restored = runtime.upgrade_components(_MINUTE, provisional=True)
    _assert_upgrade(runtime, rolled_back, restored)
    assert runtime.store.rows('market_state_latest', runtime.store.snapshot()) == provisional_cube
    assert runtime.store.execute(
        "SELECT count() FROM origo.source_component_log WHERE component='market_state_latest'"
    ) == [(1,)]
    canonical = runtime.build(_DAY)
    assert len(canonical.component_hashes) == 8
    assert runtime.store.snapshot().records == (canonical,)
    assert runtime.store.rows('market_state_latest', runtime.store.snapshot()) == []
    assert runtime.store.execute(
        'SELECT sum(toUInt64(trade_count)), sum(toUInt64(taker_buy_trade_count)) '
        'FROM origo.binance_spot_trades_market_state_current'
    ) == runtime.store.execute(
        'SELECT count(), countIf(is_buyer_maker=0) FROM origo.binance_spot_trades_raw_current'
    )
    assert runtime.store.execute(
        'SELECT count() FROM origo.binance_spot_trades_market_state_latest_revisions'
    ) == [(len(provisional_cube),)]


@pytest.mark.parametrize('scenario', ['upgrade', 'transient', 'repair'])
def test_market_state_native_backfill_and_gap_selection(
    registered_cube: CubeRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str
) -> None:
    runtime = registered_cube.runtime

    @dataclass(frozen=True)
    class MeasuredVolume:
        identity: str = 'test-volume'
        path: Path = tmp_path

        def sample(self) -> tuple[int, int, int, int]:
            return 10**12, 9 * 10**11, 10**8, 9 * 10**7

    def measured_volumes(current: SourceRuntime) -> tuple[MeasuredVolume, ...]:
        return (MeasuredVolume(':'.join(sorted(current.store.enabled_groups()))),)

    monkeypatch.setattr(capacity, '_volumes', measured_volumes)
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'publications'))
    source = build_source_bundle(runtime.spec)
    asset = next(item for item in source.assets if item.key == AssetKey(_ASSET))
    definitions = Definitions(assets=source.assets, jobs=source.jobs, sensors=source.sensors)
    sensor = next(item for item in source.sensors if item.name.endswith('_reconciliation_sensor'))
    native_job = Definitions(assets=[asset]).get_implicit_global_asset_job_def()
    dagster_root = tmp_path / 'dagster'
    dagster_root.mkdir()
    with DagsterInstance.local_temp(str(dagster_root)) as instance:
        assert asset.backfill_policy is not None and asset.backfill_policy.max_partitions_per_run == 1
        result = native_job.execute_in_process(
            instance=instance,
            tags={'dagster/asset_partition_range_start': _DAY, 'dagster/asset_partition_range_end': _DAY},
            asset_selection=[asset.key],
        )
        assert result.success
        before = runtime.store.records(canonical_only=True)[0]
        assert len(before.component_hashes) == 7
        assert instance.get_materialized_partitions(asset.key) == {_DAY}

        def tick() -> list[RunRequest]:
            with build_sensor_context(instance=instance, definitions=definitions) as context:
                return list(sensor.evaluate_tick(context).run_requests or ())

        assert not [request for request in tick() if request.partition_key]
        runtime.enable_components('market_state')
        requests = [request for request in tick() if request.partition_key]
        assert [request.partition_key for request in requests] == [_DAY]
        request = requests[0]
        job = next(item for item in source.jobs if item.name == request.job_name)
        if scenario == 'transient':
            execute = SourceStore.execute
            interrupted = False

            def unavailable(
                store: SourceStore, query: str, params: object | None = None,
                *, settings: Mapping[str, object] | None = None,
            ) -> list[Row]:
                nonlocal interrupted
                if query.startswith('INSERT INTO origo.binance_spot_trades_market_state_revisions') and not interrupted:
                    interrupted = True
                    raise OSError('Injected temporary storage connection loss')
                return execute(store, query, params, settings=settings)

            with monkeypatch.context() as patch:
                patch.setattr(SourceStore, 'execute', unavailable)
                failed = job.execute_in_process(
                    instance=instance, partition_key=request.partition_key,
                    run_config=request.run_config, tags=request.tags, raise_on_error=False,
                )
            assert interrupted and not failed.success
            failed_run = instance.get_run_by_id(failed.run_id)
            assert failed_run is not None and failed_run.tags['origo_source_verdict'] == ''
            assert runtime.store.record(before.partition) == before
            assert runtime.store.canonical_ready()

            class LaterClock:
                @staticmethod
                def now(zone: tzinfo) -> datetime:
                    return datetime.now(zone) + timedelta(minutes=2)

            with monkeypatch.context() as patch:
                patch.setattr(dagit, 'datetime', LaterClock)
                retries = [item for item in tick() if item.partition_key]
            assert len(retries) == 1 and retries[0].tags['origo_source_retry_attempt'] == '2'
            request = retries[0]
        if scenario == 'repair':
            repair = next(item for item in source.jobs if item.name.startswith('repair_'))
            assert repair.execute_in_process(instance=instance, partition_key=_DAY).success
            # Repair owns its own asset; canonical reconciliation records the new generation.
            assert [item.partition_key for item in tick() if item.partition_key] == [_DAY]
        assert job.execute_in_process(
            instance=instance, partition_key=request.partition_key,
            run_config=request.run_config, tags=request.tags,
        ).success
        assert runtime.store.execute(
            "SELECT countIf(successful) FROM origo.source_capacity_log WHERE volume_id='market_state'"
        )[0][0] >= 1
        after = runtime.store.records(canonical_only=True)[0]
        _assert_upgrade(runtime, before, after)
        assert _cube_count(runtime) > 0
        assert registered_cube.archive.fetched == 1
        assert not [request for request in tick() if request.partition_key]
        assert native_job.execute_in_process(
            instance=instance,
            tags={'dagster/asset_partition_range_start': _DAY, 'dagster/asset_partition_range_end': _DAY},
            asset_selection=[asset.key],
        ).success
        assert runtime.store.records(canonical_only=True) == (after,)


def test_market_state_deployment_enablement_is_fenced(
    registered_cube: CubeRuntime, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = registered_cube.runtime
    started = Event()
    monkeypatch.setattr(rollout, 'SOURCE_REGISTRY', (runtime.spec,))

    def enable() -> None:
        started.set()
        rollout.main()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with source_lock(runtime.lock_root, runtime.spec.key, 'heavy', shared=True):
            pending = executor.submit(enable)
            assert started.wait(timeout=10)
            with pytest.raises(TimeoutError):
                pending.result(timeout=0.1)
            assert runtime.store.enabled_groups() == frozenset()
        pending.result(timeout=10)
    assert runtime.store.enabled_groups() == frozenset({'market_state'})
    runtime.enable_components('market_state')
    assert runtime.store.execute('SELECT count() FROM origo.source_component_rollout_log') == [(1,)]
    with pytest.raises(ValueError, match='Unknown component activation group'):
        runtime.enable_components('undeclared')

    workflow = (Path(__file__).resolve().parents[2] / '.github/workflows/deploy_on_merge.yml').read_text()
    snapshot = workflow.index('prior_cube_writers=()')
    inventory = workflow.index('ps -q dagster dagit provisional-worker')
    retire = workflow.index('--force-recreate dagster dagit provisional-worker')
    proof = workflow.index('for worker_id in "${prior_cube_writers[@]}"; do')
    enable_command = workflow.index('python -m origo.sources.rollout')
    assert snapshot < inventory < retire < proof < enable_command
    assert 'exit 1' in workflow[proof:enable_command]
    assert 'docker inspect --format' in workflow[proof:enable_command]
