from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query.binance_spot_kline_rollups import dollar_month, time_month
from origo.sources import publication
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Partition, RolloutStage, Snapshot, StateRecord
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles import consumer_base, spot_consumers
from origo.sources.profiles.formulas.huggingface_dollar import get_binance_spot_dollar_klines
from origo.sources.profiles.formulas.huggingface_time import (
    get_binance_spot_klines_from_1m_projection,
)
from origo.sources.profiles.formulas.spot_series import SPECS
from origo.sources.profiles.spot_consumers import HUGGINGFACE_DATASETS
from origo.sources.storage import SourceStore
from origo.utils.arrow_store import build_series_frame

from .test_binance_daily_source_adapter import archive_response
from .test_market_state_registration import CapturedArchive


class FakeHfApi:
    calls: list[tuple[str, dict[str, object]]] = []

    def __init__(self, token: str) -> None:
        assert token == 'test-token'

    def create_repo(self, **kwargs: object) -> None:
        self.calls.append(('create_repo', kwargs))

    def upload_folder(self, **kwargs: object) -> None:
        self.calls.append(('upload_folder', kwargs))


def test_spot_consumers_publish_public_identities_from_one_pinned_state(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = '2020-01-01'
    monkeypatch.setenv(
        'BINANCE_SPOT_DAILY_TRADES_BASE_URL',
        binance_fixture_server_root_url + '/spot/daily/trades/revisioned/',
    )
    monkeypatch.setattr(daily, 'get_response', archive_response)
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    FakeHfApi.calls = []
    monkeypatch.setattr(spot_consumers, 'HfApi', FakeHfApi)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.LIVE)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup(anchor=datetime(2020, 1, 1, tzinfo=UTC))
        record = runtime.build(day)
        # The legacy table names are views over the built day, so the original monthly
        # readers produce the expected files from the same canonical rows.
        expected_root = tmp_path / 'legacy-parquet'
        for series in SPECS:
            if series.family == 'time':
                frame = time_month(interval_minutes=series.size, year=2020, month=1)
            else:
                frame = dollar_month(ratio=series.size, year=2020, month=1)
            target = expected_root / series.sub_path / '2020/01.parquet'
            target.parent.mkdir(parents=True, exist_ok=True)
            frame.write_parquet(target)
        source_root = tmp_path / spec.key

        # The mount consumer owns the public Parquet mirror and Arrow bar store, and sweeps the
        # staging a render that died left on the mirror.
        stale = tmp_path / 'parquet' / '.staging-stale'
        stale.mkdir(parents=True)
        (stale / 'x.parquet').write_bytes(b'x')
        os.utime(stale, (time.time() - 7200, time.time() - 7200))
        mount = source_root / 'mount'
        snapshot = runtime.publish('mount', str(mount))
        assert not stale.exists()
        manifest = json.loads((mount / 'latest.json').read_text())
        assert manifest['state_token'] == snapshot.token == manifest['pinned_token']
        assert list(manifest['month_tokens']) == ['2020-01']
        assert len(manifest['files']) == 24
        for entry in manifest['files']:
            assert Path(entry['path']).is_absolute() and Path(entry['path']).is_file()
        for series in SPECS:
            month = tmp_path / 'parquet' / series.sub_path / '2020/01.parquet'
            assert_frame_equal(
                pl.read_parquet(month),
                pl.read_parquet(expected_root / series.sub_path / '2020/01.parquet'),
            )
            assert_frame_equal(
                pl.read_ipc(tmp_path / 'arrow' / series.name / 'latest.arrow'),
                build_series_frame(series, expected_root).df,
            )
        assert publication.publication_current(
            spec, 'mount', store.snapshot().token, root=tmp_path, pinned=True
        )

        # A render whose months are unchanged reuses every file and rebuilds no series.
        written = {
            entry['path']: Path(entry['path']).stat().st_mtime_ns for entry in manifest['files']
        }
        with monkeypatch.context() as patch:
            patch.setattr(publication, 'publication_current', lambda *a, **k: False)
            runtime.publish('mount', str(mount))
        repeated = json.loads((mount / 'latest.json').read_text())
        assert {e['path']: Path(e['path']).stat().st_mtime_ns for e in repeated['files']} == written

        # A provisional interval after the canonical day refreshes only its month, which is
        # January again here, and the manifest follows the pinned state.
        start = datetime(2020, 1, 2, tzinfo=UTC)
        store.insert_activation(
            StateRecord(
                Partition(
                    start.strftime('%Y-%m-%dT%H:%M:%SZ'), start, start + timedelta(minutes=1), True
                ),
                1,
                'provisional',
                uuid4(),
                (),
            ),
            'provisional-refresh',
        )
        pinned = store.snapshot()
        assert pinned.token != snapshot.token
        runtime.publish('mount', str(mount))
        refreshed = json.loads((mount / 'latest.json').read_text())
        assert refreshed['pinned_token'] == pinned.token
        assert refreshed['state_token'] == snapshot.token
        assert refreshed['month_tokens']['2020-01'] != manifest['month_tokens']['2020-01']

        # The huggingface consumer uploads the twelve public datasets from the canonical state.
        huggingface = source_root / 'huggingface'
        canonical = runtime.publish('huggingface', str(huggingface))
        manifest = json.loads((huggingface / 'latest.json').read_text())
        assert manifest['state_token'] == canonical.token == snapshot.token
        assert manifest['export_end_date'] == day
        assert len(manifest['uploads']) == 12 and len(manifest['files']) == 12
        version = huggingface / 'versions' / manifest['version']
        uploads = {upload['series']: upload for upload in manifest['uploads']}
        for series in SPECS:
            repo_id, _env, prefix, _resolution = HUGGINGFACE_DATASETS[series.name]
            upload = uploads[series.name]
            assert upload['repo_id'] == repo_id
            assert upload['file_name'] == f'{prefix}20200101.parquet'
            actual = pl.read_parquet(version / series.name / upload['file_name'])
            if series.family == 'time':
                expected = get_binance_spot_klines_from_1m_projection(
                    kline_size_seconds=series.size * 60,
                    start_date_limit='2020-01-01',
                    end_date_limit='2020-01-02 00:00:00',
                    table_name='binance_spot_klines',
                    database_name='origo',
                )
            else:
                expected = get_binance_spot_dollar_klines(
                    dollar_size=float(series.size * 1000000),
                    start_date_limit='2020-01-01',
                    end_date_limit='2020-01-02 00:00:00',
                    table_name='binance_spot_dollar_klines',
                    database_name='origo',
                )
            assert_frame_equal(actual, expected)
            assert (version / series.name / 'README.md').is_file()
            assert (
                json.loads((version / series.name / 'latest.json').read_text())['file_name']
                == (upload['file_name'])
            )
        assert [call for call, _ in FakeHfApi.calls] == ['create_repo', 'upload_folder'] * 12
        assert {kwargs['repo_id'] for call, kwargs in FakeHfApi.calls} == {
            repo for repo, _, _, _ in HUGGINGFACE_DATASETS.values()
        }
        assert all(
            kwargs['delete_patterns'] == [f'{HUGGINGFACE_DATASETS[s.name][2]}*.parquet']
            for s, (call, kwargs) in zip(
                SPECS, [c for c in FakeHfApi.calls if c[0] == 'upload_folder'], strict=True
            )
        )

        # A canonical change during a render discards the render, records the failure and
        # leaves the public roots untouched: a month removed beforehand stays absent, no
        # series flips and nothing staged remains.
        before = (mount / 'latest.json').read_bytes()
        removed = tmp_path / 'parquet' / SPECS[0].sub_path / '2020/01.parquet'
        removed.unlink()
        targets = {s.name: (tmp_path / 'arrow' / s.name / 'latest.arrow').resolve() for s in SPECS}
        original = store.snapshot
        reads = 0

        def advance_before_commit(*, canonical_only: bool = False) -> Snapshot:
            nonlocal reads
            reads += 1
            # Publication reads the canonical state, then pins; the renderer's pre-commit
            # recheck is the third read, where a canonical change must be observed.
            if reads == 3:
                captured = replace(spec, canonical=CapturedArchive())
                SourceRuntime(
                    captured, SourceStore(client, 'origo', captured), runtime.lock_root, runtime.run_id
                ).build('2024-12-31')
            return original(canonical_only=canonical_only)

        with monkeypatch.context() as patch:
            patch.setattr(store, 'snapshot', advance_before_commit)
            patch.setattr(publication, 'publication_current', lambda *a, **k: False)
            with pytest.raises(RuntimeError, match='state changed'):
                runtime.publish('mount', str(mount))
        assert (mount / 'latest.json').read_bytes() == before
        assert not removed.exists()
        assert {s.name: (tmp_path / 'arrow' / s.name / 'latest.arrow').resolve() for s in SPECS} == targets
        assert not list((tmp_path / 'parquet').glob('.staging-*'))
        assert store.generation(record.partition) == 1
        assert store.record(spec.canonical.partition('2024-12-31')) is not None
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' AND blocking_scope='CONSUMER' AND event_type='FAILED'"
        ) == [(1,)]
        runtime.publish('mount', str(mount))
        assert removed.is_file() and not list((tmp_path / 'parquet').glob('.staging-*'))
        assert (
            json.loads((mount / 'latest.json').read_text())['state_token']
            == original(canonical_only=True).token
        )
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' AND event_type='RECOVERED'"
        ) == [(1,)]

        # A changed canonical input replaces the local snapshot; a current-state
        # publication also reclaims a killed render without rewriting current files.
        runtime.publish('huggingface', str(huggingface))
        assert not version.exists()
        current = json.loads((huggingface / 'latest.json').read_text())
        current_version = huggingface / 'versions' / current['version']
        assert set((huggingface / 'versions').iterdir()) == {current_version}
        preserved = _publication_bytes(huggingface)
        orphan = huggingface / 'versions' / (snapshot.token + '-' + uuid4().hex)
        shutil.copytree(current_version, orphan)
        calls = list(FakeHfApi.calls)
        runtime.publish('huggingface', str(huggingface))
        assert not orphan.exists()
        assert _publication_bytes(huggingface) == preserved
        assert FakeHfApi.calls == calls
    finally:
        client.disconnect()


def test_public_consumers_require_the_live_stage() -> None:
    canary = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    with pytest.raises(RuntimeError, match='LIVE'):
        canary.require_enabled('publish', public=True)
    assert all(consumer.public for consumer in BINANCE_SPOT_TRADES_SPEC.consumers)


def _build_one_day(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_root_url: str):
    """Mirror the publish test setup: env, fixture archive, and one built canonical day."""
    monkeypatch.setenv(
        'BINANCE_SPOT_DAILY_TRADES_BASE_URL',
        fixture_root_url + '/spot/daily/trades/revisioned/',
    )
    monkeypatch.setattr(daily, 'get_response', archive_response)
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.LIVE)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    runtime.setup(anchor=datetime(2020, 1, 1, tzinfo=UTC))
    runtime.build('2020-01-01')
    return spec, client, runtime


def _publication_bytes(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob('*') if path.is_file()
    }


@pytest.fixture
def published_huggingface(
    origo_test_env: dict[str, str], binance_fixture_server_root_url: str,
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[SourceRuntime, Path]]:
    spec, client, runtime = _build_one_day(tmp_path, monkeypatch, binance_fixture_server_root_url)
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    FakeHfApi.calls = []
    monkeypatch.setattr(spot_consumers, 'HfApi', FakeHfApi)
    root = tmp_path / spec.key / 'huggingface'
    try:
        runtime.publish('huggingface', str(root))
        yield runtime, root
    finally:
        client.disconnect()


def test_huggingface_failure_cleanup_respects_committed_visibility(
    published_huggingface: tuple[SourceRuntime, Path], monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, root = published_huggingface
    previous = _publication_bytes(root)
    manifest = json.loads((root / 'latest.json').read_text())
    version = root / 'versions' / manifest['version']

    def interrupted(*args: object, **kwargs: object) -> None:
        raise OSError('Injected publication interruption')

    # With no manifest, only the failing attempt is owned: an older genuine
    # uncommitted copy must survive for explicit investigation.
    fresh = root.parent.parent / 'first-attempt' / runtime.spec.key / 'huggingface'
    orphan = fresh / 'versions' / version.name
    shutil.copytree(version, orphan)
    initial = _publication_bytes(fresh)
    with monkeypatch.context() as patch:
        patch.setattr(FakeHfApi, 'upload_folder', interrupted)
        with pytest.raises(OSError, match='Injected publication interruption'):
            runtime.publish('huggingface', str(fresh))
    assert _publication_bytes(fresh) == initial
    assert set((fresh / 'versions').iterdir()) == {orphan}

    captured = replace(runtime.spec, canonical=CapturedArchive())
    changed = SourceRuntime(
        captured, SourceStore(runtime.store.client, runtime.store.database, captured),
        runtime.lock_root, runtime.run_id,
    )
    changed.build('2024-12-31')
    for target, attribute in (
        (spot_consumers.time_snapshot, 'get_binance_spot_klines_from_1m_projection'),
        (FakeHfApi, 'upload_folder'),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(target, attribute, interrupted)
            with pytest.raises(OSError, match='Injected publication interruption'):
                runtime.publish('huggingface', str(root))
        assert _publication_bytes(root) == previous
        assert set((root / 'versions').iterdir()) == {version}

    commit = consumer_base._commit_manifest

    def canonical_drift(
        reader: SourceStore, snapshot: Snapshot, destination: Path, value: dict[str, object],
    ) -> None:
        changed.build('2025-01-01')
        commit(reader, snapshot, destination, value)

    with monkeypatch.context() as patch:
        patch.setattr(consumer_base, '_commit_manifest', canonical_drift)
        with pytest.raises(RuntimeError, match='Canonical state changed'):
            runtime.publish('huggingface', str(root))
    assert _publication_bytes(root) == previous
    assert set((root / 'versions').iterdir()) == {version}

    write = consumer_base._write_manifest

    def lost_acknowledgement(destination: Path, value: dict[str, object]) -> None:
        write(destination, value)
        raise OSError('Injected lost manifest acknowledgement')

    with monkeypatch.context() as patch:
        patch.setattr(consumer_base, '_write_manifest', lost_acknowledgement)
        with pytest.raises(OSError, match='lost manifest acknowledgement'):
            runtime.publish('huggingface', str(root))
    current = json.loads((root / 'latest.json').read_text())
    current_version = root / 'versions' / current['version']
    assert current_version != version and not version.exists()
    assert set((root / 'versions').iterdir()) == {current_version}
    assert publication.publication_current(
        runtime.spec, 'huggingface', runtime.store.snapshot(canonical_only=True).token,
        root=root.parent.parent,
    )


def test_retention_refuses_unproved_visibility_and_preserves_unowned_paths(
    published_huggingface: tuple[SourceRuntime, Path], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources import publication_retention
    from origo.sources.contracts import SourceError
    from origo.sources.locking import source_lock

    runtime, original = published_huggingface
    for fault in (
        'missing_manifest', 'malformed_manifest', 'wrong_source', 'invalid_version',
        'missing_files', 'absolute_file', 'escaping_file', 'missing_current',
        'missing_file', 'symlink_file', 'symlink_candidate',
    ):
        root = tmp_path / fault / runtime.spec.key / 'huggingface'
        shutil.copytree(original, root)
        manifest_path = root / 'latest.json'
        manifest = json.loads(manifest_path.read_text())
        current = root / 'versions' / manifest['version']
        obsolete = current.with_name(manifest['state_token'] + '-' + uuid4().hex)
        shutil.copytree(current, obsolete)
        referenced = current / manifest['files'][0]['path']
        if fault == 'missing_manifest':
            manifest_path.unlink()
        elif fault == 'malformed_manifest':
            manifest_path.write_text('{deliberately invalid JSON')
        elif fault == 'wrong_source':
            manifest['source_key'] = 'wrong_source'
        elif fault == 'invalid_version':
            manifest['version'] = '../outside'
        elif fault == 'missing_files':
            manifest.pop('files')
        elif fault in ('absolute_file', 'escaping_file'):
            manifest['files'][0]['path'] = str(referenced) if fault == 'absolute_file' else '../outside'
        elif fault == 'missing_current':
            shutil.rmtree(current)
        elif fault == 'missing_file':
            referenced.unlink()
        elif fault == 'symlink_file':
            referenced.unlink()
            referenced.symlink_to(obsolete / manifest['files'][0]['path'])
        elif fault == 'symlink_candidate':
            shutil.rmtree(obsolete)
            obsolete.symlink_to(current, target_is_directory=True)
        if fault not in ('missing_manifest', 'malformed_manifest'):
            manifest_path.write_text(json.dumps(manifest))
        before = _publication_bytes(root)
        directories = set((root / 'versions').iterdir())
        with source_lock(runtime.lock_root, runtime.spec.key, 'consumer_huggingface'):
            with pytest.raises((ValueError, FileNotFoundError)):
                publication_retention.retain_latest(root)
        assert _publication_bytes(root) == before, fault
        assert set((root / 'versions').iterdir()) == directories, fault

    manifest = json.loads((original / 'latest.json').read_text())
    current = original / 'versions' / manifest['version']
    obsolete = current.with_name(manifest['state_token'] + '-' + uuid4().hex)
    unknown = current.with_name('operator-kept')
    shutil.copytree(current, obsolete)
    shutil.copytree(current, unknown)
    before = _publication_bytes(original)
    alias = tmp_path / 'aliased' / runtime.spec.key / 'huggingface'
    alias.parent.mkdir(parents=True)
    alias.symlink_to(original, target_is_directory=True)
    monkeypatch.setattr('sys.argv', [
        'publication_retention', '--root', str(original.parent.parent),
        '--lock-root', str(runtime.lock_root), '--apply',
    ])
    with source_lock(runtime.lock_root, runtime.spec.key, 'consumer_huggingface'):
        renderer = next(consumer for consumer in runtime.spec.consumers if consumer.key == 'huggingface')
        with pytest.raises(ValueError, match='symlink'):
            renderer.publish(runtime.store, runtime.store.snapshot(canonical_only=True), str(alias))
        assert _publication_bytes(original) == before
        ancestor = tmp_path / 'aliased-parent'
        ancestor.symlink_to(original.parent.parent, target_is_directory=True)
        with pytest.raises(ValueError, match='symlink'):
            publication_retention.retain_latest(ancestor / runtime.spec.key / 'huggingface')
        assert _publication_bytes(original) == before
        with pytest.raises(SourceError) as raised:
            publication_retention.main()
        assert raised.value.code == 'SOURCE_LOCK_BUSY'
        assert _publication_bytes(original) == before
        assert publication_retention.retain_latest(original, apply=False) == (obsolete,)
        assert _publication_bytes(original) == before
        assert publication_retention.retain_latest(original) == (obsolete,)
        assert publication_retention.retain_latest(original) == ()
    assert current.is_dir() and unknown.is_dir() and not obsolete.exists()
    assert _publication_bytes(current) == _publication_bytes(unknown)


def test_mount_render_defers_past_the_worker_month_cap(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.contracts import SourceError
    from origo.sources.profiles import consumer_base

    spec, client, runtime = _build_one_day(tmp_path, monkeypatch, binance_fixture_server_root_url)
    mount = tmp_path / spec.key / 'mount'
    try:
        monkeypatch.setattr(consumer_base, 'MOUNT_WORKER_MONTH_CAP', 0)
        with pytest.raises(SourceError) as raised:
            runtime.publish('mount', str(mount))
        assert raised.value.code == 'RENDER_DEFERRED'
        assert 'allow_full_history' in raised.value.safe_message
        assert not (mount / 'latest.json').exists()
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' "
            "AND error_code='RENDER_DEFERRED' AND event_type='FAILED'"
        ) == [(1,)]
    finally:
        client.disconnect()


def test_mount_allow_full_renders_past_the_cap(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.bundle import SourceRunConfig, execute_source
    from origo.sources.profiles import consumer_base

    spec, client, _runtime = _build_one_day(tmp_path, monkeypatch, binance_fixture_server_root_url)
    try:
        monkeypatch.setattr(consumer_base, 'MOUNT_WORKER_MONTH_CAP', 0)
        # Both entry points thread the flag: direct publish and the Dagster op path.
        mount = tmp_path / spec.key / 'mount'
        store = SourceStore(client, 'origo', spec)
        runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
        runtime.publish('mount', str(mount), allow_full=True)
        assert list(json.loads((mount / 'latest.json').read_text())['month_tokens']) == ['2020-01']
        via_job = tmp_path / 'jobroot' / spec.key / 'mount'
        execute_source(
            spec,
            'consumer_mount',
            SourceRunConfig(destination=str(via_job), allow_full_history=True),
            run_id=f'test:{uuid4()}',
        )
        assert (via_job / 'latest.json').is_file()
    finally:
        client.disconnect()


def test_prune_bounds_cover_the_month_with_margin() -> None:
    from origo.sources.profiles.consumer_base import PRUNE_MARGIN_DAYS, _prune_bounds

    assert _prune_bounds(2020, 1) == ('2019-12-25', '2020-02-08')
    assert _prune_bounds(2020, 2) == ('2020-01-25', '2020-03-08')
    assert _prune_bounds(2020, 12) == ('2020-11-24', '2021-01-08')
    assert PRUNE_MARGIN_DAYS == 7


def test_every_consumer_renderer_accepts_allow_full() -> None:
    import inspect

    from origo.sources.profiles import (
        perp_agg_consumers,
        perp_consumers,
        spot_agg_consumers,
        spot_consumers,
    )

    # SourceRuntime.publish always passes allow_full=, so every wired renderer
    # must accept it or its consumer TypeErrors on every run.
    specs = (
        *perp_agg_consumers.PERP_AGG_CONSUMERS,
        *perp_consumers.PERP_CONSUMERS,
        *spot_agg_consumers.SPOT_AGG_CONSUMERS,
        *spot_consumers.SPOT_CONSUMERS,
    )
    assert len(specs) == 8
    for spec in specs:
        assert 'allow_full' in inspect.signature(spec.publish).parameters, spec.key
    # The CANARY template for perp-agg's future live huggingface consumer is
    # unwired today, but it must satisfy the protocol it will be published
    # through after promotion.
    assert 'allow_full' in inspect.signature(perp_agg_consumers._huggingface).parameters


def test_worker_publication_does_not_wait_for_consumer_lock(
    origo_test_env: dict[str, str], tmp_path: Path,
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from origo.sources.contracts import SourceError
    from origo.sources.locking import source_lock

    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.LIVE)
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'worker:provisional:lock-test',
    )
    try:
        runtime.setup(anchor=datetime(2020, 1, 1, tzinfo=UTC))
        with ThreadPoolExecutor(max_workers=1) as pool:
            with source_lock(runtime.lock_root, spec.key, 'consumer_mount'):
                attempt = pool.submit(runtime.publish, 'mount', str(tmp_path / 'mount'))
                with pytest.raises(SourceError) as raised:
                    attempt.result(timeout=2)
                assert raised.value.code == 'SOURCE_LOCK_BUSY'
    finally:
        client.disconnect()
