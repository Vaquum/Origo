from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from origo.sources.contracts import Snapshot
from origo.sources.hashing import state_token
from origo.sources.profiles import consumer_base, spot_consumers
from origo.sources.profiles.formulas import huggingface_dollar, huggingface_time
from origo.sources.profiles.formulas.spot_series import SPECS

from .test_market_state_registration import CubeRuntime
from .test_market_state_registration import registered_cube as registered_cube
from .test_revisioned_source_framework_consumers import FakeHfApi

_HISTORY = '2024-12-31'
_MINUTE = '2025-01-01T00:00:00Z'
_NEXT_MINUTE = '2025-01-01T00:01:00Z'
Manifest = dict[str, object]


@pytest.fixture
def publication_cube(
    registered_cube: CubeRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[CubeRuntime]:
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    FakeHfApi.calls = []
    monkeypatch.setattr(spot_consumers, 'HfApi', FakeHfApi)
    yield registered_cube


def _root(cube: CubeRuntime, kind: str) -> Path:
    return cube.runtime.lock_root.parent / cube.runtime.spec.key / kind


def _manifest(root: Path) -> Manifest:
    value: object = json.loads((root / 'latest.json').read_text())
    assert isinstance(value, dict)
    return cast(Manifest, value)


def _entries(manifest: Manifest) -> list[Manifest]:
    return cast(list[Manifest], manifest['files'])


def _physical_files(root: Path, manifest: Manifest) -> dict[str, tuple[str, int, int, str]]:
    result: dict[str, tuple[str, int, int, str]] = {}
    for entry in _entries(manifest):
        path = Path(str(entry['path']))
        if not path.is_absolute():
            path = root / 'versions' / str(manifest['version']) / path
        metadata = path.stat()
        result[str(path)] = (
            str(path.resolve()), metadata.st_ino, metadata.st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    assert result
    return result


def _legacy_manifest(root: Path) -> Manifest:
    manifest = _manifest(root)
    manifest.pop('input_token', None)
    manifest.pop('month_input_tokens', None)
    (root / 'latest.json').write_text(json.dumps(manifest))
    return manifest


def _forbid_render(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError('Unconsumed cube activation must not render files or construct an uploader.')

    for name in ('_month_frame', 'build_series_frame', 'stage_series'):
        monkeypatch.setattr(consumer_base, name, forbidden)
    monkeypatch.setattr(spot_consumers, 'HfApi', forbidden)
    monkeypatch.setattr(huggingface_time, 'get_binance_spot_klines_from_1m_projection', forbidden)
    monkeypatch.setattr(huggingface_dollar, 'get_binance_spot_dollar_klines', forbidden)


@pytest.mark.parametrize('legacy', [False, True])
def test_cube_upgrade_republishes_metadata_without_rewriting_products(
    publication_cube: CubeRuntime, monkeypatch: pytest.MonkeyPatch, legacy: bool
) -> None:
    cube = publication_cube
    runtime = cube.runtime
    before = runtime.build(_HISTORY)
    baseline: dict[str, Manifest] = {}
    files: dict[str, dict[str, tuple[str, int, int, str]]] = {}
    for kind in ('mount', 'huggingface'):
        root = _root(cube, kind)
        runtime.publish(kind, str(root))
        baseline[kind] = _legacy_manifest(root) if legacy else _manifest(root)
        files[kind] = _physical_files(root, baseline[kind])
    old_arrow = {
        series.name: (runtime.lock_root.parent / 'arrow' / series.name / 'latest.arrow').resolve()
        for series in SPECS
    }
    uploads = list(FakeHfApi.calls)
    assert uploads
    runtime.enable_components('market_state')
    upgraded = runtime.upgrade_components(_HISTORY)
    if legacy:
        # A real retained-generation selection puts the last legacy proof two generations back.
        runtime.rollback(upgraded, operator='test', reason='Verify historical base-only activation lookup')
        current = runtime.store.records(canonical_only=True)[0]
        assert current.generation == before.generation + 2
        assert runtime.store.legacy_equivalent_snapshot(runtime.store.snapshot()).records == (before,)
    current_snapshot = runtime.store.snapshot()
    assert current_snapshot.token != baseline['mount']['pinned_token']
    with monkeypatch.context() as patch:
        _forbid_render(patch)
        for kind in ('mount', 'huggingface'):
            runtime.publish(kind, str(_root(cube, kind)))
    for kind in ('mount', 'huggingface'):
        updated = _manifest(_root(cube, kind))
        assert updated['state_token'] == updated['pinned_token'] == current_snapshot.token
        assert updated['files'] == baseline[kind]['files']
        assert _physical_files(_root(cube, kind), updated) == files[kind]
        assert isinstance(updated['input_token'], str)
        if not legacy:
            assert updated['input_token'] == baseline[kind]['input_token']
        if kind == 'huggingface':
            assert updated['version'] == baseline[kind]['version']
            assert updated['uploads'] == baseline[kind]['uploads']
            assert len(list((_root(cube, kind) / 'versions').iterdir())) == 1
    assert FakeHfApi.calls == uploads
    assert {
        series.name: (runtime.lock_root.parent / 'arrow' / series.name / 'latest.arrow').resolve()
        for series in SPECS
    } == old_arrow
    assert _manifest(_root(cube, 'mount'))['month_tokens'] != baseline['mount']['month_tokens']


@pytest.mark.parametrize('legacy', [False, True])
def test_live_month_changes_while_cube_history_reuses_untouched_month(
    publication_cube: CubeRuntime, monkeypatch: pytest.MonkeyPatch, legacy: bool
) -> None:
    cube = publication_cube
    runtime = cube.runtime
    runtime.build(_HISTORY)
    runtime.build(_MINUTE, provisional=True)
    root = _root(cube, 'mount')
    runtime.publish('mount', str(root))
    before = _legacy_manifest(root) if legacy else _manifest(root)
    original_files = _physical_files(root, before)
    history_paths = {str(entry['path']) for entry in _entries(before) if entry.get('month') == '2024-12'}
    live_paths = {str(entry['path']) for entry in _entries(before) if entry.get('month') == '2025-01'}
    assert len(history_paths) == len(live_paths) == len(SPECS)
    runtime.enable_components('market_state')
    runtime.upgrade_components(_HISTORY)
    runtime.build(_NEXT_MINUTE, provisional=True)
    rendered: list[str] = []
    month_frame = getattr(consumer_base, '_month_frame')

    def counted(*args: object, **kwargs: object) -> pl.DataFrame:
        month = f'{args[1]:04d}-{args[2]:02d}'
        assert month != '2024-12', 'Cube-only history must reuse the historical month.'
        rendered.append(month)
        return month_frame(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(consumer_base, '_month_frame', counted)
        runtime.publish('mount', str(root))
    assert rendered == ['2025-01'] * len(SPECS)
    after = _manifest(root)
    new_files = _physical_files(root, after)
    assert {path: new_files[path] for path in history_paths} == {
        path: original_files[path] for path in history_paths
    }
    assert all(new_files[path][2] != original_files[path][2] for path in live_paths)
    assert after['pinned_token'] == runtime.store.snapshot().token
    assert after['state_token'] == runtime.store.snapshot(canonical_only=True).token


@pytest.mark.parametrize('change', ['cube_and_live', 'canonical_input'])
def test_publication_verifies_canonical_inputs_and_preserves_original_live_pin(
    publication_cube: CubeRuntime, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    cube = publication_cube
    runtime = cube.runtime
    runtime.build(_HISTORY)
    root = _root(cube, 'mount')
    runtime.publish('mount', str(root))
    old_manifest = (root / 'latest.json').read_bytes()
    before_files = _physical_files(root, _manifest(root))
    minute = runtime.build(_MINUTE, provisional=True)
    pinned = runtime.store.snapshot()
    runtime.enable_components('market_state')
    read_snapshot = runtime.store.snapshot
    advanced = False

    def concurrent_change(*, canonical_only: bool = False) -> Snapshot:
        nonlocal advanced
        if canonical_only and not advanced:
            advanced = True
            if change == 'cube_and_live':
                runtime.upgrade_components(_HISTORY)
                runtime.build(_NEXT_MINUTE, provisional=True)
            else:
                runtime.build('2025-01-01')
        return read_snapshot(canonical_only=canonical_only)

    renderer = next(item for item in runtime.spec.consumers if item.key == 'mount')
    with monkeypatch.context() as patch:
        patch.setattr(runtime.store, 'snapshot', concurrent_change)
        if change == 'canonical_input':
            with pytest.raises(RuntimeError, match='state changed'):
                renderer.publish(runtime.store, pinned, str(root))
        else:
            renderer.publish(runtime.store, pinned, str(root))
    assert advanced
    if change == 'canonical_input':
        assert (root / 'latest.json').read_bytes() == old_manifest
        assert _physical_files(root, _manifest(root)) == before_files
        assert not list((runtime.lock_root.parent / 'parquet').glob('.staging-*'))
        return
    current_canonical = read_snapshot(canonical_only=True)
    expected = Snapshot(
        state_token(runtime.spec.key, (*current_canonical.records, minute)),
        (*current_canonical.records, minute),
    )
    manifest = _manifest(root)
    assert manifest['state_token'] == current_canonical.token
    assert manifest['pinned_token'] == expected.token != read_snapshot().token
    assert manifest['active_through'] == minute.partition.end.isoformat()
    current_live_file = runtime.lock_root.parent / 'parquet/time/1m/2025/01.parquet'
    frame = pl.read_parquet(current_live_file)
    assert frame.height == 1
    assert frame['datetime'].max() == datetime(2025, 1, 1, tzinfo=UTC)


def test_absent_legacy_activation_proof_requires_normal_render(
    publication_cube: CubeRuntime,
) -> None:
    cube = publication_cube
    runtime = cube.runtime
    runtime.enable_components('market_state')
    record = runtime.build(_HISTORY)
    previous: dict[str, Manifest] = {}
    physical: dict[str, dict[str, tuple[str, int, int, str]]] = {}
    for kind in ('mount', 'huggingface'):
        root = _root(cube, kind)
        runtime.publish(kind, str(root))
        previous[kind] = _legacy_manifest(root)
        physical[kind] = _physical_files(root, previous[kind])
    uploads = len(FakeHfApi.calls)
    runtime.rollback(record, operator='test', reason='Select retained build with no legacy activation')
    current = runtime.store.snapshot()
    assert runtime.store.legacy_equivalent_snapshot(current) == current
    for kind in ('mount', 'huggingface'):
        root = _root(cube, kind)
        runtime.publish(kind, str(root))
        after = _manifest(root)
        assert after['state_token'] == current.token
        assert _physical_files(root, after) != physical[kind]
    assert _manifest(_root(cube, 'huggingface'))['version'] != previous['huggingface']['version']
    assert len(FakeHfApi.calls) == uploads * 2
