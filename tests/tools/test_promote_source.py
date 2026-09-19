from __future__ import annotations

import importlib.util
import py_compile
import shutil
import sys
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

KEY: Final[str] = 'binance_perp_aggtrades'


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        'promote_source', REPO_ROOT / 'tools' / 'promote_source.py'
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules['promote_source'] = module
    spec.loader.exec_module(module)
    return module


def _scratch_repo(tmp_path: Path, module: ModuleType, key: str) -> Path:
    naming = module.naming_for(key)
    paths = {transform.path for transform in module.transforms(naming)}
    paths.add(f'tests/origo_source_native/test_{naming.prefix}_backfill_job.py')
    scratch = tmp_path / 'repo'
    for rel in paths:
        destination = scratch / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / rel, destination)
    return scratch


def test_naming_derives_every_promotion_path() -> None:
    naming = _load().naming_for('binance_perp_aggtrades')
    assert (naming.stem, naming.prefix) == ('perp_aggtrades', 'perp_agg')
    assert naming.const == 'BINANCE_PERP_AGGTRADES_SPEC'
    assert naming.consumers == 'PERP_AGG_CONSUMERS'
    with pytest.raises(ValueError, match='Not a revisioned Binance source key'):
        _load().naming_for('binance_spot_trades_x')
    with pytest.raises(ValueError, match='Not a revisioned Binance source key'):
        _load().naming_for('spot')


def test_promote_rejects_live_and_unknown_sources(tmp_path: Path) -> None:
    module = _load()
    live = tmp_path / 'live'
    spec_rel = 'origo/sources/binance_spot_aggtrades.py'
    (live / spec_rel).parent.mkdir(parents=True)
    shutil.copyfile(REPO_ROOT / spec_rel, live / spec_rel)
    with pytest.raises(ValueError, match='not a promotable canary'):
        module.promote(live, module.naming_for('binance_spot_aggtrades'))
    with pytest.raises(ValueError, match='Promotion file is missing'):
        module.promote(tmp_path / 'empty', module.naming_for('binance_perp_aggtrades'))


def test_promote_perp_agg_produces_the_live_shape(tmp_path: Path) -> None:
    module = _load()
    naming = module.naming_for(KEY)
    scratch = _scratch_repo(tmp_path, module, KEY)
    results = module.promote(scratch, naming)
    assert set(results) == {
        f'origo/sources/{KEY}.py',
        'origo/sources/profiles/perp_agg_consumers.py',
        'tests/origo_source_native/test_binance_source_baseline.py',
        'tests/origo_source_native/test_publish_sensor_run_keys.py',
        'tests/origo_source_native/test_revisioned_source_framework_contract.py',
        'tests/origo_source_native/test_perp_agg_backfill_job.py',
    }
    texts = {rel: after for rel, (_before, after) in results.items()}

    spec = texts[f'origo/sources/{KEY}.py']
    assert 'rollout_stage=RolloutStage.LIVE' in spec and 'CANARY' not in spec

    consumers = texts['origo/sources/profiles/perp_agg_consumers.py']
    assert 'shadow' not in consumers.lower()
    assert consumers.count('public=True') == 2
    assert 'upload=upload' not in consumers and 'kind=kind' not in consumers
    assert 'huggingface_shadow' not in consumers

    baseline = texts['tests/origo_source_native/test_binance_source_baseline.py']
    assert 'RolloutStage.CANARY' not in baseline
    assert "('mount', True),\n        ('huggingface', True)," in baseline

    sensors = texts['tests/origo_source_native/test_publish_sensor_run_keys.py']
    assert f"'{KEY}_huggingface_sensor'," in sensors
    assert 'shadow' not in sensors

    contract = texts['tests/origo_source_native/test_revisioned_source_framework_contract.py']
    assert 'def test_perp_aggtrades_is_registered_live_with_its_bundle' in contract
    assert 'def test_perp_aggtrades_is_registered_canary_with_its_bundle' not in contract
    assert 'BINANCE_PERP_AGGTRADES_SPEC.rollout_stage == RolloutStage.LIVE' in contract
    assert 'BINANCE_PERP_AGGTRADES_SPEC.rollout_stage == RolloutStage.CANARY' not in contract

    backfill = texts['tests/origo_source_native/test_perp_agg_backfill_job.py']
    assert 'huggingface_shadow' not in backfill
    assert 'def test_perp_agg_huggingface_shadow_renders_locally_without_uploading(' not in backfill
    assert 'def test_perp_agg_huggingface_upload_records_every_rendered_series(' in backfill
    assert '# The LIVE huggingface consumer ran and rendered' in backfill
    assert "== {'mount', 'huggingface'}" in backfill

    for rel in results:
        promoted = scratch / rel
        promoted.write_text(texts[rel], encoding='utf-8')
        py_compile.compile(str(promoted), doraise=True)


def test_delete_shadow_test_is_a_noop_without_the_marker() -> None:
    module = _load()
    text = 'def test_other() -> None:\n    pass\n'
    assert module.delete_shadow_test(text, 'perp_agg') == (text, False)


def test_delete_shadow_test_refuses_a_foreign_span() -> None:
    module = _load()
    text = (
        'def test_perp_agg_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    with pytest.raises(ValueError, match='expected shadow assertions'):
        module.delete_shadow_test(text, 'perp_agg')
