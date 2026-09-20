from __future__ import annotations

import importlib.util
import py_compile
import sys
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

KEY: Final[str] = 'binance_wib_trades'


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        'promote_source', REPO_ROOT / 'tools' / 'promote_source.py'
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules['promote_source'] = module
    spec.loader.exec_module(module)
    return module


CANARY_SPEC: Final[str] = """\
from .contracts import RevisionedSourceSpec, RolloutStage

BINANCE_WIB_TRADES_SPEC = RevisionedSourceSpec(
    key='binance_wib_trades',
    rollout_stage=RolloutStage.CANARY,
)
"""

LIVE_SPEC: Final[str] = """\
from .contracts import RevisionedSourceSpec, RolloutStage

BINANCE_WIB_TRADES_SPEC = RevisionedSourceSpec(
    key='binance_wib_trades',
    rollout_stage=RolloutStage.LIVE,
)
"""

CANARY_CONSUMERS: Final[str] = '''\
"""Wib publications.

Declaration over consumer_base: the twelve-series dataset map, the
wib-scoped series specs, and the CANARY consumer tuple (local mount plus
upload-less shadow). Series paths carry the wib prefix so the aggregate
mirror never collides with the trades sources'.
"""

from .consumer_base import ConsumerDeclaration
from .consumer_base import huggingface as _render_huggingface
from .consumer_base import huggingface_shadow as _render_shadow
from .consumer_base import mount as _render_mount


def _huggingface(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    upload: bool = True,
    kind: str = 'huggingface',
) -> None:
    # Snapshot callables resolve through their modules at call time (patch seams).
    _render_huggingface(
        reader,
        snapshot,
        destination,
        decl=_DECL,
        hf_api=HfApi,
        time_klines=agg_snapshot.get_wib_klines_from_1m_projection,
        dollar_klines=agg_snapshot.get_wib_dollar_klines,
        time_card=agg_snapshot.build_time_dataset_card,
        dollar_card=agg_snapshot.build_dollar_dataset_card,
        upload=upload,
        kind=kind,
    )


def _huggingface_shadow(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    allow_full: bool = False,
) -> None:
    """Render the snapshot files locally without uploading; the CANARY shadow publication."""
    _ = allow_full  # Snapshot renders always run whole; no worker cap applies.
    _render_shadow(
        reader,
        snapshot,
        destination,
        decl=_DECL,
        hf_api=HfApi,
        time_klines=agg_snapshot.get_wib_klines_from_1m_projection,
        dollar_klines=agg_snapshot.get_wib_dollar_klines,
        time_card=agg_snapshot.build_time_dataset_card,
        dollar_card=agg_snapshot.build_dollar_dataset_card,
    )


Renderer = Callable[[SnapshotReader, Snapshot, str], None]

WIB_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount)),
    ConsumerSpec('huggingface_shadow', cast(Renderer, _huggingface_shadow), canonical_only=True),
)
'''

LIVE_CONSUMERS: Final[str] = '''\
"""Wib publications.

Declaration over consumer_base: the twelve-series dataset map, the
wib-scoped series specs, and the LIVE consumer tuple (local mount plus
uploading huggingface). Series paths carry the wib prefix so the aggregate
mirror never collides with the trades sources'.
"""

from .consumer_base import ConsumerDeclaration
from .consumer_base import huggingface as _render_huggingface
from .consumer_base import mount as _render_mount


def _huggingface(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    allow_full: bool = False,
) -> None:
    _ = allow_full  # Snapshot renders always run whole; no worker cap applies.
    # Snapshot callables resolve through their modules at call time (patch seams).
    _render_huggingface(
        reader,
        snapshot,
        destination,
        decl=_DECL,
        hf_api=HfApi,
        time_klines=agg_snapshot.get_wib_klines_from_1m_projection,
        dollar_klines=agg_snapshot.get_wib_dollar_klines,
        time_card=agg_snapshot.build_time_dataset_card,
        dollar_card=agg_snapshot.build_dollar_dataset_card,
    )


Renderer = Callable[[SnapshotReader, Snapshot, str], None]

WIB_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount), public=True),
    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),
)
'''

CANARY_BASELINE: Final[str] = """\
def test_wib_source_identity_contract() -> None:
    spec = BINANCE_WIB_TRADES_SPEC
    assert (spec.key, spec.rollout_stage) == (
        'binance_wib_trades',
        RolloutStage.CANARY,
    )
    assert [(consumer.key, consumer.public) for consumer in spec.consumers] == [
        ('mount', False),
        ('huggingface_shadow', False),
    ]
"""

LIVE_BASELINE: Final[str] = """\
def test_wib_source_identity_contract() -> None:
    spec = BINANCE_WIB_TRADES_SPEC
    assert (spec.key, spec.rollout_stage) == (
        'binance_wib_trades',
        RolloutStage.LIVE,
    )
    assert [(consumer.key, consumer.public) for consumer in spec.consumers] == [
        ('mount', True),
        ('huggingface', True),
    ]
"""

CANARY_SENSORS: Final[str] = """\
def test_existing_publish_sensors_are_unchanged() -> None:
    existing = {
        'binance_wib_trades_huggingface_shadow_sensor',
    }
"""

LIVE_SENSORS: Final[str] = """\
def test_existing_publish_sensors_are_unchanged() -> None:
    existing = {
        'binance_wib_trades_huggingface_sensor',
    }
"""

CANARY_CONTRACT: Final[str] = """\
def test_wib_trades_is_registered_canary_with_its_bundle() -> None:
    assert BINANCE_WIB_TRADES_SPEC.rollout_stage == RolloutStage.CANARY
    names = {'x'}
    assert 'binance_wib_trades_mount_sensor' not in names
    assert 'binance_wib_trades_huggingface_sensor' not in names
    assert 'binance_wib_trades_huggingface_shadow_sensor' in names
"""

LIVE_CONTRACT: Final[str] = """\
def test_wib_trades_is_registered_live_with_its_bundle() -> None:
    assert BINANCE_WIB_TRADES_SPEC.rollout_stage == RolloutStage.LIVE
    names = {'x'}
    assert 'binance_wib_trades_mount_sensor' not in names
    assert 'binance_wib_trades_huggingface_sensor' in names
    assert 'binance_wib_trades_huggingface_shadow_sensor' not in names
"""

CANARY_BACKFILL: Final[str] = """\
def test_one_job() -> None:
    assert {consumer.key for consumer in store.spec.consumers} == {'mount', 'huggingface_shadow'}
    # The CANARY shadow consumer renders locally and never uploads.
    assert FakeHfApi.calls == []
    shadow = json.loads(
        (tmp_path / 'files' / store.spec.key / 'huggingface_shadow' / 'latest.json').read_text()
    )
    assert shadow['kind'] == 'huggingface_shadow' and shadow['uploads'] == []


def test_wib_file_uses_shadow_path() -> None:
    assert not (tmp_path / 'files' / spec.key / 'huggingface_shadow' / 'latest.json').exists()
    assert [consumer.key for consumer in canonical_only] == ['huggingface_shadow']
    assert set(sensors) == {'huggingface_shadow'}


def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:
    assert FakeHfApi.calls == []
    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)
    assert manifest['uploads'] == []


@pytest.mark.slow
def test_wib_mount_renders() -> None:
    pass
"""

LIVE_BACKFILL: Final[str] = """\
def test_one_job() -> None:
    assert {consumer.key for consumer in store.spec.consumers} == {'mount', 'huggingface'}
    # The LIVE huggingface consumer ran and rendered: the pre-cutoff fixture
    # day yields an empty manifest, so it correctly records no uploads. The
    # positive path is pinned by
    # test_wib_huggingface_upload_records_every_rendered_series.
    assert FakeHfApi.calls == []
    live = json.loads(
        (tmp_path / 'files' / store.spec.key / 'huggingface' / 'latest.json').read_text()
    )
    assert live['kind'] == 'huggingface' and live['uploads'] == []


def test_wib_file_uses_shadow_path() -> None:
    assert not (tmp_path / 'files' / spec.key / 'huggingface' / 'latest.json').exists()
    assert [consumer.key for consumer in canonical_only] == ['huggingface']
    assert set(sensors) == {'huggingface'}


@pytest.mark.slow
def test_wib_mount_renders() -> None:
    pass
"""

CANARY_TREE: Final[dict[str, str]] = {
    f'origo/sources/{KEY}.py': CANARY_SPEC,
    'origo/sources/profiles/wib_consumers.py': CANARY_CONSUMERS,
    'tests/origo_source_native/test_binance_source_baseline.py': CANARY_BASELINE,
    'tests/origo_source_native/test_publish_sensor_run_keys.py': CANARY_SENSORS,
    'tests/origo_source_native/test_revisioned_source_framework_contract.py': CANARY_CONTRACT,
    'tests/origo_source_native/test_wib_backfill_job.py': CANARY_BACKFILL,
}

LIVE_TREE: Final[dict[str, str]] = {
    f'origo/sources/{KEY}.py': LIVE_SPEC,
    'origo/sources/profiles/wib_consumers.py': LIVE_CONSUMERS,
    'tests/origo_source_native/test_binance_source_baseline.py': LIVE_BASELINE,
    'tests/origo_source_native/test_publish_sensor_run_keys.py': LIVE_SENSORS,
    'tests/origo_source_native/test_revisioned_source_framework_contract.py': LIVE_CONTRACT,
    'tests/origo_source_native/test_wib_backfill_job.py': LIVE_BACKFILL,
}


def _scratch(tree: dict[str, str], tmp_path: Path) -> Path:
    scratch = tmp_path / 'repo'
    for rel, content in tree.items():
        destination = scratch / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding='utf-8')
    return scratch


def test_naming_derives_every_promotion_path() -> None:
    module = _load()
    naming = module.naming_for('binance_perp_aggtrades')
    assert (naming.stem, naming.prefix) == ('perp_aggtrades', 'perp_agg')
    assert naming.const == 'BINANCE_PERP_AGGTRADES_SPEC'
    assert naming.consumers == 'PERP_AGG_CONSUMERS'
    with pytest.raises(ValueError, match='Not a revisioned Binance source key'):
        module.naming_for('binance_spot_trades_x')
    with pytest.raises(ValueError, match='Not a revisioned Binance source key'):
        module.naming_for('spot')


def test_promote_rejects_live_and_unknown_sources(tmp_path: Path) -> None:
    module = _load()
    live = _scratch({f'origo/sources/{KEY}.py': LIVE_SPEC}, tmp_path / 'live')
    with pytest.raises(ValueError, match='not a promotable canary'):
        module.promote(live, module.naming_for(KEY))
    with pytest.raises(ValueError, match='Promotion file is missing'):
        module.promote(tmp_path / 'empty', module.naming_for(KEY))


def test_promote_produces_the_live_shape_byte_exact(tmp_path: Path) -> None:
    module = _load()
    naming = module.naming_for(KEY)
    scratch = _scratch(CANARY_TREE, tmp_path)
    results = module.promote(scratch, naming)
    assert set(results) == set(LIVE_TREE)
    for rel, (_before, after) in results.items():
        assert after == LIVE_TREE[rel]
        promoted = scratch / rel
        promoted.write_text(after, encoding='utf-8')
        py_compile.compile(str(promoted), doraise=True)


def test_promote_spans_the_older_canary_generation(tmp_path: Path) -> None:
    """The S395 generation (two-line sensors, plain def, no shadow test) converges alike."""
    module = _load()
    naming = module.naming_for(KEY)
    old_contract = CANARY_CONTRACT.replace(
        "    assert 'binance_wib_trades_huggingface_sensor' not in names\n", ''
    )
    old_consumers = CANARY_CONSUMERS.replace(
        'def _huggingface(\n'
        '    reader: SnapshotReader,\n'
        '    snapshot: Snapshot,\n'
        '    destination: str,\n'
        '    *,\n'
        '    upload: bool = True,\n'
        "    kind: str = 'huggingface',\n"
        ') -> None:\n',
        'def _huggingface(\n'
        '    reader: SnapshotReader,\n'
        '    snapshot: Snapshot,\n'
        '    destination: str,\n'
        '    *,\n'
        '    allow_full: bool = False,\n'
        ') -> None:\n'
        '    _ = allow_full  # Snapshot renders always run whole; no worker cap applies.\n',
    ).replace('        upload=upload,\n        kind=kind,\n', '')
    old_backfill = CANARY_BACKFILL.replace(
        '\n\ndef test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        "    assert manifest['uploads'] == []\n",
        '',
    )
    tree = dict(CANARY_TREE)
    tree['tests/origo_source_native/test_revisioned_source_framework_contract.py'] = old_contract
    tree['origo/sources/profiles/wib_consumers.py'] = old_consumers
    tree['tests/origo_source_native/test_wib_backfill_job.py'] = old_backfill
    results = module.promote(_scratch(tree, tmp_path), naming)
    assert {rel: after for rel, (_before, after) in results.items()} == LIVE_TREE


def test_delete_shadow_test_is_a_noop_without_the_marker() -> None:
    module = _load()
    text = 'def test_other() -> None:\n    pass\n'
    assert module.delete_shadow_test(text, 'wib') == (text, False)


def test_delete_shadow_test_keeps_two_blank_lines_before_a_class() -> None:
    module = _load()
    text = (
        'def test_before() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        '\n'
        '\n'
        'class TestNext:\n'
        '    pass\n'
    )
    updated, deleted = module.delete_shadow_test(text, 'wib')
    assert deleted is True
    assert updated == (
        'def test_before() -> None:\n' '    pass\n' '\n' '\n' 'class TestNext:\n' '    pass\n'
    )


def test_delete_shadow_test_ends_the_file_with_one_newline() -> None:
    module = _load()
    text = (
        'def test_before() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
    )
    updated, deleted = module.delete_shadow_test(text, 'wib')
    assert deleted is True
    assert updated == 'def test_before() -> None:\n    pass\n'


def test_delete_shadow_test_skips_a_multiline_signature() -> None:
    module = _load()
    text = (
        'def test_before() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_wib_huggingface_shadow_renders_locally_without_uploading(\n'
        '    tmp_path: Path,\n'
        ') -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    updated, deleted = module.delete_shadow_test(text, 'wib')
    assert deleted is True
    assert updated == (
        'def test_before() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )


def test_delete_shadow_test_refuses_a_foreign_span() -> None:
    module = _load()
    text = (
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    with pytest.raises(ValueError, match='expected shadow assertions'):
        module.delete_shadow_test(text, 'wib')


def test_delete_shadow_test_refuses_an_indented_marker() -> None:
    module = _load()
    text = (
        'class TestGroup:\n'
        '    def test_wib_huggingface_shadow_renders_locally_without_uploading(self) -> None:\n'
        '        assert FakeHfApi.calls == []\n'
        '        wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
    )
    with pytest.raises(ValueError, match='not a top-level def'):
        module.delete_shadow_test(text, 'wib')


def test_delete_shadow_test_refuses_a_decorated_shadow() -> None:
    module = _load()
    text = (
        '@pytest.mark.slow\n'
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    with pytest.raises(ValueError, match='carries decorators'):
        module.delete_shadow_test(text, 'wib')


def test_delete_shadow_test_preserves_a_following_module_statement() -> None:
    module = _load()
    text = (
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        '\n'
        '\n'
        'MARKER = 1\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    updated, deleted = module.delete_shadow_test(text, 'wib')
    assert deleted is True
    assert updated == (
        'MARKER = 1\n' '\n' '\n' 'def test_next() -> None:\n' '    pass\n'
    )


def test_delete_shadow_test_tolerates_a_trailing_comment_on_the_def() -> None:
    module = _load()
    text = (
        'def test_before() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:  # noqa\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    updated, deleted = module.delete_shadow_test(text, 'wib')
    assert deleted is True
    assert updated == (
        'def test_before() -> None:\n'
        '    pass\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )


def test_delete_shadow_test_refuses_a_multiline_decorator() -> None:
    module = _load()
    text = (
        '@pytest.mark.parametrize(\n'
        '    "x",\n'
        '    [1, 2],\n'
        ')\n'
        'def test_wib_huggingface_shadow_renders_locally_without_uploading() -> None:\n'
        '    assert FakeHfApi.calls == []\n'
        '    wib_consumers._huggingface_shadow(store, store.snapshot(), destination)\n'
        '\n'
        '\n'
        'def test_next() -> None:\n'
        '    pass\n'
    )
    with pytest.raises(ValueError, match='carries decorators'):
        module.delete_shadow_test(text, 'wib')
