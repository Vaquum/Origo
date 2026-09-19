#!/usr/bin/env python3
"""Promote a CANARY revisioned source to LIVE (PRD-0015 row 8 tooling).

Applies the mechanical S395 transform set to the six promotion-touched files
of ``--source``: the spec stage flip, the shadow retirement in the consumers
module, and the four test-file mirrors (baseline identity, sensor run keys,
framework contract, backfill job). Every transform is an exact-snippet
replacement that must match exactly once; anything else fails loud with the
file and snippet named.

Three conditional transforms span both canary generations: the ``upload``/``kind``
parameter strip is a no-op when the canary already declares the plain
``_huggingface`` shape, the contract sensor block accepts the two-line (S395)
or three-line (current) canary shape, and the dedicated shadow-test deletion
is a no-op when the canary predates that test. Everything else about a
promotion is exact.

Version and CHANGELOG stay human: the trail records the promotion's evidence
(history range verified, open failures at zero), which no tool can write.

Usage:

  python tools/promote_source.py --source binance_perp_aggtrades        # dry run: diff only
  python tools/promote_source.py --source binance_perp_aggtrades --apply # write the files

Exit codes:
  0 -- every transform applied (or would apply)
  1 -- a precondition or transform failed
  2 -- bad arguments
"""

from __future__ import annotations

import argparse
import ast
import difflib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final


@dataclass(frozen=True)
class Naming:
    key: str  # binance_perp_aggtrades
    stem: str  # perp_aggtrades (test/function stems)
    prefix: str  # perp_agg (profile/series/test-file prefix)
    const: str  # BINANCE_PERP_AGGTRADES_SPEC
    consumers: str  # PERP_AGG_CONSUMERS


def naming_for(source_key: str) -> Naming:
    if not source_key.startswith('binance_') or not source_key.endswith('trades'):
        raise ValueError(f'Not a revisioned Binance source key: {source_key!r}.')
    stem = source_key.removeprefix('binance_')
    prefix = stem.removesuffix('trades').rstrip('_')
    if not prefix or not stem:
        raise ValueError(f'Not a revisioned Binance source key: {source_key!r}.')
    return Naming(source_key, stem, prefix, source_key.upper() + '_SPEC', prefix.upper() + '_CONSUMERS')


@dataclass(frozen=True)
class Transform:
    path: str
    old: str
    new: str
    group: str = ''


def transforms(naming: Naming) -> tuple[Transform, ...]:
    key, stem, prefix, const = naming.key, naming.stem, naming.prefix, naming.const
    consumers_tuple = naming.consumers
    spec = f'origo/sources/{key}.py'
    consumers = f'origo/sources/profiles/{prefix}_consumers.py'
    baseline = 'tests/origo_source_native/test_binance_source_baseline.py'
    sensors = 'tests/origo_source_native/test_publish_sensor_run_keys.py'
    contract = 'tests/origo_source_native/test_revisioned_source_framework_contract.py'
    backfill = f'tests/origo_source_native/test_{prefix}_backfill_job.py'
    return (
        Transform(spec, '    rollout_stage=RolloutStage.CANARY,\n', '    rollout_stage=RolloutStage.LIVE,\n'),
        Transform(
            consumers,
            f'{prefix}-scoped series specs, and the CANARY consumer tuple (local mount plus\n'
            f'upload-less shadow). Series paths carry the {prefix} prefix so the aggregate\n',
            f'{prefix}-scoped series specs, and the LIVE consumer tuple (local mount plus\n'
            f'uploading huggingface). Series paths carry the {prefix} prefix so the aggregate\n',
        ),
        Transform(
            consumers,
            'from .consumer_base import huggingface_shadow as _render_shadow\n',
            '',
        ),
        Transform(
            consumers,
            'def _huggingface(\n'
            '    reader: SnapshotReader,\n'
            '    snapshot: Snapshot,\n'
            '    destination: str,\n'
            '    *,\n'
            "    upload: bool = True,\n"
            "    kind: str = 'huggingface',\n"
            ') -> None:\n'
            '    # Snapshot callables resolve through their modules at call time (patch seams).\n'
            '    _render_huggingface(\n'
            '        reader,\n'
            '        snapshot,\n'
            '        destination,\n'
            '        decl=_DECL,\n'
            '        hf_api=HfApi,\n'
            f'        time_klines=agg_snapshot.get_{prefix}_klines_from_1m_projection,\n'
            f'        dollar_klines=agg_snapshot.get_{prefix}_dollar_klines,\n'
            '        time_card=agg_snapshot.build_time_dataset_card,\n'
            '        dollar_card=agg_snapshot.build_dollar_dataset_card,\n'
            '        upload=upload,\n'
            '        kind=kind,\n'
            '    )\n',
            'def _huggingface(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:\n'
            '    # Snapshot callables resolve through their modules at call time (patch seams).\n'
            '    _render_huggingface(\n'
            '        reader,\n'
            '        snapshot,\n'
            '        destination,\n'
            '        decl=_DECL,\n'
            '        hf_api=HfApi,\n'
            f'        time_klines=agg_snapshot.get_{prefix}_klines_from_1m_projection,\n'
            f'        dollar_klines=agg_snapshot.get_{prefix}_dollar_klines,\n'
            '        time_card=agg_snapshot.build_time_dataset_card,\n'
            '        dollar_card=agg_snapshot.build_dollar_dataset_card,\n'
            '    )\n',
        ),
        Transform(
            consumers,
            'def _huggingface_shadow(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:\n'
            '    """Render the snapshot files locally without uploading; the CANARY shadow publication."""\n'
            '    _render_shadow(\n'
            '        reader,\n'
            '        snapshot,\n'
            '        destination,\n'
            '        decl=_DECL,\n'
            '        hf_api=HfApi,\n'
            f'        time_klines=agg_snapshot.get_{prefix}_klines_from_1m_projection,\n'
            f'        dollar_klines=agg_snapshot.get_{prefix}_dollar_klines,\n'
            '        time_card=agg_snapshot.build_time_dataset_card,\n'
            '        dollar_card=agg_snapshot.build_dollar_dataset_card,\n'
            '    )\n'
            '\n'
            '\n',
            '',
        ),
        Transform(
            consumers,
            f'{consumers_tuple} = (\n'
            "    ConsumerSpec('mount', cast(Renderer, _mount)),\n"
            "    ConsumerSpec('huggingface_shadow', cast(Renderer, _huggingface_shadow), canonical_only=True),\n"
            ')\n',
            f'{consumers_tuple} = (\n'
            "    ConsumerSpec('mount', cast(Renderer, _mount), public=True),\n"
            "    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),\n"
            ')\n',
        ),
        Transform(
            baseline,
            '        RolloutStage.CANARY,\n',
            '        RolloutStage.LIVE,\n',
        ),
        Transform(
            baseline,
            '    assert [(consumer.key, consumer.public) for consumer in spec.consumers] == [\n'
            "        ('mount', False),\n"
            "        ('huggingface_shadow', False),\n"
            '    ]\n',
            '    assert [(consumer.key, consumer.public) for consumer in spec.consumers] == [\n'
            "        ('mount', True),\n"
            "        ('huggingface', True),\n"
            '    ]\n',
        ),
        Transform(
            sensors,
            f"        '{key}_huggingface_shadow_sensor',\n",
            f"        '{key}_huggingface_sensor',\n",
        ),
        Transform(
            contract,
            f'def test_{stem}_is_registered_canary_with_its_bundle() -> None:\n',
            f'def test_{stem}_is_registered_live_with_its_bundle() -> None:\n',
        ),
        Transform(
            contract,
            f'    assert {const}.rollout_stage == RolloutStage.CANARY\n',
            f'    assert {const}.rollout_stage == RolloutStage.LIVE\n',
        ),
        Transform(
            contract,
            f"    assert '{key}_mount_sensor' not in names\n"
            f"    assert '{key}_huggingface_sensor' not in names\n"
            f"    assert '{key}_huggingface_shadow_sensor' in names\n",
            f"    assert '{key}_mount_sensor' not in names\n"
            f"    assert '{key}_huggingface_sensor' in names\n"
            f"    assert '{key}_huggingface_shadow_sensor' not in names\n",
            group='contract_sensors',
        ),
        Transform(
            contract,
            f"    assert '{key}_mount_sensor' not in names\n"
            f"    assert '{key}_huggingface_shadow_sensor' in names\n",
            f"    assert '{key}_mount_sensor' not in names\n"
            f"    assert '{key}_huggingface_sensor' in names\n"
            f"    assert '{key}_huggingface_shadow_sensor' not in names\n",
            group='contract_sensors',
        ),
        Transform(
            backfill,
            "assert {consumer.key for consumer in store.spec.consumers} == {'mount', 'huggingface_shadow'}",
            "assert {consumer.key for consumer in store.spec.consumers} == {'mount', 'huggingface'}",
        ),
        Transform(
            backfill,
            '    # The CANARY shadow consumer renders locally and never uploads.\n'
            '    assert FakeHfApi.calls == []\n'
            '    shadow = json.loads(\n'
            "        (tmp_path / 'files' / store.spec.key / 'huggingface_shadow' / 'latest.json').read_text()\n"
            '    )\n'
            "    assert shadow['kind'] == 'huggingface_shadow' and shadow['uploads'] == []\n",
            '    # The LIVE huggingface consumer ran and rendered: the pre-cutoff fixture\n'
            '    # day yields an empty manifest, so it correctly records no uploads. The\n'
            '    # positive path is pinned by\n'
            f'    # test_{prefix}_huggingface_upload_records_every_rendered_series.\n'
            '    assert FakeHfApi.calls == []\n'
            '    live = json.loads(\n'
            "        (tmp_path / 'files' / store.spec.key / 'huggingface' / 'latest.json').read_text()\n"
            '    )\n'
            "    assert live['kind'] == 'huggingface' and live['uploads'] == []\n",
        ),
        Transform(
            backfill,
            "assert not (tmp_path / 'files' / spec.key / 'huggingface_shadow' / 'latest.json').exists()",
            "assert not (tmp_path / 'files' / spec.key / 'huggingface' / 'latest.json').exists()",
        ),
        Transform(
            backfill,
            "assert [consumer.key for consumer in canonical_only] == ['huggingface_shadow']",
            "assert [consumer.key for consumer in canonical_only] == ['huggingface']",
        ),
        Transform(
            backfill,
            "    assert set(sensors) == {'huggingface_shadow'}",
            "    assert set(sensors) == {'huggingface'}",
        ),
    )


PLAIN_HUGGINGFACE_DEF: Final[str] = (
    'def _huggingface(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:\n'
)

def delete_shadow_test(text: str, prefix: str) -> tuple[str, bool]:
    """Delete the dedicated shadow-render test; no-op when the canary predates it.

    The span is the test's own AST node: exactly one undecorated top-level
    ``def`` located by name, so multi-line signatures, trailing comments,
    and following decorators, classes, or module statements can neither
    widen the span nor be eaten by it. A decorated, nested, or
    assertion-less shadow test fails loud instead of deleting wrong. The
    survivors rejoin with exactly two blank lines (or one trailing newline
    when the shadow test was last), matching the file's own rhythm.
    """
    name = f'test_{prefix}_huggingface_shadow_renders_locally_without_uploading'
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        raise ValueError(f'Cannot parse the backfill test file: {exc}.') from exc
    target: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            target = node
            break
    if target is None:
        if name in text:
            raise ValueError(f'Shadow test {name!r} is not a top-level def; refusing to delete.')
        return text, False
    if target.decorator_list:
        raise ValueError('Shadow test carries decorators; extend the tool instead of guessing.')
    if target.end_lineno is None:
        raise ValueError('Shadow test has no end line; refusing to delete.')
    lines = text.split('\n')
    span = '\n'.join(lines[target.lineno - 1 : target.end_lineno])
    if 'FakeHfApi.calls == []' not in span or '_huggingface_shadow(' not in span:
        raise ValueError('Shadow test span lacks the expected shadow assertions; refusing to delete.')
    head = '\n'.join(lines[: target.lineno - 1]).rstrip('\n')
    tail = '\n'.join(lines[target.end_lineno :]).lstrip('\n')
    if not tail:
        return (head + '\n' if head else ''), True
    if not head:
        return tail, True
    return head + '\n\n\n' + tail, True


def promote(repo: Path, naming: Naming) -> dict[str, tuple[str, str]]:
    """Apply every transform in memory; return path -> (before, after) for changed files."""
    results: dict[str, tuple[str, str]] = {}
    cache: dict[str, str] = {}

    def read(rel: str) -> str:
        if rel not in cache:
            path = repo / rel
            if not path.is_file():
                raise ValueError(f'Promotion file is missing: {rel}.')
            cache[rel] = path.read_text(encoding='utf-8')
        return cache[rel]

    spec_text = read(f'origo/sources/{naming.key}.py')
    if spec_text.count('rollout_stage=RolloutStage.CANARY') != 1:
        raise ValueError(
            f'{naming.key} is not a promotable canary: expected exactly one CANARY stage.'
        )
    pending = list(transforms(naming))
    consumers_rel = f'origo/sources/profiles/{naming.prefix}_consumers.py'
    params_form = next(t for t in pending if t.old.startswith('def _huggingface(\n'))
    if params_form.old not in read(consumers_rel):
        if PLAIN_HUGGINGFACE_DEF not in read(consumers_rel):
            raise ValueError(
                f'{consumers_rel} has neither the parameterized nor the plain _huggingface shape.'
            )
        pending.remove(params_form)
    def apply_exact(transform: Transform) -> None:
        text = read(transform.path)
        found = text.count(transform.old)
        if found != 1:
            raise ValueError(
                f'{transform.path}: expected exactly one promotion site, found {found}: '
                f'{transform.old[:80]!r}.'
            )
        cache[transform.path] = text.replace(transform.old, transform.new)

    grouped: dict[str, list[Transform]] = {}
    for transform in pending:
        if transform.group:
            grouped.setdefault(transform.group, []).append(transform)
        else:
            apply_exact(transform)
    for name, members in grouped.items():
        counts = [(member, read(member.path).count(member.old)) for member in members]
        winners = [member for member, found in counts if found == 1]
        if len(winners) != 1 or any(found > 1 for _, found in counts):
            raise ValueError(
                f'{members[0].path}: promotion group {name!r} must match exactly one'
                f' alternative, found {[found for _, found in counts]}.'
            )
        apply_exact(winners[0])
    backfill_rel = f'tests/origo_source_native/test_{naming.prefix}_backfill_job.py'
    updated, _deleted = delete_shadow_test(read(backfill_rel), naming.prefix)
    cache[backfill_rel] = updated
    leftover = re.search(
        rf'(?m)^(?:async )?def test_{re.escape(naming.prefix)}_huggingface_shadow', updated
    )
    if leftover is not None:
        raise ValueError(f'{backfill_rel}: shadow test survives promotion: {leftover.group(0)!r}.')
    for rel, after in cache.items():
        before = (repo / rel).read_text(encoding='utf-8')
        if before != after:
            results[rel] = (before, after)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Promote a CANARY source to LIVE.')
    parser.add_argument('--source', required=True, help='Source key, e.g. binance_perp_aggtrades.')
    parser.add_argument('--repo', default='.', help='Repository root (default: cwd).')
    parser.add_argument('--apply', action='store_true', help='Write the files (default: diff only).')
    args = parser.parse_args(argv)
    try:
        naming = naming_for(args.source)
    except ValueError as exc:
        print(f'promote_source: {exc}', file=sys.stderr)
        return 2
    repo = Path(args.repo)
    try:
        results = promote(repo, naming)
    except ValueError as exc:
        print(f'promote_source: {exc}', file=sys.stderr)
        return 1
    if not results:
        print('promote_source: no changes; the source already reads LIVE.')
        return 0
    for rel in sorted(results):
        before, after = results[rel]
        sys.stdout.writelines(
            difflib.unified_diff(
                before.splitlines(keepends=True),
                after.splitlines(keepends=True),
                fromfile=f'a/{rel}',
                tofile=f'b/{rel}',
            )
        )
        if args.apply:
            (repo / rel).write_text(after, encoding='utf-8')
    print(
        f'promote_source: {"wrote" if args.apply else "would write"} {len(results)} file(s).',
        file=sys.stderr,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
