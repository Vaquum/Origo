"""Retain the committed Hugging Face version under the consumer's source lock."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from pathlib import Path
from typing import cast

from .contracts import identifier
from .locking import source_lock

KINDS = frozenset(('huggingface', 'huggingface_shadow'))
VERSION = re.compile(r'[0-9a-f]{64}-[0-9a-f]{32}')


def _current_version(root: Path) -> str:
    if not root.is_absolute() or root.name not in KINDS:
        raise ValueError('Retention requires an absolute Hugging Face consumer root.')
    identifier(root.parent.name)
    manifest = root / 'latest.json'
    versions = root / 'versions'
    if any(path.is_symlink() for path in (root.parent, root, manifest, versions)):
        raise ValueError('Publication retention refuses symlink roots and manifests.')
    document: object = json.loads(manifest.read_text())
    if not isinstance(document, dict):
        raise ValueError('Publication manifest must be an object.')
    data = cast(dict[str, object], document)
    if data.get('source_key') != root.parent.name or data.get('kind') != root.name:
        raise ValueError('Publication manifest does not identify this source and consumer.')
    version = data.get('version')
    if not isinstance(version, str) or VERSION.fullmatch(version) is None:
        raise ValueError('Publication manifest has an invalid version directory.')
    current = versions / version
    if current.is_symlink() or not current.is_dir():
        raise ValueError('Current publication version is missing or is a symlink.')
    files = data.get('files')
    if not isinstance(files, list) or not files:
        raise ValueError('Publication manifest requires current file references.')
    for entry in cast(list[object], files):
        if not isinstance(entry, dict):
            raise ValueError('Publication file reference must be an object.')
        relative = cast(dict[str, object], entry).get('path')
        if not isinstance(relative, str) or not relative:
            raise ValueError('Publication file reference requires a relative path.')
        path = Path(relative)
        if path.is_absolute() or not path.parts or '..' in path.parts:
            raise ValueError('Publication file reference escapes its version directory.')
        location = current
        for part in path.parts:
            location /= part
            if location.is_symlink():
                raise ValueError('Current publication file references a symlink.')
        if not location.is_file():
            raise ValueError(f'Current publication file is missing: {location}')
    return version


def retain_latest(root: Path, *, apply: bool = True) -> tuple[Path, ...]:
    """Plan or remove obsolete generated directories; caller must hold its consumer lock.

    A missing or invalid manifest prevents every deletion. The caller must not have
    an unfinished render: a generated directory not selected by latest is obsolete.
    """
    current = _current_version(root)
    obsolete: list[Path] = []
    for path in sorted((root / 'versions').iterdir()):
        if VERSION.fullmatch(path.name) is None:
            continue
        if path.is_symlink() or not path.is_dir():
            raise ValueError(f'Generated publication version is not a directory: {path}')
        if path.name != current:
            obsolete.append(path)
    if apply:
        for path in obsolete:
            shutil.rmtree(path)
    return tuple(obsolete)


def finish_attempt(root: Path, build: Path) -> None:
    """Recheck committed visibility before reclaiming a finished or failed render."""
    manifest = root / 'latest.json'
    if manifest.exists() or manifest.is_symlink():
        retain_latest(root)
    elif build.exists() or build.is_symlink():
        if build.parent != root / 'versions' or VERSION.fullmatch(build.name) is None:
            raise ValueError('An uncommitted render must identify its own generated directory.')
        if any(path.is_symlink() for path in (root.parent, root, root / 'versions', build)):
            raise ValueError('An uncommitted render directory must not be a symlink.')
        shutil.rmtree(build)


def _allocated(paths: tuple[Path, ...]) -> int:
    seen: set[tuple[int, int]] = set()
    total = 0
    for root in paths:
        for path in (root, *root.rglob('*')):
            info = path.lstat()
            identity = (info.st_dev, info.st_ino)
            if identity not in seen:
                seen.add(identity)
                total += info.st_blocks * 512
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(os.environ.get(
        'ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow',
    )))
    parser.add_argument('--lock-root', type=Path, default=Path(os.environ.get(
        'ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks',
    )))
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if not args.root.is_absolute() or args.root.is_symlink() or not args.root.is_dir():
        raise ValueError('Publication root must be an absolute, existing directory, not a symlink.')
    for source in sorted(args.root.iterdir()):
        if source.is_symlink():
            raise ValueError(f'Publication source root is a symlink: {source}')
        if not source.is_dir():
            continue
        identifier(source.name)
        for kind in sorted(KINDS):
            root = source / kind
            if not root.exists() and not root.is_symlink():
                continue
            with source_lock(args.lock_root, source.name, 'consumer_' + kind):
                planned = retain_latest(root, apply=False)
                allocated = _allocated(planned)
                before = os.statvfs(root)
                if args.apply:
                    retain_latest(root)
                after = os.statvfs(root)
                print(json.dumps({
                    'root': str(root), 'current_version': _current_version(root),
                    'apply': args.apply, 'directories': [str(path) for path in planned],
                    'candidate_count': len(planned), 'candidate_allocated_bytes': allocated,
                    'removed_count': len(planned) if args.apply else 0,
                    'removed_allocated_bytes': allocated if args.apply else 0,
                    'filesystem_free_bytes_before': before.f_bavail * before.f_frsize,
                    'filesystem_free_bytes_after': after.f_bavail * after.f_frsize,
                }, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
