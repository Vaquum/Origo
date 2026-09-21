"""Bounded file reads for untrusted, offline acceptance bundles."""
from __future__ import annotations

import hashlib
from pathlib import Path

MAX_FILE_BYTES = 256 * 1024 * 1024
MAX_BUNDLE_FILES = 30000


def evidence_path(root: Path, relative: str) -> Path:
    """Resolve a regular artifact without following links outside its bundle."""
    base = root.resolve(strict=True)
    part = Path(relative)
    if not part.parts or part.is_absolute() or '..' in part.parts:
        raise ValueError('An evidence artifact must have a relative bundle path.')
    target = base / part
    if not target.resolve(strict=True).is_relative_to(base):
        raise ValueError('Evidence artifact escapes its bundle.')
    cursor = base
    for name in part.parts:
        cursor = cursor / name
        if cursor.is_symlink():
            raise ValueError('Symlinks are not acceptance artifacts.')
    if not target.is_file() or target.stat().st_size > MAX_FILE_BYTES:
        raise ValueError('Evidence artifact is absent or exceeds its read bound.')
    return target


def file_digest(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def validate_inventory(root: Path, names: list[str]) -> dict[str, Path]:
    if not names or len(names) > MAX_BUNDLE_FILES:
        raise ValueError('Evidence inventory is empty or exceeds its file-count bound.')
    return {name: evidence_path(root, name) for name in names}
