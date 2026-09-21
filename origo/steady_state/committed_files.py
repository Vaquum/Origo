"""Immutable publication receipts alongside the compatible mutable mirror paths.

Keep the committed and immediately preceding generations readable across an interrupted
mirror switch. Hard links pin bytes on each output's own filesystem, not a cross-volume
transaction. The source consumer lock serializes publication and generation pruning.
"""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Sequence
from pathlib import Path

from origo.sources.contracts import identifier

from .publication import file_identity, file_sha256


def _name(value: object) -> str:
    text = str(value)
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,160}', text) or text in ('.', '..'):
        raise ValueError('Committed publication member name is invalid.')
    return text


def _directory(root: Path, source: str, token: str) -> Path:
    return root / ('.committed-' + identifier(source)) / _name(token)


def pin_files(
    entries: Sequence[dict[str, object]],
    *,
    source: str,
    token: str,
    parquet_root: Path,
    arrow_root: Path,
) -> list[dict[str, object]]:
    pinned: list[dict[str, object]] = []
    for entry in entries:
        source_path = Path(str(entry['path']))
        root = arrow_root if entry.get('kind') == 'arrow' else parquet_root
        if not source_path.resolve().is_relative_to(root.resolve()):
            raise ValueError('Publication file must remain on its owned output volume.')
        relative = _name(entry.get('series')) + '/' + _name(entry.get('month', 'all'))
        suffix = '.arrow' if entry.get('kind') == 'arrow' else '.parquet'
        target = _directory(root, source, token) / (relative + suffix)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            os.link(source_path, target)
            with target.open('rb') as handle:
                os.fsync(handle.fileno())
        elif not os.path.samefile(source_path, target):
            if file_sha256(target) != entry['sha256']:
                raise ValueError('A committed token was reused for different file bytes.')
        pinned.append(
            {
                **entry,
                'path': str(target),
                'mirror_path': str(entry.get('mirror_path', source_path)),
                'identity': file_identity(target),
            }
        )
    return pinned


def prune_generations(
    *,
    source: str,
    retained_tokens: Sequence[str],
    parquet_root: Path,
    arrow_root: Path,
) -> None:
    keep = {_name(token) for token in retained_tokens if token}
    if not keep:
        raise ValueError('A committed generation must remain retained.')
    for root in (parquet_root, arrow_root):
        directory = root / ('.committed-' + identifier(source))
        if not directory.is_dir():
            continue
        for path in directory.iterdir():
            if path.name not in keep:
                if path.is_symlink() or not path.is_dir():
                    raise ValueError('Unexpected entry in the owned generation directory.')
                shutil.rmtree(path)
