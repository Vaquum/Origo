import json
import os
from pathlib import Path
from typing import cast

from .contracts import RevisionedSourceSpec


def publication_current(
    spec: RevisionedSourceSpec,
    consumer: str,
    token: str,
    *,
    root: Path | None = None,
    pinned: bool = False,
) -> bool:
    """Whether the consumer's manifest publishes ``token`` and every listed file exists.

    ``pinned`` compares the token of the state a renderer pinned (canonical and provisional
    partitions); otherwise the canonical currency token is compared.
    """
    path = (
        (root or Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')))
        / spec.key
        / consumer
        / 'latest.json'
    )
    if not path.exists():
        return False
    manifest: object = json.loads(path.read_text())
    if not isinstance(manifest, dict):
        raise ValueError('Publication manifest must be an object.')
    data = cast(dict[str, object], manifest)
    if data.get('pinned_token' if pinned else 'state_token') != token:
        return False
    files = data.get('files')
    version = data.get('version')
    if not isinstance(files, list) or not isinstance(version, str):
        raise ValueError('Publication manifest lacks file/version evidence.')
    for entry in cast(list[object], files):
        if not isinstance(entry, dict):
            raise ValueError('Publication file evidence must be an object.')
        relative = cast(dict[str, object], entry).get('path')
        if not isinstance(relative, str):
            raise ValueError('Publication file evidence requires a path.')
        location = Path(relative)
        if not location.is_absolute():
            location = path.parent / 'versions' / version / relative
        if not location.is_file():
            return False
    return True
