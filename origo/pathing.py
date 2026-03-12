from __future__ import annotations

from pathlib import Path


def monorepo_root() -> Path:
    # /<repo>/origo/pathing.py -> /<repo>
    return Path(__file__).resolve().parents[1]


def resolve_repo_relative_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return monorepo_root() / path
