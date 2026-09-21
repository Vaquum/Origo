"""Resumable mount publication (S439 SS-02/05/06/09).

Three concerns the mount renderer delegates here:

- ``CheckpointJournal``: every completed month render is kept beside the mirror as an
  immutable file keyed by ``(series, month, month token)`` until the manifest supersedes
  it. A retry after process death reuses verified checkpoints and queries only the
  months that are missing; a token change prunes exactly the affected month.
- Commit: checkpoints are hard-linked into the mirror (``install``), so the mirror never
  holds a torn file and an interrupted commit resumes from the same checkpoints. The
  manifest (``latest.json``) remains the generation record every reader honours.
- ``delivered_coverage``: the coverage a manifest may claim is the contiguous accepted
  coverage from the source anchor (``P``), never ``max(end)`` across a hidden gap.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl

from origo.sources.contracts import Snapshot
from origo.sources.profiles.formulas.spot_series import MountKlineSpec

from .coverage import contiguous_end, selected_intervals

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
TOKEN_PREFIX = 16
# A partial checkpoint file older than this belongs to a write that died mid-stream.
PARTIAL_MAX_AGE_SECONDS = 3600


def file_sha256(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def file_identity(path: Path) -> list[int]:
    """Content-independent identity (inode, size, mtime) that a reuse check can compare
    without re-hashing the world every render; a changed identity falls back to the hash."""
    stat = path.stat()
    return [stat.st_ino, stat.st_size, stat.st_mtime_ns]


def _fsync(path: Path) -> None:
    with path.open('rb') as handle:
        os.fsync(handle.fileno())


def write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.parent / f'.{path.name}.partial-{uuid4().hex}'
    pending.write_bytes(payload)
    _fsync(pending)
    os.replace(pending, path)


def write_parquet(frame: pl.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    pending = target.parent / f'.{target.name}.partial-{uuid4().hex}'
    frame.write_parquet(pending, compression='zstd')
    _fsync(pending)
    os.replace(pending, target)


def _object(text: str) -> dict[str, object]:
    value: object = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError('Checkpoint record must be an object.')
    return cast(dict[str, object], value)


@dataclass(frozen=True)
class MonthCheckpoint:
    series: str
    month: str
    month_token: str
    path: Path
    sha256: str
    row_count: int


class CheckpointJournal:
    """Completed month renders of the pinned state, kept until the manifest supersedes them.

    The directory lives beside the mirror (same filesystem, so a commit is a link, not a
    copy) and is hidden from the mirror readers, which glob only the series sub-paths.
    """

    def __init__(self, parquet_root: Path, owner: str | None) -> None:
        self.parquet_root = parquet_root
        self.directory = parquet_root / (f'.checkpoints-{owner}' if owner else '.checkpoints')

    def _base(self, series: MountKlineSpec, month: str, token: str) -> Path:
        return self.directory / series.sub_path / f'{month}.{token[:TOKEN_PREFIX]}'

    def _record(self, sidecar: Path) -> dict[str, object] | None:
        try:
            return _object(sidecar.read_text())
        except ValueError:
            # A torn sidecar is not completed work; the month is rendered again.
            sidecar.unlink()
            return None

    def find_month(
        self, series: MountKlineSpec, month: str, token: str, *, verify: bool = True
    ) -> MonthCheckpoint | None:
        """The verified checkpoint for this series/month/token, or None.

        ``verify`` re-hashes the bytes before reuse; the cheaper identity check
        (sidecar + size) is enough to count pending work before a render starts.
        """
        base = self._base(series, month, token)
        sidecar = base.parent / (base.name + '.json')
        if not sidecar.is_file():
            return None
        record = self._record(sidecar)
        if record is None:
            return None
        if (
            record.get('series') != series.name
            or record.get('month') != month
            or record.get('month_token') != token
        ):
            raise ValueError(f'Checkpoint {sidecar} does not describe its own name.')
        row_count = int(str(record['row_count']))
        digest = str(record['sha256'])
        parquet = base.parent / (base.name + '.parquet')
        if row_count == 0:
            return MonthCheckpoint(series.name, month, token, parquet, '', 0)
        complete = parquet.is_file() and parquet.stat().st_size == int(str(record['size']))
        if complete and verify:
            complete = file_sha256(parquet) == digest
        if not complete:
            sidecar.unlink()
            parquet.unlink(missing_ok=True)
            return None
        return MonthCheckpoint(series.name, month, token, parquet, digest, row_count)

    def write_month(
        self,
        series: MountKlineSpec,
        month: str,
        token: str,
        frame: pl.DataFrame,
        *,
        render_seconds: float,
    ) -> MonthCheckpoint:
        base = self._base(series, month, token)
        parquet = base.parent / (base.name + '.parquet')
        digest, size = '', 0
        if frame.height:
            write_parquet(frame, parquet)
            digest, size = file_sha256(parquet), parquet.stat().st_size
        write_atomic(
            base.parent / (base.name + '.json'),
            json.dumps(
                {
                    'schema_version': 1,
                    'series': series.name,
                    'month': month,
                    'month_token': token,
                    'row_count': frame.height,
                    'sha256': digest,
                    'size': size,
                    'render_seconds': round(render_seconds, 3),
                    'rendered_at': datetime.now(UTC).isoformat(),
                },
                sort_keys=True,
            ).encode(),
        )
        return MonthCheckpoint(series.name, month, token, parquet, digest, frame.height)

    def covered(self, specs: tuple[MountKlineSpec, ...], month: str, token: str) -> bool:
        """Whether every series of the month is checkpointed, so no query is pending."""
        return all(
            self.find_month(series, month, token, verify=False) is not None for series in specs
        )

    def prune(self, tokens: dict[str, str]) -> int:
        """Drop every checkpoint whose month token is no longer pinned; returns the count."""
        removed = 0
        if not self.directory.is_dir():
            return removed
        for sidecar in sorted(self.directory.rglob('*.json')):
            record = self._record(sidecar)
            if record is None:
                removed += 1
                continue
            if tokens.get(str(record.get('month'))) != record.get('month_token'):
                sidecar.unlink()
                parquet = sidecar.with_name(sidecar.name[: -len('.json')] + '.parquet')
                parquet.unlink(missing_ok=True)
                removed += 1
        return removed

    def sweep_partials(self, now: float) -> None:
        if not self.directory.is_dir():
            return
        cutoff = now - PARTIAL_MAX_AGE_SECONDS
        for orphan in self.directory.rglob('.*.partial-*'):
            if orphan.is_file() and orphan.stat().st_mtime < cutoff:
                orphan.unlink(missing_ok=True)

    @staticmethod
    def install(checkpoint: MonthCheckpoint, target: Path) -> None:
        """Link the checkpoint's bytes into the mirror atomically; a second call is a no-op."""
        if target.is_file() and os.path.samefile(target, checkpoint.path):
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        pending = target.parent / f'.{target.name}.partial-{uuid4().hex}'
        os.link(checkpoint.path, pending)
        os.replace(pending, target)

    def clear(self) -> None:
        if self.directory.is_dir():
            shutil.rmtree(self.directory)


@dataclass(frozen=True)
class DeliveredCoverage:
    """``P``: the end of contiguous accepted coverage from the anchor, with the closed-bar
    boundary each series may claim at or before it. ``newest_end`` is ``N``."""

    anchor: datetime
    delivered_through: datetime
    newest_end: datetime
    eligible: dict[str, datetime]

    @property
    def hidden_gap(self) -> bool:
        return self.newest_end > self.delivered_through

    def manifest(self) -> dict[str, object]:
        return {
            'anchor': self.anchor.isoformat(),
            'delivered_through': self.delivered_through.isoformat(),
            'newest_end': self.newest_end.isoformat(),
            'hidden_gap': self.hidden_gap,
            'series': {name: value.isoformat() for name, value in sorted(self.eligible.items())},
        }


def delivered_coverage(
    anchor: datetime, snapshot: Snapshot, specs: tuple[MountKlineSpec, ...]
) -> DeliveredCoverage:
    """Certified continuity of a pinned state, from the same interval selection the
    worker's frontier uses (one truth), never the newest partition end."""
    if anchor.tzinfo is None:
        raise ValueError('Delivered coverage requires a timezone-aware anchor.')
    anchor = anchor.astimezone(UTC)
    selected = selected_intervals(tuple(record.partition for record in snapshot.records))
    delivered = contiguous_end(anchor, selected)
    newest = max((item.end for item in selected), default=anchor)
    eligible: dict[str, datetime] = {}
    for series in specs:
        if series.family == 'time':
            # A time bar is eligible once closed: the last bucket boundary at or before P.
            step = timedelta(minutes=series.size)
            eligible[series.name] = EPOCH + step * ((delivered - EPOCH) // step)
        else:
            # Dollar bars are day-scoped and close on volume; every bar ending before P is
            # represented and the open bar carries P as its input bound.
            eligible[series.name] = delivered
    return DeliveredCoverage(anchor, delivered, newest, eligible)


def manifest_delivered_through(manifest: dict[str, object]) -> datetime:
    """``P`` as a manifest claims it; a manifest without the claim is not current evidence."""
    value = manifest.get('delivered_through')
    if not isinstance(value, str):
        raise ValueError('Publication manifest carries no delivered coverage.')
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError('Delivered coverage must be timezone-aware.')
    return parsed.astimezone(UTC)
