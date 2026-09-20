"""The versioned, mmap-ready Arrow bar store shared by the spot mount consumer and the depth store."""

import fcntl
import hashlib
import io
import json
import os
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import polars as pl
from origo.sources.profiles.formulas.spot_series import DEFAULT_MOUNT_DIR, SPECS, MountKlineSpec

DEFAULT_ARROW_DIR = '/opt/arrow'


LATEST_NAME = 'latest.arrow'


RETENTION_KEEP = 3


REAP_GRACE_SECONDS = 600


ORPHAN_TMP_MAX_AGE_SECONDS = 3600


VERSION_HEX = 16


_VALUE_COLUMNS: tuple[str, ...] = (
    'open',
    'high',
    'low',
    'close',
    'mean',
    'std',
    'volume',
    'maker_ratio',
    'no_of_trades',
    'open_liquidity',
    'high_liquidity',
    'low_liquidity',
    'close_liquidity',
    'liquidity_sum',
    'maker_volume',
    'maker_liquidity',
)


_TIME_OUTPUT_COLUMNS: tuple[str, ...] = ('ts', *_VALUE_COLUMNS)


_DOLLAR_OUTPUT_COLUMNS: tuple[str, ...] = ('ts', 'start_ts', 'dollar_bar_id', *_VALUE_COLUMNS)


@dataclass(frozen=True)
class BarSeriesBuild:
    """The shaped frame plus provenance counts surfaced by the asset."""

    df: pl.DataFrame
    source_rows: int
    dropped_duplicate_ts: int


@dataclass(frozen=True)
class PublishOutcome:
    status: str  # "published" | "skipped_unchanged" | "skipped_not_newer" | "skipped_empty"
    version: str | None
    reaped: tuple[str, ...] = ()


BAR_STORE_SERIES: tuple[str, ...] = tuple(spec.name for spec in SPECS)


def parquet_source_root() -> Path:
    return Path(os.environ.get('LOCAL_PARQUET_DIR', DEFAULT_MOUNT_DIR))


def arrow_store_root() -> Path:
    return Path(os.environ.get('LOCAL_ARROW_DIR', DEFAULT_ARROW_DIR))


def series_store_dir(series: str) -> Path:
    return arrow_store_root() / series


def spec_for_series(series: str) -> MountKlineSpec:
    for spec in SPECS:
        if spec.name == series:
            return spec
    raise ValueError(f'Unknown bar-store series: {series}')


def series_source_files(spec: MountKlineSpec, parquet_root: Path) -> list[Path]:
    base = parquet_root / spec.sub_path
    if not base.exists():
        return []
    return sorted(base.glob('**/*.parquet'))


def source_identity(spec: MountKlineSpec, root: Path) -> str | None:
    """Content-independent identity of a series' mirror files (path, inode, size, mtime)."""
    files = sorted((root / spec.sub_path).glob('*/*.parquet'))
    if not files:
        return None
    generations: list[tuple[str, int, int, int]] = []
    for path in files:
        stat = path.stat()
        generations.append(
            (str(path.relative_to(root)), stat.st_ino, stat.st_size, stat.st_mtime_ns)
        )
    return hashlib.sha256(json.dumps(generations, separators=(',', ':')).encode()).hexdigest()


def _output_columns(family: str) -> tuple[str, ...]:
    return _TIME_OUTPUT_COLUMNS if family == 'time' else _DOLLAR_OUTPUT_COLUMNS


def build_series_frame(
    spec: MountKlineSpec, parquet_root: Path, files: Sequence[Path] | None = None
) -> BarSeriesBuild:
    """Read, shape, sort, dedupe, and single-batch one series at full precision.

    ``ts`` is Int64 nanoseconds (UTC): the bar ``datetime`` for time series, the
    bar ``end_datetime`` for dollar series. Measure columns are carried verbatim
    (no downcast) so the frame is bit-for-bit reproducible against the Parquet.
    Duplicate ``ts`` rows (rare; the mirror is already grouped) keep the last
    occurrence so the index is strictly increasing for searchsorted. ``files``
    replaces the mirror's own month files, for a render staged beside the mirror."""
    files = list(files) if files is not None else series_source_files(spec, parquet_root)
    if not files:
        return BarSeriesBuild(pl.DataFrame(), 0, 0)

    raw = pl.read_parquet([str(path) for path in files])
    source_rows = raw.height

    ts_source = 'datetime' if spec.family == 'time' else 'end_datetime'
    exprs: list[pl.Expr] = [pl.col(ts_source).dt.epoch(time_unit='ns').cast(pl.Int64).alias('ts')]
    if spec.family == 'dollar':
        exprs.append(
            pl.col('start_datetime').dt.epoch(time_unit='ns').cast(pl.Int64).alias('start_ts')
        )
        exprs.append(pl.col('dollar_bar_id'))
    exprs.extend(pl.col(name) for name in _VALUE_COLUMNS)

    shaped = raw.select(exprs).select(_output_columns(spec.family))
    del raw
    shaped = shaped.sort('ts')
    before = shaped.height
    # The input is ascending, so keep='last' with maintain_order=True preserves the
    # ascending order while keeping the last occurrence: one sort, no second pass.
    deduped = shaped.unique(subset=['ts'], keep='last', maintain_order=True)
    del shaped
    out = deduped.rechunk() if deduped.n_chunks() > 1 else deduped
    return BarSeriesBuild(out, source_rows, before - deduped.height)


def _series_max_ts(frame: pl.DataFrame) -> int | None:
    value = frame.get_column('ts').max()
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raise RuntimeError(f'Unexpected ts max type {type(value)!r}')


def _existing_max_ts(path: Path) -> int | None:
    frame = pl.read_ipc(path, columns=['ts'], memory_map=True)
    return _series_max_ts(frame)


def _link_version(latest: Path) -> str | None:
    """Parse the version (content hash) out of the ``latest`` symlink target."""
    if not latest.is_symlink():
        return None
    name = os.path.basename(os.readlink(latest))
    if not name.endswith('.arrow'):
        return None
    parts = name[: -len('.arrow')].split('.')
    return parts[-1] if len(parts) >= 2 else None


def _fsync_file(handle: io.BufferedWriter) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_swap_symlink(latest: Path, relative_target: str) -> None:
    tmp = latest.parent / f'.{LATEST_NAME}.tmp-{os.getpid()}-{uuid.uuid4().hex}'
    tmp.unlink(missing_ok=True)
    os.symlink(relative_target, tmp)
    os.replace(tmp, latest)


def _clear_orphan_tmp(directory: Path, now: float) -> None:
    cutoff = now - ORPHAN_TMP_MAX_AGE_SECONDS
    for orphan in directory.glob('.*.tmp-*'):
        if orphan.stat().st_mtime < cutoff:
            orphan.unlink(missing_ok=True)


def reap_old_versions(
    directory: Path, series: str, *, keep: int, grace_seconds: float, now: float
) -> tuple[str, ...]:
    """Keep the newest ``keep`` versions by mtime; reap older ones, but only once
    they are older than ``grace_seconds`` and never the one ``latest`` points to."""
    protected = _link_version(directory / LATEST_NAME)
    versions = [
        path
        for path in directory.glob(f'{series}.*.arrow')
        if path.is_file() and not path.is_symlink()
    ]
    versions.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)

    reaped: list[str] = []
    for index, path in enumerate(versions):
        if index < keep:
            continue
        if protected is not None and path.name.endswith(f'.{protected}.arrow'):
            continue
        if path.stat().st_mtime >= now - grace_seconds:
            continue
        path.unlink(missing_ok=True)
        reaped.append(path.name)
    return tuple(reaped)


@dataclass(frozen=True)
class StagedSeries:
    """A version file written (or already present) under the series directory, not yet
    the ``latest`` target."""

    series: str
    version: str
    target: Path
    row_count: int
    new_max: int | None
    written: bool


def stage_series(series: str, build: BarSeriesBuild) -> StagedSeries | None:
    """Write the shaped series as a content-hash version without flipping ``latest``."""
    df = build.df
    if df.height == 0:
        return None
    directory = series_store_dir(series)
    directory.mkdir(parents=True, exist_ok=True)
    tmp = directory / f'.{series}.tmp-stage-{os.getpid()}-{uuid.uuid4().hex}'
    with open(tmp, 'wb') as handle:
        # record_batch_size >= height forces a single record batch. Without it polars
        # splits frames past its default (~122k rows) into multiple batches, which a
        # ``memory_map=True`` reader surfaces as multiple chunks -- breaking the single
        # batch / zero-copy ``ts`` contract for every non-trivial series. The bytes
        # stream to disk instead of piling up beside the frame.
        df.write_ipc(handle, compression='uncompressed', record_batch_size=max(df.height, 1))
        _fsync_file(handle)
    version = _sha256_file(tmp)[:VERSION_HEX]
    target = directory / f'{series}.{version}.arrow'
    written = not target.exists()
    if written:
        os.replace(tmp, target)
    else:
        tmp.unlink(missing_ok=True)
    return StagedSeries(series, version, target, df.height, _series_max_ts(df), written)


def activate_series(staged: StagedSeries) -> PublishOutcome:
    """Flip ``latest`` to a staged version under the series lock, then reap old versions.

    An unchanged content hash is a no-op (no churn), and a slow out-of-order tick
    whose data is staler than ``latest`` is skipped."""
    directory = staged.target.parent
    latest = directory / LATEST_NAME
    now = time.time()
    _clear_orphan_tmp(directory, now)
    # Lock-free fast path: the sensor fired but the bytes are unchanged.
    if _link_version(latest) == staged.version and staged.target.exists():
        return PublishOutcome('skipped_unchanged', staged.version)
    lock_path = directory / f'.{staged.series}.lock'
    with open(lock_path, 'w', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        if _link_version(latest) == staged.version:
            status = 'skipped_unchanged'
        else:
            existing_max = _existing_max_ts(latest) if latest.is_symlink() else None
            if (
                existing_max is not None
                and staged.new_max is not None
                and staged.new_max < existing_max
            ):
                status = 'skipped_not_newer'
            else:
                _atomic_swap_symlink(latest, staged.target.name)
                status = 'published'
        reaped = reap_old_versions(
            directory,
            staged.series,
            keep=RETENTION_KEEP,
            grace_seconds=REAP_GRACE_SECONDS,
            now=now,
        )
    return PublishOutcome(status, staged.version, reaped)


def discard_series(staged: StagedSeries) -> None:
    """Remove a version this render wrote and never activated, so a discarded or
    skipped render leaves no unreferenced version to occupy a retention slot."""
    if staged.written and _link_version(staged.target.parent / LATEST_NAME) != staged.version:
        staged.target.unlink(missing_ok=True)


def publish_series(series: str, build: BarSeriesBuild) -> PublishOutcome:
    """Atomically publish the shaped series and flip ``latest`` to it."""
    staged = stage_series(series, build)
    if staged is None:
        return PublishOutcome('skipped_empty', None)
    outcome = activate_series(staged)
    if outcome.status == 'skipped_not_newer':
        discard_series(staged)
    return outcome
