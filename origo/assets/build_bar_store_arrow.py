"""Project the 12 Parquet mirror series into a versioned, mmap-ready Arrow bar store.

The Parquet mirror (``publish_binance_spot_klines_to_mount``) keeps a tree of
monthly files under ``LOCAL_PARQUET_DIR``. This asset reads one whole series,
shapes it into a single uncompressed Arrow IPC (Feather v2) record batch, and
publishes it under ``LOCAL_ARROW_DIR`` so a consumer can ``mmap`` the file and
``searchsorted`` over a zero-copy ``ts`` view. The shaping contract is:

1. read every monthly Parquet file for the series;
2. sort ascending by ``ts`` (Int64 nanoseconds, UTC) and drop duplicate ``ts``
   so the index is strictly increasing (the searchsorted correctness contract);
3. carry every measure column verbatim at full precision -- no downcast. The store
   is a re-layout of the mirror, not a lossy projection, so it stays bit-for-bit
   reproducible against the Parquet (and thus ClickHouse / Hugging Face). The only
   derived columns are ``ts`` / ``start_ts``, an exact ms->ns re-encode of the bar
   datetimes;
4. write one record batch -- ``record_batch_size`` forces a single batch (polars
   otherwise splits large frames, and a ``memory_map`` reader surfaces each batch
   as its own chunk), so the consumer sees one contiguous chunk;
5. write Feather v2 / Arrow IPC, uncompressed (mmap needs raw bytes).

Publishing is atomic and never in place: the bytes land in a same-dir temp file
that is fsynced and ``os.replace``-d into ``<series>.<version>.arrow``, then a
fresh relative symlink is ``os.replace``-d onto ``latest.arrow`` last. The
version is the content hash, so a byte-identical rebuild is a no-op (no churn)
and a pinned version is reproducible. A per-series lock serializes the
check-publish-flip-reap critical section, a monotonic freshness guard keeps a
slow out-of-order run from flipping ``latest`` back to staler data, and retention
keeps the last ``RETENTION_KEEP`` versions, reaping older ones only after a grace
window so in-flight ``mmap`` and pinned-version reads never ``ENOENT`` mid-swap.

Dagster (this asset) is the sole writer to ``LOCAL_ARROW_DIR``; consumers
bind-mount it read-only.
"""

import fcntl
import hashlib
import io
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Config, RunRequest, StaticPartitionsDefinition, asset

from origo.assets.publish_binance_spot_klines_to_mount import (
    DEFAULT_MOUNT_DIR,
    SPECS,
    MountKlineSpec,
)

DEFAULT_ARROW_DIR = "/opt/arrow"
LATEST_NAME = "latest.arrow"
# How many published versions to retain per series, and how long a superseded
# version must linger before it can be reaped. >= 3 versions and a ~10-minute
# grace keep in-flight mmap / pinned-version reads from racing a reap.
RETENTION_KEEP = 3
REAP_GRACE_SECONDS = 600
ORPHAN_TMP_MAX_AGE_SECONDS = 3600
# Truncated SHA-256 of the IPC payload; 16 hex chars (64 bits) is collision-safe
# for a per-series version line that turns over a few times an hour.
VERSION_HEX = 16

# Measure columns shared by both families, carried verbatim from the Parquet mirror
# at their native dtype (Float64 / Int64) -- no downcast, so the store is bit-for-bit
# reproducible against the mirror. Canonical order matches the mirror / HF exports.
_VALUE_COLUMNS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "mean",
    "std",
    "volume",
    "maker_ratio",
    "no_of_trades",
    "open_liquidity",
    "high_liquidity",
    "low_liquidity",
    "close_liquidity",
    "liquidity_sum",
    "maker_volume",
    "maker_liquidity",
)
# Output column order. ``ts`` is the canonical index (datetime for time bars,
# end_datetime for dollar bars); dollar bars also carry their open time and id.
_TIME_OUTPUT_COLUMNS: tuple[str, ...] = ("ts", *_VALUE_COLUMNS)
_DOLLAR_OUTPUT_COLUMNS: tuple[str, ...] = ("ts", "start_ts", "dollar_bar_id", *_VALUE_COLUMNS)


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
BAR_STORE_PARTITIONS = StaticPartitionsDefinition(list(BAR_STORE_SERIES))


class BarStoreConfig(Config):
    # Shape and report without writing; useful to inspect a series from Dagit.
    dry_run: bool = False


def parquet_source_root() -> Path:
    return Path(os.environ.get("LOCAL_PARQUET_DIR", DEFAULT_MOUNT_DIR))


def arrow_store_root() -> Path:
    return Path(os.environ.get("LOCAL_ARROW_DIR", DEFAULT_ARROW_DIR))


def series_store_dir(series: str) -> Path:
    return arrow_store_root() / series


def spec_for_series(series: str) -> MountKlineSpec:
    for spec in SPECS:
        if spec.name == series:
            return spec
    raise ValueError(f"Unknown bar-store series: {series}")


def series_source_files(spec: MountKlineSpec, parquet_root: Path) -> list[Path]:
    base = parquet_root / spec.sub_path
    if not base.exists():
        return []
    return sorted(base.glob("**/*.parquet"))


def source_identity(spec: MountKlineSpec, root: Path) -> str | None:
    """Content-independent identity of a series' mirror files (path, inode, size, mtime)."""
    files = sorted((root / spec.sub_path).glob("*/*.parquet"))
    if not files:
        return None
    generations: list[tuple[str, int, int, int]] = []
    for path in files:
        stat = path.stat()
        generations.append(
            (str(path.relative_to(root)), stat.st_ino, stat.st_size, stat.st_mtime_ns)
        )
    return hashlib.sha256(json.dumps(generations, separators=(",", ":")).encode()).hexdigest()


def _output_columns(family: str) -> tuple[str, ...]:
    return _TIME_OUTPUT_COLUMNS if family == "time" else _DOLLAR_OUTPUT_COLUMNS


def build_series_frame(spec: MountKlineSpec, parquet_root: Path) -> BarSeriesBuild:
    """Read, shape, sort, dedupe, and single-batch one series at full precision.

    ``ts`` is Int64 nanoseconds (UTC): the bar ``datetime`` for time series, the
    bar ``end_datetime`` for dollar series. Measure columns are carried verbatim
    (no downcast) so the frame is bit-for-bit reproducible against the Parquet.
    Duplicate ``ts`` rows (rare; the mirror is already grouped) keep the last
    occurrence so the index is strictly increasing for searchsorted."""
    files = series_source_files(spec, parquet_root)
    if not files:
        return BarSeriesBuild(pl.DataFrame(), 0, 0)

    raw = pl.read_parquet([str(path) for path in files])
    source_rows = raw.height

    ts_source = "datetime" if spec.family == "time" else "end_datetime"
    exprs: list[pl.Expr] = [pl.col(ts_source).dt.epoch(time_unit="ns").cast(pl.Int64).alias("ts")]
    if spec.family == "dollar":
        exprs.append(pl.col("start_datetime").dt.epoch(time_unit="ns").cast(pl.Int64).alias("start_ts"))
        exprs.append(pl.col("dollar_bar_id"))
    exprs.extend(pl.col(name) for name in _VALUE_COLUMNS)

    shaped = raw.select(exprs).select(_output_columns(spec.family)).sort("ts")
    deduped = shaped.unique(subset=["ts"], keep="last").sort("ts")
    dropped = shaped.height - deduped.height
    return BarSeriesBuild(deduped.rechunk(), source_rows, dropped)


def _series_max_ts(frame: pl.DataFrame) -> int | None:
    value = frame.get_column("ts").max()
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raise RuntimeError(f"Unexpected ts max type {type(value)!r}")


def _existing_max_ts(path: Path) -> int | None:
    frame = pl.read_ipc(path, columns=["ts"], memory_map=True)
    return _series_max_ts(frame)


def _link_version(latest: Path) -> str | None:
    """Parse the version (content hash) out of the ``latest`` symlink target."""
    if not latest.is_symlink():
        return None
    name = os.path.basename(os.readlink(latest))
    if not name.endswith(".arrow"):
        return None
    parts = name[: -len(".arrow")].split(".")
    return parts[-1] if len(parts) >= 2 else None


def _fsync_file(handle: io.BufferedWriter) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _atomic_write_bytes(target: Path, payload: bytes) -> None:
    tmp = target.parent / f".{target.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    with open(tmp, "wb") as handle:
        handle.write(payload)
        _fsync_file(handle)
    os.replace(tmp, target)


def _atomic_swap_symlink(latest: Path, relative_target: str) -> None:
    tmp = latest.parent / f".{LATEST_NAME}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    tmp.unlink(missing_ok=True)
    os.symlink(relative_target, tmp)
    os.replace(tmp, latest)


def _clear_orphan_tmp(directory: Path, now: float) -> None:
    cutoff = now - ORPHAN_TMP_MAX_AGE_SECONDS
    for orphan in directory.glob(".*.tmp-*"):
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
        for path in directory.glob(f"{series}.*.arrow")
        if path.is_file() and not path.is_symlink()
    ]
    versions.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)

    reaped: list[str] = []
    for index, path in enumerate(versions):
        if index < keep:
            continue
        if protected is not None and path.name.endswith(f".{protected}.arrow"):
            continue
        if path.stat().st_mtime >= now - grace_seconds:
            continue
        path.unlink(missing_ok=True)
        reaped.append(path.name)
    return tuple(reaped)


def _ipc_payload(df: pl.DataFrame) -> bytes:
    sink = io.BytesIO()
    # record_batch_size >= height forces a single record batch. Without it polars
    # splits frames past its default (~122k rows) into multiple batches, which a
    # ``memory_map=True`` reader surfaces as multiple chunks -- breaking the single
    # batch / zero-copy ``ts`` contract for every non-trivial series.
    df.write_ipc(sink, compression="uncompressed", record_batch_size=max(df.height, 1))
    return sink.getvalue()


def publish_series(series: str, build: BarSeriesBuild) -> PublishOutcome:
    """Atomically publish the shaped series and flip ``latest`` to it.

    The version bytes are written (and ``latest`` flipped) only when this tick
    actually publishes: an unchanged content hash is a no-op (no churn), and a
    slow out-of-order tick whose data is staler than ``latest`` is skipped
    without writing -- so a skipped tick never leaves an unreferenced version on
    disk to steal a retention slot from a published one or churn disk."""
    df = build.df
    if df.height == 0:
        return PublishOutcome("skipped_empty", None)

    payload = _ipc_payload(df)
    version = hashlib.sha256(payload).hexdigest()[:VERSION_HEX]
    new_max = _series_max_ts(df)

    directory = series_store_dir(series)
    directory.mkdir(parents=True, exist_ok=True)
    now = time.time()
    _clear_orphan_tmp(directory, now)

    target = directory / f"{series}.{version}.arrow"
    latest = directory / LATEST_NAME

    # Lock-free fast path: the sensor fired but the bytes are unchanged.
    if _link_version(latest) == version and target.exists():
        return PublishOutcome("skipped_unchanged", version)

    lock_path = directory / f".{series}.lock"
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        if _link_version(latest) == version:
            status = "skipped_unchanged"
        else:
            existing_max = _existing_max_ts(latest) if latest.is_symlink() else None
            if existing_max is not None and new_max is not None and new_max < existing_max:
                status = "skipped_not_newer"
            else:
                # Write the version file only when we are actually going to publish it,
                # under the lock: a skipped (stale/unchanged) tick then never leaves an
                # unreferenced version on disk to occupy a retention slot or churn disk.
                if not target.exists():
                    _atomic_write_bytes(target, payload)
                _atomic_swap_symlink(latest, target.name)
                status = "published"
        reaped = reap_old_versions(
            directory, series, keep=RETENTION_KEEP, grace_seconds=REAP_GRACE_SECONDS, now=now
        )
    return PublishOutcome(status, version, reaped)


def bar_store_partition_run_requests(source_run_id: str) -> list[RunRequest]:
    """One RunRequest per series, run-keyed to the triggering mirror run so each
    successful mirror tick rebuilds every series exactly once. Driven by the
    bar_store_source_sensor when publish_binance_spot_klines_to_mount_job succeeds."""
    return [
        RunRequest(partition_key=series, run_key=f"{series}:{source_run_id}")
        for series in BAR_STORE_SERIES
    ]


@asset(
    name="build_bar_store_arrow",
    partitions_def=BAR_STORE_PARTITIONS,
    group_name="binance_bar_store",
    description=(
        "Projects one Parquet mirror series (partition = series, e.g. time_1m) into a "
        "versioned, mmap-ready Arrow IPC file under LOCAL_ARROW_DIR (default "
        f"{DEFAULT_ARROW_DIR}). Single uncompressed record batch, Int64-ns `ts` index, "
        "measures carried verbatim at full precision (no downcast, bit-for-bit "
        "reproducible vs the mirror), atomic `latest.arrow` symlink, content-hash "
        f"versions, last {RETENTION_KEEP} retained. Triggered by bar_store_source_sensor "
        "when the Parquet mirror job succeeds."
    ),
)
def build_bar_store_arrow(
    context: AssetExecutionContext, config: BarStoreConfig
) -> dict[str, object]:
    series = context.partition_key
    spec = spec_for_series(series)
    parquet_root = parquet_source_root()

    input_identity = source_identity(spec, parquet_root)
    build = build_series_frame(spec, parquet_root)
    if build.df.height == 0:
        context.log.warning(
            f"{series}: no source Parquet under {parquet_root / spec.sub_path}; nothing to publish."
        )
        return {"status": "no_source", "series": series, "source_rows": build.source_rows}

    if build.dropped_duplicate_ts > 0:
        context.log.warning(
            f"{series}: dropped {build.dropped_duplicate_ts} row(s) with duplicate ts to keep "
            "the index strictly increasing."
        )

    if config.dry_run:
        return {
            "status": "dry_run",
            "series": series,
            "rows": build.df.height,
            "source_rows": build.source_rows,
        }

    outcome = publish_series(series, build)
    if input_identity is not None:
        context.add_output_metadata({'source_identity': input_identity})
    context.log.info(
        f"{series}: {outcome.status} version={outcome.version} rows={build.df.height} "
        f"reaped={len(outcome.reaped)}"
    )
    return {
        "status": "success",
        "series": series,
        "rows": build.df.height,
        "source_rows": build.source_rows,
        "dropped_duplicate_ts": build.dropped_duplicate_ts,
        "version": outcome.version,
        "outcome": outcome.status,
        "reaped": len(outcome.reaped),
    }
