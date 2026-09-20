from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest
from polars.testing import assert_frame_equal

# Importing the asset package reads CLICKHOUSE_PASSWORD at import time; this asset
# itself never touches ClickHouse (it reads the Parquet mirror off disk), so the
# tests below need no container -- the placeholder just lets collection succeed.
os.environ.setdefault("CLICKHOUSE_PASSWORD", "import-guard")

from origo.sources.profiles.formulas.spot_series import MountKlineSpec  # noqa: E402
from origo.utils.arrow_store import (  # noqa: E402
    BAR_STORE_SERIES,
    LATEST_NAME,
    REAP_GRACE_SECONDS,
    RETENTION_KEEP,
    arrow_store_root,
    build_series_frame,
    publish_series,
    reap_old_versions,
    series_store_dir,
    spec_for_series,
    stage_series,
)

# 2024-01-01T00:00:00Z in epoch-milliseconds; the mirror writes Datetime("ms", "UTC").
BASE_MS = 1_704_067_200_000
NS_PER_MS = 1_000_000

_FLOAT_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "mean",
    "std",
    "volume",
    "maker_ratio",
    "open_liquidity",
    "high_liquidity",
    "low_liquidity",
    "close_liquidity",
    "liquidity_sum",
    "maker_volume",
    "maker_liquidity",
)


def _floats(n: int) -> dict[str, pl.Series]:
    return {
        name: pl.Series(name, [float(row) + index * 0.5 for row in range(n)], dtype=pl.Float64)
        for index, name in enumerate(_FLOAT_COLUMNS)
    }


def _time_frame(datetimes_ms: list[int]) -> pl.DataFrame:
    n = len(datetimes_ms)
    columns: dict[str, pl.Series] = {
        "datetime": pl.Series("datetime", datetimes_ms, dtype=pl.Int64).cast(
            pl.Datetime("ms", time_zone="UTC")
        ),
        "no_of_trades": pl.Series("no_of_trades", [10 + row for row in range(n)], dtype=pl.Int64),
        **_floats(n),
    }
    return pl.DataFrame(columns)


def _dollar_frame(start_ms: list[int], end_ms: list[int], bar_ids: list[int]) -> pl.DataFrame:
    n = len(start_ms)
    columns: dict[str, pl.Series] = {
        "start_datetime": pl.Series("start_datetime", start_ms, dtype=pl.Int64).cast(
            pl.Datetime("ms", time_zone="UTC")
        ),
        "end_datetime": pl.Series("end_datetime", end_ms, dtype=pl.Int64).cast(
            pl.Datetime("ms", time_zone="UTC")
        ),
        "dollar_bar_id": pl.Series("dollar_bar_id", bar_ids, dtype=pl.Int64),
        "no_of_trades": pl.Series("no_of_trades", [10 + row for row in range(n)], dtype=pl.Int64),
        **_floats(n),
    }
    return pl.DataFrame(columns)


def _write_month(parquet_root: Path, spec: MountKlineSpec, df: pl.DataFrame, year: int, month: int) -> None:
    path = parquet_root / spec.sub_path / f"{year:04d}" / f"{month:02d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_store_covers_twelve_series() -> None:
    assert len(BAR_STORE_SERIES) == 12
    assert "time_1m" in BAR_STORE_SERIES
    assert "dollar_1M" in BAR_STORE_SERIES


def test_build_time_frame_shapes_sorts_dedupes(tmp_path: Path) -> None:
    spec = spec_for_series("time_1m")
    # Month 1 is out of order; month 2 repeats BASE_MS+2m so the seam carries a duplicate ts.
    _write_month(tmp_path, spec, _time_frame([BASE_MS + 120_000, BASE_MS, BASE_MS + 60_000]), 2024, 1)
    _write_month(tmp_path, spec, _time_frame([BASE_MS + 120_000, BASE_MS + 180_000]), 2024, 2)

    build = build_series_frame(spec, tmp_path)
    df = build.df

    assert df.columns[0] == "ts"
    assert df.schema["ts"] == pl.Int64
    # No downcast: measures keep their Parquet-native dtype (bit-for-bit reproducible).
    assert df.schema["no_of_trades"] == pl.Int64
    assert all(df.schema[name] == pl.Float64 for name in _FLOAT_COLUMNS)

    ts = df["ts"].to_list()
    assert ts == sorted(ts)
    assert len(set(ts)) == len(ts)
    assert ts[0] == BASE_MS * NS_PER_MS
    assert build.source_rows == 5
    assert build.dropped_duplicate_ts == 1
    assert df.n_chunks() == 1


def test_build_keep_last_wins_across_files(tmp_path: Path) -> None:
    spec = spec_for_series("time_1m")
    # The same ts in two month files: the later file's row (no_of_trades 99) must win,
    # and the output stays ascending without a second sort pass.
    _write_month(tmp_path, spec, _time_frame([BASE_MS, BASE_MS + 60_000]), 2024, 1)
    later = _time_frame([BASE_MS + 60_000, BASE_MS + 120_000])
    later = later.with_columns(
        pl.when(pl.col("datetime") == pl.lit(BASE_MS + 60_000).cast(pl.Datetime("ms", time_zone="UTC")))
        .then(pl.lit(99))
        .otherwise(pl.col("no_of_trades"))
        .alias("no_of_trades")
    )
    _write_month(tmp_path, spec, later, 2024, 2)

    build = build_series_frame(spec, tmp_path)
    df = build.df

    assert df["ts"].to_list() == sorted(df["ts"].to_list())
    assert build.source_rows == 4
    assert build.dropped_duplicate_ts == 1
    row = df.filter(pl.col("ts") == (BASE_MS + 60_000) * NS_PER_MS)
    assert row.height == 1
    assert row["no_of_trades"].to_list() == [99]
    assert df.n_chunks() == 1


def test_build_dollar_frame_uses_end_as_ts(tmp_path: Path) -> None:
    spec = spec_for_series("dollar_1M")
    _write_month(
        tmp_path,
        spec,
        _dollar_frame(
            start_ms=[BASE_MS, BASE_MS + 1_000],
            end_ms=[BASE_MS + 999, BASE_MS + 1_999],
            bar_ids=[0, 1],
        ),
        2024,
        1,
    )

    df = build_series_frame(spec, tmp_path).df

    assert df.columns[:3] == ["ts", "start_ts", "dollar_bar_id"]
    assert df.schema["ts"] == pl.Int64
    assert df.schema["start_ts"] == pl.Int64
    assert df.schema["dollar_bar_id"] == pl.Int64  # verbatim, no downcast
    assert df["ts"].to_list() == [(BASE_MS + 999) * NS_PER_MS, (BASE_MS + 1_999) * NS_PER_MS]
    assert df["start_ts"].to_list() == [BASE_MS * NS_PER_MS, (BASE_MS + 1_000) * NS_PER_MS]


def test_store_values_are_bit_identical_to_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    source = _time_frame([BASE_MS, BASE_MS + 60_000, BASE_MS + 120_000])
    _write_month(tmp_path / "parquet", spec, source, 2024, 1)

    publish_series("time_1m", build_series_frame(spec, tmp_path / "parquet"))
    stored = pl.read_ipc(series_store_dir("time_1m") / LATEST_NAME, memory_map=True, rechunk=False)

    # Every measure column survives at the exact same dtype and bits as the mirror
    # (no downcast) -- the store must not diverge from the rest of the system.
    measures = [*_FLOAT_COLUMNS, "no_of_trades"]
    expected = source.sort("datetime").select(measures)
    assert_frame_equal(stored.select(measures), expected)


def test_publish_series_writes_mmap_ready_arrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    _write_month(
        tmp_path / "parquet",
        spec,
        _time_frame([BASE_MS, BASE_MS + 60_000, BASE_MS + 120_000]),
        2024,
        1,
    )

    outcome = publish_series("time_1m", build_series_frame(spec, tmp_path / "parquet"))
    assert outcome.status == "published"

    latest = series_store_dir("time_1m") / LATEST_NAME
    assert latest.is_symlink()

    # Acceptance: one record batch, zero-copy int64 ts view, strictly increasing.
    frame = pl.read_ipc(latest, memory_map=True, rechunk=False)
    assert frame.n_chunks() == 1
    ts = frame["ts"].to_numpy(allow_copy=False)
    assert ts.dtype == "int64"
    assert list(ts) == sorted(ts)
    assert len(set(ts.tolist())) == len(ts)


def test_large_series_publishes_single_record_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Far past polars' default ~122k-row IPC batch size: a naive write_ipc splits into
    # several record batches that a memory_map=True reader exposes as multiple chunks,
    # which breaks the zero-copy ts view. stage_series' record_batch_size forces one
    # batch. (The small-frame tests above never crossed the threshold, so they missed
    # this -- it surfaced only in production on million-row series.)
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    rows = 200_000
    minute_ms = 60_000
    _write_month(
        tmp_path / "parquet",
        spec,
        _time_frame([BASE_MS + index * minute_ms for index in range(rows)]),
        2024,
        1,
    )

    outcome = publish_series("time_1m", build_series_frame(spec, tmp_path / "parquet"))
    assert outcome.status == "published"

    frame = pl.read_ipc(series_store_dir("time_1m") / LATEST_NAME, memory_map=True, rechunk=False)
    assert frame.height == rows
    assert frame.n_chunks() == 1  # one record batch even well past the split threshold
    frame["ts"].to_numpy(allow_copy=False)  # zero-copy must succeed (raises if multi-chunk)


def test_atomic_swap_keeps_pre_open_handle_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    parquet_root = tmp_path / "parquet"

    _write_month(parquet_root, spec, _time_frame([BASE_MS, BASE_MS + 60_000]), 2024, 1)
    first = publish_series("time_1m", build_series_frame(spec, parquet_root))
    assert first.status == "published"

    latest = series_store_dir("time_1m") / LATEST_NAME
    version_one = latest.resolve()

    # Open a handle BEFORE the swap; it pins the v1 inode.
    handle = pa_ipc.open_file(pa.memory_map(str(latest), "r"))
    assert handle.read_all().num_rows == 2

    # Publish a fresher version (3 rows) over the same month.
    _write_month(parquet_root, spec, _time_frame([BASE_MS, BASE_MS + 60_000, BASE_MS + 120_000]), 2024, 1)
    second = publish_series("time_1m", build_series_frame(spec, parquet_root))
    assert second.status == "published"
    assert second.version != first.version

    # The pre-opened handle still reads v1; a fresh read sees v2; v1 is retained.
    assert handle.read_all().num_rows == 2
    assert pl.read_ipc(latest, memory_map=True, rechunk=False).height == 3
    assert version_one.exists()
    assert latest.resolve() != version_one


def test_content_hash_skips_unchanged_and_is_reproducible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    parquet_root = tmp_path / "parquet"
    _write_month(parquet_root, spec, _time_frame([BASE_MS, BASE_MS + 60_000]), 2024, 1)

    first = publish_series("time_1m", build_series_frame(spec, parquet_root))
    second = publish_series("time_1m", build_series_frame(spec, parquet_root))

    assert first.status == "published"
    assert second.status == "skipped_unchanged"
    # Identical content -> identical version id (content-addressed, reproducible).
    assert second.version == first.version
    versions = list(series_store_dir("time_1m").glob("time_1m.*.arrow"))
    assert len(versions) == 1


def test_monotonic_guard_skips_staler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    parquet_root = tmp_path / "parquet"

    _write_month(parquet_root, spec, _time_frame([BASE_MS, BASE_MS + 60_000, BASE_MS + 120_000]), 2024, 1)
    assert publish_series("time_1m", build_series_frame(spec, parquet_root)).status == "published"

    # A staler snapshot (older max ts) must never flip `latest` backwards.
    _write_month(parquet_root, spec, _time_frame([BASE_MS, BASE_MS + 60_000]), 2024, 1)
    staler = publish_series("time_1m", build_series_frame(spec, parquet_root))
    assert staler.status == "skipped_not_newer"

    latest = series_store_dir("time_1m") / LATEST_NAME
    assert pl.read_ipc(latest, memory_map=True, rechunk=False).height == 3


def test_retention_keeps_min_versions_and_respects_grace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path))
    directory = series_store_dir("time_1m")
    directory.mkdir(parents=True, exist_ok=True)
    now = time.time()

    # Two stale versions (older than grace) + four recent ones; KEEP == 3.
    ages = {0: 3 * REAP_GRACE_SECONDS, 1: 2 * REAP_GRACE_SECONDS, 2: 3, 3: 2, 4: 1, 5: 0}
    paths = []
    for index, age in ages.items():
        path = directory / f"time_1m.{index:016x}.arrow"
        path.write_bytes(b"x")
        os.utime(path, (now - age, now - age))
        paths.append(path)
    latest = directory / LATEST_NAME
    os.symlink(paths[5].name, latest)

    reaped = reap_old_versions(
        directory, "time_1m", keep=RETENTION_KEEP, grace_seconds=REAP_GRACE_SECONDS, now=now
    )

    remaining = {path.name for path in directory.glob("time_1m.*.arrow")}
    # Only the two beyond-KEEP *and* older-than-grace versions are reaped.
    assert set(reaped) == {paths[0].name, paths[1].name}
    # A 4th recent version survives despite KEEP == 3 (grace protects it mid-swap).
    assert paths[2].name in remaining
    assert paths[5].name in remaining  # the live `latest` target
    assert len(remaining) == 4


def test_failed_stage_leaves_no_orphan_tmp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # ENOSPC mid-stream must not leave a hidden multi-GB orphan behind: stage_series
    # unlinks its tmp on any failure (the TTL reaper only covers hard kills).
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path / "parquet"))
    monkeypatch.setenv("LOCAL_ARROW_DIR", str(tmp_path / "arrow"))
    spec = spec_for_series("time_1m")
    _write_month(
        tmp_path / "parquet",
        spec,
        _time_frame([BASE_MS, BASE_MS + 60_000]),
        2024,
        1,
    )
    build = build_series_frame(spec, tmp_path / "parquet")

    def _boom(*args: object, **kwargs: object) -> None:
        raise OSError("No space left on device")

    monkeypatch.setattr(pl.DataFrame, "write_ipc", _boom)
    with pytest.raises(OSError, match="No space left on device"):
        stage_series("time_1m", build)
    assert list(series_store_dir("time_1m").glob(".*.tmp-stage-*")) == []

