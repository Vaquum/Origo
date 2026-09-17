from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import polars as pl
import pytest
from dagster import materialize
from polars.testing import assert_frame_equal

# origo.assets.__init__ reads CLICKHOUSE_PASSWORD at import time; provide a
# placeholder so test collection succeeds. ClickHouse-backed tests receive the real
# container password from the origo fixtures (monkeypatch.setenv) before any query runs.
os.environ.setdefault("CLICKHOUSE_PASSWORD", "import-guard")

from origo.assets.publish_binance_spot_klines_to_mount import (  # noqa: E402
    MOUNT_EXPORT_ASSETS,
    SPECS,
    MountKlineSpec,
    month_path,
    months_for_run,
    write_month_atomic,
)
from origo.query.binance_spot_kline_rollups import dollar_month, time_month  # noqa: E402

from .helpers import ORIGO_DATABASE  # noqa: E402

JANUARY_2024_START = "2024-01-01 00:00:00"
FEBRUARY_2024_START = "2024-02-01 00:00:00"
JANUARY_2024 = datetime(2024, 1, 1, tzinfo=UTC)
FEBRUARY_2024 = datetime(2024, 2, 1, tzinfo=UTC)
JANUARY_2_2024 = datetime(2024, 1, 2, tzinfo=UTC)
_DATETIME_COLUMNS = ["start_datetime", "end_datetime"]


def _hf_time_projection() -> Callable[..., pl.DataFrame]:
    module = importlib.import_module(
        "origo.utils.publish_binance_spot_kline_snapshot_to_huggingface"
    )
    return getattr(module, "_get_binance_spot_klines_from_1m_projection")


def _hf_dollar_projection() -> Callable[..., pl.DataFrame]:
    module = importlib.import_module(
        "origo.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface"
    )
    return getattr(module, "_get_binance_spot_dollar_klines")


def _spec(name: str) -> MountKlineSpec:
    return next(s for s in SPECS if s.name == name)


def _materialize_time_base(origo_assets: dict[str, Any], *partition_keys: str) -> None:
    for partition_key in partition_keys or ("2024-01-01",):
        result = materialize(
            [
                origo_assets["create_origo_database"],
                origo_assets["create_binance_daily_spot_trades_table_origo"],
                origo_assets["create_binance_spot_klines_table_origo"],
                origo_assets["create_binance_spot_latest_tables_origo"],
                origo_assets["insert_daily_binance_spot_trades_to_origo"],
                origo_assets["refresh_binance_spot_klines_origo"],
            ],
            partition_key=partition_key,
        )
        assert result.success


def test_mount_export_factory_registers_twelve_series() -> None:
    assert len(SPECS) == 12
    assert len(MOUNT_EXPORT_ASSETS) == 12

    asset_keys = {asset.key.to_user_string() for asset in MOUNT_EXPORT_ASSETS}
    expected_keys = {f"export_{spec.name}_to_mount" for spec in SPECS}
    assert asset_keys == expected_keys

    assert {spec.family for spec in SPECS} == {"time", "dollar"}
    assert {s.sub_path for s in SPECS if s.family == "time"} == {
        "time/1m",
        "time/15m",
        "time/30m",
        "time/1h",
        "time/2h",
        "time/4h",
    }
    assert {s.sub_path for s in SPECS if s.family == "dollar"} == {
        "dollar/1M",
        "dollar/15M",
        "dollar/30M",
        "dollar/60M",
        "dollar/120M",
        "dollar/240M",
    }


def test_mount_export_months_for_run() -> None:
    assert months_for_run("tick", datetime(2024, 3, 15)) == [(2024, 2), (2024, 3)]
    assert months_for_run("backfill", datetime(2020, 3, 1)) == [(2020, 1), (2020, 2), (2020, 3)]


def test_time_month_rollup_matches_hf_projection(origo_assets: dict[str, Any]) -> None:
    _materialize_time_base(origo_assets)

    actual = time_month(interval_minutes=15, year=2024, month=1)
    expected = _hf_time_projection()(
        kline_size_seconds=900,
        start_date_limit=JANUARY_2024_START,
        end_date_limit=FEBRUARY_2024_START,
        table_name="binance_spot_klines",
        database_name=ORIGO_DATABASE,
    )

    assert actual.height > 0
    assert_frame_equal(actual, expected)


def test_time_month_splices_base_and_rolling_latest(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[object, ...]]],
) -> None:
    # base holds day 1 + day 2; capture the canonical both-days aggregation
    _materialize_time_base(origo_assets, "2024-01-01", "2024-01-02")
    expected = _hf_time_projection()(
        kline_size_seconds=900,
        start_date_limit=JANUARY_2024_START,
        end_date_limit=FEBRUARY_2024_START,
        table_name="binance_spot_klines",
        database_name=ORIGO_DATABASE,
    )

    # move day 2 into the rolling-latest table and drop it from the base, so day 2 can
    # only appear in the rollup via the base_cut UNION latest splice. (REMOVE TTL first:
    # the rolling table TTLs rows older than 2 days and the fixture is from 2024.)
    query_origo("ALTER TABLE binance_spot_klines_latest REMOVE TTL")
    query_origo(
        "INSERT INTO binance_spot_klines_latest "
        "SELECT * FROM binance_spot_klines WHERE toDate(datetime) = toDate('2024-01-02')"
    )
    query_origo(
        "ALTER TABLE binance_spot_klines DELETE "
        "WHERE toDate(datetime) = toDate('2024-01-02') SETTINGS mutations_sync = 2"
    )

    actual = time_month(interval_minutes=15, year=2024, month=1)

    # the splice reconstructs the full month from base(day 1) + latest(day 2)
    assert_frame_equal(actual, expected)
    # day 2 is present only via the latest branch, and the seam has no duplicate interval
    assert actual["datetime"].max() >= JANUARY_2_2024
    assert actual["datetime"].n_unique() == actual.height


def test_dollar_month_rollup_matches_hf_day_scoped(origo_assets: dict[str, Any]) -> None:
    result = materialize(
        [
            origo_assets["create_origo_database"],
            origo_assets["create_binance_daily_spot_trades_table_origo"],
            origo_assets["create_binance_spot_dollar_klines_table_origo"],
            origo_assets["create_binance_spot_latest_tables_origo"],
            origo_assets["insert_daily_binance_spot_trades_to_origo"],
            origo_assets["refresh_binance_spot_dollar_klines_origo"],
        ],
        partition_key="2024-01-01",
    )
    assert result.success

    actual = dollar_month(ratio=15, year=2024, month=1)
    expected = _hf_dollar_projection()(
        dollar_size=15_000_000.0,
        start_date_limit=JANUARY_2024_START,
        end_date_limit=FEBRUARY_2024_START,
        table_name="binance_spot_dollar_klines",
        database_name=ORIGO_DATABASE,
    )

    assert actual.height > 0
    # dollar_month and the HF dollar export now share identical timestamp handling
    # (toDateTime64(_, 3) -> Datetime("ms", "UTC")), so the full frames match -- including the
    # datetime columns that previously had to be dropped while the HF export mis-scaled
    # second-precision timestamps to ~1970 under polars >=1.40.
    assert_frame_equal(actual, expected)
    assert JANUARY_2024 <= actual["start_datetime"].min() < FEBRUARY_2024
    assert JANUARY_2024 <= actual["end_datetime"].max() < FEBRUARY_2024


def test_month_partition_path_atomic_roundtrip(
    origo_assets: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path))
    _materialize_time_base(origo_assets)

    spec = _spec("time_15m")
    df = time_month(interval_minutes=15, year=2024, month=1)
    assert df.height > 0

    outcome = write_month_atomic(df, spec, 2024, 1)
    assert outcome == "written"

    target = month_path(spec.sub_path, 2024, 1)
    assert target == tmp_path / "time" / "15m" / "2024" / "01.parquet"
    assert target.exists()
    assert_frame_equal(pl.read_parquet(target), df)
    assert list(target.parent.glob(".01.parquet.partial-*")) == []


def test_monotonic_writer_skips_stale_replace(
    origo_assets: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    monkeypatch.setenv("LOCAL_PARQUET_DIR", str(tmp_path))
    _materialize_time_base(origo_assets, "2024-01-01", "2024-01-02")

    spec = _spec("time_15m")
    df_full = time_month(interval_minutes=15, year=2024, month=1)
    assert df_full.height >= 2
    df_older = df_full.head(df_full.height - 1)

    # an older snapshot writes first, then the newer one replaces it
    assert write_month_atomic(df_older, spec, 2024, 1) == "written"
    assert write_month_atomic(df_full, spec, 2024, 1) == "written"

    target = month_path(spec.sub_path, 2024, 1)
    assert_frame_equal(pl.read_parquet(target), df_full)

    # a slow, stale tick that finishes last must NOT regress the file
    assert write_month_atomic(df_older, spec, 2024, 1) == "skipped_not_newer"
    assert_frame_equal(pl.read_parquet(target), df_full)


def test_dollar_open_day_from_raw_matches_base_refresh(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[object, ...]]],
) -> None:
    # base + raw hold only 2024-01-01; the dollar base is finalized through day 1.
    first_day = materialize(
        [
            origo_assets["create_origo_database"],
            origo_assets["create_binance_daily_spot_trades_table_origo"],
            origo_assets["create_binance_spot_dollar_klines_table_origo"],
            origo_assets["create_binance_spot_latest_tables_origo"],
            origo_assets["insert_daily_binance_spot_trades_to_origo"],
            origo_assets["refresh_binance_spot_dollar_klines_origo"],
        ],
        partition_key="2024-01-01",
    )
    assert first_day.success

    # 2024-01-02 raw trades land (real fixture), but the dollar base is NOT refreshed for it,
    # so day 2 is the still-open day the mirror must recompute from the rolling raw table.
    second_day_raw = materialize(
        [
            origo_assets["create_origo_database"],
            origo_assets["create_binance_daily_spot_trades_table_origo"],
            origo_assets["insert_daily_binance_spot_trades_to_origo"],
        ],
        partition_key="2024-01-02",
    )
    assert second_day_raw.success
    # the rolling raw table TTLs rows older than 2 days; the 2024 fixture would be purged
    # immediately, so drop the TTL on the ephemeral test table before backfilling day 2.
    query_origo("ALTER TABLE binance_spot_trades_latest REMOVE TTL")
    query_origo(
        """
        INSERT INTO binance_spot_trades_latest
            (minute_start, trade_id, price, quantity, quote_quantity, timestamp,
             is_buyer_maker, is_best_match, datetime)
        SELECT
            toStartOfMinute(datetime), trade_id, price, quantity, quote_quantity, timestamp,
            is_buyer_maker, is_best_match, datetime
        FROM binance_daily_spot_trades
        WHERE toDate(datetime) = toDate('2024-01-02')
        """
    )

    # day 1 from the base + day 2 recomputed from the rolling raw trades
    actual = dollar_month(ratio=1, year=2024, month=1)

    # finalize day 2 in the base and read the canonical answer for both days
    second_day_base = materialize(
        [
            origo_assets["create_origo_database"],
            origo_assets["create_binance_daily_spot_trades_table_origo"],
            origo_assets["create_binance_spot_dollar_klines_table_origo"],
            origo_assets["insert_daily_binance_spot_trades_to_origo"],
            origo_assets["refresh_binance_spot_dollar_klines_origo"],
        ],
        partition_key="2024-01-02",
    )
    assert second_day_base.success
    expected = _hf_dollar_projection()(
        dollar_size=1_000_000.0,
        start_date_limit=JANUARY_2024_START,
        end_date_limit=FEBRUARY_2024_START,
        table_name="binance_spot_dollar_klines",
        database_name=ORIGO_DATABASE,
    )

    assert actual.height > 0
    # the open-day recompute must produce the same bar values as the finalized base build
    assert_frame_equal(actual.drop(_DATETIME_COLUMNS), expected.drop(_DATETIME_COLUMNS))
    # and the recomputed day-2 bars must carry day-2 datetimes (not regress to day 1 or epoch)
    assert actual["start_datetime"].max() >= JANUARY_2_2024
    assert JANUARY_2024 <= actual["start_datetime"].min() < FEBRUARY_2024
