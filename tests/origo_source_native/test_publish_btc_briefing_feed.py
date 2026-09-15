from __future__ import annotations

import hashlib
import importlib
import inspect
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from dagster import materialize

import origo.assets.publish_btc_briefing_feed as publish_btc_briefing_feed_module
from origo.assets.daily_trades_to_origo import daily_partitions
from origo.assets.publish_btc_briefing_feed import (
    FEED_SECTIONS,
    FEED_VERSION,
    _make_clickhouse_arrow_client,
    build_briefing_feed,
    publish_briefing_feed_to_huggingface,
)

DAY = date(2024, 1, 1)
DAY_START_EPOCH = 1_704_067_200
TRADES_IN_DAY = 4800
REPLACED_MINUTE_MID_PRICE = 99999.0
# Mirrors the trade generator in _insert_day_trades: trade n has quantity
# 0.001 + (n % 7) * 0.00000001 BTC, i.e. exactly 100_000 + (n % 7) satoshis.
# Computed in pure Python so the conservation test does not compare
# ClickHouse rounding against itself.
EXPECTED_TOTAL_SATS = sum(100_000 + n % 7 for n in range(TRADES_IN_DAY))


def _create_briefing_tables(origo_assets: dict[str, Any]) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_daily_spot_trades_table_origo'],
            origo_assets['create_binance_spot_klines_table_origo'],
            origo_assets['create_binance_spot_depth200_1m_table_origo'],
        ]
    )
    assert result.success


def _insert_minute_klines(
    query_origo: Callable[[str], list[tuple[Any, ...]]],
    minutes: int,
    first_minute: int = 0,
) -> None:
    query_origo(
        f"""
        INSERT INTO binance_spot_klines
            (datetime, open, high, low, close, mean, std, median, iqr, volume,
             maker_ratio, no_of_trades, open_liquidity, high_liquidity, low_liquidity,
             close_liquidity, liquidity_sum, maker_volume, maker_liquidity)
        SELECT
            toDateTime('{DAY.isoformat()} 00:00:00') + 60 * number AS datetime,
            42000 + (number % 96) AS open,
            open + 5 AS high,
            open - 5 AS low,
            open + 2 AS close,
            open AS mean,
            1.5 AS std,
            open AS median,
            1.0 AS iqr,
            2.5 AS volume,
            0.5 AS maker_ratio,
            10 AS no_of_trades,
            100.0 AS open_liquidity,
            110.0 AS high_liquidity,
            90.0 AS low_liquidity,
            105.0 AS close_liquidity,
            1000.0 AS liquidity_sum,
            1.25 AS maker_volume,
            500.0 AS maker_liquidity
        FROM numbers({first_minute}, {minutes})
        """
    )


def _insert_day_trades(query_origo: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # Quantities carry a live 8th decimal so the satoshi conversion is
    # exercised at its full precision, not only on round numbers.
    query_origo(
        f"""
        INSERT INTO binance_daily_spot_trades
            (trade_id, price, quantity, quote_quantity, timestamp,
             is_buyer_maker, is_best_match, datetime)
        SELECT
            number AS trade_id,
            42000 + (number % 50) * 0.01 AS price,
            0.001 + (number % 7) * 0.00000001 AS quantity,
            price * quantity AS quote_quantity,
            number AS timestamp,
            number % 3 = 0 AS is_buyer_maker,
            1 AS is_best_match,
            toDateTime64('{DAY.isoformat()} 00:00:00', 6) + 18 * number AS datetime
        FROM numbers({TRADES_IN_DAY})
        """
    )


def _insert_day_book_minutes(query_origo: Callable[[str], list[tuple[Any, ...]]]) -> None:
    query_origo(
        f"""
        INSERT INTO binance_spot_depth200_1m
            (datetime, source_timestamp_ms, book_mid_price, book_spread_bps,
             book_bid_depth_200_notional, book_ask_depth_200_notional, book_imbalance_200)
        SELECT
            toDateTime('{DAY.isoformat()} 00:00:00') + 60 * number AS datetime,
            toUnixTimestamp(datetime) * 1000 AS source_timestamp_ms,
            42000 + (number % 60) AS book_mid_price,
            1 + (number % 10) / 10 AS book_spread_bps,
            1000000 + 1000 * (number % 20) AS book_bid_depth_200_notional,
            1000000 - 1000 * (number % 20) AS book_ask_depth_200_notional,
            (book_bid_depth_200_notional - book_ask_depth_200_notional)
              / (book_bid_depth_200_notional + book_ask_depth_200_notional) AS book_imbalance_200
        FROM numbers(1440)
        """
    )


def _insert_adjacent_day_rows(query_origo: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # One row per table on each side of the day, sitting exactly on or past
    # the window edges. Every one of them must stay out of every section, so
    # the assertions on counts and totals below double as proof that the
    # day-window predicates in the SQL files are load-bearing.
    for kline_datetime in ('2023-12-31 23:59:00', '2024-01-02 00:00:00'):
        query_origo(
            f"""
            INSERT INTO binance_spot_klines
                (datetime, open, high, low, close, mean, std, median, iqr, volume,
                 maker_ratio, no_of_trades, open_liquidity, high_liquidity, low_liquidity,
                 close_liquidity, liquidity_sum, maker_volume, maker_liquidity)
            SELECT toDateTime('{kline_datetime}'), 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0,
                   0.0, 1.0, 0.5, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 0.5, 0.5
            """
        )
    for trade_datetime in ('2023-12-31 23:59:59.999999', '2024-01-02 00:00:00.000000'):
        query_origo(
            f"""
            INSERT INTO binance_daily_spot_trades
                (trade_id, price, quantity, quote_quantity, timestamp,
                 is_buyer_maker, is_best_match, datetime)
            SELECT 999999999, 42000.0, 9.0, 378000.0, 0, 0, 1,
                   toDateTime64('{trade_datetime}', 6)
            """
        )
    for book_datetime in ('2023-12-31 23:59:00', '2024-01-02 00:00:00'):
        query_origo(
            f"""
            INSERT INTO binance_spot_depth200_1m
                (datetime, source_timestamp_ms, book_mid_price, book_spread_bps,
                 book_bid_depth_200_notional, book_ask_depth_200_notional, book_imbalance_200)
            SELECT toDateTime('{book_datetime}'), 1, 1.0, 1.0, 1.0, 1.0, 0.0
            """
        )


def _insert_replaced_book_minute(query_origo: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # Re-insert the day's first minute with a higher ReplacingMergeTree
    # version and a sentinel mid price: the feed must carry the replacement
    # exactly once, which only holds if the book queries read FINAL.
    query_origo(
        f"""
        INSERT INTO binance_spot_depth200_1m
            (datetime, source_timestamp_ms, book_mid_price, book_spread_bps,
             book_bid_depth_200_notional, book_ask_depth_200_notional, book_imbalance_200)
        SELECT toDateTime('{DAY.isoformat()} 00:00:00'), {DAY_START_EPOCH * 1000 + 1},
               {REPLACED_MINUTE_MID_PRICE}, 1.0, 1000000.0, 1000000.0, 0.0
        """
    )


def _populate_complete_day(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1440)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)
    _insert_adjacent_day_rows(query_origo)
    _insert_replaced_book_minute(query_origo)


def _build_feed_for_day(day: date) -> dict[str, Any]:
    client = _make_clickhouse_arrow_client()
    try:
        return build_briefing_feed(client, day)
    finally:
        client.close()


def test_build_briefing_feed_signature_is_client_and_day() -> None:
    signature = inspect.signature(build_briefing_feed)
    assert list(signature.parameters) == ['client', 'day']
    assert (
        signature.parameters['client'].annotation
        is publish_btc_briefing_feed_module._ClickHouseArrowClientProtocol
    )
    assert signature.parameters['day'].annotation is date
    assert signature.return_annotation == dict[str, object]


def test_the_feed_declares_its_version_and_sections() -> None:
    assert FEED_VERSION == 'btc_briefing/2'
    assert sorted(FEED_SECTIONS) == [
        'bars_15m',
        'bars_1d',
        'book_percentiles',
        'book_series',
        'book_sessions',
        'volume_at_price',
    ]


def test_the_book_sections_are_built_from_the_depth200_projection() -> None:
    for sql_file_name in (
        'briefing_book_percentiles.sql',
        'briefing_book_series.sql',
        'briefing_book_sessions.sql',
    ):
        query = (publish_btc_briefing_feed_module._SQL_DIR / sql_file_name).read_text(
            encoding='utf-8'
        )
        assert query.count('binance_spot_depth200_1m') == 1
        assert 'binance_spot_depth20_1m' not in query


def test_the_book_metrics_are_named_for_the_depth_they_measure(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)

    feed = _build_feed_for_day(DAY)

    assert feed['feed_version'] == FEED_VERSION
    assert feed['day'] == DAY.isoformat()
    for section in FEED_SECTIONS:
        assert section in feed
        assert len(feed[section]) > 0
    assert {row['metric'] for row in feed['book_percentiles']} == {
        'book_mid_price',
        'book_spread_bps',
        'book_bid_depth_200_notional',
        'book_ask_depth_200_notional',
        'book_imbalance_200',
    }
    assert set(feed['book_series'][0]) == {
        'minute_start',
        'book_mid_price',
        'book_spread_bps',
        'book_bid_depth_200_notional',
        'book_ask_depth_200_notional',
        'book_imbalance_200',
    }
    assert set(feed['book_sessions'][0]) == {
        'session_start',
        'minutes',
        'open_mid_price',
        'close_mid_price',
        'avg_spread_bps',
        'avg_bid_depth_200_notional',
        'avg_ask_depth_200_notional',
        'avg_imbalance_200',
    }


def test_bars_cover_the_day_with_no_short_bar(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)

    feed = _build_feed_for_day(DAY)

    assert len(feed['bars_15m']) == 96
    assert all(bar['source_minutes'] == 15 for bar in feed['bars_15m'])
    assert len(feed['bars_1d']) == 1
    assert feed['bars_1d'][0]['source_minutes'] == 1440


def test_volume_at_price_conserves_in_integer_units(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)

    feed = _build_feed_for_day(DAY)
    volume_at_price = feed['volume_at_price']

    for row in volume_at_price:
        assert isinstance(row['taker_buy_sats'], int)
        assert isinstance(row['taker_sell_sats'], int)
        assert isinstance(row['total_sats'], int)
        assert row['taker_buy_sats'] + row['taker_sell_sats'] == row['total_sats']

    assert sum(row['total_sats'] for row in volume_at_price) == EXPECTED_TOTAL_SATS
    assert sum(row['trades'] for row in volume_at_price) == TRADES_IN_DAY


def test_incomplete_day_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1439)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    with pytest.raises(RuntimeError, match='refusing to build a short briefing feed'):
        _build_feed_for_day(DAY)


def test_time_fields_are_declared_epoch_seconds(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)

    feed = _build_feed_for_day(DAY)

    assert [bar['bar_start'] for bar in feed['bars_15m'][:2]] == [
        DAY_START_EPOCH,
        DAY_START_EPOCH + 900,
    ]
    assert feed['bars_1d'][0]['bar_start'] == DAY_START_EPOCH
    assert feed['book_series'][0]['minute_start'] == DAY_START_EPOCH
    assert [session['session_start'] for session in feed['book_sessions']] == [
        DAY_START_EPOCH,
        DAY_START_EPOCH + 28800,
        DAY_START_EPOCH + 57600,
    ]
    assert all(
        isinstance(value, int)
        for bar in feed['bars_15m']
        for value in [bar['bar_start']]
    )


def test_day_window_excludes_adjacent_day_rows(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    # _populate_complete_day plants rows on 2023-12-31 and 2024-01-02 in all
    # three source tables, sitting exactly on the window edges: every count
    # and total below only holds if each SQL file's day predicate excludes
    # them.
    _populate_complete_day(origo_assets, query_origo)

    feed = _build_feed_for_day(DAY)

    assert len(feed['bars_15m']) == 96
    assert feed['bars_1d'][0]['source_minutes'] == 1440
    assert sum(row['trades'] for row in feed['volume_at_price']) == TRADES_IN_DAY
    assert sum(row['total_sats'] for row in feed['volume_at_price']) == EXPECTED_TOTAL_SATS
    assert len(feed['book_series']) == 1440
    assert [session['minutes'] for session in feed['book_sessions']] == [480, 480, 480]
    assert all(row['minutes'] == 1440 for row in feed['book_percentiles'])


def test_book_reads_collapse_replaced_minutes_via_final(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    # _populate_complete_day re-inserts the day's first minute with a higher
    # ReplacingMergeTree version and a sentinel mid price: the series must
    # carry the replacement exactly once, and the session and percentile
    # minute counts must not count the superseded row.
    _populate_complete_day(origo_assets, query_origo)

    feed = _build_feed_for_day(DAY)

    assert len(feed['book_series']) == 1440
    assert feed['book_series'][0]['book_mid_price'] == REPLACED_MINUTE_MID_PRICE
    assert [session['minutes'] for session in feed['book_sessions']] == [480, 480, 480]
    assert all(row['minutes'] == 1440 for row in feed['book_percentiles'])


def test_duplicated_minute_cannot_mask_a_missing_one(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    # Minute 1439 is missing and minute 1425 is duplicated, so the last 15m
    # bar holds 15 rows but only 14 distinct minutes: a plain count() would
    # report it complete while double counting the duplicate.
    _insert_minute_klines(query_origo, 1439)
    _insert_minute_klines(query_origo, 1, first_minute=1425)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    with pytest.raises(RuntimeError, match='refusing to build a short briefing feed'):
        _build_feed_for_day(DAY)


def test_duplicated_minute_alone_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1440)
    _insert_minute_klines(query_origo, 1, first_minute=1425)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    with pytest.raises(RuntimeError, match='duplicated 1m source rows'):
        _build_feed_for_day(DAY)


def test_job_is_registered_in_definitions() -> None:
    definitions_module = importlib.import_module('origo.definitions')
    assert 'publish_btc_briefing_feed_job' in [
        job.name for job in definitions_module.defs.jobs
    ]


def _install_recording_hf_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    existing_latest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    uploaded: dict[str, Any] = {}

    class RecordingHfApi:
        def __init__(self, token: str) -> None:
            uploaded['token'] = token

        def repo_exists(self, *, repo_id: str, repo_type: str) -> bool:
            return existing_latest is not None

        def file_exists(self, *, repo_id: str, filename: str, repo_type: str) -> bool:
            return existing_latest is not None

        def hf_hub_download(self, *, repo_id: str, filename: str, repo_type: str) -> str:
            latest_path = tmp_path / 'existing_latest.json'
            latest_path.write_text(json.dumps(existing_latest))
            return str(latest_path)

        def create_repo(self, *, repo_id: str, repo_type: str, exist_ok: bool) -> None:
            uploaded['repo_id'] = repo_id
            uploaded['repo_type'] = repo_type
            uploaded['exist_ok'] = exist_ok

        def upload_folder(
            self,
            *,
            folder_path: str,
            repo_id: str,
            repo_type: str,
            commit_message: str,
        ) -> None:
            folder = Path(folder_path)
            uploaded['upload_repo_id'] = repo_id
            uploaded['commit_message'] = commit_message
            uploaded['files'] = sorted(path.name for path in folder.iterdir())
            if (folder / 'latest.json').exists():
                uploaded['latest'] = json.loads((folder / 'latest.json').read_text())
                uploaded['readme'] = (folder / 'README.md').read_text()
                feed_file = uploaded['latest']['file_name']
                uploaded['feed_bytes'] = (folder / feed_file).read_bytes()
            else:
                uploaded['feed_bytes'] = (folder / uploaded['files'][0]).read_bytes()

    monkeypatch.setattr(publish_btc_briefing_feed_module, 'HfApi', RecordingHfApi)
    return uploaded


def test_asset_is_daily_partitioned() -> None:
    assert (
        publish_btc_briefing_feed_module.publish_btc_briefing_feed.partitions_def
        is daily_partitions
    )


def test_dataset_card_scopes_the_contract_to_each_file() -> None:
    card = publish_btc_briefing_feed_module._build_dataset_card(
        day=DAY.isoformat(),
        file_name='btc_briefing_20240101.json',
        sha256='abc123',
    )

    assert 'historical files keep the contract under which they were' in card
    assert f'latest snapshot uses `{FEED_VERSION}`' in card


def test_sensor_is_registered_in_definitions() -> None:
    definitions_module = importlib.import_module('origo.definitions')
    assert any(
        sensor.name == 'publish_btc_briefing_feed_sensor'
        for sensor in definitions_module.defs.sensors
    )


def test_publish_uploads_feed_snapshot_with_latest_pointer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)
    feed = _build_feed_for_day(DAY)
    uploaded = _install_recording_hf_api(monkeypatch, tmp_path)

    result = publish_briefing_feed_to_huggingface(
        feed, repo_id='test/btc-briefing', token='test-token'
    )

    assert uploaded['token'] == 'test-token'
    assert uploaded['repo_id'] == 'test/btc-briefing'
    assert uploaded['upload_repo_id'] == 'test/btc-briefing'
    assert uploaded['repo_type'] == 'dataset'
    assert uploaded['files'] == ['README.md', 'btc_briefing_20240101.json', 'latest.json']
    assert json.loads(uploaded['feed_bytes']) == feed
    assert result['file_name'] == 'btc_briefing_20240101.json'
    assert result['sha256'] == hashlib.sha256(uploaded['feed_bytes']).hexdigest()


def test_latest_pointer_declares_version_and_day(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)
    feed = _build_feed_for_day(DAY)
    uploaded = _install_recording_hf_api(monkeypatch, tmp_path)

    publish_briefing_feed_to_huggingface(
        feed, repo_id='test/btc-briefing', token='test-token'
    )

    assert uploaded['latest']['feed_version'] == 'btc_briefing/2'
    assert uploaded['latest']['day'] == DAY.isoformat()
    assert uploaded['latest']['file_name'] == 'btc_briefing_20240101.json'
    assert (
        uploaded['latest']['sha256']
        == hashlib.sha256(uploaded['feed_bytes']).hexdigest()
    )


def test_backfill_of_older_day_does_not_move_latest_pointer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)
    feed = _build_feed_for_day(DAY)
    uploaded = _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        existing_latest={'feed_version': FEED_VERSION, 'day': '2024-01-02'},
    )

    result = publish_briefing_feed_to_huggingface(
        feed, repo_id='test/btc-briefing', token='test-token'
    )

    assert uploaded['files'] == ['btc_briefing_20240101.json']
    assert result['latest_updated'] is False


def test_same_day_republish_refreshes_latest_pointer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)
    feed = _build_feed_for_day(DAY)
    uploaded = _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        existing_latest={'feed_version': FEED_VERSION, 'day': DAY.isoformat()},
    )

    result = publish_briefing_feed_to_huggingface(
        feed, repo_id='test/btc-briefing', token='test-token'
    )

    assert result['latest_updated'] is True
    assert uploaded['latest']['sha256'] == hashlib.sha256(uploaded['feed_bytes']).hexdigest()


def test_newer_day_advances_latest_pointer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)
    feed = _build_feed_for_day(DAY)
    uploaded = _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        existing_latest={'feed_version': FEED_VERSION, 'day': '2023-12-31'},
    )

    result = publish_briefing_feed_to_huggingface(
        feed, repo_id='test/btc-briefing', token='test-token'
    )

    assert uploaded['latest']['day'] == DAY.isoformat()
    assert result['latest_updated'] is True


def test_backfill_partition_publishes_that_day(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_day(origo_assets, query_origo)
    uploaded = _install_recording_hf_api(monkeypatch, tmp_path)
    monkeypatch.setenv('HF_TOKEN', 'test-token')

    result = materialize(
        [publish_btc_briefing_feed_module.publish_btc_briefing_feed],
        partition_key=DAY.isoformat(),
    )

    assert result.success
    assert uploaded['latest']['day'] == DAY.isoformat()
    assert uploaded['latest']['file_name'] == 'btc_briefing_20240101.json'
