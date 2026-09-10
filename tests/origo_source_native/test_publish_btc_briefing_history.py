from __future__ import annotations

import hashlib
import importlib
import inspect
import json
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
from dagster import (
    AssetMaterialization,
    DagsterInstance,
    MaterializeResult,
    RunRequest,
    SkipReason,
    asset,
    build_sensor_context,
    materialize,
)

import origo.assets.publish_btc_briefing_history as publish_btc_briefing_history_module
from origo.assets.daily_trades_to_origo import daily_partitions
from origo.assets.publish_btc_briefing_history import (
    HISTORY_1D_DAYS,
    HISTORY_15M_DAYS,
    HISTORY_FILE_NAME,
    HISTORY_SECTIONS,
    HISTORY_VERSION,
    _make_clickhouse_arrow_client,
    build_briefing_history,
    publish_briefing_history_to_huggingface,
)

THROUGH_DAY = date(2024, 1, 1)
THROUGH_DAY_EPOCH = 1_704_067_200
SPAN_15M_START = THROUGH_DAY - timedelta(days=HISTORY_15M_DAYS)
SPAN_15M_START_EPOCH = THROUGH_DAY_EPOCH - HISTORY_15M_DAYS * 86400
SPAN_1D_START_EPOCH = THROUGH_DAY_EPOCH - HISTORY_1D_DAYS * 86400
SPAN_MINUTES = HISTORY_15M_DAYS * 1440
BARS_15M_IN_SPAN = HISTORY_15M_DAYS * 96
# The 1d span opens this many minutes after the 15m span, so a bar's expected
# OHLC can be computed from its minute index in the generator below.
MINUTES_BEFORE_1D_SPAN = (HISTORY_15M_DAYS - HISTORY_1D_DAYS) * 1440
# A 15m bar inside the days only the 15m span covers: dropping it leaves every
# other 15m bar and the whole 1d span complete.
ABSENT_BAR_FIRST_MINUTE = 6000


def _minute_price(minute: int) -> float:
    """The open the generator below gives minute ``minute`` of the span.

    The ramp makes every minute distinct, and the two bumps make the price
    oscillate inside every bar: the first minute of a bar is never its
    cheapest and the last is never its dearest. A rollup that reached for
    min(open)/max(close) instead of the time-ordered argMin/argMax would
    therefore report different numbers than the ones asserted below.
    """
    bumps = {0: 100.0, 7: 200.0}
    return 42000.0 + minute + bumps.get(minute % 15, 0.0)


def _expected_bar(first_minute: int, minutes: int) -> dict[str, float]:
    """The OHLCV the generator below implies for a bar, computed in pure Python.

    Minute n carries open ``_minute_price(n)``, high open + 5, low open - 5,
    close open + 2 and volume 2.5, so a bar's four prices and its volume are
    known without asking ClickHouse what it produced.
    """
    prices = [_minute_price(minute) for minute in range(first_minute, first_minute + minutes)]
    return {
        'open': prices[0],
        'high': max(prices) + 5,
        'low': min(prices) - 5,
        'close': prices[-1] + 2,
        'volume': 2.5 * minutes,
    }


def _create_history_tables(origo_assets: dict[str, Any]) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_klines_table_origo'],
        ]
    )
    assert result.success


def _insert_span_minute_klines(
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
            toDateTime('{SPAN_15M_START.isoformat()} 00:00:00') + 60 * number AS datetime,
            42000 + number + multiIf(number % 15 = 0, 100, number % 15 = 7, 200, 0) AS open,
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


def _insert_kline_at(
    query_origo: Callable[[str], list[tuple[Any, ...]]],
    kline_datetime: str,
) -> None:
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


def _populate_complete_span(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_history_tables(origo_assets)
    _insert_span_minute_klines(query_origo, SPAN_MINUTES)
    # One row on each side of the span, sitting exactly on the edges: folded
    # into the grid either one would make a bar short or spurious, so every
    # count below doubles as proof that the SQL's span predicate is
    # load-bearing and that through_day itself is excluded.
    _insert_kline_at(query_origo, f'{(SPAN_15M_START - timedelta(days=1)).isoformat()} 23:59:00')
    _insert_kline_at(query_origo, f'{THROUGH_DAY.isoformat()} 00:00:00')


def _build_history_through(through_day: date) -> dict[str, Any]:
    client = _make_clickhouse_arrow_client()
    try:
        return build_briefing_history(client, through_day)
    finally:
        client.close()


def _content_sha256(history: dict[str, Any]) -> str:
    payload = {key: value for key, value in history.items() if key != 'sha256'}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()


def test_history_declares_its_version_and_sections() -> None:
    assert HISTORY_VERSION == 'btc_briefing_history/1'
    assert sorted(HISTORY_SECTIONS) == ['bars_15m', 'bars_1d']
    # The file name is the dataset's public contract: consumers fetch this
    # exact path, so renaming it is a breaking change, not an implementation
    # detail.
    assert HISTORY_FILE_NAME == 'btc_briefing_history.json'


def test_build_briefing_history_signature_is_pinned() -> None:
    signature = inspect.signature(build_briefing_history)
    assert list(signature.parameters) == ['client', 'through_day']
    assert (
        signature.parameters['client'].annotation
        is publish_btc_briefing_history_module._ClickHouseArrowClientProtocol
    )
    assert signature.parameters['through_day'].annotation is date
    assert signature.return_annotation == dict[str, object]


def test_history_spans_the_days_the_briefing_fits_on(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    assert HISTORY_15M_DAYS == 70
    assert HISTORY_1D_DAYS == 61

    _populate_complete_span(origo_assets, query_origo)

    history = _build_history_through(THROUGH_DAY)

    assert history['version'] == HISTORY_VERSION
    assert history['through_day'] == THROUGH_DAY.isoformat()
    assert len(history['bars_15m']) == BARS_15M_IN_SPAN
    assert len(history['bars_1d']) == HISTORY_1D_DAYS
    assert all(bar['source_minutes'] == 15 for bar in history['bars_15m'])
    assert all(bar['source_minutes'] == 1440 for bar in history['bars_1d'])


def test_bar_grid_is_midnight_aligned_across_the_whole_span(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_span(origo_assets, query_origo)

    history = _build_history_through(THROUGH_DAY)
    bars_15m = history['bars_15m']
    bars_1d = history['bars_1d']

    assert all(bar['bar_start'] % 900 == 0 for bar in bars_15m)
    assert bars_15m[0]['bar_start'] == SPAN_15M_START_EPOCH
    assert bars_15m[-1]['bar_start'] == THROUGH_DAY_EPOCH - 900
    assert [bar['bar_start'] for bar in bars_15m] == [
        SPAN_15M_START_EPOCH + 900 * index for index in range(BARS_15M_IN_SPAN)
    ]

    assert all(bar['bar_start'] % 86400 == 0 for bar in bars_1d)
    assert bars_1d[0]['bar_start'] == SPAN_1D_START_EPOCH
    assert bars_1d[-1]['bar_start'] == THROUGH_DAY_EPOCH - 86400


def test_bars_carry_the_rollup_of_their_own_minutes(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    # The expected prices come from the generator's definition, computed in
    # Python: a grid anchored one bucket off, or a rollup that mixed minutes
    # from the neighbouring bar, would miss them.
    _populate_complete_span(origo_assets, query_origo)

    history = _build_history_through(THROUGH_DAY)
    bars_15m = history['bars_15m']
    bars_1d = history['bars_1d']

    for bar, expected in [
        (bars_15m[0], _expected_bar(0, 15)),
        (bars_15m[-1], _expected_bar(SPAN_MINUTES - 15, 15)),
        (bars_1d[0], _expected_bar(MINUTES_BEFORE_1D_SPAN, 1440)),
        (bars_1d[-1], _expected_bar(SPAN_MINUTES - 1440, 1440)),
    ]:
        # The cheapest open in a bar is low + 5 and the dearest close is
        # high - 3. Both differ from the time-ordered open and close, so these
        # two lines are what let the assertions below tell argMin/argMax from
        # a plain min/max.
        assert expected['open'] != expected['low'] + 5
        assert expected['close'] != expected['high'] - 3
        for field, value in expected.items():
            assert bar[field] == pytest.approx(value), field


def test_span_short_by_one_bar_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_history_tables(origo_assets)
    _insert_span_minute_klines(query_origo, SPAN_MINUTES - 15)

    # 70 days x 96 bars: matching the count message rather than the shared
    # 'refusing to build a short briefing history' tail keeps the per-bar
    # checks from standing in for the bar-count check.
    with pytest.raises(RuntimeError, match='has 6719 bars_15m bars where 6720 are required'):
        _build_history_through(THROUGH_DAY)


def test_bar_absent_from_the_15m_only_days_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    # One whole 15m bar is missing from the nine days that the 15m span covers
    # and the 1d span does not. Every bar that is present is built from its
    # full 15 minutes and the 1d span is untouched, so the bar-count check is
    # the only check that can fire.
    _create_history_tables(origo_assets)
    _insert_span_minute_klines(query_origo, ABSENT_BAR_FIRST_MINUTE)
    _insert_span_minute_klines(
        query_origo,
        SPAN_MINUTES - ABSENT_BAR_FIRST_MINUTE - 15,
        first_minute=ABSENT_BAR_FIRST_MINUTE + 15,
    )

    with pytest.raises(RuntimeError, match='has 6719 bars_15m bars where 6720 are required'):
        _build_history_through(THROUGH_DAY)


def test_single_missing_minute_inside_the_span_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    # Minute 20_000 sits in the middle of the span: the bar count is still
    # exactly right, so only the per-bar source_minutes check can catch it.
    _create_history_tables(origo_assets)
    _insert_span_minute_klines(query_origo, 20_000)
    _insert_span_minute_klines(query_origo, SPAN_MINUTES - 20_001, first_minute=20_001)

    with pytest.raises(RuntimeError, match='distinct source minutes'):
        _build_history_through(THROUGH_DAY)


def test_duplicated_minute_inside_the_span_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_history_tables(origo_assets)
    _insert_span_minute_klines(query_origo, SPAN_MINUTES)
    _insert_span_minute_klines(query_origo, 1, first_minute=20_000)

    with pytest.raises(RuntimeError, match='duplicated 1m source rows'):
        _build_history_through(THROUGH_DAY)


def test_history_hashes_to_its_own_content(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_span(origo_assets, query_origo)

    history = _build_history_through(THROUGH_DAY)

    assert history['sha256'] == _content_sha256(history)
    tampered = dict(history)
    tampered['bars_15m'] = [
        {**history['bars_15m'][0], 'close': 1.0},
        *history['bars_15m'][1:],
    ]
    assert _content_sha256(tampered) != history['sha256']


def test_asset_is_daily_partitioned() -> None:
    assert (
        publish_btc_briefing_history_module.publish_btc_briefing_history.partitions_def
        is daily_partitions
    )


def test_history_job_is_registered() -> None:
    definitions_module = importlib.import_module('origo.definitions')
    assert 'publish_btc_briefing_history_job' in [job.name for job in definitions_module.defs.jobs]


def test_history_sensor_is_registered() -> None:
    definitions_module = importlib.import_module('origo.definitions')
    assert any(
        sensor.name == 'publish_btc_briefing_history_sensor'
        for sensor in definitions_module.defs.sensors
    )


def _history_sensor_tick(instance: DagsterInstance) -> tuple[list[RunRequest], str | None]:
    """The run requests and skip message the history sensor produces from ``instance``."""
    definitions_module = importlib.import_module('origo.definitions')
    context = build_sensor_context(
        instance=instance,
        repository_def=definitions_module.defs.get_repository_def(),
    )
    tick = definitions_module.publish_btc_briefing_history_sensor.evaluate_tick(context)
    return list(tick.run_requests), tick.skip_message


def test_history_sensor_requests_the_partition_the_feed_just_published() -> None:
    # The run request has to carry the feed materialization's own partition:
    # that is the whole point of hanging the history off the feed instead of
    # off a clock.
    @asset(name='publish_btc_briefing_feed', partitions_def=daily_partitions)
    def source_feed() -> MaterializeResult:
        return MaterializeResult()

    with DagsterInstance.ephemeral() as instance:
        source_result = materialize(
            [source_feed],
            partition_key='2024-03-05',
            instance=instance,
        )
        assert source_result.success
        run_requests, _ = _history_sensor_tick(instance)

    assert [(request.partition_key, request.run_key) for request in run_requests] == [
        (
            '2024-03-05',
            f'publish_btc_briefing_history::2024-03-05::{source_result.run_id}',
        )
    ]


def test_history_sensor_skips_until_the_briefing_feed_materializes() -> None:
    with DagsterInstance.ephemeral() as instance:
        instance.report_runless_asset_event(
            AssetMaterialization(
                asset_key='refresh_binance_spot_klines_origo', partition='2024-03-05'
            )
        )
        run_requests, skip_message = _history_sensor_tick(instance)

    assert run_requests == []
    assert skip_message is not None
    assert 'publish_btc_briefing_feed' in skip_message


def test_history_sensor_skips_an_unpartitioned_feed_materialization() -> None:
    with DagsterInstance.ephemeral() as instance:
        instance.report_runless_asset_event(
            AssetMaterialization(asset_key='publish_btc_briefing_feed')
        )
        run_requests, skip_message = _history_sensor_tick(instance)

    assert run_requests == []
    assert skip_message == 'BTC briefing feed materialization did not include a partition key.'


def test_history_sensor_skips_an_event_without_a_dagster_event() -> None:
    class EventWithoutDagsterEvent:
        dagster_event = None

    definitions_module = importlib.import_module('origo.definitions')

    skip = definitions_module._publish_btc_briefing_history_run_request(EventWithoutDagsterEvent())

    assert isinstance(skip, SkipReason)
    assert skip.skip_message == (
        'No Dagster event was attached to the BTC briefing feed materialization.'
    )


def _install_recording_hf_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    published: dict[str, Any] | None = None,
) -> dict[str, Any]:
    uploaded: dict[str, Any] = {}

    class RecordingHfApi:
        def __init__(self, token: str) -> None:
            uploaded['token'] = token

        def repo_exists(self, *, repo_id: str, repo_type: str) -> bool:
            return published is not None

        def file_exists(self, *, repo_id: str, filename: str, repo_type: str) -> bool:
            uploaded['read_file_name'] = filename
            return published is not None

        def hf_hub_download(self, *, repo_id: str, filename: str, repo_type: str) -> str:
            published_path = tmp_path / 'published_history.json'
            published_path.write_text(json.dumps(published))
            return str(published_path)

        def create_repo(self, *, repo_id: str, repo_type: str, exist_ok: bool) -> None:
            uploaded['repo_id'] = repo_id
            uploaded['repo_type'] = repo_type

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
            uploaded['history_bytes'] = (folder / HISTORY_FILE_NAME).read_bytes()

    monkeypatch.setattr(publish_btc_briefing_history_module, 'HfApi', RecordingHfApi)
    return uploaded


def test_published_through_day_is_none_before_the_first_publish(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_recording_hf_api(monkeypatch, tmp_path)
    api = publish_btc_briefing_history_module._make_hf_api('test-token')

    assert (
        publish_btc_briefing_history_module._published_through_day(api, 'test/btc-briefing') is None
    )


def test_published_through_day_reads_the_rolling_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    uploaded = _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        published={'version': HISTORY_VERSION, 'through_day': '2024-01-02'},
    )
    api = publish_btc_briefing_history_module._make_hf_api('test-token')

    published_through_day = publish_btc_briefing_history_module._published_through_day(
        api, 'test/btc-briefing'
    )

    assert published_through_day == date(2024, 1, 2)
    assert uploaded['read_file_name'] == HISTORY_FILE_NAME


def test_a_published_file_under_another_contract_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The staleness guard must not read a day out of a payload it does not know.

    Whether a freshly built window may replace the published one is decided from
    this file. A payload written under another contract may carry a through_day
    that means something else, so trusting it would answer the question from the
    wrong field rather than failing.
    """
    _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        published={'version': 'btc_briefing_history/0', 'through_day': '2099-01-01'},
    )
    api = publish_btc_briefing_history_module._make_hf_api('test-token')

    with pytest.raises(RuntimeError, match="declares version 'btc_briefing_history/0'"):
        publish_btc_briefing_history_module._published_through_day(api, 'test/btc-briefing')


def test_a_published_file_with_no_version_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        published={'through_day': '2099-01-01'},
    )
    api = publish_btc_briefing_history_module._make_hf_api('test-token')

    with pytest.raises(RuntimeError, match='declares version None'):
        publish_btc_briefing_history_module._published_through_day(api, 'test/btc-briefing')


def test_publish_uploads_one_rolling_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_span(origo_assets, query_origo)
    history = _build_history_through(THROUGH_DAY)
    uploaded = _install_recording_hf_api(monkeypatch, tmp_path)

    result = publish_briefing_history_to_huggingface(
        history, repo_id='test/btc-briefing', token='test-token'
    )

    assert uploaded['token'] == 'test-token'
    assert uploaded['repo_id'] == 'test/btc-briefing'
    assert uploaded['repo_type'] == 'dataset'
    assert uploaded['files'] == ['btc_briefing_history.json']
    assert json.loads(uploaded['history_bytes']) == history
    assert result == {
        'repo_id': 'test/btc-briefing',
        'file_name': HISTORY_FILE_NAME,
        'through_day': THROUGH_DAY.isoformat(),
        'sha256': history['sha256'],
    }


def test_publishing_an_older_span_is_refused_before_the_rollup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # No ClickHouse credential is in the environment, so this partition can
    # only succeed by refusing before build_briefing_history is reached: the
    # staleness guard is cheap and the rollup it guards is two full-span scans.
    monkeypatch.delenv('CLICKHOUSE_PASSWORD', raising=False)
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    uploaded = _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        published={'version': HISTORY_VERSION, 'through_day': '2024-01-02'},
    )

    result = materialize(
        [publish_btc_briefing_history_module.publish_btc_briefing_history],
        partition_key=THROUGH_DAY.isoformat(),
    )

    assert result.success
    assert result.output_for_node('publish_btc_briefing_history') == {
        'repo_id': 'vaquum/btc_briefing_feed',
        'file_name': HISTORY_FILE_NAME,
        'through_day': THROUGH_DAY.isoformat(),
        'uploaded': False,
    }
    assert 'files' not in uploaded


def test_a_missing_huggingface_token_fails_before_the_rollup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Same ordering argument as above, one guard earlier: a credential that is
    # not there fails the partition without spending the rollup on it.
    monkeypatch.delenv('CLICKHOUSE_PASSWORD', raising=False)
    monkeypatch.delenv('HF_TOKEN', raising=False)
    monkeypatch.delenv('HUGGINGFACE_HUB_TOKEN', raising=False)

    with pytest.raises(RuntimeError, match='HF_TOKEN or HUGGINGFACE_HUB_TOKEN must be set'):
        materialize(
            [publish_btc_briefing_history_module.publish_btc_briefing_history],
            partition_key=THROUGH_DAY.isoformat(),
        )


def test_republish_of_the_published_span_rewrites_the_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_span(origo_assets, query_origo)
    uploaded = _install_recording_hf_api(
        monkeypatch,
        tmp_path,
        published={'version': HISTORY_VERSION, 'through_day': THROUGH_DAY.isoformat()},
    )
    monkeypatch.setenv('HF_TOKEN', 'test-token')

    result = materialize(
        [publish_btc_briefing_history_module.publish_btc_briefing_history],
        partition_key=THROUGH_DAY.isoformat(),
    )

    assert result.success
    output = result.output_for_node('publish_btc_briefing_history')
    assert output['uploaded'] is True
    assert json.loads(uploaded['history_bytes'])['sha256'] == output['sha256']


def test_partition_materializes_and_publishes_the_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _populate_complete_span(origo_assets, query_origo)
    uploaded = _install_recording_hf_api(monkeypatch, tmp_path)
    monkeypatch.setenv('HF_TOKEN', 'test-token')

    result = materialize(
        [publish_btc_briefing_history_module.publish_btc_briefing_history],
        partition_key=THROUGH_DAY.isoformat(),
    )

    assert result.success
    published = json.loads(uploaded['history_bytes'])
    assert published['version'] == HISTORY_VERSION
    assert published['through_day'] == THROUGH_DAY.isoformat()
    assert len(published['bars_15m']) == BARS_15M_IN_SPAN
    assert len(published['bars_1d']) == HISTORY_1D_DAYS
    assert result.output_for_node('publish_btc_briefing_history')['uploaded'] is True
