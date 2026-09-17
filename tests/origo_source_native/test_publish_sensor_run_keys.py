from __future__ import annotations

from dataclasses import dataclass

from datetime import date

from dagster import RunRequest, SkipReason

from origo.definitions import (
    BRIEFING_FIRST_DAY,
    _AssetEventLike,
    _partitioned_run_request,
    _publish_btc_briefing_history_run_request,
    defs,
)

PARTITION_KEY = '2026-06-05'
RUN_KEY_PREFIXES = ('publish_btc_briefing_feed', 'publish_btc_briefing_history')


@dataclass(frozen=True)
class _DagsterEvent:
    partition: str | None


@dataclass(frozen=True)
class _AssetEvent:
    dagster_event: _DagsterEvent | None
    run_id: str


def _publish_requests(asset_event: _AssetEvent) -> list[RunRequest | SkipReason]:
    return [
        _partitioned_run_request(asset_event, run_key_prefix=RUN_KEY_PREFIXES[0]),
        _publish_btc_briefing_history_run_request(asset_event),
    ]


def _run_keys(asset_event: _AssetEvent) -> list[str]:
    keys = []
    for result in _publish_requests(asset_event):
        assert isinstance(result, RunRequest)
        assert result.run_key is not None
        keys.append(result.run_key)
    return keys


def test_no_publish_sensor_keys_a_run_on_the_partition_alone() -> None:
    run_id = 'source-run-1'

    assert _run_keys(_AssetEvent(_DagsterEvent(PARTITION_KEY), run_id)) == [
        f'{prefix}::{PARTITION_KEY}::{run_id}' for prefix in RUN_KEY_PREFIXES
    ]


def test_the_event_protocol_exposes_the_triggering_run() -> None:
    assert sorted(_AssetEventLike.__annotations__) == ['dagster_event', 'run_id']


def test_a_rematerialized_partition_requests_a_second_run() -> None:
    first = _run_keys(_AssetEvent(_DagsterEvent(PARTITION_KEY), 'source-run-1'))
    second = _run_keys(_AssetEvent(_DagsterEvent(PARTITION_KEY), 'source-run-2'))

    assert all(left != right for left, right in zip(first, second, strict=True))


def test_one_materialization_keys_one_run() -> None:
    first = _run_keys(_AssetEvent(_DagsterEvent(PARTITION_KEY), 'source-run-1'))
    repeated = _run_keys(_AssetEvent(_DagsterEvent(PARTITION_KEY), 'source-run-1'))

    assert repeated == first


def test_a_partitionless_materialization_is_skipped() -> None:
    missing_event = _publish_requests(_AssetEvent(None, 'source-run-1'))
    missing_partition = _publish_requests(_AssetEvent(_DagsterEvent(None), 'source-run-1'))

    assert all(isinstance(result, SkipReason) for result in missing_event)
    assert all(isinstance(result, SkipReason) for result in missing_partition)


def test_existing_publish_sensors_are_unchanged() -> None:
    existing = {
        'publish_btc_briefing_feed_sensor',
        'publish_btc_briefing_history_sensor',
        'binance_spot_trades_huggingface_sensor',
    }
    names = {sensor.name for sensor in defs.sensors}
    assert existing <= names
    # The depth Arrow chunks and the mount publication moved to the feed workers.
    assert not names & {'depth_snapshot_store_source_sensor', 'binance_spot_trades_mount_sensor'}
    assert not any(
        'to_huggingface_sensor' in name or name == 'bar_store_source_sensor' for name in names
    )


def test_briefing_sensors_skip_days_before_the_book_projection() -> None:
    assert BRIEFING_FIRST_DAY == date(2026, 5, 14)
    relaunch = _partitioned_run_request(
        _AssetEvent(_DagsterEvent('2018-01-16'), 'relaunch-1'), run_key_prefix=RUN_KEY_PREFIXES[0]
    )
    assert isinstance(relaunch, SkipReason)
    assert '2018-01-16' in relaunch.skip_message and '2026-05-14' in relaunch.skip_message
    first = _partitioned_run_request(
        _AssetEvent(_DagsterEvent('2026-05-14'), 'run-1'), run_key_prefix=RUN_KEY_PREFIXES[0]
    )
    assert isinstance(first, RunRequest) and first.partition_key == '2026-05-14'
