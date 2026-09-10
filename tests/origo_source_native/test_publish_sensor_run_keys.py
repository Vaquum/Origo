from __future__ import annotations

from dataclasses import dataclass

from dagster import RunRequest, SkipReason

from origo.definitions import (
    _AssetEventLike,
    _publish_binance_spot_dollar_klines_to_hf_run_request,
    _publish_binance_spot_klines_to_hf_run_request,
    _publish_btc_briefing_history_run_request,
    defs,
)

PARTITION_KEY = '2024-03-05'
RUN_KEY_PREFIXES = (
    'publish_binance_spot_klines_to_hf',
    'publish_binance_spot_1M_dollar_klines_to_hf',
    'publish_btc_briefing_history',
)


@dataclass(frozen=True)
class _DagsterEvent:
    partition: str | None


@dataclass(frozen=True)
class _AssetEvent:
    dagster_event: _DagsterEvent | None
    run_id: str


def _publish_requests(asset_event: _AssetEvent) -> list[RunRequest | SkipReason]:
    return [
        _publish_binance_spot_klines_to_hf_run_request(
            asset_event,
            run_key_prefix=RUN_KEY_PREFIXES[0],
        ),
        _publish_binance_spot_dollar_klines_to_hf_run_request(
            asset_event,
            run_key_prefix=RUN_KEY_PREFIXES[1],
        ),
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
    missing_partition = _publish_requests(
        _AssetEvent(_DagsterEvent(None), 'source-run-1')
    )

    assert all(isinstance(result, SkipReason) for result in missing_event)
    assert all(isinstance(result, SkipReason) for result in missing_partition)


def test_sensor_count_is_unchanged() -> None:
    assert len(defs.sensors) == 16
