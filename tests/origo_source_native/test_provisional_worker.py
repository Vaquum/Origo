from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from dagster import AssetKey, FreshnessPolicy

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import bundle
from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters import binance_spot_rest as rest
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import OrchestrationSpec, RevisionedSourceSpec, RolloutStage
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.workers import provisional
from origo.workers.dagster_reader import DagsterReader, DagsterUnreachable
from origo.workers.provisional import ProvisionalFeed, canonical_asset, live_feed_asset
from origo.workers.receipts import ensure_monitoring_tables
from origo.workers.report import Reporter
from origo.workers.runtime import LIVE_FEED_FRESHNESS_WINDOW

from .helpers import ORIGO_DATABASE
from .test_binance_daily_source_adapter import REST

PROVENANCE = json.loads((REST / 'provenance.json').read_text())
ANCHOR = datetime.fromisoformat(PROVENANCE['minute_start']).astimezone(UTC)
KEY = ANCHOR.strftime('%Y-%m-%dT%H:%M:%SZ')
# The replayed minute is the last completed one at this tick time.
NOW = ANCHOR + timedelta(minutes=1, seconds=5)
RECEIPTS = (
    f'SELECT series, status, rows, error_code FROM {ORIGO_DATABASE}.worker_minute_log '
    "WHERE feed = 'provisional' ORDER BY recorded_at"
)


class _Reporter:
    def __init__(self) -> None:
        self.materializations: list[tuple[str, str | None, dict[str, object]]] = []

    def materialized(
        self, asset_key: str, *, partition: str | None, metadata: dict[str, object]
    ) -> bool:
        self.materializations.append((asset_key, partition, dict(metadata)))
        return True


class _Dagster:
    def __init__(self) -> None:
        self.in_flight = False
        self.unreachable = False
        self.asked: list[str] = []

    def backfill_in_flight(self, asset_key: str) -> bool:
        self.asked.append(asset_key)
        if self.unreachable:
            raise DagsterUnreachable('Backfills: HTTP 502')
        return self.in_flight


def _feed(
    spec: RevisionedSourceSpec,
    tmp_path: Path,
    dagster: _Dagster,
    reporter: _Reporter,
    clock: Any = None,
) -> ProvisionalFeed:
    return ProvisionalFeed(
        [spec],
        publication_root=tmp_path / 'source-files',
        reporter=cast(Reporter, reporter),
        dagster=cast(DagsterReader, dagster),
        host='test-host',
        **({'clock': clock} if clock is not None else {}),
    )


@pytest.fixture()
def spot(
    origo_test_env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[RevisionedSourceSpec, list[dict[str, Any]]]]:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.delenv('BINANCE_SPOT_REST_BASE_URL', raising=False)
    requests: list[dict[str, Any]] = list(PROVENANCE['requests'])

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> daily.Response:
        item = requests.pop(0)
        assert url == item['url'] and params == item['params']
        return daily.Response((REST / item['file']).read_bytes(), {}, 200)

    monkeypatch.setattr(rest, 'get_response', captured)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        runtime = SourceRuntime(
            spec, SourceStore(client, ORIGO_DATABASE, spec), tmp_path / 'locks', str(uuid4())
        )
        runtime.setup(anchor=ANCHOR)
        ensure_monitoring_tables(client, ORIGO_DATABASE)
    finally:
        client.disconnect()
    yield spec, requests


def test_provisional_cron_must_be_one_minute() -> None:
    with pytest.raises(ValueError, match='provisional worker'):
        OrchestrationSpec(
            canonical_cron='5 0 * * *', provisional_cron='*/5 * * * *', audit_cron='0 * * * *'
        )
    assert BINANCE_SPOT_TRADES_SPEC.orchestration.provisional_cron == '* * * * *'


def test_provisional_tick_builds_the_closed_minute_and_publishes_pinned_consumers_once(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    query_origo: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, requests = spot
    dagster, reporter = _Dagster(), _Reporter()
    real_execute = provisional.execute_source
    operations: list[tuple[str, str, str]] = []

    def recording(
        executed: RevisionedSourceSpec, operation: str, config: Any, *, run_id: str
    ) -> dict[str, object]:
        assert executed.key == spec.key
        assert run_id.startswith('worker:provisional:test-host:')
        operations.append((operation, config.partition_key, config.destination))
        if operation == 'provisional':
            return real_execute(executed, operation, config, run_id=run_id)
        return {}

    monkeypatch.setattr(provisional, 'execute_source', recording)
    feed = _feed(spec, tmp_path, dagster, reporter)

    # A native backfill owns publication: the minute is still built, nothing is published.
    dagster.in_flight = True
    first = feed.tick(NOW)
    assert first.feed == 'provisional'
    assert first.processed == (f'binance_spot_trades:{KEY}',) and first.failed == ()
    assert not requests, 'the closed minute was fetched through the source runtime'
    raw = query_origo(f'SELECT count() FROM {ORIGO_DATABASE}.binance_spot_trades_raw_current')
    assert raw[0][0] > 1000
    assert operations == [('provisional', KEY, '')]
    assert dagster.asked == [canonical_asset(spec)]

    # The minute is covered now, so the next tick builds nothing and publishes the mount
    # consumer, which pins provisional rows. huggingface is canonical-only: its sensor
    # publishes it, never the worker.
    dagster.in_flight = False
    second = feed.tick(NOW)
    assert second.processed == ('binance_spot_trades:mount',) and second.failed == ()
    assert operations[1:] == [
        ('consumer_mount', '', str(tmp_path / 'source-files' / 'binance_spot_trades' / 'mount'))
    ]

    # Publication follows the pinned state: current files mean no publication, and an
    # unreadable Dagster does not stop the decision.
    monkeypatch.setattr(provisional, 'publication_current', lambda *args, **kwargs: True)
    dagster.unreachable = True
    third = feed.tick(NOW)
    assert third.processed == () and third.failed == ()
    assert len(operations) == 2

    receipts = query_origo(RECEIPTS)
    assert [(series, status, error) for series, status, _, error in receipts] == [
        ('binance_spot_trades', 'OK', ''),
        ('binance_spot_trades:mount', 'OK', ''),
    ]
    assert receipts[0][2] > 1000 and receipts[1][2] == 1
    assert [key for key, _, _ in reporter.materializations] == [live_feed_asset(spec)] * 3
    assert reporter.materializations[0][2]['intervals'] == 1
    assert reporter.materializations[1][2]['publications'] == 1
    assert reporter.materializations[2][2]['failed'] == 0


def test_provisional_failures_back_off_and_stop_at_the_attempt_limit(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    query_origo: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    spec, _ = spot
    spec = replace(spec, orchestration=replace(spec.orchestration, retry_count=2, retry_delay=3600))

    def failing(executed: object, operation: str, config: object, *, run_id: str) -> dict[str, object]:
        raise RuntimeError('binance unavailable')

    monkeypatch.setattr(provisional, 'execute_source', failing)
    start = datetime.now(UTC)
    offset = [timedelta(0)]
    feed = _feed(spec, tmp_path, _Dagster(), _Reporter(), clock=lambda: start + offset[0])
    key = f'binance_spot_trades:{KEY}'

    with caplog.at_level(logging.ERROR, logger='origo.workers.provisional'):
        assert feed.tick(NOW).failed == (key,)
        # One minute must pass after the first failure before the second attempt.
        assert feed.tick(NOW).failed == ()
        offset[0] = timedelta(minutes=2)
        assert feed.tick(NOW).failed == (key,)
        # Two failures reach the limit: no more attempts, an ERROR line names the minute.
        offset[0] = timedelta(hours=10)
        assert feed.tick(NOW).failed == ()

    assert query_origo(RECEIPTS) == [
        ('binance_spot_trades', 'FAILED', 0, 'RuntimeError'),
        ('binance_spot_trades', 'FAILED', 0, 'RuntimeError'),
    ]
    assert f'partition={KEY} attempts exhausted after 2 failures' in caplog.text
    assert 'binance unavailable' in caplog.text


def test_dormant_sources_are_skipped_and_the_bundle_declares_the_feed_not_a_schedule() -> None:
    dormant = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.DORMANT)
    feed = ProvisionalFeed(
        [dormant],
        publication_root=Path('/nonexistent'),
        reporter=cast(Reporter, _Reporter()),
        dagster=cast(DagsterReader, _Dagster()),
    )
    assert feed.specs == ()
    for stage in (RolloutStage.LIVE, RolloutStage.DORMANT):
        spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=stage)
        built = bundle.build_source_bundle(spec)
        schedules = {schedule.name: schedule.cron_schedule for schedule in built.schedules}
        assert not [name for name in schedules if name.endswith('_provisional_schedule')]
        assert '* * * * *' not in schedules.values()
        sensors = {sensor.name for sensor in built.sensors}
        assert 'binance_spot_trades_mount_sensor' not in sensors
        assert 'binance_spot_trades_huggingface_sensor' in sensors
        live_key = AssetKey(live_feed_asset(spec))
        live_spec = next(
            asset_spec
            for definition in built.assets
            for asset_spec in definition.specs
            if asset_spec.key == live_key
        )
        assert live_spec.group_name == spec.key
        if stage == RolloutStage.DORMANT:
            assert live_spec.freshness_policy is None
        else:
            assert live_spec.freshness_policy == FreshnessPolicy.time_window(
                fail_window=LIVE_FEED_FRESHNESS_WINDOW
            )
