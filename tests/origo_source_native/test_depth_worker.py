from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.workers import depth
from origo.workers.depth import (
    DEPTH20_LIVE_RECONCILIATION_SPEC,
    DEPTH_SPECS,
    LIVE_FEED_ASSET,
    DepthFeed,
    DepthLiveStoreStatus,
    candidate_minutes,
    last_completed_minute,
)
from origo.workers.receipts import ensure_monitoring_tables
from origo.workers.report import Reporter

from .helpers import ORIGO_DATABASE

Query = Callable[[str], list[tuple[object, ...]]]
RECEIPTS = (
    "SELECT series, formatDateTime(minute, '%Y-%m-%dT%H:%i:%SZ', 'UTC'), rows, sha256, status, "
    f"error_code, error FROM {ORIGO_DATABASE}.worker_minute_log WHERE feed = 'depth' "
    "ORDER BY minute, recorded_at"
)


def _utc(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=UTC)


class _Reporter:
    def __init__(self) -> None:
        self.materializations: list[tuple[str, str | None, dict[str, object]]] = []

    def materialized(
        self, asset_key: str, *, partition: str | None, metadata: dict[str, object]
    ) -> bool:
        self.materializations.append((asset_key, partition, dict(metadata)))
        return True


class _Publish:
    def __init__(self, version: str) -> None:
        self.version = version


@pytest.fixture()
def feed(origo_test_env: dict[str, str]) -> Iterator[tuple[DepthFeed, _Reporter]]:
    client = make_clickhouse_client(get_clickhouse_settings())
    ensure_monitoring_tables(client, ORIGO_DATABASE)
    reporter = _Reporter()
    worker = DepthFeed(
        client, ORIGO_DATABASE, (DEPTH20_LIVE_RECONCILIATION_SPEC,), cast(Reporter, reporter)
    )
    worker.lookback_minutes = 4
    try:
        yield worker, reporter
    finally:
        client.disconnect()


def test_candidate_minutes_cover_the_lookback_oldest_first_within_the_partition_calendar() -> None:
    spec = DEPTH20_LIVE_RECONCILIATION_SPEC
    now = _utc(2026, 5, 14, 10, 33, 30)
    assert last_completed_minute(now) == _utc(2026, 5, 14, 10, 32)
    assert last_completed_minute(datetime(2026, 5, 14, 10, 33, 30)) == _utc(2026, 5, 14, 10, 32)
    # Minutes before the series' partition calendar are never candidates.
    first = spec.partitions.start.astimezone(UTC)
    first_key = spec.partitions.get_first_partition_key()
    assert first_key is not None
    assert candidate_minutes(spec, first + timedelta(minutes=2, seconds=30), 5) == (
        (first, first_key),
        (first + timedelta(minutes=1), spec.partitions.get_next_partition_key(first_key)),
    )
    later = candidate_minutes(spec, _utc(2026, 6, 1, 0, 0, 5), 3)
    assert [key for _, key in later] == [
        '2026-05-31T23:57:00+0000',
        '2026-05-31T23:58:00+0000',
        '2026-05-31T23:59:00+0000',
    ]
    with pytest.raises(RuntimeError, match='at least one minute'):
        candidate_minutes(spec, now, 0)
    assert {item.series for item in DEPTH_SPECS} == {'depth20_snapshots', 'depth200_snapshots'}
    assert DepthFeed.lookback_minutes == depth.DEPTH_SOURCE_LOOKBACK_MINUTES == 15


def test_depth_tick_completes_the_incomplete_minutes_oldest_first_and_records_receipts(
    feed: tuple[DepthFeed, _Reporter],
    query_origo: Query,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, reporter = feed
    manifest_minute = _utc(2026, 5, 14, 10, 31)
    m32, m33, m34 = _utc(2026, 5, 14, 10, 32), _utc(2026, 5, 14, 10, 33), _utc(2026, 5, 14, 10, 34)
    statuses = {
        # complete: raw rows, projection row and Arrow chunk all present
        _utc(2026, 5, 14, 10, 31): DepthLiveStoreStatus(60, 1, True, manifest_minute),
        # nothing stored and the collector has nothing either
        m32: DepthLiveStoreStatus(0, 0, False, manifest_minute),
        # nothing stored, the collector serves the minute
        m33: DepthLiveStoreStatus(0, 0, False, manifest_minute),
        # raw rows present, projection and Arrow chunk missing
        m34: DepthLiveStoreStatus(60, 0, False, manifest_minute),
    }
    calls: list[tuple[str, datetime]] = []

    def fake_status(client: object, database: str, spec: object, minute_start: datetime) -> object:
        return statuses[minute_start]

    def fake_source_has_rows(spec: object, minute_start: datetime) -> bool:
        assert minute_start in (m32, m33), 'only minutes without raw rows ask the collector'
        calls.append(('source', minute_start))
        return minute_start == m33

    def fake_sync(spec: object, client: object, database: str, minute: datetime) -> int:
        calls.append(('sync', minute))
        return 60

    def fake_refresh(spec: object, client: object, database: str, minute: datetime) -> int:
        calls.append(('refresh', minute))
        return 1

    def fake_frame(client: object, database: str, spec: object, minute_start: datetime) -> object:
        calls.append(('frame', minute_start))
        return object()

    def fake_publish(series: str, partition_key: str, build: object) -> _Publish:
        calls.append(('publish', depth.minute_start_from_partition_key(partition_key)))
        return _Publish(f'sha-{partition_key[11:16]}')

    monkeypatch.setattr(depth, 'store_status', fake_status)
    monkeypatch.setattr(depth, 'source_has_rows', fake_source_has_rows)
    monkeypatch.setattr(depth, '_sync', fake_sync)
    monkeypatch.setattr(depth, '_refresh', fake_refresh)
    monkeypatch.setattr(depth, 'build_depth_snapshot_frame', fake_frame)
    monkeypatch.setattr(depth, 'publish_depth_snapshot_chunk', fake_publish)

    outcome = worker.tick(_utc(2026, 5, 14, 10, 35, 20))

    assert outcome.feed == 'depth'
    assert outcome.minute == m34
    assert outcome.processed == (
        'depth20_snapshots:2026-05-14T10:33:00+0000',
        'depth20_snapshots:2026-05-14T10:34:00+0000',
    )
    assert outcome.failed == ()
    assert calls == [
        ('source', m32),
        ('source', m33),
        ('sync', m33),
        ('refresh', m33),
        ('frame', m33),
        ('publish', m33),
        ('refresh', m34),
        ('frame', m34),
        ('publish', m34),
    ]
    assert query_origo(RECEIPTS) == [
        ('depth20_snapshots', '2026-05-14T10:33:00Z', 0, '', 'STARTED', '', ''),
        ('depth20_snapshots', '2026-05-14T10:33:00Z', 60, 'sha-10:33', 'OK', '', ''),
        ('depth20_snapshots', '2026-05-14T10:34:00Z', 0, '', 'STARTED', '', ''),
        ('depth20_snapshots', '2026-05-14T10:34:00Z', 60, 'sha-10:34', 'OK', '', ''),
    ]
    assert [(key, partition) for key, partition, _ in reporter.materializations] == [
        ('sync_binance_spot_depth20_snapshots_to_origo', '2026-05-14T10:33:00+0000'),
        ('refresh_binance_spot_depth20_1m_origo', '2026-05-14T10:33:00+0000'),
        ('refresh_binance_spot_depth20_1m_origo', '2026-05-14T10:34:00+0000'),
        (LIVE_FEED_ASSET, None),
    ]
    live = reporter.materializations[-1][2]
    assert (live['minute'], live['processed'], live['failed']) == ('2026-05-14T10:34:00+00:00', 2, 0)
    assert isinstance(live['rss_bytes'], int) and live['rss_bytes'] > 0

    # An unchanged store on the next tick is skipped entirely: every candidate is complete.
    monkeypatch.setattr(
        depth,
        'store_status',
        lambda client, database, spec, minute_start: DepthLiveStoreStatus(60, 1, True, m34),
    )
    again = worker.tick(_utc(2026, 5, 14, 10, 35, 50))
    assert again.processed == () and again.failed == ()
    assert len(query_origo(RECEIPTS)) == 4
    assert reporter.materializations[-1][0] == LIVE_FEED_ASSET


def test_depth_tick_records_a_failed_minute_and_continues_with_the_next(
    feed: tuple[DepthFeed, _Reporter],
    query_origo: Query,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker, reporter = feed
    worker.lookback_minutes = 2

    def fake_refresh(spec: object, client: object, database: str, minute: datetime) -> int:
        if minute == _utc(2026, 5, 14, 10, 33):
            raise RuntimeError('projection failed')
        return 1

    monkeypatch.setattr(
        depth,
        'store_status',
        lambda client, database, spec, minute_start: DepthLiveStoreStatus(60, 0, False, None),
    )
    monkeypatch.setattr(depth, '_refresh', fake_refresh)
    monkeypatch.setattr(
        depth, 'build_depth_snapshot_frame', lambda client, database, spec, minute_start: object()
    )
    monkeypatch.setattr(
        depth, 'publish_depth_snapshot_chunk', lambda series, key, build: _Publish('v1')
    )
    with caplog.at_level(logging.ERROR, logger='origo.workers.depth'):
        outcome = worker.tick(_utc(2026, 5, 14, 10, 35, 20))

    assert outcome.failed == ('depth20_snapshots:2026-05-14T10:33:00+0000',)
    assert outcome.processed == ('depth20_snapshots:2026-05-14T10:34:00+0000',)
    assert query_origo(RECEIPTS) == [
        ('depth20_snapshots', '2026-05-14T10:33:00Z', 0, '', 'STARTED', '', ''),
        ('depth20_snapshots', '2026-05-14T10:33:00Z', 0, '', 'FAILED', 'RuntimeError', 'projection failed'),
        ('depth20_snapshots', '2026-05-14T10:34:00Z', 0, '', 'STARTED', '', ''),
        ('depth20_snapshots', '2026-05-14T10:34:00Z', 60, 'v1', 'OK', '', ''),
    ]
    assert 'depth20_snapshots 2026-05-14T10:33:00+0000 failed' in caplog.text
    assert 'projection failed' in caplog.text
    assert reporter.materializations[-1][0] == LIVE_FEED_ASSET
    assert reporter.materializations[-1][2]['failed'] == 1


def test_source_has_rows_is_false_when_the_collector_request_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_get(*args: object, **kwargs: object) -> object:
        raise depth.requests.RequestException('source unavailable')

    monkeypatch.setenv('BINANCE_SPOT_DEPTH20_BASE_URL', 'https://source.example')
    monkeypatch.setenv('BINANCE_SPOT_DEPTH20_AUTH_TOKEN', 'test-token')
    monkeypatch.setattr(depth.requests, 'get', fail_get)
    assert depth.source_has_rows(DEPTH20_LIVE_RECONCILIATION_SPEC, _utc(2026, 5, 14, 10, 34)) is False
    assert depth.source_history_url('https://source.example/', _utc(2026, 5, 14, 10, 34)) == (
        'https://source.example/history?from=1778754840&to=1778754840'
    )
