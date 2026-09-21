from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Iterator
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
from origo.sources.bundle import SourceRunConfig
from origo.sources.contracts import OrchestrationSpec, RevisionedSourceSpec, RolloutStage
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.workers import provisional
from origo.workers.dagster_reader import DagsterReader, DagsterUnreachable
from origo.workers.provisional import ProvisionalFeed, live_feed_asset
from origo.workers.receipts import (
    ensure_monitoring_tables,
    failed_attempts,
    reconcile_died_receipts,
    record_receipt,
)
from origo.workers.report import Reporter
from origo.workers.runtime import LIVE_FEED_FRESHNESS_WINDOW

from .helpers import ORIGO_DATABASE
from .test_binance_daily_source_adapter import REST

PROVENANCE = json.loads((REST / 'provenance.json').read_text())
ANCHOR = datetime.fromisoformat(PROVENANCE['minute_start']).astimezone(UTC)
KEY = ANCHOR.strftime('%Y-%m-%dT%H:%M:%SZ')
# The replayed minute is the last completed one at this tick time.
NOW = ANCHOR + timedelta(minutes=1, seconds=5)
Query = Callable[[str], list[tuple[object, ...]]]
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
        self.owned = False
        self.unreachable = False
        self.asked: list[str] = []

    def backfill_owns_publication(self, source_key: str) -> bool:
        self.asked.append(source_key)
        if self.unreachable:
            raise DagsterUnreachable('Backfills: HTTP 502')
        return self.owned


def _feed(
    spec: RevisionedSourceSpec,
    tmp_path: Path,
    dagster: _Dagster,
    reporter: _Reporter,
    clock: Callable[[], datetime] | None = None,
    heartbeat: Path | None = None,
) -> ProvisionalFeed:
    return ProvisionalFeed(
        [spec],
        publication_root=tmp_path / 'source-files',
        reporter=cast(Reporter, reporter),
        dagster=cast(DagsterReader, dagster),
        host='test-host',
        heartbeat=heartbeat,
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
    query_origo: Query,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, requests = spot
    dagster, reporter = _Dagster(), _Reporter()
    real_execute = provisional.execute_source
    operations: list[tuple[str, str, str]] = []

    def recording(
        executed: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
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
    dagster.owned = True
    first = feed.tick(NOW)
    assert first.feed == 'provisional'
    assert first.processed == (f'binance_spot_trades:{KEY}',) and first.failed == ()
    assert not requests, 'the closed minute was fetched through the source runtime'
    raw = query_origo(f'SELECT count() FROM {ORIGO_DATABASE}.binance_spot_trades_raw_current')
    assert raw[0][0] > 1000
    assert operations == [('provisional', KEY, '')]
    assert dagster.asked == [spec.key]

    # The minute is covered now, so the next tick builds nothing and publishes the mount
    # consumer, which pins provisional rows. huggingface is canonical-only: its sensor
    # publishes it, never the worker.
    dagster.owned = False
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
        ('binance_spot_trades', 'STARTED', ''),
        ('binance_spot_trades', 'OK', ''),
        ('binance_spot_trades:mount', 'STARTED', ''),
        ('binance_spot_trades:mount', 'OK', ''),
    ]
    # The build receipt counts the interval's own raw rows (the component log of that
    # build), the publication receipt the pinned partitions.
    raw_rows = query_origo(
        f"SELECT max(row_count) FROM {ORIGO_DATABASE}.source_component_log "
        f"WHERE partition_key = '{KEY}' AND provisional = 1"
    )[0][0]
    assert receipts[1][2] == raw_rows > 1000 and receipts[3][2] == 1
    assert [key for key, _, _ in reporter.materializations] == [live_feed_asset(spec)] * 3
    assert reporter.materializations[0][2]['intervals'] == 1
    assert reporter.materializations[1][2]['publications'] == 1
    assert reporter.materializations[2][2]['failed'] == 0


def test_provisional_minute_failures_back_off_without_an_attempt_limit(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    query_origo: Query,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A minute never exhausts: a permanently skipped hole would freeze the
    # current-view frontier, so holes keep retrying on the capped delay until
    # they build. Only pinned publications stop for an operator run.
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
        # The delays count from the recorded failures, which the real clock stamps within
        # seconds of ``start``: one minute before the first retry, two before the second.
        assert feed.tick(NOW).failed == (key,)
        offset[0] = timedelta(seconds=30)
        assert feed.tick(NOW).failed == ()
        offset[0] = timedelta(seconds=90)
        assert feed.tick(NOW).failed == (key,)
        offset[0] = timedelta(seconds=100)
        assert feed.tick(NOW).failed == ()
        offset[0] = timedelta(seconds=200)
        assert feed.tick(NOW).failed == (key,)
        # Past retry_count the capped delay still admits the hole: a fourth
        # attempt runs instead of freezing the frontier, and nothing logs
        # exhaustion for a minute.
        offset[0] = timedelta(hours=10)
        assert feed.tick(NOW).failed == (key,)

    assert query_origo(RECEIPTS) == [
        ('binance_spot_trades', 'STARTED', 0, ''),
        ('binance_spot_trades', 'FAILED', 0, 'RuntimeError'),
    ] * 4
    assert 'attempts exhausted' not in caplog.text
    assert 'binance unavailable' in caplog.text


def test_provisional_tick_touches_the_heartbeat_per_unit_of_work(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, _ = spot
    beats: list[Path] = []
    monkeypatch.setattr(provisional, 'touch_heartbeat', beats.append)
    real_execute = provisional.execute_source

    def recording(
        executed: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
    ) -> dict[str, object]:
        if operation == 'provisional':
            return real_execute(executed, operation, config, run_id=run_id)
        return {}

    monkeypatch.setattr(provisional, 'execute_source', recording)
    dagster = _Dagster()
    heartbeat = tmp_path / 'worker.heartbeat'
    feed = _feed(spec, tmp_path, dagster, _Reporter(), heartbeat=heartbeat)
    # The owned backfill still builds the minute: one beat for the interval.
    dagster.owned = True
    assert feed.tick(NOW).processed == (f'binance_spot_trades:{KEY}',)
    assert beats == [heartbeat]
    # The next tick publishes the mount consumer: one beat for the publication.
    dagster.owned = False
    assert feed.tick(NOW).processed == ('binance_spot_trades:mount',)
    assert beats == [heartbeat, heartbeat]


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


def test_provisional_publication_failures_back_off_per_pinned_state(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    query_origo: Query,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    spec, _ = spot
    spec = replace(spec, orchestration=replace(spec.orchestration, retry_count=1, retry_delay=3600))
    real_execute = provisional.execute_source
    renders: list[str] = []

    def failing_render(
        executed: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
    ) -> dict[str, object]:
        if operation == 'provisional':
            return real_execute(executed, operation, config, run_id=run_id)
        renders.append(operation)
        raise RuntimeError('render failed')

    monkeypatch.setattr(provisional, 'execute_source', failing_render)
    start = datetime.now(UTC)
    offset = [timedelta(0)]
    feed = _feed(spec, tmp_path, _Dagster(), _Reporter(), clock=lambda: start + offset[0])
    series = f'{spec.key}:mount'

    with caplog.at_level(logging.ERROR, logger='origo.workers.provisional'):
        # The minute is built; the publication of the new pinned state fails once.
        first = feed.tick(NOW)
        assert first.processed == (f'{spec.key}:{KEY}',) and first.failed == (series,)
        assert renders == ['consumer_mount']
        # Inside the delay the same pinned state is not rendered again.
        assert feed.tick(NOW).failed == ()
        assert renders == ['consumer_mount']
        # After the delay the single retry runs and fails.
        offset[0] = timedelta(minutes=2)
        assert feed.tick(NOW).failed == (series,)
        assert renders == ['consumer_mount', 'consumer_mount']
        # The retry budget for this pinned state is spent: no more renders, an ERROR line.
        offset[0] = timedelta(hours=10)
        assert feed.tick(NOW).failed == ()
        assert renders == ['consumer_mount', 'consumer_mount']

    token = query_origo(
        f"SELECT sha256 FROM {ORIGO_DATABASE}.worker_minute_log "
        f"WHERE series = '{series}' ORDER BY recorded_at LIMIT 1"
    )[0][0]
    assert isinstance(token, str) and len(token) == 64
    assert query_origo(RECEIPTS) == [
        ('binance_spot_trades', 'STARTED', 0, ''),
        ('binance_spot_trades', 'OK', query_origo(RECEIPTS)[1][2], ''),
        (series, 'STARTED', 0, ''),
        (series, 'FAILED', 0, 'RuntimeError'),
        (series, 'STARTED', 0, ''),
        (series, 'FAILED', 0, 'RuntimeError'),
    ]
    assert f'consumer=mount state={token[:12]} attempts exhausted after 2 failures' in caplog.text


def test_reader_mirrors_the_backfill_ownership_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = DagsterReader('http://dagit.invalid')
    source = 'binance_spot_trades'
    asset = {'path': [f'build_{source}_canonical_revision_origo']}
    state: dict[str, object] = {}

    def run(status: str, created: float, job: str, **tags: str) -> dict[str, object]:
        return {
            'runId': f'run-{created}',
            'status': status,
            'creationTime': created,
            'jobName': job,
            'tags': [{'key': key, 'value': value} for key, value in tags.items()],
            'assetSelection': [asset],
        }

    def canned(operation: str, query: str, variables: object = None) -> dict[str, object]:
        if operation == 'Backfills':
            return {
                'partitionBackfillsOrError': {
                    '__typename': 'PartitionBackfills',
                    'results': state.get('backfills', []),
                }
            }
        # A run listing pages newest first: the fake serves the state's active list
        # from the cursor, ``limit`` at a time. The reader asks for active runs only;
        # terminal verdicts never reach the ownership rule.
        assert isinstance(variables, dict)
        run_filter = variables['filter']
        assert isinstance(run_filter, dict)
        assert 'statuses' in run_filter
        listed = cast(list[dict[str, object]], state.get('active', []))
        start = 0
        if variables.get('cursor'):
            start = next(i for i, run in enumerate(listed) if run['runId'] == variables['cursor']) + 1
        limit = cast(int, variables['limit'])
        return {'runsOrError': {'__typename': 'Runs', 'results': listed[start : start + limit]}}

    monkeypatch.setattr(reader, 'query', canned)
    backfill_job = f'backfill_{source}_source_job'
    canonical_job = f'refresh_{source}_canonical_source_job'

    def backfill(status: str, stamp: float, backfill_id: str = 'bf') -> dict[str, object]:
        return {'id': backfill_id, 'status': status, 'timestamp': stamp, 'assetSelection': [asset]}

    # Nothing native and no backfill run: nothing owns publication.
    assert reader.backfill_owns_publication(source) is False
    # A requested, cancelling or failing native backfill owns it.
    for status in ('REQUESTED', 'CANCELING', 'FAILING'):
        state['backfills'] = [backfill(status, 100.0)]
        assert reader.backfill_owns_publication(source) is True
    # A terminal native selection releases publication.
    for status in ('FAILED', 'COMPLETED_FAILED', 'CANCELED', 'COMPLETED', 'COMPLETED_SUCCESS'):
        state['backfills'] = [backfill(status, 100.0)]
        assert reader.backfill_owns_publication(source) is False
    # An active backfill run of the source owns publication.
    state.clear()
    state['active'] = [run('STARTED', 300.0, backfill_job)]
    assert reader.backfill_owns_publication(source) is True
    # An active canonical range run of the source is a backfill in flight; a reconciliation
    # run is not.
    state['active'] = [
        run('STARTED', 300.0, canonical_job, **{'dagster/asset_partition_range_start': '2020-01-01'})
    ]
    assert reader.backfill_owns_publication(source) is True
    state['active'] = [run('STARTED', 300.0, canonical_job, origo_source_reconciliation='true')]
    assert reader.backfill_owns_publication(source) is False
    # Another source's native selection is not this source's, even when active: the
    # asset selection scopes the rule, not the status.
    state.clear()
    state['backfills'] = [{'id': 'x', 'status': 'REQUESTED', 'timestamp': 1.0, 'assetSelection': [{'path': ['other']}]}]
    assert reader.backfill_owns_publication(source) is False


def test_reconcile_converts_died_started_receipts_to_failed(
    origo_test_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # A STARTED row with no terminal row means its process died (watchdog exit,
    # SIGKILL): reconcile appends FAILED/WORKER_DIED so backoff and paging see
    # the death instead of retrying instantly forever. The terminal match is on
    # the unit (feed, series, minute), never the hash: STARTED rows are written
    # before the hash exists, so the OK row below carries a real sha256.
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        now = datetime.now(UTC)
        old = now - timedelta(hours=1)
        rows = [
            ('probe', 'alive', old, '', 0, 'STARTED', '', now),
            ('probe', 'done', old, '', 0, 'STARTED', '', old),
            ('probe', 'done', old, 'sha-done', 5, 'OK', '', old + timedelta(seconds=10)),
        ]
        rows.extend(
            ('probe', f'died-{n:02d}', old, '', 0, 'STARTED', '', old) for n in range(25)
        )
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.worker_minute_log '
            '(feed, series, minute, sha256, rows, status, error_code, recorded_at) VALUES',
            [
                (feed, series, minute.replace(tzinfo=None), sha, n, status, code, at.replace(tzinfo=None))
                for feed, series, minute, sha, n, status, code, at in rows
            ],
        )
        selects = 0
        real_execute = client.execute

        def counting(query: object, *args: object, **kwargs: object) -> object:
            nonlocal selects
            if isinstance(query, str) and query.lstrip().upper().startswith('SELECT'):
                selects += 1
            return real_execute(query, *args, **kwargs)

        monkeypatch.setattr(client, 'execute', counting)
        # One scan marks every outstanding unit no matter how many STARTED rows
        # the feed has accumulated: 25 died units, still a single SELECT.
        assert reconcile_died_receipts(client, ORIGO_DATABASE, feed='probe', now=now) == 25
        assert selects == 1
        # The deaths count toward backoff for those units only: the completed
        # unit (hash-carrying OK) and the fresh unit get no spurious marker.
        attempts, _ = failed_attempts(client, ORIGO_DATABASE, feed='probe', series='died-00', minute=old)
        assert attempts == 1
        attempts, _ = failed_attempts(client, ORIGO_DATABASE, feed='probe', series='done', minute=old)
        assert attempts == 0
        attempts, _ = failed_attempts(client, ORIGO_DATABASE, feed='probe', series='alive', minute=old)
        assert attempts == 0
        # Idempotent: the appended rows guard the next pass.
        selects = 0
        assert reconcile_died_receipts(client, ORIGO_DATABASE, feed='probe', now=now) == 0
        assert selects == 1
    finally:
        client.disconnect()


def test_beat_writes_only_with_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from origo.sources.contracts import WORKER_HEARTBEAT_ENV, beat_worker

    target = tmp_path / 'worker.heartbeat'
    monkeypatch.delenv(WORKER_HEARTBEAT_ENV, raising=False)
    beat_worker()
    assert not target.exists()
    monkeypatch.setenv(WORKER_HEARTBEAT_ENV, str(target))
    before = time.time()
    beat_worker()
    stamped = float(target.read_text())
    assert stamped >= before - 5


def test_slow_rest_fetch_beats_during_requests(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Beats land between paged requests, while the fetch is still running: the
    # check before each transport call sees every earlier request's beat.
    from origo.sources.contracts import WORKER_HEARTBEAT_ENV

    spec, _ = spot
    target = tmp_path / 'worker.heartbeat'
    monkeypatch.setenv(WORKER_HEARTBEAT_ENV, str(target))
    previous = rest.get_response
    seen: list[bool] = []

    def observing(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> daily.Response:
        seen.append(target.exists())
        return previous(url, params=params, headers=headers, weight=weight)

    monkeypatch.setattr(rest, 'get_response', observing)
    adapter = spec.provisional
    assert adapter is not None
    adapter.fetch(adapter.partition(KEY))
    assert len(seen) >= 2
    assert seen[0] is False and all(seen[1:])


def test_parallel_builds_complete_all_candidates(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Six 2s units would take 12s sequentially; the pool overlaps them. Ten
    # minutes past the anchor yields five backfill holes plus the current minute.
    spec, _ = spot
    late = ANCHOR + timedelta(minutes=10, seconds=5)
    threads: set[int] = set()
    calls: list[str] = []

    def slow(
        executed: RevisionedSourceSpec, operation: str, config: SourceRunConfig, *, run_id: str
    ) -> dict[str, object]:
        threads.add(threading.get_ident())
        calls.append(config.partition_key)
        time.sleep(2)
        return {'build_id': '00000000-0000-0000-0000-000000000000'}

    monkeypatch.setattr(provisional, 'execute_source', slow)
    feed = _feed(spec, tmp_path, _Dagster(), _Reporter())
    started = time.monotonic()
    outcome = feed.tick(late)
    elapsed = time.monotonic() - started
    assert len(outcome.processed) == 6 and outcome.failed == ()
    assert len(threads) > 1
    assert elapsed < 9


def test_provisional_build_beats_once_per_component(
    spot: tuple[RevisionedSourceSpec, list[dict[str, Any]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The REST beats stop at the last paged request; a fat minute then spends
    # its time in Arrow builds and inserts, so each finished component beats.
    # Only the lifecycle call sites are counted here; the adapter's own
    # per-request beats are covered by test_slow_rest_fetch_beats_during_requests.
    spec, requests = spot
    beats: list[None] = []
    monkeypatch.setattr('origo.sources.lifecycle.beat_worker', lambda: beats.append(None))
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        store = SourceStore(client, ORIGO_DATABASE, spec)
        runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
        assert spec.provisional is not None
        expected = len(store.components(spec.provisional.partition(KEY)))
        assert expected >= 1
        record = runtime.build(KEY, provisional=True)
        assert record.partition.key == KEY
        assert not requests
        assert len(beats) == expected
    finally:
        client.disconnect()
