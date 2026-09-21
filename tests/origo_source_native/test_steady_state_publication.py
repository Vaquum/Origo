"""S439 M07/M08: resumable bulk publication and the completion barrier, on authentic
fixtures (the official 2020-01-01 spot archive and the captured 2025-01-01T00:00 REST
minute) against the pinned ClickHouse image. Small CI cases, not full-scale trials."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query.binance_spot_kline_rollups import dollar_month, time_month
from origo.sources import capacity, publication
from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters import binance_spot_rest as rest
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import SourceRunConfig, execute_source
from origo.sources.contracts import SourceError
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.sources.profiles import consumer_base, spot_consumers
from origo.sources.profiles.formulas.spot_series import SPECS
from origo.sources.storage import SourceStore
from origo.steady_state.prerequisites import (
    may_attempt_prerequisite,
    open_prerequisites,
    prerequisite_key,
    render_active,
)
from origo.steady_state.publication import (
    CheckpointJournal,
    delivered_coverage,
    manifest_delivered_through,
)
from origo.utils.arrow_store import build_series_frame, series_source_files
from origo.workers.receipts import ensure_monitoring_tables

from .helpers import ORIGO_DATABASE
from .test_binance_daily_source_adapter import REST, archive_response
from .test_revisioned_source_framework_consumers import FakeHfApi

PROVENANCE = json.loads((REST / 'provenance.json').read_text())
ANCHOR = datetime.fromisoformat(PROVENANCE['minute_start']).astimezone(UTC)
MINUTE_KEY = ANCHOR.strftime('%Y-%m-%dT%H:%M:%SZ')
CANONICAL_DAY = '2020-01-01'
SPEC = BINANCE_SPOT_TRADES_SPEC
MONTHS = ((2020, 1), (2025, 1))
ENVIRONMENT_KEYS = (
    'CLICKHOUSE_HOST',
    'CLICKHOUSE_PORT',
    'CLICKHOUSE_HTTP_PORT',
    'CLICKHOUSE_USER',
    'CLICKHOUSE_PASSWORD',
    'CLICKHOUSE_DATABASE',
    'LOCAL_PARQUET_DIR',
    'LOCAL_ARROW_DIR',
    'ORIGO_SOURCE_LOCK_DIR',
    'ORIGO_SOURCE_PUBLICATION_ROOT',
)


def _render_then_die(environment: dict[str, str], destination: str, die_after: int) -> None:
    """A native full-history publication that the kernel kills after ``die_after`` month
    queries: no exception, no ``finally``, exactly what a watchdog or OOM kill leaves."""
    os.environ.update(environment)
    real = consumer_base._month_frame
    calls = [0]

    def counting(*args: object, **kwargs: object) -> pl.DataFrame:
        if calls[0] == die_after:
            os.kill(os.getpid(), signal.SIGKILL)
        calls[0] += 1
        return real(*args, **kwargs)

    consumer_base._month_frame = counting
    execute_source(
        SPEC,
        'consumer_mount',
        SourceRunConfig(destination=destination, allow_full_history=True),
        run_id=f'test-full-render:{uuid4()}',
    )


def _kill_full_render_after(environment: dict[str, str], destination: Path, die_after: int) -> None:
    context = multiprocessing.get_context('spawn')
    process = context.Process(
        target=_render_then_die, args=(environment, str(destination), die_after)
    )
    process.start()
    process.join(300)
    assert not process.is_alive(), 'the full render did not die'
    assert process.exitcode == -signal.SIGKILL, process.exitcode


class _Prepared:
    def __init__(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture_root_url: str
    ) -> None:
        monkeypatch.setenv(
            'BINANCE_SPOT_DAILY_TRADES_BASE_URL',
            fixture_root_url + '/spot/daily/trades/revisioned/',
        )
        monkeypatch.setattr(daily, 'get_response', archive_response)
        monkeypatch.delenv('BINANCE_SPOT_REST_BASE_URL', raising=False)
        self.requests: list[dict[str, object]] = list(PROVENANCE['requests'])

        def captured(
            url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
        ) -> daily.Response:
            item = self.requests.pop(0)
            assert url == item['url'] and params == item['params']
            return daily.Response((REST / str(item['file'])).read_bytes(), {}, 200)

        monkeypatch.setattr(rest, 'get_response', captured)
        monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
        monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
        monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
        monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'shadow'))
        monkeypatch.setenv('HF_TOKEN', 'test-token')
        FakeHfApi.calls = []
        monkeypatch.setattr(spot_consumers, 'HfApi', FakeHfApi)
        self.tmp_path = tmp_path
        self.parquet_root = tmp_path / 'parquet'
        self.arrow_root = tmp_path / 'arrow'
        self.lock_root = tmp_path / 'locks'
        self.publication_root = tmp_path / 'shadow'
        self.mount = self.publication_root / SPEC.key / 'mount'
        self.environment = {key: os.environ[key] for key in ENVIRONMENT_KEYS}
        self.client = make_clickhouse_client(get_clickhouse_settings())
        self.store = SourceStore(self.client, ORIGO_DATABASE, SPEC)
        self.runtime = SourceRuntime(SPEC, self.store, self.lock_root, str(uuid4()))
        self.runtime.setup(anchor=ANCHOR)
        ensure_monitoring_tables(self.client, ORIGO_DATABASE)

    def close(self) -> None:
        self.client.disconnect()

    def worker_publish(self) -> None:
        """One worker tick's publication: its own run identity, the worker month cap."""
        SourceRuntime(SPEC, self.store, self.lock_root, f'worker:test:{uuid4()}').publish(
            'mount', str(self.mount)
        )

    def build_minute(self) -> None:
        execute_source(
            SPEC, 'provisional', SourceRunConfig(partition_key=MINUTE_KEY), run_id=str(uuid4())
        )
        assert not self.requests, 'the captured minute was fetched through the source runtime'

    def publish_mount(self, *, allow_full: bool) -> dict[str, object]:
        execute_source(
            SPEC,
            'consumer_mount',
            SourceRunConfig(destination=str(self.mount), allow_full_history=allow_full),
            run_id=f'test-publish:{uuid4()}',
        )
        return self.manifest()

    def manifest(self) -> dict[str, object]:
        return json.loads((self.mount / 'latest.json').read_text())

    def count_month_queries(
        self, monkeypatch: pytest.MonkeyPatch, action: Callable[[], object]
    ) -> int:
        real = consumer_base._month_frame
        calls = [0]

        def counting(*args: object, **kwargs: object) -> pl.DataFrame:
            calls[0] += 1
            return real(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(consumer_base, '_month_frame', counting)
            action()
        return calls[0]

    def mirror_identities(self) -> dict[str, tuple[int, int, int]]:
        found: dict[str, tuple[int, int, int]] = {}
        for series in SPECS:
            for path in series_source_files(series, self.parquet_root):
                stat = path.stat()
                found[str(path)] = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        return found

    def arrow_targets(self) -> dict[str, Path | None]:
        latest = {series.name: self.arrow_root / series.name / 'latest.arrow' for series in SPECS}
        return {
            name: path.resolve() if path.is_symlink() else None for name, path in latest.items()
        }

    def assert_reader_contents(self, months: tuple[tuple[int, int], ...]) -> None:
        """The mirror month files and Arrow series equal the original monthly readers over
        the legacy alias views: an independently encoded oracle on the same rows."""
        expected_root = self.tmp_path / f'oracle-{uuid4().hex}'
        for series in SPECS:
            for year, month in months:
                if series.family == 'time':
                    frame = time_month(interval_minutes=series.size, year=year, month=month)
                else:
                    frame = dollar_month(ratio=series.size, year=year, month=month)
                assert frame.height > 0
                target = expected_root / series.sub_path / f'{year:04d}' / f'{month:02d}.parquet'
                target.parent.mkdir(parents=True, exist_ok=True)
                frame.write_parquet(target)
                mirrored = (
                    self.parquet_root / series.sub_path / f'{year:04d}' / f'{month:02d}.parquet'
                )
                assert_frame_equal(pl.read_parquet(mirrored), frame)
            assert_frame_equal(
                pl.read_ipc(self.arrow_root / series.name / 'latest.arrow'),
                build_series_frame(series, expected_root).df,
            )


def _checkpoint_sidecars(journal: CheckpointJournal) -> list[dict[str, object]]:
    records = [json.loads(path.read_text()) for path in journal.directory.rglob('*.json')]
    return sorted(records, key=lambda item: (str(item['month']), str(item['series'])))


def _failure_events(store: SourceStore, code: str) -> list[tuple[object, ...]]:
    return store.execute(
        'SELECT failure_key, event_type, consumer FROM origo.source_failure_log '
        "WHERE operation='consumer' AND error_code=%(code)s ORDER BY event_time",
        {'code': code},
    )


def _month_tokens(store: SourceStore) -> dict[str, str]:
    return consumer_base.month_tokens(SPEC.key, store.snapshot(), export_start_date='2020-01-01')


def test_bulk_render_resumes_without_blocking_minute_work(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _Prepared(tmp_path, monkeypatch, binance_fixture_server_root_url)
    store, runtime, mount = prepared.store, prepared.runtime, prepared.mount
    journal = CheckpointJournal(prepared.parquet_root, None)
    try:
        # Minute ingestion is admitted while a publication of the same source holds the
        # consumer lock: the worker's probe sees the render, the build does not wait on it.
        held = threading.Event()
        release = threading.Event()

        def hold_consumer_lock() -> None:
            with source_lock(prepared.lock_root, SPEC.key, 'consumer_mount', wait=True):
                held.set()
                release.wait(60)

        holder = threading.Thread(target=hold_consumer_lock)
        holder.start()
        assert held.wait(10)
        try:
            assert render_active(prepared.lock_root, SPEC.key, 'mount')
            prepared.build_minute()
        finally:
            release.set()
            holder.join(10)
        assert not render_active(prepared.lock_root, SPEC.key, 'mount')
        record = runtime.build(CANONICAL_DAY)
        assert sorted(_month_tokens(store)) == ['2020-01', '2025-01']

        # The worker path defers a render past its cap with a stable prerequisite: the
        # failure key is the consumer blocker, not the state token.
        monkeypatch.setattr(consumer_base, 'MOUNT_WORKER_MONTH_CAP', 0)
        with pytest.raises(SourceError) as deferred:
            prepared.worker_publish()
        assert deferred.value.code == 'RENDER_DEFERRED'
        assert f'publish_{SPEC.key}_mount_job' in deferred.value.safe_message
        assert 'with allow_full_history' not in deferred.value.safe_message
        key = prerequisite_key(SPEC.key, 'mount', 'RENDER_DEFERRED')
        assert _failure_events(store, 'RENDER_DEFERRED') == [(key, 'FAILED', 'mount')]
        (blocker,) = open_prerequisites(store)
        assert (blocker.failure_key, blocker.consumer, blocker.attempts) == (key, 'mount', 1)
        first_failed = blocker.last_failed_at
        assert not may_attempt_prerequisite(
            store, SPEC, consumer='mount', error_code='RENDER_DEFERRED', now=first_failed
        )
        assert may_attempt_prerequisite(
            store,
            SPEC,
            consumer='mount',
            error_code='RENDER_DEFERRED',
            now=first_failed + timedelta(seconds=61),
        )
        assert not (mount / 'latest.json').exists()

        # The native full-history publisher (code-owned allow_full, no typed flag) dies
        # after fourteen month queries: twelve of 2020-01 and two of 2025-01 are kept as
        # verified checkpoints, the mirror and Arrow store are untouched, and the mirror
        # readers never see the checkpoints.
        _kill_full_render_after(prepared.environment, mount, die_after=14)
        sidecars = _checkpoint_sidecars(journal)
        assert [(item['month'], item['series']) for item in sidecars] == sorted(
            [('2020-01', series.name) for series in SPECS]
            + [('2025-01', series.name) for series in SPECS[:2]]
        )
        assert all(item['row_count'] > 0 and item['size'] > 0 for item in sidecars)
        assert not (mount / 'latest.json').exists()
        assert prepared.mirror_identities() == {}
        assert all(target is None for target in prepared.arrow_targets().values())
        assert all(series_source_files(series, prepared.parquet_root) == [] for series in SPECS)
        assert not list(prepared.parquet_root.glob('.staging-*'))

        # A canonical correction changes 2020-01's token: the same prerequisite counts a
        # second attempt (new token, same key) and only 2020-01's checkpoints are
        # invalidated; 2025-01's survive the token change.
        first_tokens = _month_tokens(store)
        runtime.rollback(record, operator='test', reason='canonical correction during S439 M07')
        second_tokens = _month_tokens(store)
        assert second_tokens['2020-01'] != first_tokens['2020-01']
        assert second_tokens['2025-01'] == first_tokens['2025-01']
        with pytest.raises(SourceError) as deferred_again:
            prepared.worker_publish()
        assert deferred_again.value.code == 'RENDER_DEFERRED'
        assert '2 months' in deferred_again.value.safe_message
        (blocker,) = open_prerequisites(store, consumer='mount')
        assert (blocker.failure_key, blocker.attempts) == (key, 2)
        assert not may_attempt_prerequisite(
            store,
            SPEC,
            consumer='mount',
            error_code='RENDER_DEFERRED',
            now=blocker.last_failed_at + timedelta(seconds=61),
        )
        assert may_attempt_prerequisite(
            store,
            SPEC,
            consumer='mount',
            error_code='RENDER_DEFERRED',
            now=blocker.last_failed_at + timedelta(seconds=121),
        )
        assert [
            (item['month'], item['series']) for item in _checkpoint_sidecars(journal)
        ] == sorted(('2025-01', series.name) for series in SPECS[:2])

        # The resumed native run queries only the missing work: twelve months of the
        # corrected 2020-01 and the ten remaining 2025-01 series; the two checkpoints are
        # reused byte-for-byte and the commit publishes the whole pinned state.
        monkeypatch.setattr(consumer_base, 'MOUNT_WORKER_MONTH_CAP', 4)
        reused_inodes: dict[str, int] = {}
        for series in SPECS[:2]:
            checkpoint = journal.find_month(series, '2025-01', second_tokens['2025-01'])
            assert checkpoint is not None
            reused_inodes[series.name] = checkpoint.path.stat().st_ino
        queries = prepared.count_month_queries(
            monkeypatch, lambda: prepared.publish_mount(allow_full=True)
        )
        assert queries == 22
        manifest = prepared.manifest()
        assert manifest['render']['month_queries'] == 22
        assert manifest['render']['checkpoints_reused'] == 2
        assert manifest['generation'] == 1
        assert len(manifest['files']) == 36
        assert not journal.directory.exists()
        for series in SPECS[:2]:
            installed = prepared.parquet_root / series.sub_path / '2025' / '01.parquet'
            assert installed.stat().st_ino == reused_inodes[series.name]
        assert publication.publication_current(
            SPEC, 'mount', store.snapshot().token, root=prepared.publication_root, pinned=True
        )
        assert open_prerequisites(store) == ()
        assert [event for _, event, _ in _failure_events(store, 'RENDER_DEFERRED')] == [
            'FAILED',
            'FAILED',
            'RECOVERED',
        ]
        prepared.assert_reader_contents(MONTHS)

        # Delivered coverage is certified continuity from the anchor, never max(end).
        assert manifest_delivered_through(manifest) == ANCHOR + timedelta(minutes=1)
        assert manifest['coverage']['newest_end'] == manifest['active_through']
        assert manifest['coverage']['hidden_gap'] is False
        assert (
            manifest['coverage']['series']['time_1m'] == (ANCHOR + timedelta(minutes=1)).isoformat()
        )
        assert manifest['coverage']['series']['time_4h'] == ANCHOR.isoformat()
        gapped = delivered_coverage(datetime(2020, 1, 1, tzinfo=UTC), store.snapshot(), SPECS)
        assert gapped.delivered_through == datetime(2020, 1, 2, tzinfo=UTC)
        assert gapped.newest_end == ANCHOR + timedelta(minutes=1)
        assert gapped.hidden_gap

        # An unchanged state renders nothing: every month is reused through its recorded
        # identity, no file is rewritten and no checkpoint is created.
        before = prepared.mirror_identities()
        assert (
            prepared.count_month_queries(
                monkeypatch,
                lambda: spot_consumers._mount(store, store.snapshot(), str(mount)),
            )
            == 0
        )
        repeated = prepared.manifest()
        assert repeated['generation'] == 2 and repeated['render']['month_queries'] == 0
        assert prepared.mirror_identities() == before
        assert not journal.directory.exists()
    finally:
        prepared.close()


def test_history_load_preserves_live_currency_and_completion_barrier(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _Prepared(tmp_path, monkeypatch, binance_fixture_server_root_url)
    store, mount = prepared.store, prepared.mount
    journal = CheckpointJournal(prepared.parquet_root, None)
    # The storage-capacity admission of the canonical path is stubbed exactly as the
    # backfill suite stubs it; the load here is the real build, not a capacity proof.
    monkeypatch.setattr(
        capacity, '_volumes', lambda runtime: (capacity._Volume('test-volume', tmp_path),)
    )
    monkeypatch.setattr(
        capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7)
    )
    try:
        # LIVE delivery first: the captured minute is built and published as generation 1.
        prepared.build_minute()
        live = prepared.publish_mount(allow_full=False)
        live_token = str(live['pinned_token'])
        live_bytes = (mount / 'latest.json').read_bytes()
        live_files = prepared.mirror_identities()
        live_arrow = prepared.arrow_targets()
        assert live['generation'] == 1 and len(live['files']) == 24
        assert manifest_delivered_through(live) == ANCHOR + timedelta(minutes=1)
        prepared.assert_reader_contents(((2025, 1),))

        # A native partitioned history load (the backfill's per-day call) activates a
        # canonical day. The live generation stays exactly as committed and readable.
        loaded = execute_source(
            SPEC,
            'canonical',
            SourceRunConfig(partition_key=CANONICAL_DAY, automatic_capacity=True),
            run_id=f'test-backfill:{uuid4()}',
        )
        assert loaded['partition_key'] == CANONICAL_DAY
        assert store.snapshot().token != live_token
        assert (mount / 'latest.json').read_bytes() == live_bytes
        assert prepared.mirror_identities() == live_files
        assert prepared.arrow_targets() == live_arrow
        assert publication.publication_current(
            SPEC, 'mount', live_token, root=prepared.publication_root, pinned=True
        )

        # The completion barrier: the source cannot report the backfill complete while a
        # required consumer has not published the loaded state.
        with pytest.raises(SourceError) as blocked:
            execute_source(SPEC, 'complete', SourceRunConfig(), run_id=str(uuid4()))
        assert blocked.value.code == 'GENERATION_CHANGED'

        # The full render of the loaded history dies after six month queries. Nothing
        # staged reaches the mirror, the Arrow store or the manifest; the live generation
        # is still what every reader sees.
        _kill_full_render_after(prepared.environment, mount, die_after=6)
        assert [
            (item['month'], item['series']) for item in _checkpoint_sidecars(journal)
        ] == sorted(('2020-01', series.name) for series in SPECS[:6])
        assert (mount / 'latest.json').read_bytes() == live_bytes
        assert prepared.mirror_identities() == live_files
        assert prepared.arrow_targets() == live_arrow
        assert all(
            not path.is_relative_to(journal.directory)
            for series in SPECS
            for path in series_source_files(series, prepared.parquet_root)
        )
        with pytest.raises(SourceError) as still_blocked:
            execute_source(SPEC, 'complete', SourceRunConfig(), run_id=str(uuid4()))
        assert still_blocked.value.code == 'GENERATION_CHANGED'

        # The resumed render queries the six missing 2020-01 series only: 2025-01 is
        # outside the loaded history's range and is reused from the live generation.
        queries = prepared.count_month_queries(
            monkeypatch, lambda: prepared.publish_mount(allow_full=True)
        )
        assert queries == 6
        manifest = prepared.manifest()
        assert manifest['render'] == {
            'month_queries': 6,
            'checkpoints_reused': 6,
            'checkpoints_invalidated': 0,
            'months_changed': 1,
            'seconds': manifest['render']['seconds'],
        }
        assert manifest['generation'] == 2 and len(manifest['files']) == 36
        assert manifest['state_token'] == store.snapshot(canonical_only=True).token
        assert manifest_delivered_through(manifest) == ANCHOR + timedelta(minutes=1)
        january = {path: identity for path, identity in live_files.items() if '/2025/' in path}
        assert {
            path: identity
            for path, identity in prepared.mirror_identities().items()
            if '/2025/' in path
        } == january
        prepared.assert_reader_contents(MONTHS)

        # Every required publication, then completion: the canonical-only consumer is
        # still missing, so the barrier holds until it too has published.
        with pytest.raises(SourceError) as mount_only:
            execute_source(SPEC, 'complete', SourceRunConfig(), run_id=str(uuid4()))
        assert mount_only.value.code == 'GENERATION_CHANGED'
        execute_source(
            SPEC,
            'consumer_huggingface',
            SourceRunConfig(destination=str(prepared.publication_root / SPEC.key / 'huggingface')),
            run_id=str(uuid4()),
        )
        assert len([call for call, _ in FakeHfApi.calls if call == 'upload_folder']) == 12
        completed = execute_source(SPEC, 'complete', SourceRunConfig(), run_id=str(uuid4()))
        assert completed['healthy'] is True
    finally:
        prepared.close()
