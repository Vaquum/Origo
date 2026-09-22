from __future__ import annotations

import itertools
import logging
import multiprocessing
import os
import time
from datetime import UTC, datetime, timedelta
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid5

import pytest
from clickhouse_driver import Client as NativeClient

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.contracts import SourceError
from origo.steady_state import receipt_identity
from origo.steady_state.contracts import AttemptIdentity
from origo.steady_state.ownership import (
    WorkerOwner,
    assert_execution_owner,
    fence_retired_owners,
    prune_released_owners,
    require_retired_owner,
    worker_execution,
)
from origo.steady_state.receipt_identity import (
    ZERO_UUID,
    legacy_unresolved_count,
    legacy_unresolved_page,
    outstanding_owner_epochs,
    recover_confirmed_dead,
)
from origo.workers.receipts import (
    ensure_monitoring_tables,
    failed_attempts,
    failed_receipts_since,
    reconcile_died_receipts,
    record_receipt,
)

from .helpers import ORIGO_DATABASE

MINUTE = datetime(2026, 9, 21, 6, 45, tzinfo=UTC)
TOKEN = '46baddf2d912ca975168191c4387445d14b3b873ab7c2b8b8c3aba3154435bb0'
TABLE = f'{ORIGO_DATABASE}.worker_minute_log'
# The pre-S439 table exactly as production created it: eleven columns, this key.
LEGACY_DDL = f"""CREATE TABLE {TABLE} (
    feed LowCardinality(String), series LowCardinality(String), minute DateTime,
    rows UInt64, sha256 String, duration_ms UInt32, status LowCardinality(String),
    error_code LowCardinality(String), error String, worker_host String,
    recorded_at DateTime64(3, 'UTC'))
    ENGINE=ReplacingMergeTree ORDER BY (feed,series,minute,recorded_at)"""
IDENTITY = 'work_id, attempt_id, owner_epoch, state_token, prerequisite_key, event_id'


def _legacy_row(
    feed: str,
    series: str,
    status: str,
    recorded_at: datetime,
    *,
    minute: datetime = MINUTE,
    sha256: str = '',
    rows: int = 0,
    error_code: str = '',
) -> tuple[object, ...]:
    return (
        feed,
        series,
        minute.replace(tzinfo=None),
        rows,
        sha256,
        0,
        status,
        error_code,
        '',
        'legacy-host',
        recorded_at,
    )


def _old_writer(client: NativeClient, *rows: tuple[object, ...]) -> None:
    """An old binary's insert: eleven positional values, no column list."""
    client.execute(f'INSERT INTO {TABLE} VALUES', list(rows))


def _started_owner_child(environment: dict[str, str], root: str, sender: Connection) -> None:
    os.environ['ORIGO_SOURCE_LOCK_DIR'] = root
    owner = WorkerOwner(Path(root), 'provisional')
    client = NativeClient(
        host=environment['CLICKHOUSE_HOST'],
        port=int(environment['CLICKHOUSE_PORT']),
        user=environment['CLICKHOUSE_USER'],
        password=environment['CLICKHOUSE_PASSWORD'],
    )
    attempt = owner.attempt('perp:mount:' + TOKEN, state_token=TOKEN, prerequisite_key='perp:mount')
    record_receipt(
        client,
        ORIGO_DATABASE,
        feed='provisional',
        series='perp:mount',
        minute=MINUTE,
        rows=0,
        sha256=TOKEN,
        duration_ms=0,
        status='STARTED',
        attempt=attempt,
    )
    sender.send((owner.epoch, str(attempt.attempt_id)))
    sender.close()
    multiprocessing.Event().wait()
    client.disconnect()
    owner.close()


def test_dead_attempt_preserves_token_and_fences_its_owner(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert origo_test_env['CLICKHOUSE_HOST'] == '127.0.0.1'
    root = tmp_path / 'locks'
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(root))
    client = make_clickhouse_client(get_clickhouse_settings())
    context = multiprocessing.get_context('spawn')
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_started_owner_child, args=(origo_test_env, str(root), sender))
    live_owner = WorkerOwner(root, 'provisional')
    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        process.start()
        sender.close()
        assert receiver.poll(15), 'The real child did not persist its STARTED receipt'
        epoch, attempt_text = receiver.recv()
        identity = AttemptIdentity(
            'perp:mount:' + TOKEN, UUID(attempt_text), epoch, TOKEN, 'perp:mount'
        )
        live = live_owner.attempt(
            'perp:mount:new', state_token='b' * 64, prerequisite_key='perp:mount'
        )
        record_receipt(
            client,
            ORIGO_DATABASE,
            feed='provisional',
            series='perp:mount',
            minute=MINUTE,
            rows=0,
            sha256='b' * 64,
            duration_ms=0,
            status='STARTED',
            attempt=live,
        )
        # The observer's clock advancing an hour past both STARTED rows proves nothing:
        # the child still holds its lifetime lock, so nothing retires and nothing dies.
        later = datetime.now(UTC) + timedelta(hours=1)
        epochs = outstanding_owner_epochs(client, ORIGO_DATABASE, 'provisional')
        assert set(epochs) == {epoch, live_owner.epoch}
        probe = fence_retired_owners(root, 'provisional', epochs)
        assert probe.retired == () and probe.unknown == ()
        assert (
            reconcile_died_receipts(
                client,
                ORIGO_DATABASE,
                feed='provisional',
                now=later,
                confirmed_dead_owner_epochs=probe.retired,
            )
            == 0
        )
        assert prune_released_owners(root, 'provisional', outstanding=epochs) == ()
        with pytest.raises(SourceError, match='No durable retirement evidence'):
            recover_confirmed_dead(client, ORIGO_DATABASE, feed='provisional', owner_epochs=[epoch])

        process.terminate()
        process.join(5)
        assert not process.is_alive()
        probe = fence_retired_owners(root, 'provisional', epochs)
        assert probe.retired == (epoch,) and probe.unknown == ()
        assert (
            reconcile_died_receipts(
                client,
                ORIGO_DATABASE,
                feed='provisional',
                now=later,
                confirmed_dead_owner_epochs=probe.retired,
            )
            == 1
        )
        assert (
            failed_attempts(
                client, ORIGO_DATABASE, feed='provisional', series='perp:mount', token=TOKEN
            )[0]
            == 1
        )
        assert (
            failed_attempts(
                client, ORIGO_DATABASE, feed='provisional', series='perp:mount', token='b' * 64
            )[0]
            == 0
        )
        terminal = client.execute(
            f'SELECT status,error_code,sha256,state_token,work_id,attempt_id,owner_epoch,prerequisite_key '
            f"FROM {TABLE} WHERE attempt_id=%(attempt)s AND status!='STARTED'",
            {'attempt': identity.attempt_id},
        )
        assert terminal == [
            (
                'FAILED',
                'WORKER_DIED',
                TOKEN,
                TOKEN,
                identity.work_id,
                identity.attempt_id,
                epoch,
                'perp:mount',
            )
        ]
        # Recovery is idempotent and the dead epoch can neither restart nor succeed.
        assert (
            reconcile_died_receipts(
                client,
                ORIGO_DATABASE,
                feed='provisional',
                now=later,
                confirmed_dead_owner_epochs=probe.retired,
            )
            == 0
        )
        with pytest.raises(SourceError, match='retired'):
            record_receipt(
                client,
                ORIGO_DATABASE,
                feed='provisional',
                series='perp:mount',
                minute=MINUTE,
                rows=1,
                sha256=TOKEN,
                duration_ms=1,
                status='OK',
                attempt=identity,
            )
        with pytest.raises(SourceError, match='retired'):
            record_receipt(
                client,
                ORIGO_DATABASE,
                feed='provisional',
                series='perp:mount',
                minute=MINUTE,
                rows=0,
                sha256=TOKEN,
                duration_ms=0,
                status='STARTED',
                attempt=identity,
            )
        assert client.execute(
            f'SELECT count() FROM {TABLE} WHERE attempt_id=%(a)s', {'a': identity.attempt_id}
        ) == [(2,)]
        assert outstanding_owner_epochs(client, ORIGO_DATABASE, 'provisional') == (
            live_owner.epoch,
        )

        # The live attempt on the same minute was untouched and completes normally, once.
        record_receipt(
            client,
            ORIGO_DATABASE,
            feed='provisional',
            series='perp:mount',
            minute=MINUTE,
            rows=3,
            sha256='b' * 64,
            duration_ms=5,
            status='OK',
            attempt=live,
        )
        with pytest.raises(SourceError, match='already has a terminal'):
            record_receipt(
                client,
                ORIGO_DATABASE,
                feed='provisional',
                series='perp:mount',
                minute=MINUTE,
                rows=0,
                sha256='b' * 64,
                duration_ms=5,
                status='FAILED',
                attempt=live,
            )
        outstanding = outstanding_owner_epochs(client, ORIGO_DATABASE, 'provisional')
        assert outstanding == ()
        # Retention: the retired, fully recovered owner is pruned; the live one keeps its marker.
        assert prune_released_owners(root, 'provisional', outstanding=outstanding) == (epoch,)
        assert sorted(path.name for path in (root / 'worker-owners' / 'provisional').iterdir()) == [
            live_owner.epoch + '.json'
        ]
        with pytest.raises(SourceError, match='No durable retirement evidence'):
            require_retired_owner(root, 'provisional', epoch)

        with worker_execution(live_owner):
            assert_execution_owner()
            live_owner.close()
            with pytest.raises(SourceError, match='retired'):
                assert_execution_owner()
    finally:
        if process.pid is not None and process.is_alive():
            process.terminate()
            process.join(5)
        receiver.close()
        live_owner.close()
        client.disconnect()


def test_receipt_schema_keeps_old_writers_through_upgrade_merges_and_rollback(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    root = Path(os.environ['ORIGO_SOURCE_LOCK_DIR'])
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute(f'CREATE DATABASE IF NOT EXISTS {ORIGO_DATABASE}')
        client.execute(LEGACY_DDL)
        _old_writer(client, _legacy_row('legacy', 'perp:mount', 'STARTED', MINUTE, sha256=TOKEN))
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        assert client.execute(f'SELECT count(), any(sha256) FROM {TABLE}') == [(1, TOKEN)]

        # The old eleven-value writer keeps working after the upgrade, as do the
        # column-subset inserts other suites use; both read an empty identity.
        _old_writer(
            client,
            _legacy_row(
                'legacy', 'other', 'OK', MINUTE + timedelta(seconds=1), sha256='legacy', rows=1
            ),
            _legacy_row(
                'legacy', 'other', 'FAILED', MINUTE + timedelta(seconds=2), error_code='LEGACY'
            ),
        )
        client.execute(
            f'INSERT INTO {TABLE} (feed, series, minute, sha256, rows, status, error_code, recorded_at) VALUES',
            [('legacy', 'subset', MINUTE.replace(tzinfo=None), '', 0, 'STARTED', '', MINUTE)],
        )
        owner = WorkerOwner(root, 'legacy')
        attempt = owner.attempt(
            'perp:mount:' + TOKEN, state_token=TOKEN, prerequisite_key='perp:mount'
        )
        for status, rows in (('STARTED', 0), ('OK', 7)):
            record_receipt(
                client,
                ORIGO_DATABASE,
                feed='legacy',
                series='perp:mount',
                minute=MINUTE,
                rows=rows,
                sha256=TOKEN,
                duration_ms=1,
                status=status,
                attempt=attempt,
            )
        failing = owner.attempt('other:' + str(MINUTE))
        record_receipt(
            client,
            ORIGO_DATABASE,
            feed='legacy',
            series='other',
            minute=MINUTE,
            rows=0,
            sha256='',
            duration_ms=1,
            status='FAILED',
            error_code='PROBE',
            attempt=failing,
        )

        def snapshot() -> list[tuple[object, ...]]:
            return client.execute(
                f'SELECT *, {IDENTITY} FROM {TABLE} ORDER BY feed, series, recorded_at'
            )

        before = snapshot()
        assert len(before) == 7
        assert all(len(row) == 11 for row in client.execute(f'SELECT * FROM {TABLE}'))
        legacy = [row for row in before if row[9] == 'legacy-host' or row[1] == 'subset']
        assert len(legacy) == 4
        assert all(row[11:] == ('', ZERO_UUID, '', '', '', ZERO_UUID) for row in legacy)
        modern = [row for row in before if row[12] == attempt.attempt_id]
        assert [row[6] for row in modern] == ['STARTED', 'OK']
        assert all(
            row[11:16] == (attempt.work_id, attempt.attempt_id, owner.epoch, TOKEN, 'perp:mount')
            for row in modern
        )
        assert modern[0][16] != modern[1][16] and modern[0][16] != ZERO_UUID
        assert [row[4] for row in modern] == [TOKEN, TOKEN]

        # Merges keep every row and every identity: the sorting key is unchanged and
        # the materialized columns are persisted, not recomputed from a lost value.
        client.execute(f'OPTIMIZE TABLE {TABLE} FINAL')
        assert snapshot() == before
        assert client.execute(
            f'SELECT count() FROM system.parts WHERE database=%(d)s AND table=%(t)s AND active',
            {'d': ORIGO_DATABASE, 't': 'worker_minute_log'},
        ) == [(1,)]

        # Rollback: the old writer runs against the upgraded table, then the newer
        # binary restarts; nothing is rewritten and every identity is still there.
        _old_writer(
            client, _legacy_row('legacy', 'rollback', 'OK', MINUTE + timedelta(seconds=3), rows=2)
        )
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        after = snapshot()
        rollback = [row for row in after if row[1] == 'rollback']
        assert len(after) == 8 and before == [row for row in after if row[1] != 'rollback']
        assert rollback[0][11:] == ('', ZERO_UUID, '', '', '', ZERO_UUID)

        # Readers that predate identities see old and new failures alike.
        failed = failed_receipts_since(
            client,
            ORIGO_DATABASE,
            MINUTE - timedelta(days=1),
            datetime.now(UTC) + timedelta(minutes=1),
        )
        assert [(row.feed, row.series, row.error_code) for row in failed] == [
            ('legacy', 'other', 'LEGACY'),
            ('legacy', 'other', 'PROBE'),
        ]
        assert failed_attempts(
            client, ORIGO_DATABASE, feed='legacy', series='other', minute=MINUTE
        ) == (
            2,
            failed[1].recorded_at,
        )

        # The legacy start on perp:mount was paired by the later OK for that unit; the
        # subset start was not and is reported as UNKNOWN, never failed by inference.
        with caplog.at_level(logging.ERROR, logger='origo.workers.receipts'):
            assert (
                reconcile_died_receipts(
                    client, ORIGO_DATABASE, feed='legacy', now=MINUTE + timedelta(days=1)
                )
                == 0
            )
        assert [
            '1 unpaired legacy receipts retain UNKNOWN ownership' in record.message
            for record in caplog.records
        ] == [True]
        assert legacy_unresolved_page(
            client, ORIGO_DATABASE, 'legacy', MINUTE + timedelta(days=1)
        ).rows == (receipt_identity.LegacyReceipt('subset', MINUTE, '', MINUTE),)
        assert client.execute(f"SELECT count() FROM {TABLE} WHERE error_code='WORKER_DIED'") == [
            (0,)
        ]
        assert outstanding_owner_epochs(client, ORIGO_DATABASE, 'legacy') == ()
        owner.close()

        # Any other shape is refused rather than silently used.
        client.execute(f'ALTER TABLE {TABLE} DROP COLUMN event_id')
        with pytest.raises(SourceError, match='identity schema is inconsistent'):
            ensure_monitoring_tables(client, ORIGO_DATABASE)
        client.execute(f'DROP TABLE {TABLE} SYNC')
        client.execute(
            LEGACY_DDL.replace(
                "recorded_at DateTime64(3, 'UTC'))",
                "recorded_at DateTime64(3, 'UTC'), "
                'work_id String, attempt_id UUID, owner_epoch String, state_token String, '
                'prerequisite_key String, event_id UUID)',
            ).replace(
                'ORDER BY (feed,series,minute,recorded_at)',
                'ORDER BY (feed,series,minute,recorded_at,event_id)',
            )
        )
        with pytest.raises(SourceError, match='identity schema is inconsistent'):
            ensure_monitoring_tables(client, ORIGO_DATABASE)
    finally:
        client.disconnect()


def test_same_minute_receipts_never_collapse_and_conflicting_retries_fail(
    origo_test_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(os.environ['ORIGO_SOURCE_LOCK_DIR'])
    client = make_clickhouse_client(get_clickhouse_settings())
    first = WorkerOwner(root, 'depth')
    second = WorkerOwner(root, 'depth')

    def depth(
        status: Literal['OK', 'FAILED', 'STARTED'],
        *,
        attempt: AttemptIdentity | None = None,
        rows: int = 0,
        sha256: str = '',
        duration_ms: int = 0,
        error_code: str = '',
    ) -> None:
        record_receipt(
            client,
            ORIGO_DATABASE,
            feed='depth',
            series='depth20_snapshots',
            minute=MINUTE,
            rows=rows,
            sha256=sha256,
            duration_ms=duration_ms,
            status=status,
            error_code=error_code,
            attempt=attempt,
        )

    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        one = first.attempt('depth20_snapshots:' + str(MINUTE))
        two = second.attempt('depth20_snapshots:' + str(MINUTE))
        # A clock advancing 0.4 ms per reading lands consecutive writes on one stored
        # millisecond: four events of two attempts on one triple, then three
        # identity-less failures of the same triple.
        base = datetime(2026, 9, 21, 7, 0, tzinfo=UTC)
        ticks = itertools.count()
        monkeypatch.setattr(
            receipt_identity, '_now', lambda: base + timedelta(microseconds=400 * next(ticks))
        )
        depth('STARTED', attempt=one)
        depth('STARTED', attempt=two)
        depth('OK', attempt=one, rows=40, sha256='v1', duration_ms=3)
        depth('FAILED', attempt=two, duration_ms=3, error_code='PROBE')
        for _ in range(3):
            depth('FAILED', error_code='TICK')
        stamps = [
            row[0]
            for row in client.execute(f'SELECT recorded_at FROM {TABLE} ORDER BY recorded_at')
        ]
        assert stamps == [base + timedelta(milliseconds=offset) for offset in range(7)]
        monkeypatch.setattr(receipt_identity, '_now', lambda: datetime.now(UTC))
        client.execute(f'OPTIMIZE TABLE {TABLE} FINAL')
        assert client.execute(f'SELECT count() FROM {TABLE} FINAL') == [(7,)]
        assert client.execute(
            f'SELECT status, event_id FROM {TABLE} WHERE attempt_id=%(a)s ORDER BY recorded_at',
            {'a': one.attempt_id},
        ) == [('STARTED', uuid5(one.attempt_id, 'STARTED')), ('OK', uuid5(one.attempt_id, 'OK'))]

        # A repeated identical event is a no-op; a conflicting one is an error; both add nothing.
        depth('OK', attempt=one, rows=40, sha256='v1', duration_ms=99)
        with pytest.raises(SourceError, match='conflicting contents'):
            depth('OK', attempt=one, rows=41, sha256='v1', duration_ms=3)
        with pytest.raises(SourceError, match='conflicting contents'):
            depth('OK', attempt=one, rows=40, sha256='v2', duration_ms=3)
        depth('STARTED', attempt=one)
        with pytest.raises(SourceError, match='already has a terminal'):
            depth('FAILED', attempt=one, error_code='LATE')
        with pytest.raises(SourceError, match='already has a terminal'):
            depth('OK', attempt=two)
        assert client.execute(f'SELECT count() FROM {TABLE}') == [(7,)]
        assert outstanding_owner_epochs(client, ORIGO_DATABASE, 'depth') == ()

        # A publication receipt carries exactly its intended token.
        publish = first.attempt(
            'perp:mount:' + TOKEN, state_token=TOKEN, prerequisite_key='perp:mount'
        )
        with pytest.raises(ValueError, match='intended state token'):
            record_receipt(
                client,
                ORIGO_DATABASE,
                feed='depth',
                series='perp:mount',
                minute=MINUTE,
                rows=0,
                sha256='x',
                duration_ms=0,
                status='STARTED',
                attempt=publish,
            )
        assert client.execute(f'SELECT count() FROM {TABLE}') == [(7,)]
    finally:
        first.close()
        second.close()
        client.disconnect()


def test_receipt_clock_collisions_are_bounded(
    origo_test_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())

    def probe(rows: int) -> None:
        record_receipt(
            client,
            ORIGO_DATABASE,
            feed='probe',
            series='unit',
            minute=MINUTE,
            rows=rows,
            sha256='',
            duration_ms=0,
            status='OK',
        )

    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        stored = datetime(2026, 9, 21, 7, 0, 0, 500000, tzinfo=UTC)
        _old_writer(client, _legacy_row('probe', 'unit', 'STARTED', stored))
        # A clock stuck on the stored millisecond waits the bound, then fails loudly.
        monkeypatch.setattr(receipt_identity, '_now', lambda: stored)
        began = time.monotonic()
        with pytest.raises(SourceError, match='did not advance'):
            probe(0)
        assert 2 <= time.monotonic() - began < 4
        # A stored stamp far ahead of the clock is refused without waiting.
        monkeypatch.setattr(receipt_identity, '_now', lambda: stored - timedelta(seconds=10))
        began = time.monotonic()
        with pytest.raises(SourceError, match='beyond the collision bound'):
            probe(0)
        assert time.monotonic() - began < 1
        assert client.execute(f'SELECT count() FROM {TABLE}') == [(1,)]
        # A clock that advances past the stored millisecond is used as read, never invented.
        readings = iter([stored, stored, stored + timedelta(milliseconds=5, microseconds=700)])
        monkeypatch.setattr(receipt_identity, '_now', lambda: next(readings))
        probe(1)
        assert client.execute(f'SELECT recorded_at FROM {TABLE} ORDER BY recorded_at') == [
            (stored,),
            (stored + timedelta(milliseconds=5),),
        ]
    finally:
        client.disconnect()


def test_legacy_unresolved_receipts_are_paged_and_never_failed(
    origo_test_env: dict[str, str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    root = Path(os.environ['ORIGO_SOURCE_LOCK_DIR'])
    client = make_clickhouse_client(get_clickhouse_settings())
    owner = WorkerOwner(root, 'probe')
    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        now = datetime.now(UTC)
        old = now - timedelta(hours=1)
        minutes = [MINUTE + timedelta(minutes=index) for index in range(2300)]
        # 2,300 identity-less starts stamped identically, 100 of them paired later,
        # plus a fresh one inside the horizon and a modern start that is not legacy.
        _old_writer(
            client,
            *(_legacy_row('probe', 'unit', 'STARTED', old, minute=minute) for minute in minutes),
        )
        _old_writer(
            client,
            *(
                _legacy_row('probe', 'unit', 'OK', old, minute=minute, rows=1)
                for minute in minutes[:100]
            ),
        )
        _old_writer(
            client,
            _legacy_row('probe', 'unit', 'STARTED', now, minute=MINUTE - timedelta(minutes=1)),
        )
        record_receipt(
            client,
            ORIGO_DATABASE,
            feed='probe',
            series='unit',
            minute=MINUTE - timedelta(minutes=2),
            rows=0,
            sha256='',
            duration_ms=0,
            status='STARTED',
            attempt=owner.attempt('unit:modern'),
        )
        cutoff = now - timedelta(seconds=300)
        seen: list[datetime] = []
        cursor = None
        pages = 0
        while True:
            page = legacy_unresolved_page(client, ORIGO_DATABASE, 'probe', cutoff, after=cursor)
            pages += 1
            seen.extend(row.minute for row in page.rows)
            assert all(
                row.series == 'unit'
                and row.recorded_at == old.replace(microsecond=old.microsecond // 1000 * 1000)
                for row in page.rows
            )
            cursor = page.next
            if cursor is None:
                break
        assert pages == 3 and seen == minutes[100:]
        assert legacy_unresolved_count(
            client, ORIGO_DATABASE, 'probe', cutoff, page_bound=2
        ) == receipt_identity.LegacyUnresolved(2000, False)
        assert legacy_unresolved_count(
            client, ORIGO_DATABASE, 'probe', cutoff
        ) == receipt_identity.LegacyUnresolved(2200, True)
        with caplog.at_level(logging.ERROR, logger='origo.workers.receipts'):
            assert reconcile_died_receipts(client, ORIGO_DATABASE, feed='probe', now=now) == 0
        assert [record.message for record in caplog.records] == [
            'probe: 2200 unpaired legacy receipts retain UNKNOWN ownership; no death was inferred'
        ]
        assert client.execute(f"SELECT count() FROM {TABLE} WHERE status='FAILED'") == [(0,)]
        assert outstanding_owner_epochs(client, ORIGO_DATABASE, 'probe') == (owner.epoch,)
    finally:
        owner.close()
        client.disconnect()


def test_concurrent_worker_startup_installs_one_compatible_receipt_schema(
    origo_test_env: dict[str, str],
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    def initialize(_index: int) -> None:
        client = make_clickhouse_client(get_clickhouse_settings())
        try:
            ensure_monitoring_tables(client, ORIGO_DATABASE)
        finally:
            client.disconnect()

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(initialize, range(8)))
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        schema = client.execute(
            'SELECT name, default_kind FROM system.columns WHERE database=%(database)s '
            "AND table='worker_minute_log' ORDER BY position",
            {'database': ORIGO_DATABASE},
        )
        assert len(schema) == 18
        assert sum(kind == 'MATERIALIZED' for _, kind in schema) == 6
        assert ('_attempt', 'EPHEMERAL') in schema
    finally:
        client.disconnect()


def test_prerequisite_backoff_survives_token_changes_and_redrives(
    origo_test_env: dict[str, str], binance_fixture_server_root_url: str,
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace
    from dagster import Definitions
    from origo.sources.bundle import build_source_bundle
    from origo.sources.profiles import consumer_base
    from origo.sources.storage import SourceStore
    from origo.steady_state.prerequisites import may_attempt_prerequisite, open_prerequisites
    from .test_steady_state_publication import _Prepared, SPEC, CANONICAL_DAY

    prepared = _Prepared(tmp_path, monkeypatch, binance_fixture_server_root_url)
    limited = replace(SPEC, orchestration=replace(SPEC.orchestration, retry_count=1))
    try:
        prepared.build_minute()
        record = prepared.runtime.build(CANONICAL_DAY)
        before = prepared.store.snapshot().token
        monkeypatch.setattr(consumer_base, 'MOUNT_WORKER_MONTH_CAP', 0)
        with pytest.raises(SourceError, match='months'):
            prepared.worker_publish()
        (first,) = open_prerequisites(prepared.store)
        prepared.runtime.rollback(record, operator='test', reason='Retain genuine rows, change selected generation')
        assert prepared.store.snapshot().token != before
        with pytest.raises(SourceError, match='months'):
            prepared.worker_publish()
        # Reconstruct the reader as a new process would; no in-memory retry state survives.
        resumed = SourceStore(prepared.client, ORIGO_DATABASE, SPEC)
        (second,) = open_prerequisites(resumed)
        assert second.failure_key == first.failure_key and second.attempts == 2
        assert not may_attempt_prerequisite(
            resumed, limited, consumer='mount', error_code='RENDER_DEFERRED',
            now=second.last_failed_at + timedelta(hours=24),
        )
        prepared.runtime.failures.record(
            operation='consumer', consumer='unrelated', scope='CONSUMER',
            error_code='UNRELATED_TEST_FAULT', message='An independently scoped controlled fault.',
        )
        # Native publisher owns full-history admission; there is no typed allow_full flag.
        # This isolated fixture declared a shortened coverage anchor at setup.
        # Native preparation must repeat that same declaration, not mutate its anchor.
        fixture_spec = replace(SPEC, partitions=replace(
            SPEC.partitions, first_day=prepared.store.anchor().date(),
        ))
        bundle = build_source_bundle(fixture_spec)
        definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs)
        job = definitions.resolve_job_def(f'publish_{SPEC.key}_mount_job')
        result = job.execute_in_process()
        assert result.success
        assert open_prerequisites(resumed) == ()
        assert may_attempt_prerequisite(
            resumed, limited, consumer='mount', error_code='RENDER_DEFERRED', now=datetime.now(UTC),
        )
        remaining = prepared.client.execute(
            f'SELECT argMax(event_type,event_time) FROM {ORIGO_DATABASE}.source_failure_log '
            "WHERE error_code='UNRELATED_TEST_FAULT' GROUP BY failure_key",
        )
        assert remaining == [('FAILED',)]
        assert prepared.manifest()['pinned_token'] == prepared.store.snapshot().token
    finally:
        prepared.close()
