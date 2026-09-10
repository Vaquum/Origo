from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import RolloutStage
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response


def test_real_spot_partition_activates_only_one_complete_generation(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        record = runtime.build('2017-08-17')
        assert record.generation == 1
        assert len(record.component_hashes) == 7
        assert runtime.store.execute(
            'SELECT count() FROM origo.binance_spot_trades_raw_current'
        ) == [(3427,)]
        assert runtime.build('2017-08-17') == record
        assert runtime.store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]
        snapshot = runtime.store.snapshot()
        assert len(snapshot.records) == 1
        for component in runtime.store.components(record.partition):
            assert runtime.store.rows(component.key, snapshot)
    finally:
        client.disconnect()


def test_spot_revision_validation_correction_and_lock_failures_preserve_current_state(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    import io
    import subprocess
    import sys
    import zipfile

    from origo.sources.contracts import Row

    from .test_binance_daily_source_adapter import ARCHIVES

    monkeypatch.setattr(daily, 'get_response', archive_response)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup()
        first = runtime.build('2017-08-17')
        first_rows = store.rows('raw', store.snapshot())
        # Explicit test packaging: the same unmodified official CSV, different ZIP compression.
        # This models a changed opaque revision; the wrapper is not claimed as an official object.
        content = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.csv').read_bytes()
        packaged = io.BytesIO()
        with zipfile.ZipFile(packaged, 'w', compression=zipfile.ZIP_STORED) as archive:
            archive.writestr('BTCUSDT-trades-2017-08-17.csv', content)
        body = packaged.getvalue()
        checksum = hashlib.sha256(body).hexdigest()

        def correction(url: str) -> daily.Response:
            payload = (
                f'{checksum}  BTCUSDT-trades-2017-08-17.zip'.encode()
                if url.endswith('CHECKSUM')
                else body
            )
            return daily.Response(payload, {}, 200)

        monkeypatch.setattr(daily, 'get_response', correction)
        execute = store.execute
        interrupted = False

        def fail_component(query: str, params: object | None = None) -> list[Row]:
            nonlocal interrupted
            if (
                'INSERT INTO origo.binance_spot_trades_dollar_revisions' in query
                and not interrupted
            ):
                interrupted = True
                raise OSError('Injected component write interruption')
            return execute(query, params)

        monkeypatch.setattr(store, 'execute', fail_component)
        with pytest.raises(OSError):
            runtime.build('2017-08-17')
        assert store.generation(first.partition) == 1
        assert store.execute(
            "SELECT operation, component FROM origo.source_failure_log WHERE event_type='FAILED'"
        ) == [('component', 'dollar')]
        assert store.rows('raw', store.snapshot()) == first_rows
        monkeypatch.setattr(store, 'execute', execute)
        committed = False

        def uncertain_insert(query: str, params: object | None = None) -> list[Row]:
            nonlocal committed
            rows = execute(query, params)
            if query.startswith('INSERT INTO origo.source_activation_log') and not committed:
                committed = True
                raise OSError('Injected lost acknowledgement after commit')
            return rows

        monkeypatch.setattr(store, 'execute', uncertain_insert)
        second = runtime.build('2017-08-17')
        assert committed and second.generation == 2
        assert second.revision == checksum
        assert store.rows('raw', store.snapshot()) == first_rows
        assert store.execute('SELECT count() FROM origo.source_activation_log') == [(2,)]
        monkeypatch.setattr(store, 'execute', execute)
        with pytest.raises(RuntimeError, match='Stale expected generation'):
            runtime._activate(first, expected=1)
        assert store.generation(first.partition) == 2
        assert store.execute('SELECT count() FROM origo.source_activation_log') == [(2,)]
        with pytest.raises(RuntimeError, match='Official Binance revision changed'):
            runtime.rollback(
                first, operator='test', reason='Older official revision requires quarantine'
            )
        restored = runtime.rollback(
            first, operator='test', reason='Explicit quarantine proof', quarantine=True
        )
        assert restored.generation == 3
        assert store.rows('raw', store.snapshot()) == first_rows
        assert runtime.cleanup() == ()

        child = subprocess.Popen(
            [
                sys.executable,
                '-c',
                'import sys; from pathlib import Path; from origo.sources.locking import source_lock; '
                'lock=source_lock(Path(sys.argv[1]), "binance_spot_trades", "heavy"); '
                'lock.__enter__(); print("locked", flush=True); sys.stdin.read()',
                str(runtime.lock_root),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            assert child.stdout is not None and child.stdout.readline().strip() == 'locked'
            with pytest.raises(RuntimeError, match='already held'):
                runtime.build('2017-08-17')
            assert store.generation(first.partition) == 3
        finally:
            child.kill()
            child.wait(timeout=10)
        assert runtime.build('2017-08-17').generation == 4
        failed = store.execute(
            'SELECT build_id FROM origo.source_build_log WHERE build_id NOT IN '
            '(SELECT build_id FROM origo.source_activation_log)'
        )[0][0]
        # Only attempt timestamps are aged; every market row remains the real fixture row.
        client.execute(
            'ALTER TABLE origo.source_build_log UPDATE started_at=now()-INTERVAL 8 DAY '
            'WHERE build_id IN %(builds)s',
            {'builds': (failed, second.build_id)},
            settings={'mutations_sync': 2},
        )
        assert set(runtime.cleanup()) == {str(failed), str(second.build_id)}
        assert (
            runtime.rollback(
                second, operator='test', reason='Reactivate a retained build after cleanup planning'
            ).generation
            == 5
        )
        assert runtime.cleanup(dry_run=False) == (str(failed),)
        assert store.rows('raw', store.snapshot()) == first_rows
        assert store.execute(
            'SELECT count() FROM origo.binance_spot_trades_raw_revisions WHERE build_id=%(build)s',
            {'build': failed},
        ) == [(0,)]
        assert runtime.cleanup() == ()
        with pytest.raises(RuntimeError, match='do not share'):
            replace(runtime, lock_root=tmp_path / 'unshared').setup()
    finally:
        client.disconnect()


def test_real_provisional_components_share_one_generation_and_frontier(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    from datetime import UTC, datetime

    from origo.sources.adapters import binance_spot_rest as rest

    from .test_binance_daily_source_adapter import REST

    provenance = json.loads((REST / 'provenance.json').read_text())
    requests = list(provenance['requests'])

    def captured(
        url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int
    ) -> daily.Response:
        item = requests.pop(0)
        assert url == item['url'] and params == item['params']
        return daily.Response((REST / item['file']).read_bytes(), {}, 200)

    monkeypatch.delenv('BINANCE_SPOT_REST_BASE_URL', raising=False)
    monkeypatch.setattr(rest, 'get_response', captured)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        anchor = datetime.fromisoformat(provenance['minute_start']).astimezone(UTC)
        runtime.setup(anchor=anchor)
        key = anchor.strftime('%Y-%m-%dT%H:%M:%SZ')
        record = runtime.build(key, provisional=True)
        assert len(record.component_hashes) == 3 and record.generation == 1
        assert not requests
        snapshot = store.snapshot()
        assert snapshot.records == (record,)
        raw = store.rows('raw_latest', snapshot)
        assert len(raw) > 1000
        assert store.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
            (len(raw),)
        ]
        assert store.execute('SELECT count() FROM origo.binance_spot_trades_time_current') == [(1,)]
        assert (
            store.execute('SELECT count() FROM origo.binance_spot_trades_dollar_current')[0][0] > 0
        )
        from origo.assets.create_binance_spot_latest_tables_origo import (
            _dollar_kline_table_sql,
            _kline_table_sql,
            _latest_raw_table_sql,
        )
        from origo.assets.refresh_binance_spot_dollar_klines_latest_origo import (
            _insert_minute_rows as insert_dollar,
        )
        from origo.assets.refresh_binance_spot_klines_latest_origo import (
            _insert_minute_rows as insert_time,
        )
        from origo.assets.sync_binance_spot_trades_latest_origo import _insert_latest_rows
        from origo.utils.binance_spot_latest import _parse_historical_trade

        settings = get_clickhouse_settings()
        legacy_names = {
            'raw_latest': 'binance_spot_trades_latest',
            'time_latest': 'binance_spot_klines_latest',
            'dollar_latest': 'binance_spot_dollar_klines_latest',
        }
        for ddl in (
            _latest_raw_table_sql(settings),
            _kline_table_sql(settings, legacy_names['time_latest']),
            _dollar_kline_table_sql(settings, legacy_names['dollar_latest']),
        ):
            client.execute(ddl)
        # Historical real rows require disabling retention on these disposable legacy tables.
        for table in legacy_names.values():
            client.execute(f'ALTER TABLE origo.{table} REMOVE TTL')
        legacy_rows = tuple(
            _parse_historical_trade(item)
            for page in sorted(REST.glob('page-*.json'))
            for item in json.loads(page.read_text())
            if int(anchor.timestamp()) * 1000
            <= item['time']
            < int(record.partition.end.timestamp()) * 1000
        )
        _insert_latest_rows(client, 'origo', minute_start=anchor, rows=legacy_rows)
        insert_time(client, 'origo', anchor)
        insert_dollar(client, 'origo', anchor)
        for component in store.components(record.partition):
            columns = ', '.join(column.name for column in component.columns)
            order = ', '.join(component.primary_key)
            expected = client.execute(
                f'SELECT {columns} FROM origo.{legacy_names[component.key]} ORDER BY {order}'
            )
            assert store.rows(component.key, snapshot) == expected, component.key
        # A repeated fetch of the real interval must not create another activation or duplicate rows.
        requests.extend(provenance['requests'])
        assert runtime.build(key, provisional=True) == record
        assert store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]
    finally:
        client.disconnect()
