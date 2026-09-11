from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import RolloutStage
from origo.sources.failures import FailureLog
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response


def test_every_failure_is_visible_in_one_log_without_blocking_unrelated_work(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup()
        log = FailureLog(store, runtime.lock_root, runtime.run_id)
        cases = [
            ('provider', 'PARTITION'),
            ('transport', 'PARTITION'),
            ('parser', 'PARTITION'),
            ('component', 'PARTITION'),
            ('activation', 'PARTITION'),
            ('lock', 'PARTITION'),
            ('cleanup', 'NONE'),
            ('audit', 'NONE'),
            ('repair', 'PARTITION'),
            ('consumer', 'CONSUMER'),
            ('certification', 'PARTITION'),
        ]
        for operation, scope in cases:
            key = log.record(
                operation=operation,
                error_code='RECORDED_FAILURE',
                scope=scope,
                partition='2020-01-01',
                consumer='parquet' if scope == 'CONSUMER' else None,
            )
            assert (
                log.record(
                    operation=operation,
                    error_code='RECORDED_FAILURE',
                    scope=scope,
                    partition='2020-01-01',
                    consumer='parquet' if scope == 'CONSUMER' else None,
                )
                == key
            )
        assert client.execute(
            'SELECT count(), uniqExact(event_id) FROM origo.source_failure_log'
        ) == [(len(cases), len(cases))]
        # Noncritical errors and an unrelated partition cannot prevent a complete real day from activating.
        record = runtime.build('2017-08-17')
        assert record.generation == 1
        assert client.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
            (3427,)
        ]
        for operation, scope in cases:
            log.recover(
                operation=operation,
                partition='2020-01-01',
                consumer='parquet' if scope == 'CONSUMER' else None,
            )
        assert client.execute(
            "SELECT count(), uniqExact(failure_key) FROM origo.source_failure_log WHERE event_type='RECOVERED'"
        ) == [(len(cases), len(cases))]
        log.record(
            operation='provider',
            error_code='RECORDED_FAILURE',
            scope='PARTITION',
            partition='2020-01-01',
            event_type='ACKNOWLEDGED',
        )
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE event_type='ACKNOWLEDGED'"
        ) == [(1,)]
        runtime.certify('2017-08-17', review_state='PENDING')
        assert client.execute('SELECT count() FROM origo.source_certification_log') == [(1,)]
        assert runtime.audit() == ()
        assert runtime.repair('2017-08-17') == record
        guide = (Path(__file__).resolve().parents[2] / 'origo/sources/README.md').read_text()
        query = guide.split('```sql\n', 1)[1].split('```', 1)[0]
        assert len(client.execute(query)) == len(cases)

        def unavailable(url: str) -> daily.Response:
            raise OSError('Injected provider transport failure')

        def checksum_mismatch(url: str) -> daily.Response:
            response = archive_response(url)
            # A changed sidecar requires a fresh fetch; an unchanged verified revision
            # deliberately reuses its retained content without downloading the ZIP.
            return (
                daily.Response(b'0' * 64 + response.body[64:], {}, 200)
                if url.endswith('CHECKSUM')
                else daily.Response(response.body + b'corrupt wrapper', {}, 200)
            )

        for response in (unavailable, checksum_mismatch):
            with monkeypatch.context() as patch:
                patch.setattr(daily, 'get_response', response)
                with pytest.raises((OSError, RuntimeError)):
                    runtime.build('2017-08-17')
            assert store.generation(record.partition) == 1
            assert client.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
                (3427,)
            ]
        assert client.execute(
            'SELECT error_code, blocking_scope FROM origo.source_failure_log '
            "WHERE operation='canonical' AND event_type='FAILED' ORDER BY error_code"
        ) == [('ARCHIVE_CHECKSUM_MISMATCH', 'PARTITION'), ('OSError', 'PARTITION')]
        assert runtime.build('2017-08-17') == record
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='canonical' AND event_type='RECOVERED'"
        ) == [(2,)]
        # Another observation in the same run needs its own recovery event, not a reused recovery UUID.
        with monkeypatch.context() as patch:
            patch.setattr(daily, 'get_response', checksum_mismatch)
            with pytest.raises(RuntimeError, match='checksum mismatch'):
                runtime.build('2017-08-17')
        runtime.build('2017-08-17')
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='canonical' AND event_type='RECOVERED'"
        ) == [(3,)]
        from origo.sources import bundle

        attempted: list[str] = []

        def one_partition_unavailable(url: str) -> daily.Response:
            attempted.append(url)
            if '2020-01-01' in url:
                raise OSError('One audited provider partition is unavailable')
            return archive_response(url)

        with monkeypatch.context() as patch:
            patch.setenv('ORIGO_SOURCE_LOCK_DIR', str(runtime.lock_root))
            patch.setattr(daily, 'get_response', one_partition_unavailable)
            patch.setattr(SourceRuntime, 'audit', lambda self: ('2020-01-01', '2017-08-17'))
            with pytest.raises(ExceptionGroup, match='Some audited partitions'):
                bundle.execute_source(
                    spec, 'audit', bundle.SourceRunConfig(), run_id=runtime.run_id
                )
        assert any('2020-01-01' in url for url in attempted)
        assert any('2017-08-17' in url for url in attempted)
        assert store.generation(record.partition) == 1
        # Corrupt a retained copy by removing one genuine row, then require a fresh complete build.
        client.execute(
            'ALTER TABLE origo.binance_spot_trades_raw_revisions DELETE '
            'WHERE build_id=%(build)s AND trade_id=0',
            {'build': record.build_id},
            settings={'mutations_sync': 2},
        )
        with monkeypatch.context() as patch:
            patch.setattr(daily, 'get_response', unavailable)
            with pytest.raises(OSError):
                runtime.repair('2017-08-17')
        assert store.generation(record.partition) == 1
        repaired = runtime.repair('2017-08-17')
        assert repaired.generation == 2 and repaired.build_id != record.build_id
        assert repaired.revision == record.revision
        assert client.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
            (3427,)
        ]
        assert client.execute(
            'SELECT error_code, arraySort(groupArray(event_type)), argMax(event_type, event_time) '
            'FROM origo.source_failure_log '
            "WHERE operation='repair' AND partition_key='2017-08-17' "
            'GROUP BY error_code ORDER BY error_code'
        ) == [
            ('OSError', ['FAILED', 'RECOVERED'], 'RECOVERED'),
            ('RETAINED_CONTENT_INVALID', ['FAILED', 'RECOVERED'], 'RECOVERED'),
        ]
    finally:
        client.disconnect()
