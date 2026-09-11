from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from origo.assets.build_bar_store_arrow import build_series_frame
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.assets.publish_binance_spot_klines_to_mount import SPECS
from origo.query.binance_spot_kline_rollups import dollar_month, time_month
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import RolloutStage, Snapshot
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface import (
    _get_binance_spot_dollar_klines,
)
from origo.utils.publish_binance_spot_kline_snapshot_to_huggingface import (
    _get_binance_spot_klines_from_1m_projection,
)

from .test_binance_daily_source_adapter import archive_response
from .test_revisioned_source_framework_projections import legacy_spot_day


def test_spot_consumers_pin_one_state_token_without_blocking_database_activation(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = '2020-01-01'
    monkeypatch.setenv(
        'BINANCE_SPOT_DAILY_TRADES_BASE_URL',
        binance_fixture_server_root_url + '/spot/daily/trades/revisioned/',
    )
    monkeypatch.setattr(daily, 'get_response', archive_response)
    legacy_spot_day(day)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup()
        record = runtime.build(day)
        # Empty legacy tail tables let the original monthly readers query the same canonical day.
        for source, legacy in [
            ('time', 'binance_spot_klines_latest'),
            ('raw', 'binance_spot_trades_latest'),
        ]:
            schema = next(value for value in spec.components if value.key == source)
            columns = ', '.join(f'{column.name} {column.sql_type}' for column in schema.columns)
            client.execute(
                f'CREATE TABLE origo.{legacy} ({columns}) ENGINE=MergeTree ORDER BY tuple()'
            )
        expected_root = tmp_path / 'legacy-parquet'
        for series in SPECS:
            if series.family == 'time':
                frame = time_month(interval_minutes=series.size, year=2020, month=1)
            else:
                frame = dollar_month(ratio=series.size, year=2020, month=1)
            target = expected_root / series.sub_path / '2020/01.parquet'
            target.parent.mkdir(parents=True, exist_ok=True)
            frame.write_parquet(target)
        source_root = tmp_path / spec.key
        for consumer in spec.consumers:
            target = source_root / consumer.key
            snapshot = runtime.publish(consumer.key, str(target))
            manifest = json.loads((target / 'latest.json').read_text())
            assert manifest['state_token'] == snapshot.token
            version = target / 'versions' / manifest['version']
            assert len(manifest['files']) == (24 if consumer.key == 'arrow' else 12)
            for series in SPECS:
                if consumer.key == 'huggingface_shadow':
                    actual = pl.read_parquet(version / f'{series.name}.parquet')
                    if series.family == 'time':
                        expected = _get_binance_spot_klines_from_1m_projection(
                            kline_size_seconds=series.size * 60,
                            start_date_limit='2020-01-01',
                            end_date_limit='2020-01-02',
                            table_name='binance_spot_klines',
                            database_name='origo',
                        )
                    else:
                        expected = _get_binance_spot_dollar_klines(
                            dollar_size=float(series.size * 1000000),
                            start_date_limit='2020-01-01',
                            end_date_limit='2020-01-02',
                            table_name='binance_spot_dollar_klines',
                            database_name='origo',
                        )
                    assert_frame_equal(actual, expected)
                else:
                    assert_frame_equal(
                        pl.read_parquet(version / series.sub_path / '2020/01.parquet'),
                        pl.read_parquet(expected_root / series.sub_path / '2020/01.parquet'),
                    )
                    if consumer.key == 'arrow':
                        assert_frame_equal(
                            pl.read_ipc(version / f'{series.name}.arrow'),
                            build_series_frame(series, expected_root).df,
                        )

        target = source_root / 'parquet'
        before = (target / 'latest.json').read_bytes()
        original = store.snapshot
        advanced = False

        def advance_before_commit(*, canonical_only: bool = False) -> Snapshot:
            nonlocal advanced
            if not advanced:
                advanced = True
                return original(canonical_only=canonical_only)
            runtime.rollback(
                record, operator='test', reason='Real-build software rollback during render'
            )
            return original(canonical_only=canonical_only)

        monkeypatch.setattr(store, 'snapshot', advance_before_commit)
        with pytest.raises(RuntimeError, match='state changed'):
            runtime.publish('parquet', str(target))
        assert (target / 'latest.json').read_bytes() == before
        assert store.generation(record.partition) == 2
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' AND blocking_scope='CONSUMER' AND event_type='FAILED'"
        ) == [(1,)]
        monkeypatch.setattr(store, 'snapshot', original)
        runtime.publish('parquet', str(target))
        assert json.loads((target / 'latest.json').read_text())['state_token'] == original().token
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' AND event_type='RECOVERED'"
        ) == [(1,)]
    finally:
        client.disconnect()
