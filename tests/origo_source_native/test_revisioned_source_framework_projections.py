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


def test_real_spot_events_produce_all_declared_components(
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
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        # Objects the retired pipeline left behind: a table under a legacy name that the
        # source now serves, a retired framework table and a retired spot table.
        client.execute('CREATE DATABASE IF NOT EXISTS origo')
        for name in ('binance_spot_klines', 'source_parity_log', 'binance_spot_15M_dollar_klines_latest'):
            client.execute(f'CREATE TABLE origo.{name} (x UInt8) ENGINE=MergeTree ORDER BY x')
        runtime.setup()
        engines = dict(
            client.execute("SELECT name, engine FROM system.tables WHERE database = 'origo'")
        )
        assert {engines.get(alias) for alias, _ in spec.aliases} == {'View'}
        assert not {'source_parity_log', *spec.retired_tables} & set(engines)
        runtime.setup()
        record = runtime.build(day)
        snapshot = store.snapshot()
        served = {key: alias for alias, key in spec.aliases}
        for component in store.components(record.partition):
            names = ', '.join(column.name for column in component.columns)
            order = ', '.join(component.primary_key)
            rows = store.rows(component.key, snapshot)
            assert rows, component.key
            assert (
                client.execute(
                    f'SELECT {names} FROM origo.binance_spot_trades_{component.key}_current ORDER BY {order}'
                )
                == rows
            ), component.key
            if component.key in served:
                assert (
                    client.execute(f'SELECT {names} FROM origo.{served[component.key]} ORDER BY {order}')
                    == rows
                ), component.key
        # The provisional aliases serve only provisional rows, and a canonical day has none.
        for alias, key in spec.aliases:
            if key.endswith('_latest'):
                assert client.execute(f'SELECT count() FROM origo.{alias}') == [(0,)], alias
    finally:
        client.disconnect()
