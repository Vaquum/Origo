from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from dagster import AssetsDefinition, materialize

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.definitions import defs
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import RolloutStage
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response


def legacy_spot_day(day: str) -> None:
    names = {
        'create_origo_database',
        'create_binance_daily_spot_trades_table_origo',
        'create_aligned_1m_exchange_table_origo',
        'insert_daily_binance_spot_trades_to_origo',
        'refresh_aligned_1m_exchange_from_binance_spot_origo',
    }
    for family in (
        'klines',
        'dollar_klines',
        'volume_klines',
        'tick_klines',
        'dollar_imbalance_klines',
    ):
        names.add(f'create_binance_spot_{family}_table_origo')
        names.add(f'refresh_binance_spot_{family}_origo')
    assets = [
        asset
        for asset in defs.assets or ()
        if isinstance(asset, AssetsDefinition) and asset.key.to_user_string() in names
    ]
    assert len(assets) == len(names)
    assert materialize(assets, partition_key=day).success


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
    legacy_spot_day(day)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        runtime.build(day)
        mapping = {
            'raw': 'binance_daily_spot_trades',
            'time': 'binance_spot_klines',
            'dollar': 'binance_spot_dollar_klines',
            'volume': 'binance_spot_volume_klines',
            'tick': 'binance_spot_tick_klines',
            'imbalance': 'binance_spot_dollar_imbalance_klines',
            'aligned': 'aligned_1m_exchange',
        }
        for component in runtime.store.components(spec.canonical.partition(day)):
            names = ', '.join(column.name for column in component.columns)
            order = ', '.join(component.primary_key)
            old = client.execute(
                f'SELECT {names} FROM origo.{mapping[component.key]} ORDER BY {order}'
            )
            new = client.execute(
                f'SELECT {names} FROM origo.binance_spot_trades_{component.key}_current ORDER BY {order}'
            )
            assert new == old, component.key
    finally:
        client.disconnect()
