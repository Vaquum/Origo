from __future__ import annotations

import inspect
import json
import subprocess
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from dagster import (
    AssetKey,
    AssetsDefinition,
    DefaultSensorStatus,
)

from origo import definitions
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query import binance_spot_kline_rollups as rollups
from origo.sources.binance_perp_trades import BINANCE_PERP_TRADES_SPEC
from origo.sources.binance_spot_aggtrades import BINANCE_SPOT_AGGTRADES_SPEC
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import OrchestrationSpec, RolloutStage
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.formulas import spot_series
from origo.sources.storage import SourceStore
from origo.utils import arrow_store

from .conftest import CLICKHOUSE_DOCKERFILE, REPO_ROOT, _make_admin_client

Query = Callable[[str], list[tuple[object, ...]]]
_MEASURES = tuple(
    (name, 'UInt64' if name == 'no_of_trades' else 'Float64')
    for name in (
        'open high low close mean std median iqr volume maker_ratio no_of_trades '
        'open_liquidity high_liquidity low_liquidity close_liquidity liquidity_sum '
        'maker_volume maker_liquidity'
    ).split()
)
_TIME_SERIES = (('1m', 1), ('15m', 15), ('30m', 30), ('1h', 60), ('2h', 120), ('4h', 240))
_DOLLAR_SERIES = (('1M', 1), ('15M', 15), ('30M', 30), ('60M', 60), ('120M', 120), ('240M', 240))


def _docker(*args: str) -> str:
    return subprocess.run(
        ['docker', *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _bar_columns(kind: str) -> tuple[tuple[str, str], ...]:
    columns = (
        ('start_datetime', 'DateTime'),
        ('end_datetime', 'DateTime'),
        (f'{kind}_bar_id', 'UInt64'),
        *_MEASURES,
    )
    if kind == 'dollar_imbalance':
        return (
            *columns,
            ('taker_buy_liquidity', 'Float64'),
            ('taker_sell_liquidity', 'Float64'),
            ('dollar_imbalance', 'Float64'),
        )
    return columns


def _assets() -> dict[str, AssetsDefinition]:
    return {
        key.to_user_string(): asset
        for asset in definitions.defs.assets or ()
        if isinstance(asset, AssetsDefinition)
        for key in asset.keys
    }


def test_clickhouse_runtime_matches_deployment(clickhouse_settings: dict[str, str]) -> None:
    pinned = 'clickhouse/clickhouse-server:25.3.2.39@sha256:8745843b17f92db1765025009772ec1d87dfdcaa95deabca6b802a66cb669d30'
    assert CLICKHOUSE_DOCKERFILE.read_text().splitlines()[0] == f'FROM {pinned}'
    client = _make_admin_client(clickhouse_settings)
    try:
        version, container = client.execute('SELECT version(), hostName()')[0]
        assert version == '25.3.2.39'
        metadata = json.loads(_docker('inspect', str(container)))[0]
        assert metadata['Name'].startswith('/origo-tests-')
        # BuildKit's cache need not expose the base image to docker image inspect.
        _docker('pull', '--quiet', pinned)
        deployed_layers = json.loads(_docker('image', 'inspect', pinned))[0]['RootFS']['Layers']
        tested_layers = json.loads(_docker('image', 'inspect', metadata['Image']))[0]['RootFS'][
            'Layers'
        ]
        assert tested_layers[: len(deployed_layers)] == deployed_layers
        copied = _docker(
            'exec', str(container), 'cat', '/etc/clickhouse-server/config.d/clickhouse-config.xml'
        )
        assert copied == (REPO_ROOT / 'clickhouse-config.xml').read_text().strip()
        # Profiles in config.d are inert; only users.d/clickhouse-users.xml changes a
        # session default. These effective values are the runtime contract.
        expected = {
            'max_memory_usage': '0',
            'max_bytes_before_external_sort': '0',
            'max_bytes_before_external_group_by': '0',
            'background_pool_size': '16',
            'max_block_size': '65409',
            'min_insert_block_size_rows': '1048449',
            'use_query_cache': '0',
            'query_cache_system_table_handling': 'throw',
            'max_execution_time': '0',
            'max_result_rows': '0',
            'receive_timeout': '300',
            'send_timeout': '300',
            'idle_connection_timeout': '3600',
            'connect_timeout': '10',
            'log_queries': '1',
            'log_queries_min_type': 'QUERY_START',
            'log_queries_min_query_duration_ms': '0',
            'log_query_threads': '0',
            'log_query_views': '1',
            'max_memory_usage_for_all_queries': '0',
            'max_bytes_to_read': '0',
            'optimize_move_to_prewhere': '1',
            'enable_optimize_predicate_expression': '1',
            'allow_experimental_analyzer': '1',
            'async_insert': '0',
            'log_processors_profiles': '0',
        }
        actual = dict(client.execute('SELECT name, value FROM system.settings'))
        assert {name: actual[name] for name in expected} == expected
        assert str(actual['max_threads']).startswith("'auto(")
        client.execute('SYSTEM FLUSH LOGS')
        stored, verbose = client.execute(
            "SELECT count(), countIf(level > 'Information') FROM system.text_log"
        )[0]
        assert stored > 0 and verbose == 0
        server = dict(client.execute('SELECT name, value FROM system.server_settings'))
        assert server['max_server_memory_usage_to_ram_ratio'] == '0.9'
        assert server['background_merges_mutations_concurrency_ratio'] == '2'
        assert 0 < int(server['max_server_memory_usage']) <= 230_000_000_000
    finally:
        client.disconnect()


def test_spot_source_identity_contract(
    origo_test_env: dict[str, str],
    query_origo: Query,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    spec = BINANCE_SPOT_TRADES_SPEC
    assert (spec.key, spec.rollout_stage, spec.partitions.first_day) == (
        'binance_spot_trades',
        RolloutStage.LIVE,
        date(2017, 8, 17),
    )
    assert spec.orchestration == OrchestrationSpec('0 4 * * *', '* * * * *', '30 * * * *')
    assert [component.key for component in spec.components] == [
        'raw',
        'time',
        'dollar',
        'volume',
        'tick',
        'imbalance',
        'aligned',
        'raw_latest',
        'time_latest',
        'dollar_latest',
    ]
    # The legacy table names stay readable as views over the components that replaced them;
    # the tables without a successor are retired.
    assert dict(spec.aliases) == {
        'binance_daily_spot_trades': 'raw',
        'binance_spot_klines': 'time',
        'binance_spot_dollar_klines': 'dollar',
        'binance_spot_volume_klines': 'volume',
        'binance_spot_tick_klines': 'tick',
        'binance_spot_dollar_imbalance_klines': 'imbalance',
        'binance_spot_trades_latest': 'raw_latest',
        'binance_spot_klines_latest': 'time_latest',
        'binance_spot_dollar_klines_latest': 'dollar_latest',
    }
    assert spec.retired_tables == (
        'binance_daily_spot_trades_ingestion',
        'binance_spot_trades_latest_ingestion',
        'binance_spot_latest_watermarks',
        *(f'binance_spot_{label}_klines_latest' for label in ('15m', '30m', '1h', '2h', '4h')),
        *(
            f'binance_spot_{label}_dollar_klines_latest'
            for label in ('15M', '30M', '60M', '120M', '240M')
        ),
    )
    assert spec.retired_rows == (('aligned_1m_exchange', "dataset_source = 'binance_spot'"),)
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'baseline').setup()
    finally:
        client.disconnect()
    columns = {
        component.key: [(column.name, column.sql_type) for column in component.columns]
        for component in spec.components
    }
    for alias, key in spec.aliases:
        described = [(row[0], row[1]) for row in query_origo(f'DESCRIBE TABLE origo.{alias}')]
        assert described == columns[key], alias
        engine = query_origo(
            f"SELECT engine FROM system.tables WHERE database = 'origo' AND name = '{alias}'"
        )
        assert engine == [('View',)], alias
    assert not any(
        name.startswith(('insert_daily_binance_spot_', 'refresh_binance_spot_klines'))
        for name in _assets()
    )
    _assert_consumers(monkeypatch)


def _assert_consumers(monkeypatch: pytest.MonkeyPatch) -> None:
    series = [(f'time_{label}', 'time', size, f'time/{label}') for label, size in _TIME_SERIES]
    series += [
        (f'dollar_{label}', 'dollar', size, f'dollar/{label}') for label, size in _DOLLAR_SERIES
    ]
    assert [(s.name, s.family, s.size, s.sub_path) for s in spot_series.SPECS] == series
    monkeypatch.delenv('LOCAL_PARQUET_DIR', raising=False)
    monkeypatch.delenv('LOCAL_ARROW_DIR', raising=False)
    assert (spot_series.EXPORT_START_YEAR, spot_series.EXPORT_START_MONTH) == (2020, 1)
    assert arrow_store.parquet_source_root() == Path('/opt/parquet')
    assert arrow_store.LATEST_NAME == 'latest.arrow'
    assert list(arrow_store.BAR_STORE_SERIES) == [s[0] for s in series]
    values = tuple(name for name, _ in _MEASURES if name not in ('median', 'iqr'))
    assert arrow_store._output_columns('time') == ('ts', *values)
    assert arrow_store._output_columns('dollar') == ('ts', 'start_ts', 'dollar_bar_id', *values)
    assert tuple(rollups.TIME_KLINE_COLUMNS) == ('datetime', *values)
    assert tuple(rollups.DOLLAR_KLINE_COLUMNS) == (
        'start_datetime',
        'end_datetime',
        'dollar_bar_id',
        *values,
    )
    for name, _, _, subpath in series:
        assert (
            spot_series.month_path(subpath, 2024, 1)
            == Path('/opt/parquet') / subpath / '2024/01.parquet'
        )
        assert (
            arrow_store.series_store_dir(name) / arrow_store.LATEST_NAME
            == Path('/opt/arrow') / name / 'latest.arrow'
        )
    for fn, size, defaults in (
        (
            rollups.time_month,
            'interval_minutes',
            {
                'base_table': 'binance_spot_klines',
                'latest_table': 'binance_spot_klines_latest',
                'database': 'origo',
            },
        ),
        (
            rollups.dollar_month,
            'ratio',
            {
                'base_table': 'binance_spot_dollar_klines',
                'raw_latest_table': 'binance_spot_trades_latest',
                'database': 'origo',
                'id_column': 'trade_id',
                'quote_expr': 'quote_quantity',
            },
        ),
    ):
        signature = inspect.signature(fn)
        parameters = signature.parameters
        assert signature.return_annotation is pl.DataFrame
        assert {n: p.annotation for n, p in parameters.items()} == {
            size: int,
            'year': int,
            'month': int,
            **{n: str for n in defaults},
        }
        assert list(parameters) == [size, 'year', 'month', *defaults]
        assert all(p.kind == inspect.Parameter.KEYWORD_ONLY for p in parameters.values())
        assert {
            n: p.default for n, p in parameters.items() if p.default is not inspect.Parameter.empty
        } == defaults
    from origo.sources.profiles.spot_consumers import HUGGINGFACE_DATASETS

    repository = definitions.defs.get_repository_def()
    for consumer in ('mount', 'huggingface'):
        assert repository.has_job(f'publish_binance_spot_trades_{consumer}_job')
    # mount pins provisional rows, so the provisional worker publishes it; huggingface is
    # canonical-only and keeps its sensor.
    assert not repository.has_sensor_def('binance_spot_trades_mount_sensor')
    sensor = repository.get_sensor_def('binance_spot_trades_huggingface_sensor')
    assert sensor.default_status == DefaultSensorStatus.RUNNING
    assert not any(
        'to_huggingface' in name or name in ('build_bar_store_arrow_job', 'publish_binance_spot_klines_to_mount_job')
        for name in (job.name for job in definitions.defs.jobs)
    )
    for family, resolutions in (('time', _TIME_SERIES), ('dollar', _DOLLAR_SERIES)):
        for label, _ in resolutions:
            dollar = 'dollar_' if family == 'dollar' else ''
            repo_id, repo_env, prefix, _resolution = HUGGINGFACE_DATASETS[f'{family}_{label}']
            assert repo_id == f'vaquum/binance_btcusdt_{label}_{dollar}klines'
            assert repo_env == ('HUGGINGFACE_DATASET_REPO_ID' if label == '1m' else None)
            assert prefix == f'btcusdt_{label}_{dollar}kline_20200101_to_'


def test_perp_source_identity_contract(
    origo_test_env: dict[str, str],
    query_origo: Query,
    tmp_path: Path,
) -> None:
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    spec = BINANCE_PERP_TRADES_SPEC
    assert (spec.key, spec.rollout_stage, spec.partitions.first_day) == (
        'binance_perp_trades',
        RolloutStage.LIVE,
        date(2019, 9, 8),
    )
    assert spec.orchestration == OrchestrationSpec('0 4 * * *', '* * * * *', '30 * * * *')
    assert [component.key for component in spec.components] == [
        'raw',
        'time',
        'dollar',
        'volume',
        'tick',
        'imbalance',
        'aligned',
        'raw_latest',
        'time_latest',
        'dollar_latest',
    ]
    raw = next(component for component in spec.components if component.key == 'raw')
    assert [column.name for column in raw.columns] == [
        'trade_id',
        'price',
        'quantity',
        'quote_quantity',
        'timestamp',
        'is_buyer_maker',
        'datetime',
    ]
    assert [(consumer.key, consumer.public) for consumer in spec.consumers] == [
        ('mount', True),
        ('huggingface', True),
    ]
    # No readers of the legacy futures tables exist, so no aliases: the spec drops them.
    assert dict(spec.aliases) == {}
    assert spec.retired_tables == (
        'binance_daily_futures_trades',
        'binance_daily_futures_trades_ingestion',
        'binance_futures_klines',
        'aligned_1m_exchange',
    )
    assert spec.retired_rows == ()
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        client.execute('CREATE DATABASE IF NOT EXISTS origo')
        for name in spec.retired_tables:
            client.execute(f'CREATE TABLE origo.{name} (x UInt8) ENGINE=Memory')
        SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'baseline').setup()
        assert (
            query_origo(
                "SELECT name FROM system.tables WHERE database = 'origo' AND "
                "name IN ('binance_daily_futures_trades', 'binance_daily_futures_trades_ingestion', "
                "'binance_futures_klines', 'aligned_1m_exchange')"
            )
            == []
        )
    finally:
        client.disconnect()
    assert not any(
        name.startswith(('insert_daily_binance_futures_', 'refresh_binance_futures_'))
        for name in _assets()
    )


def test_spot_agg_source_identity_contract(origo_test_env: dict[str, str]) -> None:
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    spec = BINANCE_SPOT_AGGTRADES_SPEC
    assert (spec.key, spec.rollout_stage, spec.partitions.first_day) == (
        'binance_spot_aggtrades',
        RolloutStage.LIVE,
        date(2017, 8, 17),
    )
    assert spec.orchestration == OrchestrationSpec('0 4 * * *', '* * * * *', '30 * * * *')
    assert [component.key for component in spec.components] == [
        'raw',
        'time',
        'dollar',
        'volume',
        'tick',
        'imbalance',
        'aligned',
        'raw_latest',
        'time_latest',
        'dollar_latest',
    ]
    raw = next(component for component in spec.components if component.key == 'raw')
    assert [column.name for column in raw.columns] == [
        'agg_trade_id',
        'price',
        'quantity',
        'first_trade_id',
        'last_trade_id',
        'timestamp',
        'is_buyer_maker',
        'is_best_match',
        'datetime',
    ]
    assert [(consumer.key, consumer.public) for consumer in spec.consumers] == [
        ('mount', True),
        ('huggingface', True),
    ]
    # No legacy aggregate pipeline is replaced: nothing is aliased or retired.
    assert dict(spec.aliases) == {}
    assert spec.retired_tables == ()
    assert spec.retired_rows == ()
