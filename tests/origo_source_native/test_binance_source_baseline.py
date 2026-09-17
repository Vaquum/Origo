from __future__ import annotations

import csv
import hashlib
import importlib
import inspect
import json
import subprocess
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock

import polars as pl
import pytest
from dagster import (
    AssetKey,
    AssetsDefinition,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    RunRequest,
    build_schedule_context,
    materialize,
)

from origo import definitions
from origo.assets import daily_futures_trades_to_origo as futures
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query import binance_spot_kline_rollups as rollups
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import OrchestrationSpec, RolloutStage
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.formulas import spot_series
from origo.sources.storage import SourceStore
from origo.utils import arrow_store
from origo.utils import daily_gap_repair as repair

from .conftest import CLICKHOUSE_DOCKERFILE, REPO_ROOT, _make_admin_client
from .helpers import BINANCE_FIXTURE_ROOT

Query = Callable[[str], list[tuple[object, ...]]]
_MEASURES = tuple(
    (name, 'UInt64' if name == 'no_of_trades' else 'Float64')
    for name in (
        'open high low close mean std median iqr volume maker_ratio no_of_trades '
        'open_liquidity high_liquidity low_liquidity close_liquidity liquidity_sum '
        'maker_volume maker_liquidity'
    ).split()
)
_TIME_COLUMNS = (('datetime', 'DateTime'), *_MEASURES)
_RAW_MEASURES = (
    ('price', 'Float64'),
    ('quantity', 'Float64'),
    ('quote_quantity', 'Float64'),
    ('timestamp', 'UInt64'),
    ('is_buyer_maker', 'UInt8'),
)
_LEDGER_COLUMNS = (
    ('source_date', 'Date'),
    ('source_file', 'String'),
    ('dagster_run_id', 'String'),
    ('dagster_partition_key', 'String'),
    ('zip_checksum', 'FixedString(64)'),
    ('csv_checksum', 'FixedString(64)'),
    ('source_row_count', 'UInt64'),
    ('inserted_row_count', 'UInt64'),
    ('loaded_at', 'DateTime'),
    ('status', 'LowCardinality(String)'),
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


def _assert_table(
    query: Query,
    name: str,
    columns: tuple[tuple[str, str], ...],
    partition: str,
    order: str,
    ttl: str = '',
) -> None:
    assert [(row[0], row[1]) for row in query(f'DESCRIBE TABLE origo.{name}')] == list(columns), (
        name
    )
    rows = query(
        'SELECT engine, partition_key, sorting_key, create_table_query FROM system.tables '
        f"WHERE database = 'origo' AND name = '{name}'"
    )
    assert len(rows) == 1, name
    assert rows[0][:3] == ('MergeTree', partition, order), name
    ddl = str(rows[0][3])
    actual_ttl = ddl.split(' TTL ', 1)[1].split(' SETTINGS ', 1)[0] if ' TTL ' in ddl else ''
    assert actual_ttl == ttl, name
    assert query(f'SELECT count() FROM origo.{name}') == [(0,)], name


def _assets() -> dict[str, AssetsDefinition]:
    return {
        key.to_user_string(): asset
        for asset in definitions.defs.assets or ()
        if isinstance(asset, AssetsDefinition)
        for key in asset.keys
    }


def _assert_asset(name: str, group: str, deps: set[str], first_day: str | None = None) -> None:
    asset = _assets()[name]
    assert asset.group_names_by_key[AssetKey(name)] == group, name
    assert {key.to_user_string() for key in asset.asset_deps[AssetKey(name)]} == deps, name
    if first_day is None:
        assert asset.partitions_def is None, name
    else:
        assert asset.partitions_def == DailyPartitionsDefinition(start_date=first_day), name
    expected_retry = (23, 3600, None, None) if name.startswith('insert_daily_') else None
    assert asset.op.retry_policy == expected_retry, name


def _assert_job(name: str, nodes: set[str]) -> None:
    assert set(definitions.defs.resolve_job_def(name).graph.node_dict) == nodes, name


def _assert_schedule(name: str, job: str, cron: str) -> None:
    schedule = definitions.defs.get_repository_def().get_schedule_def(name)
    assert (
        schedule.job_name,
        schedule.cron_schedule,
        schedule.execution_timezone,
        schedule.default_status,
    ) == (job, cron, 'UTC', DefaultScheduleStatus.RUNNING)


def _setup(market: str) -> list[AssetsDefinition]:
    names = [
        'create_origo_database',
        f'create_binance_daily_{market}_trades_table_origo',
        'create_aligned_1m_exchange_table_origo',
        f'create_binance_{market}_klines_table_origo',
    ]
    assets = _assets()
    return [assets[name] for name in names]


def _assert_daily(
    market: str,
    module: ModuleType,
    first_day: str,
    hour: int,
    query: Query,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = f'binance_{market}'
    raw = f'binance_daily_{market}_trades'
    group = 'binance_data' if market == 'spot' else 'binance_futures_data'
    insert = f'insert_daily_{prefix}_trades_to_origo'
    first = date.fromisoformat(first_day)
    assert module.daily_partitions == DailyPartitionsDefinition(start_date=first_day)
    assert module.RAW_TABLE_NAME == raw
    assert module.LEDGER_TABLE_NAME == f'{raw}_ingestion'
    _assert_table(
        query,
        f'{raw}_ingestion',
        _LEDGER_COLUMNS,
        'toYYYYMM(source_date)',
        'source_date, source_file',
    )
    _assert_table(query, f'{prefix}_klines', _TIME_COLUMNS, 'toYYYYMM(datetime)', 'datetime')
    _assert_table(
        query,
        'aligned_1m_exchange',
        (('dataset_source', 'LowCardinality(String)'), *_TIME_COLUMNS),
        'toYYYYMM(datetime)',
        'dataset_source, datetime',
    )
    aligned = importlib.import_module(
        f'origo.assets.refresh_aligned_1m_exchange_from_{prefix}_origo'
    )
    assert getattr(aligned, f'BINANCE_{market.upper()}_DATASET_SOURCE') == prefix
    for asset in _setup(market):
        name = asset.key.to_user_string()
        _assert_asset(
            name,
            'origo_setup',
            set() if name == 'create_origo_database' else {'create_origo_database'},
        )
        _assert_job(f'{name}_job', {name})
    _assert_asset(insert, group, {f'create_binance_daily_{market}_trades_table_origo'}, first_day)
    families = ('klines',)
    for family in families:
        _assert_asset(
            f'refresh_{prefix}_{family}_origo',
            group,
            {f'create_{prefix}_{family}_table_origo', insert},
            first_day,
        )
    aligned_name = f'refresh_aligned_1m_exchange_from_{prefix}_origo'
    _assert_asset(
        aligned_name,
        group,
        {
            'create_aligned_1m_exchange_table_origo',
            f'create_{prefix}_klines_table_origo',
            f'refresh_{prefix}_klines_origo',
        },
        first_day,
    )
    job = f'refresh_{prefix}_data_source_job'
    _assert_job(job, {insert, aligned_name, *(f'refresh_{prefix}_{f}_origo' for f in families)})
    schedule_name = f'daily_{prefix}_pipeline_schedule'
    _assert_schedule(schedule_name, job, f'0 {hour} * * *')
    repository = definitions.defs.get_repository_def()
    result = repository.get_schedule_def(schedule_name).evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime(2024, 1, 3, hour, tzinfo=UTC),
            repository_def=repository,
        )
    )
    assert [(r.partition_key, r.run_key) for r in result.run_requests] == [
        ('2024-01-02', '2024-01-02')
    ]
    _assert_schedule(f'{prefix}_daily_gap_repair_schedule', job, '30 * * * *')
    spec = getattr(definitions, f'{market.upper()}_DAILY_GAP_REPAIR_SPEC')
    assert (spec.market, spec.ledger_table, spec.earliest_partition) == (
        market,
        f'{raw}_ingestion',
        first,
    )
    path = 'spot' if market == 'spot' else 'futures/um'
    expected_url = f'https://data.binance.vision/data/{path}/daily/trades/BTCUSDT/'
    env = f'BINANCE_{market.upper()}_DAILY_TRADES_BASE_URL'
    monkeypatch.delenv(env, raising=False)
    getter = getattr(module, f'_get_daily_{market}_trades_base_url')
    assert getter() == spec.get_base_url() == expected_url
    monkeypatch.setenv(env, 'https://archive.example/BTCUSDT/')
    assert getter() == spec.get_base_url() == 'https://archive.example/BTCUSDT/'
    assert repair.source_filename(date(2024, 1, 1)) == 'BTCUSDT-trades-2024-01-01.zip'
    with monkeypatch.context() as patch:
        patch.setattr(repair, 'repairable_gap_days', lambda *args: [date(2024, 1, 1)])
        requests = repair.gap_repair_run_requests(Mock(), 'origo', spec, date(2024, 1, 3), set())
    assert isinstance(requests, list)
    assert [(r.partition_key, r.run_key) for r in requests] == [
        ('2024-01-01', f'daily_gap_repair:{market}:2024-01-01:2024-01-03')
    ]


def _assert_archives(module: ModuleType, market: str, days: tuple[str, ...]) -> None:
    for day in days:
        path = BINANCE_FIXTURE_ROOT / market / 'daily/trades/BTCUSDT' / f'BTCUSDT-trades-{day}.zip'
        payload = path.read_bytes()
        assert (
            hashlib.sha256(payload).hexdigest()
            == path.with_suffix('.zip.CHECKSUM').read_text().split()[0]
        )
        with zipfile.ZipFile(BytesIO(payload)) as archive:
            data = archive.read(f'BTCUSDT-trades-{day}.csv')
        source = list(csv.reader(data.decode().splitlines()))
        if market == 'futures' and day == '2024-04-20':
            assert source.pop(0) == ['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']
        rows = module._parse_trade_rows(data)
        assert len(rows) == len(source) > 0
        for row, original in zip(rows, source, strict=True):
            expected = (
                int(original[0]),
                *(float(v) for v in original[1:4]),
                int(original[4]),
                original[5].lower() == 'true',
            )
            if market == 'spot':
                expected += (original[6].lower() == 'true',)
            unit = 1_000_000 if len(original[4]) == 16 else 1000
            expected += (datetime.fromtimestamp(int(original[4]) / unit, UTC).replace(tzinfo=None),)
            assert row == expected


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
        sensor = repository.get_sensor_def(f'binance_spot_trades_{consumer}_sensor')
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


def test_futures_source_identity_contract(
    origo_test_env: dict[str, str],
    query_origo: Query,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert origo_test_env['CLICKHOUSE_DATABASE'] == 'origo'
    assert materialize(_setup('futures')).success
    _assert_daily('futures', futures, '2019-09-08', 10, query_origo, monkeypatch)
    columns = (('futures_trade_id', 'UInt64'), *_RAW_MEASURES, ('datetime', 'DateTime64(6)'))
    assert futures.FUTURES_TRADE_COLUMNS == tuple(name for name, _ in columns)
    _assert_table(
        query_origo,
        'binance_daily_futures_trades',
        columns,
        'toYYYYMM(datetime)',
        'datetime, futures_trade_id',
    )
    _assert_archives(futures, 'futures', ('2019-09-08', '2024-04-20'))
    assert not any(
        name.startswith(('publish_binance_futures_trades_', 'sync_binance_futures_trades_'))
        for name in _assets()
    )
