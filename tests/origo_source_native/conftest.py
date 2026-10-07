from __future__ import annotations

import fcntl
import importlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any
from uuid import uuid4

import pytest
from clickhouse_driver import Client as ClickhouseClient
from clickhouse_driver.errors import NetworkError
from dagster import materialize

from .helpers import BINANCE_FIXTURE_ROOT, ORIGO_DATABASE

REPO_ROOT = Path(__file__).resolve().parents[2]
CLICKHOUSE_DOCKERFILE = REPO_ROOT / 'Dockerfile.clickhouse'
RESOURCE_MODULES = ('test_alert_summary.py', 'test_law_page.py')


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Keep resource probes ahead of workers retaining large vendor/session inputs.
    items.sort(key=lambda item: RESOURCE_MODULES.index(item.path.name)
               if item.path.name in RESOURCE_MODULES else len(RESOURCE_MODULES))


@pytest.fixture(scope='module', autouse=True)
def resource_test_schedule(request: pytest.FixtureRequest) -> Iterator[None]:
    run_uid = os.environ.get('PYTEST_XDIST_TESTRUNUID')
    if run_uid is None:
        yield
    else:
        # Preserve the serial environment of unchanged wall-clock/resource assertions.
        exclusive = Path(request.module.__file__).name in RESOURCE_MODULES
        with (Path('/tmp') / f'origo-tests-{run_uid}.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield


def _wait_for_clickhouse(host: str, port: int, user: str, password: str) -> None:
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        client = None
        try:
            client = ClickhouseClient(
                host=host,
                port=port,
                user=user,
                password=password,
            )
            result = client.execute('SELECT 1')
            if result == [(1,)]:
                return
        except OSError as exc:
            last_error = exc
            time.sleep(1)
        except RuntimeError as exc:
            last_error = exc
            time.sleep(1)
        except ValueError as exc:
            last_error = exc
            time.sleep(1)
        except EOFError as exc:
            last_error = exc
            time.sleep(1)
        except NetworkError as exc:
            last_error = exc
            time.sleep(1)
        finally:
            if client is not None:
                client.disconnect()
    raise RuntimeError(f'ClickHouse container did not become ready: {last_error}')


def _clickhouse_env(native_port: int, http_port: int, password: str) -> dict[str, str]:
    return {
        'CLICKHOUSE_HOST': '127.0.0.1',
        'CLICKHOUSE_PORT': str(native_port),
        'CLICKHOUSE_HTTP_PORT': str(http_port),
        'CLICKHOUSE_USER': 'default',
        'CLICKHOUSE_PASSWORD': password,
        'CLICKHOUSE_DATABASE': ORIGO_DATABASE,
    }


def _make_admin_client(settings: dict[str, str]) -> ClickhouseClient:
    return ClickhouseClient(
        host=settings['CLICKHOUSE_HOST'],
        port=int(settings['CLICKHOUSE_PORT']),
        user=settings['CLICKHOUSE_USER'],
        password=settings['CLICKHOUSE_PASSWORD'],
    )


def _drop_origo_database(settings: dict[str, str]) -> None:
    client = _make_admin_client(settings)
    try:
        client.execute(f'DROP DATABASE IF EXISTS {ORIGO_DATABASE} SYNC')
    finally:
        client.disconnect()


def _query_rows(settings: dict[str, str], query: str) -> list[tuple[Any, ...]]:
    client = ClickhouseClient(
        host=settings['CLICKHOUSE_HOST'],
        port=int(settings['CLICKHOUSE_PORT']),
        user=settings['CLICKHOUSE_USER'],
        password=settings['CLICKHOUSE_PASSWORD'],
        database=ORIGO_DATABASE,
    )
    try:
        return client.execute(query)
    finally:
        client.disconnect()


def _reload_module(module_name: str) -> Any:
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


@pytest.fixture(scope='session')
def clickhouse_settings() -> dict[str, str]:
    if shutil.which('docker') is None:
        pytest.fail('docker CLI is required for tests/origo_source_native')

    with TemporaryDirectory(prefix='origo-tests-image-') as context:
        for filename in ('clickhouse-config.xml', 'clickhouse-users.xml'):
            shutil.copyfile(REPO_ROOT / filename, Path(context) / filename)
        image = subprocess.run(
            ['docker', 'build', '--quiet', '--file', str(CLICKHOUSE_DOCKERFILE), context],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    container_name = f'origo-tests-{uuid4().hex[:12]}'
    password = 'test-password'

    subprocess.run(
        [
            'docker',
            'run',
            '--detach',
            '--rm',
            '--name',
            container_name,
            '--tmpfs',
            '/var/lib/clickhouse:size=512m',
            '--tmpfs',
            '/var/log/clickhouse-server:size=64m',
            '--publish',
            '127.0.0.1::9000',
            '--publish',
            '127.0.0.1::8123',
            '--env',
            'CLICKHOUSE_USER=default',
            '--env',
            f'CLICKHOUSE_PASSWORD={password}',
            image,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    try:
        bindings: dict[str, list[dict[str, str]]] = json.loads(subprocess.run(
            ['docker', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', container_name],
            check=True,
            capture_output=True,
            text=True,
        ).stdout)
        settings = _clickhouse_env(
            int(bindings['9000/tcp'][0]['HostPort']),
            int(bindings['8123/tcp'][0]['HostPort']),
            password,
        )
        _wait_for_clickhouse(
            settings['CLICKHOUSE_HOST'],
            int(settings['CLICKHOUSE_PORT']),
            settings['CLICKHOUSE_USER'],
            settings['CLICKHOUSE_PASSWORD'],
        )
        yield settings
    finally:
        subprocess.run(
            ['docker', 'rm', '--force', container_name],
            check=False,
            capture_output=True,
            text=True,
        )


@pytest.fixture(scope='session')
def binance_fixture_server_root_url() -> str:
    handler = partial(SimpleHTTPRequestHandler, directory=str(BINANCE_FIXTURE_ROOT))
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
    port = server.server_port
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield f'http://127.0.0.1:{port}'
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture(scope='session')
def binance_daily_base_url(binance_fixture_server_root_url: str) -> str:
    return f'{binance_fixture_server_root_url}/spot/daily/trades/BTCUSDT/'


@pytest.fixture(scope='session')
def binance_depth200_base_url(binance_fixture_server_root_url: str) -> str:
    return f'{binance_fixture_server_root_url}/spot/depth200'


@pytest.fixture()
def origo_test_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    clickhouse_settings: dict[str, str],
    binance_daily_base_url: str,
    binance_depth200_base_url: str,
) -> dict[str, str]:
    _drop_origo_database(clickhouse_settings)
    for key, value in clickhouse_settings.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'source-files'))
    monkeypatch.setenv('BINANCE_SPOT_DAILY_TRADES_BASE_URL', binance_daily_base_url)
    monkeypatch.setenv('BINANCE_SPOT_DEPTH200_BASE_URL', binance_depth200_base_url)
    monkeypatch.setenv('BINANCE_SPOT_DEPTH200_AUTH_TOKEN', 'test-token')

    yield clickhouse_settings

    _drop_origo_database(clickhouse_settings)


@pytest.fixture()
def origo_assets(origo_test_env: dict[str, str]) -> dict[str, Any]:
    create_origo_database_module = _reload_module('origo.assets.create_origo_database')
    create_binance_spot_depth20_snapshots_table_origo_module = _reload_module(
        'origo.assets.create_binance_spot_depth20_snapshots_table_origo'
    )
    sync_binance_spot_depth20_snapshots_to_origo_module = _reload_module(
        'origo.assets.sync_binance_spot_depth20_snapshots_to_origo'
    )
    create_binance_spot_depth20_1m_table_origo_module = _reload_module(
        'origo.assets.create_binance_spot_depth20_1m_table_origo'
    )
    refresh_binance_spot_depth20_1m_origo_module = _reload_module(
        'origo.assets.refresh_binance_spot_depth20_1m_origo'
    )
    reconcile_binance_spot_depth20_partition_state_origo_module = _reload_module(
        'origo.assets.reconcile_binance_spot_depth20_partition_state_origo'
    )
    create_binance_spot_depth200_snapshots_table_origo_module = _reload_module(
        'origo.assets.create_binance_spot_depth200_snapshots_table_origo'
    )
    sync_binance_spot_depth200_snapshots_to_origo_module = _reload_module(
        'origo.assets.sync_binance_spot_depth200_snapshots_to_origo'
    )
    create_binance_spot_depth200_1m_table_origo_module = _reload_module(
        'origo.assets.create_binance_spot_depth200_1m_table_origo'
    )
    refresh_binance_spot_depth200_1m_origo_module = _reload_module(
        'origo.assets.refresh_binance_spot_depth200_1m_origo'
    )
    reconcile_binance_spot_depth200_partition_state_origo_module = _reload_module(
        'origo.assets.reconcile_binance_spot_depth200_partition_state_origo'
    )

    return {
        'create_origo_database': create_origo_database_module.create_origo_database,
        'create_binance_spot_depth20_snapshots_table_origo': (
            create_binance_spot_depth20_snapshots_table_origo_module.create_binance_spot_depth20_snapshots_table_origo
        ),
        'sync_binance_spot_depth20_snapshots_to_origo': (
            sync_binance_spot_depth20_snapshots_to_origo_module.sync_binance_spot_depth20_snapshots_to_origo
        ),
        'create_binance_spot_depth20_1m_table_origo': (
            create_binance_spot_depth20_1m_table_origo_module.create_binance_spot_depth20_1m_table_origo
        ),
        'refresh_binance_spot_depth20_1m_origo': (
            refresh_binance_spot_depth20_1m_origo_module.refresh_binance_spot_depth20_1m_origo
        ),
        'reconcile_binance_spot_depth20_partition_state_origo': (
            reconcile_binance_spot_depth20_partition_state_origo_module.reconcile_binance_spot_depth20_partition_state_origo
        ),
        'DEPTH20_SNAPSHOTS_TABLE_NAME': (
            create_binance_spot_depth20_snapshots_table_origo_module.SNAPSHOTS_TABLE_NAME
        ),
        'DEPTH20_1M_TABLE_NAME': (
            create_binance_spot_depth20_1m_table_origo_module.DEPTH20_1M_TABLE_NAME
        ),
        'create_binance_spot_depth200_snapshots_table_origo': (
            create_binance_spot_depth200_snapshots_table_origo_module.create_binance_spot_depth200_snapshots_table_origo
        ),
        'sync_binance_spot_depth200_snapshots_to_origo': (
            sync_binance_spot_depth200_snapshots_to_origo_module.sync_binance_spot_depth200_snapshots_to_origo
        ),
        'create_binance_spot_depth200_1m_table_origo': (
            create_binance_spot_depth200_1m_table_origo_module.create_binance_spot_depth200_1m_table_origo
        ),
        'refresh_binance_spot_depth200_1m_origo': (
            refresh_binance_spot_depth200_1m_origo_module.refresh_binance_spot_depth200_1m_origo
        ),
        'reconcile_binance_spot_depth200_partition_state_origo': (
            reconcile_binance_spot_depth200_partition_state_origo_module.reconcile_binance_spot_depth200_partition_state_origo
        ),
        'DEPTH200_SNAPSHOTS_TABLE_NAME': (
            create_binance_spot_depth200_snapshots_table_origo_module.SNAPSHOTS_TABLE_NAME
        ),
        'DEPTH200_1M_TABLE_NAME': (
            create_binance_spot_depth200_1m_table_origo_module.DEPTH200_1M_TABLE_NAME
        ),
    }


@pytest.fixture()
def query_origo(clickhouse_settings: dict[str, str]) -> Any:
    def _run(query: str) -> list[tuple[Any, ...]]:
        return _query_rows(clickhouse_settings, query)

    return _run


@pytest.fixture()
def origo_definitions_module(
    monkeypatch: pytest.MonkeyPatch,
    origo_test_env: dict[str, str],
) -> Any:
    return _reload_module('origo.definitions')
