from __future__ import annotations

import ast
from pathlib import Path

import pytest
from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    build_schedule_context,
    build_sensor_context,
)

from origo import definitions
from origo.sources import bundle
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import RolloutStage
from origo.sources.registry import SOURCE_REGISTRY

ROOT = Path(__file__).resolve().parents[2] / 'origo/sources'


def test_spot_trades_is_registered_dormant_without_changing_existing_definitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert SOURCE_REGISTRY == (BINANCE_SPOT_TRADES_SPEC,)
    assert BINANCE_SPOT_TRADES_SPEC.rollout_stage == RolloutStage.DORMANT
    source = bundle.build_source_bundle(BINANCE_SPOT_TRADES_SPEC)
    assert source.assets and source.jobs and source.schedules and source.sensors
    assert all(value.default_status == DefaultScheduleStatus.STOPPED for value in source.schedules)
    assert all(value.default_status == DefaultSensorStatus.STOPPED for value in source.sensors)
    names = {sensor.name for sensor in definitions.defs.sensors or ()}
    assert {sensor.name for sensor in source.sensors} <= names
    assert 'publish_binance_spot_klines_to_huggingface_sensor' in names
    assert (
        definitions.defs.get_repository_def()
        .get_schedule_def('binance_spot_latest_1m_schedule')
        .default_status
        == DefaultScheduleStatus.RUNNING
    )

    def forbidden() -> None:
        raise AssertionError('A dormant entry point constructed an external client.')

    monkeypatch.setattr(bundle, 'get_clickhouse_settings', forbidden)
    for operation in (
        'canonical',
        'provisional',
        'audit',
        'repair',
        'cleanup',
        'certify',
        'consumer_parquet',
        'consumer_arrow',
        'consumer_huggingface_shadow',
        'unknown',
    ):
        with pytest.raises(RuntimeError, match='DORMANT'):
            bundle.execute_source(
                BINANCE_SPOT_TRADES_SPEC,
                operation,
                bundle.SourceRunConfig(),
                run_id='dormant-proof',
            )
    for schedule in source.schedules:
        assert not schedule.evaluate_tick(build_schedule_context()).run_requests
    for sensor in source.sensors[:-1]:
        assert not sensor.evaluate_tick(build_sensor_context()).run_requests


def test_core_imports_no_exchange_transport_or_parser() -> None:
    forbidden = {'requests', 'urllib', 'zipfile', 'csv', 'huggingface_hub', 'pyarrow', 'polars'}
    core = {
        'contracts.py',
        'hashing.py',
        'locking.py',
        'storage.py',
        'lifecycle.py',
        'failures.py',
        'bundle.py',
    }
    for name in core:
        text = (ROOT / name).read_text()
        assert 'binance' not in text.lower(), name
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, ast.Import):
                assert not {alias.name.split('.')[0] for alias in node.names} & forbidden, name
            if isinstance(node, ast.ImportFrom):
                module = node.module or ''
                assert not any(
                    part in module.split('.') for part in (*forbidden, 'adapters', 'profiles')
                ), name


def test_source_onboarding_guide_defines_one_path_one_failure_query_and_blocking_matrix() -> None:
    text = (ROOT / 'README.md').read_text()
    assert text.count('```mermaid') == 1
    for term in ('Revision', 'Attempt', 'Component', 'Activation', 'Route', 'Lock', 'Consumer'):
        assert f'| {term} |' in text
    for scope in ('NONE', 'CONSUMER', 'PARTITION', 'SOURCE', 'ROUTE'):
        assert f'| `{scope}` |' in text
    for required in (
        'source_failure_log',
        'argMax',
        'SOURCE_REGISTRY',
        'zero-bang',
        'DORMANT',
        'CANARY',
        'LIVE',
        'binance_spot_trades.py',
        'test_binance_daily_source_adapter.py',
        'rollback',
    ):
        assert required in text


def test_failed_source_run_gets_a_new_persistent_attempt(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace
    from datetime import UTC, datetime
    from uuid import uuid4

    from dagster import DagsterInstance, DagsterRunStatus

    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.sources.adapters import binance_daily as daily
    from origo.sources.lifecycle import SourceRuntime
    from origo.sources.storage import SourceStore

    from .test_binance_daily_source_adapter import archive_response

    monkeypatch.setattr(daily, 'get_response', archive_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup()
        source = bundle.build_source_bundle(spec)
        scheduled = next(
            schedule
            for schedule in source.schedules
            if schedule.name.endswith('_canonical_schedule')
        )
        job = next(
            job
            for job in source.jobs
            if job.name == 'refresh_binance_spot_trades_canonical_source_job'
        )
        (tmp_path / 'dagster').mkdir()
        with DagsterInstance.local_temp(str(tmp_path / 'dagster')) as instance:
            context = build_schedule_context(
                instance=instance, scheduled_execution_time=datetime(2017, 8, 18, 4, tzinfo=UTC)
            )
            first = scheduled.evaluate_tick(context).run_requests[0]
            repeated = scheduled.evaluate_tick(context).run_requests[0]
            assert first.run_key == repeated.run_key
            instance.create_run_for_job(
                job, run_config=first.run_config, tags=first.tags, status=DagsterRunStatus.FAILURE
            )
            second = scheduled.evaluate_tick(context).run_requests[0]
            assert first.run_key.endswith(':0') and second.run_key.endswith(':1')
            instance.create_run_for_job(
                job, run_config=second.run_config, tags=second.tags, status=DagsterRunStatus.SUCCESS
            )
            assert scheduled.evaluate_tick(context).run_requests == []
        assert store.execute(
            'SELECT DISTINCT attempt, status FROM origo.source_run_log ORDER BY attempt, status'
        ) == [(0, 'FAILURE'), (0, 'REQUESTED'), (1, 'REQUESTED'), (1, 'SUCCESS')]
    finally:
        client.disconnect()


def test_real_source_row_has_a_frozen_typed_hash_encoding() -> None:
    import hashlib

    from origo.sources.adapters.binance_daily import BinanceSpotDaily, spot_csv_rows
    from origo.sources.hashing import content_hash

    from .test_binance_daily_source_adapter import ARCHIVES

    partition = BinanceSpotDaily().partition('2017-08-17')
    row = next(spot_csv_rows((ARCHIVES / 'BTCUSDT-trades-2017-08-17.csv').read_bytes(), partition))
    encoded = (
        b'origo-source-content-v1\n8:i1:0d7:4261.48d3:0.1d7:426.148'
        b'i13:1502942428322i1:1i1:1t16:1502942428322000'
    )
    assert content_hash((row,), schema_version=1) == hashlib.sha256(encoded).hexdigest()
