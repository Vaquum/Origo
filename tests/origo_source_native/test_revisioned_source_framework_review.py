from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from dagster import (
    DagsterInstance,
    build_run_status_sensor_context,
    build_schedule_context,
)
from dagster._core.errors import ScheduleExecutionError

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources import bundle
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import RolloutStage, SourceError
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response
from .test_revisioned_source_framework_backfill import _start_monitors


@pytest.mark.parametrize('code', ['PROVIDER_HTTP_404', 'PROVIDER_TRANSPORT_FAILED'])
def test_hourly_audit_recovers_a_day_whose_discovery_failed(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    code: str,
) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    source = bundle.build_source_bundle(spec)
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        scheduled = next(
            value for value in source.schedules if value.name.endswith('_canonical_schedule')
        )
        audit_job = next(
            value for value in source.jobs if value.tags['origo_source_operation'] == 'audit'
        )
        (tmp_path / 'dagster').mkdir()
        with DagsterInstance.local_temp(str(tmp_path / 'dagster')) as instance:
            _start_monitors(instance)
            with monkeypatch.context() as patch:

                def unavailable(url: str) -> daily.Response:
                    raise SourceError(code, 'Injected unavailable checksum response')

                patch.setattr(daily, 'get_response', unavailable)
                with build_schedule_context(
                    instance=instance,
                    scheduled_execution_time=datetime(2017, 8, 18, 4, tzinfo=UTC),
                ) as context:
                    with pytest.raises(ScheduleExecutionError, match='evaluation of schedule'):
                        scheduled.evaluate_tick(context)
                assert runtime.audit() == ()
            assert client.execute('SELECT partition_key FROM origo.source_discovery_log') == [
                ('2017-08-17',)
            ]
            assert client.execute('SELECT count() FROM origo.source_activation_log') == [(0,)]
            monkeypatch.setattr(daily, 'get_response', archive_response)
            # The hourly job retries the saved intent independently of yesterday's schedule candidate.
            assert (
                daily.BinanceSpotDaily().candidate(datetime(2017, 8, 19, 5, tzinfo=UTC)).key
                == '2017-08-18'
            )
            assert audit_job.execute_in_process(instance=instance).success
            assert client.execute(
                'SELECT partition_key, generation FROM origo.source_activation_log'
            ) == [('2017-08-17', 1)]
            assert client.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
                (3427,)
            ]
            assert runtime.audit() == ()
            assert client.execute(
                'SELECT operation, argMax(event_type, event_time) FROM origo.source_failure_log '
                'GROUP BY operation ORDER BY operation'
            ) == [('audit', 'RECOVERED'), ('discovery', 'RECOVERED')]
    finally:
        client.disconnect()


def test_quarantine_cannot_turn_provider_failure_into_revision_authority(
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
        for code in ('PROVIDER_TRANSPORT_FAILED', 'PROVIDER_HTTP_404', 'PROVIDER_HTTP_503'):
            with monkeypatch.context() as patch:

                def unavailable(url: str) -> daily.Response:
                    raise SourceError(code, 'Injected provider outage during rollback')

                patch.setattr(daily, 'get_response', unavailable)
                with pytest.raises(SourceError) as caught:
                    runtime.rollback(
                        record,
                        operator='test',
                        reason='Outage is not revision evidence',
                        quarantine=True,
                    )
                assert caught.value.code == code
            assert runtime.store.generation(record.partition) == 1
            assert client.execute(
                "SELECT count() FROM origo.source_failure_log WHERE operation='quarantine'"
            ) == [(0,)]
        assert (
            runtime.rollback(record, operator='test', reason='Authority restored').generation == 2
        )
        assert client.execute(
            'SELECT error_code, argMax(event_type, event_time) FROM origo.source_failure_log '
            "WHERE operation='rollback' GROUP BY error_code ORDER BY error_code"
        ) == [
            (code, 'RECOVERED')
            for code in ('PROVIDER_HTTP_404', 'PROVIDER_HTTP_503', 'PROVIDER_TRANSPORT_FAILED')
        ]
    finally:
        client.disconnect()


def test_setup_normalizes_equivalent_explicit_anchors_to_one_utc_instant(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    for anchor in ('2020-01-01', '2020-01-01', '2020-01-01T00:00:00Z', '2020-01-01T02:00:00+02:00'):
        assert (
            bundle.execute_source(
                BINANCE_SPOT_TRADES_SPEC,
                'setup',
                bundle.SourceRunConfig(anchor=anchor),
                run_id=str(uuid4()),
            )['status']
            == 'ready'
        )
    with pytest.raises(RuntimeError, match='anchor is immutable'):
        bundle.execute_source(
            BINANCE_SPOT_TRADES_SPEC,
            'setup',
            bundle.SourceRunConfig(anchor='2020-01-02'),
            run_id=str(uuid4()),
        )
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        store = SourceStore(client, 'origo', BINANCE_SPOT_TRADES_SPEC)
        assert store.anchor() == datetime(2020, 1, 1, tzinfo=UTC)
        assert client.execute('SELECT count() FROM origo.source_anchor_log') == [(1,)]
    finally:
        client.disconnect()


def test_untagged_worker_failures_recover_under_the_operation_context(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setattr(daily, 'get_response', archive_response)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    source = bundle.build_source_bundle(spec)
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        runtime.build('2017-08-17')
        runtime.verify('2017-08-17')
        observer = next(value for value in source.sensors if value.name.endswith('_failure_sensor'))
        (tmp_path / 'dagster').mkdir()
        with DagsterInstance.local_temp(str(tmp_path / 'dagster')) as instance:
            _start_monitors(instance)
            for operation, failure_operation, scope, partition, consumer in (
                ('setup', 'setup', 'SOURCE', None, None),
                ('audit', 'audit', 'NONE', None, None),
                ('cleanup', 'cleanup', 'NONE', None, None),
                ('certify', 'certification', 'PARTITION', '2017-08-17', None),
                ('consumer_parquet', 'consumer', 'CONSUMER', None, 'parquet'),
            ):
                job = next(
                    value
                    for value in source.jobs
                    if value.tags['origo_source_operation'] == operation
                )
                config = bundle.SourceRunConfig(
                    partition_key='old-state-token' if consumer else (partition or ''),
                    destination=str(tmp_path / spec.key / 'parquet') if consumer else '',
                )
                run_config = {'ops': {job.nodes[0].name: {'config': config.model_dump()}}}
                with monkeypatch.context() as patch:

                    def worker_crash(*args: object) -> dict[str, object]:
                        raise RuntimeError('Injected worker failure before dispatch')

                    patch.setattr(bundle, '_execute_operation', worker_crash)
                    result = job.execute_in_process(
                        instance=instance, run_config=run_config, raise_on_error=False
                    )
                    assert not result.success
                run = instance.get_run_by_id(result.run_id)
                assert run is not None
                # Older/manual runs may have no source tags, even with a valid stored config.
                with build_run_status_sensor_context(
                    sensor_name=observer.name,
                    dagster_event=next(
                        event
                        for event in result.all_events
                        if event.event_type_value == 'PIPELINE_FAILURE'
                    ),
                    dagster_instance=instance,
                    dagster_run=run._replace(tags={}),
                ) as context:
                    observer(context)
                    observer(context)
                assert client.execute(
                    'SELECT operation, blocking_scope, partition_key, consumer FROM origo.source_failure_log '
                    "WHERE error_code='RUN_FAILED' AND dagster_run_id=%(run)s",
                    {'run': run.run_id},
                ) == [(failure_operation, scope, partition, consumer)]
                bundle.execute_source(spec, operation, config, run_id=str(uuid4()))
                assert client.execute(
                    'SELECT argMax(event_type, event_time) FROM origo.source_failure_log '
                    "WHERE error_code='RUN_FAILED' AND operation=%(operation)s GROUP BY failure_key",
                    {'operation': failure_operation},
                ) == [('RECOVERED',)]
    finally:
        client.disconnect()
