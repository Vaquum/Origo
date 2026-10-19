from __future__ import annotations

import logging
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest
from dagster import AssetExecutionContext, AssetKey, DagsterInstance, Definitions, Failure, RetryRequested
from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.remote_origin import RemotePartitionSetOrigin

from origo.sources import capacity
from origo.sources.adapters import book_vendor as vendor
from origo.sources.backfill import execute_partition_backfill
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.sources.contracts import SourceError
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_book_law import authoritative_book_runtime as authoritative_book_runtime
from .test_book_vendor import (
    REAL_HOUR,
    original_vendor_archive as original_vendor_archive,
    original_vendor_following_archive as original_vendor_following_archive,
    original_vendor_preparation as original_vendor_preparation,
    original_vendor_revision as original_vendor_revision,
    original_vendor_spool as original_vendor_spool,
    vendor_session_root as vendor_session_root,
)
from .test_revisioned_source_framework_backfill import _repository_origin
from .test_revisioned_source_framework_feedback import _observe_failure


def _job(runtime: SourceRuntime):
    bundle = build_source_bundle(runtime.spec)
    repository = Definitions(assets=bundle.assets, jobs=bundle.jobs,
                             schedules=bundle.schedules, sensors=bundle.sensors).get_repository_def()
    return repository.get_job(f'backfill_{runtime.spec.key}_source_job')


def _parent(instance: DagsterInstance, job_name: str, keys: tuple[str, ...] = (REAL_HOUR,)) -> str:
    identity = str(uuid4())
    instance.add_backfill(PartitionBackfill(
        backfill_id=identity, status=BulkActionStatus.REQUESTED, from_failure=False,
        tags={}, backfill_timestamp=time.time(), partition_names=list(keys),
        partition_set_origin=RemotePartitionSetOrigin(repository_origin=_repository_origin(),
                                                      partition_set_name=job_name + '_partition_set'),
    ))
    return identity


def _capacity(runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(runtime.lock_root))
    monkeypatch.setattr(capacity, '_volumes', lambda runtime: (capacity._Volume('fixture-volume', runtime.lock_root),))
    monkeypatch.setattr(capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7))


def _failures(runtime: SourceRuntime, partition: str):
    return runtime.store.execute(
        'SELECT failure_key FROM origo.source_failure_log WHERE source_key=%(source)s '
        "AND partition_key=%(partition)s GROUP BY failure_key HAVING argMax(event_type,event_time)='FAILED'",
        {'source': runtime.spec.key, 'partition': partition},
    )


def test_historical_book_404_is_terminal_without_changing_transient_retries(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    runtime = authoritative_book_runtime
    _capacity(runtime, monkeypatch)
    home = tmp_path / 'native-404'
    home.mkdir()
    with DagsterInstance.local_temp(str(home)) as instance:
        from origo.sources import prepare
        from dagster._core.test_utils import create_run_for_test

        control = create_run_for_test(instance, job_name=f'backfill_{runtime.spec.key}_source_job')

        # Matrix cases exercise the wrapper, without sleeping in Dagster's executor.
        context = cast(AssetExecutionContext, SimpleNamespace(
            partition_keys=[REAL_HOUR], instance=instance,
            run=SimpleNamespace(run_id=control.run_id), log=logging.getLogger(__name__),
        ))
        with monkeypatch.context() as patch:
            patch.setattr(prepare, 'prepare_source', lambda spec, instance: None)
            for code in (
                'PROVIDER_HTTP_404', 'PROVIDER_TRANSPORT_FAILED', 'PROVIDER_RATE_CIRCUIT',
                'PROVIDER_HTTP_408', 'PROVIDER_HTTP_418', 'PROVIDER_HTTP_429',
                'PROVIDER_HTTP_500', 'PROVIDER_HTTP_502', 'PROVIDER_HTTP_503',
                'PROVIDER_HTTP_504', 'OFFICIAL_REVISION_CHANGED', 'GENERATION_CHANGED',
            ):
                error = SourceError(code, 'Controlled provider/control failure; no market rows.')

                def failed(day: str) -> dict[str, object]:
                    raise error

                if code == 'PROVIDER_HTTP_404':
                    with pytest.raises(Failure) as terminal:
                        execute_partition_backfill(runtime.spec, context, failed)
                    assert terminal.value.allow_retries is False and terminal.value.__cause__ is error
                    assert code in str(terminal.value)
                else:
                    with pytest.raises(RetryRequested) as retry:
                        execute_partition_backfill(runtime.spec, context, failed)
                    assert (retry.value.max_retries, retry.value.seconds_to_wait) == (23, 3600)
                    assert retry.value.__cause__ is error
            error = SourceError('PROVIDER_HTTP_404', 'Unpublished current authority.')
            context.partition_keys[:] = [runtime.spec.canonical.candidate(datetime.now(UTC)).key]
            with pytest.raises(RetryRequested) as current:
                execute_partition_backfill(runtime.spec, context, failed)
            assert (current.value.max_retries, current.value.seconds_to_wait) == (23, 3600)
            context.partition_keys[:] = ['2017-08-17']
            with pytest.raises(RetryRequested) as daily:
                execute_partition_backfill(BINANCE_SPOT_TRADES_SPEC, context, failed)
            assert (daily.value.max_retries, daily.value.seconds_to_wait) == (23, 3600)

        market = runtime.spec.canonical.market
        calls: list[str] = []

        def missing(url: str, **kwargs: object):
            params = kwargs['params']
            assert isinstance(params, dict)
            calls.append(str(params['file']))
            raise SourceError('PROVIDER_HTTP_404', 'Provider returned HTTP 404.')

        monkeypatch.setattr(vendor, 'get_response', missing)
        spec = replace(runtime.spec, canonical=vendor.CryptoHFTBookHourly(market))
        actual = replace(runtime, spec=spec, store=SourceStore(runtime.store.client, 'origo', spec))
        job = _job(actual)
        parent = _parent(instance, job.name)
        for _ in range(2):
            before = len(calls)
            result = job.execute_in_process(instance=instance, partition_key=REAL_HOUR,
                                            tags={'dagster/backfill': parent}, raise_on_error=False)
            assert not result.success and len(calls) > before
            assert not any(event.event_type_value == 'STEP_UP_FOR_RETRY' for event in result.all_events)
            selected = vendor._file(market, vendor.hour_partition(REAL_HOUR))
            assert any('Missing provider archive:' in str(event) and selected in str(event)
                       for event in result.all_events)
        assert _failures(actual, REAL_HOUR)
        assert actual.store.record(vendor.hour_partition(REAL_HOUR)) is None


def test_native_book_selection_completion_preserves_source_failures(
    authoritative_book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    runtime = authoritative_book_runtime
    _capacity(runtime, monkeypatch)
    unrelated = '2025-06-28T07Z'
    runtime.failures.record(operation='canonical', error_code='BOOK_KNOWN_DEPTH_EXHAUSTED',
                            scope='PARTITION', partition=unrelated,
                            message='Controlled unrelated source failure; no provider-coverage claim.')
    before = _failures(runtime, unrelated)
    runtime.failures.record(operation='backfill', error_code='RUN_FAILED', scope='NONE')
    home = tmp_path / 'native-completion'
    home.mkdir()
    with DagsterInstance.local_temp(str(home)) as instance:
        job = _job(runtime)
        from dagster import validate_run_config

        config = job.get_run_config_for_partition_key(REAL_HOUR)
        assert validate_run_config(job, config)['execution'][job.executor_def.name].keys() == {'multiprocess'}
        assert validate_run_config(job, {
            'execution': {'config': {'in_process': {}}},
        })['execution'][job.executor_def.name].keys() == {'in_process'}
        assert job.asset_layer.executable_asset_keys == {
            AssetKey(f'build_{runtime.spec.key}_canonical_revision_origo')
        }
        parent = _parent(instance, job.name)
        result = job.execute_in_process(instance=instance, partition_key=REAL_HOUR,
                                        tags={'dagster/backfill': parent}, raise_on_error=False)
        assert result.success
        assert _failures(runtime, unrelated) == before
        assert runtime.store.execute(
            "SELECT failure_key FROM origo.source_failure_log WHERE source_key=%(source)s "
            "AND operation='backfill' GROUP BY failure_key "
            "HAVING argMax(event_type,event_time)='FAILED'",
            {'source': runtime.spec.key},
        ) == []
        health_job = next(
            item for item in build_source_bundle(runtime.spec).jobs
            if item.name == f'reconcile_{runtime.spec.key}_source_origo_job'
        )
        health = health_job.execute_in_process(instance=instance)
        assert any(event.event_type_value == 'ASSET_CHECK_EVALUATION'
                   and event.event_specific_data.check_name == 'source_health'
                   and event.event_specific_data.passed is False for event in health.all_events)
        record = runtime.store.record(vendor.hour_partition(REAL_HOUR))
        assert record is not None and len(record.component_hashes) == 4
        assert runtime.store.execute(
            'SELECT partition_key,revision,build_id,generation,dagster_run_id '
            'FROM origo.source_backfill_log WHERE source_key=%(source)s AND backfill_id=%(parent)s',
            {'source': runtime.spec.key, 'parent': parent},
        ) == [(REAL_HOUR, record.revision, record.build_id, record.generation, result.run_id)]
        proofs = runtime.store.execute(
            'SELECT component,row_count,content_hash FROM origo.source_component_log '
            'WHERE source_key=%(source)s AND build_id=%(build)s',
            {'source': runtime.spec.key, 'build': record.build_id},
        )
        assert {key: count for key, count, _ in proofs} == {
            'depth20': 36000, 'depth200': 3600, 'depth20_1m': 60, 'depth200_1m': 60,
        }
        assert dict(record.component_hashes) == {key: digest for key, _, digest in proofs}

        # A deliberate post-canonical persistence fault is terminal, outside source retries.
        receipt_parent = _parent(instance, job.name)
        persisted = SourceStore.execute
        receipt_attempts: list[str] = []

        def unavailable_receipt(self, sql, params=None, settings=None):
            if sql.startswith('INSERT INTO origo.source_backfill_log'):
                receipt_attempts.append(sql)
                raise SourceError('PROVIDER_TRANSPORT_FAILED', 'Controlled receipt persistence fault.')
            return persisted(self, sql, params, settings)

        with monkeypatch.context() as patch:
            patch.setattr(SourceStore, 'execute', unavailable_receipt)
            failed_receipt = job.execute_in_process(
                instance=instance, partition_key=REAL_HOUR,
                tags={'dagster/backfill': receipt_parent}, raise_on_error=False,
            )
        assert not failed_receipt.success and len(receipt_attempts) == 1
        assert not any(event.event_type_value == 'STEP_UP_FOR_RETRY'
                       for event in failed_receipt.all_events)
        assert runtime.store.record(record.partition) == record
        saved_component = runtime.store.execute(
            "SELECT * FROM origo.source_component_log WHERE source_key=%(source)s "
            "AND build_id=%(build)s AND component='depth200_1m'",
            {'source': runtime.spec.key, 'build': record.build_id},
        )

        # Damage proof after native receipt insertion, beyond build/reconcile validation.
        query = SourceStore.execute
        fault_reached = [False]

        def damaged(self: SourceStore, sql: str, params=None, settings=None):
            rows = query(self, sql, params, settings)
            if sql.startswith('INSERT INTO origo.source_backfill_log'):
                fault_reached[0] = True
                query(self,
                    "ALTER TABLE origo.source_component_log DELETE WHERE source_key=%(source)s "
                    "AND build_id=%(build)s AND component='depth200_1m' SETTINGS mutations_sync=1",
                    {'source': self.spec.key, 'build': record.build_id},
                )
            return rows

        with monkeypatch.context() as patch:
            patch.setattr(SourceStore, 'execute', damaged)
            invalid = job.execute_in_process(instance=instance, partition_key=REAL_HOUR,
                                             tags={'dagster/backfill': parent}, raise_on_error=False)
        assert fault_reached[0] and not invalid.success
        assert any('BACKFILL_RECEIPT_INVALID' in str(event) for event in invalid.all_events)
        assert _failures(runtime, unrelated) == before
        runtime.store.execute('INSERT INTO origo.source_component_log VALUES', saved_component)

        # A concurrent activation can retain identical payloads but supersede this receipt.
        def activate_after_join(self: SourceStore, sql: str, params=None, settings=None):
            rows = query(self, sql, params, settings)
            if sql.startswith('SELECT component, content_hash FROM origo.source_component_log'):
                self.insert_activation(replace(record, generation=record.generation + 1),
                                       'controlled-concurrent-activation')
            return rows

        with monkeypatch.context() as patch:
            patch.setattr(SourceStore, 'execute', activate_after_join)
            superseded = job.execute_in_process(instance=instance, partition_key=REAL_HOUR,
                                                tags={'dagster/backfill': parent}, raise_on_error=False)
        assert not superseded.success and any('BACKFILL_RECEIPT_INVALID' in str(event)
                                               for event in superseded.all_events)
        current = runtime.store.record(record.partition)
        assert current is not None and current.generation == record.generation + 1
        assert current.component_hashes == record.component_hashes
        assert _failures(runtime, unrelated) == before

        # A deliberate input rejection in the first hour must not hide the genuine next hour.
        missing = '2026-10-04T08Z'
        keys = (missing, REAL_HOUR)
        ranged_parent = _parent(instance, job.name, keys)
        original_fetch = type(runtime.spec.canonical).fetch

        def rejected_first(self, partition):
            if partition.key == missing:
                raise SourceError('BOOK_SEQUENCE_GAP', 'Controlled input fault; no market rows.')
            return original_fetch(self, partition)

        with monkeypatch.context() as patch:
            patch.setattr(type(runtime.spec.canonical), 'fetch', rejected_first)
            mixed = job.execute_in_process(
                instance=instance,
                tags={'dagster/backfill': ranged_parent,
                      'dagster/asset_partition_range_start': missing,
                      'dagster/asset_partition_range_end': REAL_HOUR},
                run_config=config,
                raise_on_error=False,
            )
        assert job.backfill_policy.max_partitions_per_run == vendor.BOOK_BACKFILL_BATCH_SIZE
        assert not mixed.success
        materialized = mixed.asset_materializations_for_node(
            f'build_{runtime.spec.key}_canonical_revision_origo'
        )
        assert [event.partition for event in materialized] == [REAL_HOUR]
        assert _failures(runtime, missing) and _failures(runtime, unrelated) == before
        assert runtime.store.record(vendor.hour_partition(missing)) is None
        assert runtime.store.execute(
            'SELECT partition_key,dagster_run_id FROM origo.source_backfill_log '
            'WHERE source_key=%(source)s AND backfill_id=%(parent)s',
            {'source': runtime.spec.key, 'parent': ranged_parent},
        ) == [(REAL_HOUR, mixed.run_id)]
        assert not any(event.event_type_value == 'STEP_UP_FOR_RETRY' for event in mixed.all_events)
        source = build_source_bundle(runtime.spec)
        _observe_failure((runtime, instance, source), mixed)
        assert runtime.store.execute(
            'SELECT partition_key,error_code FROM origo.source_failure_log '
            'WHERE dagster_run_id=%(run)s ORDER BY partition_key', {'run': mixed.run_id},
        ) == [(missing, 'BOOK_SEQUENCE_GAP')]

        # An earlier rejected hour must not suppress a later receipt failure in this run.
        receipt_parent = _parent(instance, job.name, keys)
        with monkeypatch.context() as patch:
            patch.setattr(type(runtime.spec.canonical), 'fetch', rejected_first)
            patch.setattr(SourceStore, 'execute', unavailable_receipt)
            mixed_receipt = job.execute_in_process(
                instance=instance,
                tags={'dagster/backfill': receipt_parent,
                      'dagster/asset_partition_range_start': missing,
                      'dagster/asset_partition_range_end': REAL_HOUR},
                run_config=config, raise_on_error=False,
            )
        assert not mixed_receipt.success
        _observe_failure((runtime, instance, source), mixed_receipt)
        _observe_failure((runtime, instance, source), mixed_receipt)
        assert runtime.store.execute(
            'SELECT partition_key,error_code FROM origo.source_failure_log '
            'WHERE dagster_run_id=%(run)s ORDER BY partition_key', {'run': mixed_receipt.run_id},
        ) == [(missing, 'BOOK_SEQUENCE_GAP'), (REAL_HOUR, 'RUN_FAILED')]
        assert runtime.store.record(vendor.hour_partition(REAL_HOUR)) == current


def test_book_benchmark_counts_partition_coverage_separately_from_attempts() -> None:
    from tools.benchmark_book_replay import PartitionAttempt, partition_metrics

    selected = ['2025-07-15T00Z', '2025-07-15T01Z', '2025-07-15T05Z']
    observed = [PartitionAttempt(selected[0], 58.163, True),
                PartitionAttempt(selected[1], 0.0, False, 'BOOK_KNOWN_DEPTH_EXHAUSTED')]
    report = partition_metrics(selected, observed + observed)
    assert report['full_partitions'] == 1
    assert report['full_partitions_percent'] == 100 / 3
    assert report['pending_partitions'] == 1
    assert report['attempts'] == 4
    assert report['failure_rate_percent'] == 50
    assert report['errors'] == {'BOOK_KNOWN_DEPTH_EXHAUSTED': 2}
    assert report['mean_seconds_per_successful_partition'] == 58.163
    assert partition_metrics(selected, [])['failure_rate_percent'] is None
    with pytest.raises(ValueError, match='failure requires its code'):
        partition_metrics(selected, [PartitionAttempt(selected[0], 0.0, False)])
