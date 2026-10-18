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
from dagster import AssetExecutionContext, DagsterInstance, Definitions, Failure, RetryRequested
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


def _job(runtime: SourceRuntime):
    bundle = build_source_bundle(runtime.spec)
    repository = Definitions(assets=bundle.assets, jobs=bundle.jobs,
                             schedules=bundle.schedules, sensors=bundle.sensors).get_repository_def()
    return repository.get_job(f'backfill_{runtime.spec.key}_source_job')


def _parent(instance: DagsterInstance, job_name: str) -> str:
    identity = str(uuid4())
    instance.add_backfill(PartitionBackfill(
        backfill_id=identity, status=BulkActionStatus.REQUESTED, from_failure=False,
        tags={}, backfill_timestamp=time.time(), partition_names=[REAL_HOUR],
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
            assert any('Missing provider archive:' in str(event) and calls[-1] in str(event)
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
    home = tmp_path / 'native-completion'
    home.mkdir()
    with DagsterInstance.local_temp(str(home)) as instance:
        job = _job(runtime)
        parent = _parent(instance, job.name)
        result = job.execute_in_process(instance=instance, partition_key=REAL_HOUR,
                                        tags={'dagster/backfill': parent}, raise_on_error=False)
        assert result.success
        assert _failures(runtime, unrelated) == before
        assert any(event.event_type_value == 'ASSET_CHECK_EVALUATION'
                   and event.event_specific_data.check_name == 'source_health'
                   and event.event_specific_data.passed is False for event in result.all_events)
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
            if sql.startswith('SELECT uniqExact(r.partition_key), countIf('):
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
