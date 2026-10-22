from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest
from dagster import (
    AssetExecutionContext, AssetKey, DailyPartitionsDefinition, DagsterInstance, Definitions,
    Failure, build_sensor_context,
)
from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.remote_origin import RemotePartitionSetOrigin
from dagster._core.test_utils import create_run_for_test

from origo import law
from origo.law_catalog import build_catalog
from origo.sources.adapters.book_local import LocalBookCanonical
from origo.sources.backfill import publication_ready
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.bundle import SourceRunConfig, build_source_bundle, execute_source
from origo.sources.contracts import Partition, Row
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS
from origo.sources.storage import SourceStore

from .test_book_sources import book_runtime as book_runtime, _keys
from .test_current_partition_recovery import _assert_public_readers
from .test_revisioned_source_framework_backfill import _repository_origin


def _predicate(report: law.LawReport, source: str, key: law.LawPredicate) -> law.PredicateReport:
    return next(feed for feed in report['feeds'] if feed['source_key'] == source)['predicates'][key]


def test_canary_books_have_the_same_reader_law_contract(
    book_runtime: SourceRuntime,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    runtime = book_runtime
    keys = _keys(runtime)
    assert runtime.spec.provisional is not None
    partition = runtime.spec.provisional.partition(keys[-1])
    now = partition.end + timedelta(seconds=30)
    # Only the historical recording's calendar differs from the production declaration.
    monkeypatch.setattr(law, 'SOURCE_REGISTRY', tuple(
        runtime.spec if spec.key == runtime.spec.key else spec for spec in law.SOURCE_REGISTRY
    ))
    monkeypatch.setitem(law.LAW_ANCHORS, runtime.spec.key, runtime.spec.partitions.start)
    before_deadline = now.replace(hour=0, minute=1, second=0, microsecond=0)
    empty = law.evaluate(runtime.store.client, 'origo', before_deadline)
    assert _predicate(empty, runtime.spec.key, 'R1')['reason'] == 'reader_empty'
    assert _predicate(empty, runtime.spec.key, 'C1')['status'] == 'NOT_DUE'
    overdue = law.evaluate(runtime.store.client, 'origo', now)
    c1 = _predicate(overdue, runtime.spec.key, 'C1')
    assert c1['status'] == 'FAIL' and c1['reason'] == 'canonical_day_missing'
    assert c1['evidence']['day'] == (now - timedelta(days=1)).date().isoformat()
    assert c1['evidence']['canonical_interval'] == 'day'

    record = runtime.build(keys[-1], provisional=True)
    report = law.evaluate(runtime.store.client, 'origo', now)
    feed = next(item for item in report['feeds'] if item['source_key'] == runtime.spec.key)
    assert set(feed['predicates']) == {'R1', 'C1', 'C2'}
    r1 = feed['predicates']['R1']
    assert r1['status'] == 'FAIL' and r1['reason'] == 'reader_book_minutes_missing'
    assert [r1['evidence']['rows_' + key] for key in BOOK_COMPONENT_KEYS] == [600, 60, 1, 1]
    query = law._Queries(runtime.store.client)
    proofs = law._proofs(query, 'origo', runtime.spec.key, str(record.build_id), record.partition.start)
    assert len(proofs) == 1 and proofs[0].verdict()['status'] == 'PASS'
    # Deliberate proof metadata faults must not certify an incomplete or mismatched grid.
    proof = proofs[0]
    shortened = replace(proof, components=tuple(
        (key, count - 1, digest, completed) if key == 'depth200_latest'
        else (key, count, digest, completed)
        for key, count, digest, completed in proof.components
    ))
    assert shortened.verdict()['reason'] == 'book_component_grid_incomplete'
    mismatched = replace(proof, hashes={**proof.hashes, 'depth200_latest': 'deliberate-proof-fault'})
    assert mismatched.verdict()['reason'] == 'proof_identity_or_hash_invalid'
    _assert_public_readers(runtime, request, 'native_provisional')
    projections = [item for item in report['projections'] if item['id'].startswith(runtime.spec.key + ':')]
    assert len(projections) == 8
    assert all(item['status'] == 'UNKNOWN' for item in projections if not item['id'].endswith('_latest'))
    assert all(item['status'] == 'FAILED' and item['evidence_id'] for item in projections if item['id'].endswith('_latest'))
    matured = law.evaluate(runtime.store.client, 'origo', now + timedelta(days=2))
    c2 = _predicate(matured, runtime.spec.key, 'C2')
    assert c2['status'] == 'FAIL' and c2['reason'] == 'canonical_day_missing'
    assert c2['evidence']['expected_days'] == 2 and c2['evidence']['valid_days'] == 0
    _assert_book_reconciliation_uses_native_daily_status(runtime, monkeypatch)

    # A deliberate physical-row deletion must fail even while retained proofs still match.
    table = runtime.store.component_table('depth200_latest')
    runtime.store.execute(
        f'ALTER TABLE {table} DELETE WHERE build_id=%(build)s AND datetime=%(minute)s',
        {'build': record.build_id, 'minute': record.partition.start},
        settings={'mutations_sync': 2},
    )
    broken = _predicate(law.evaluate(runtime.store.client, 'origo', now), runtime.spec.key, 'R1')
    assert broken['reason'] == 'reader_book_grid_incomplete'
    assert broken['evidence']['component'] == 'depth200'
    _assert_declared_history_migration_preserves_existing_generation(runtime)


def test_book_sources_are_required_daily_reader_laws() -> None:
    catalog = build_catalog('6ac86db9fbe11e53e0fa3fb4470fc7fd8f78bb70')
    gates = {gate['id']: gate for gate in catalog['gates']}
    for source, deadline in (('binance_spot_book', '04:30'), ('binance_perp_book', '10:30')):
        for predicate in ('R1', 'C1', 'C2'):
            key = f'law.{predicate}:{source}'
            assert key in catalog['law_gate_ids'] and gates[key]['cadence'] == 'periodic'
        assert gates[f'law.C1:{source}']['thresholds'] == {'deadline_utc': deadline}
        assert gates[f'law.C2:{source}']['thresholds'] == {'anchor': '2026-10-04 00:00:00+00:00'}
        assert f'book.capture.hourly_grid:{source}' not in gates


def test_native_daily_backfill_orders_completion_after_canonical_build(
    book_runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
) -> None:
    runtime, spec = book_runtime, book_runtime.spec
    bundle = build_source_bundle(spec)
    definitions = Definitions(
        assets=bundle.assets, jobs=bundle.jobs,
        schedules=bundle.schedules, sensors=bundle.sensors,
    )
    job = definitions.get_job_def(f'backfill_{spec.key}_source_job')
    assert job.asset_layer.executable_asset_keys == {
        AssetKey(f'build_{spec.key}_canonical_revision_origo'),
    }
    canonical = next(asset for asset in bundle.assets
                     if AssetKey(f'build_{spec.key}_canonical_revision_origo') in asset.keys)
    assert isinstance(canonical.partitions_def, DailyPartitionsDefinition)
    assert canonical.partitions_def.start == spec.partitions.start
    health = definitions.get_job_def(f'reconcile_{spec.key}_source_origo_job')
    assert health.asset_layer.executable_asset_keys == {
        AssetKey(f'reconcile_{spec.key}_source_origo'),
    }
    _assert_native_selection_and_completion(runtime, monkeypatch, request, job.name)


def _assert_native_selection_and_completion(
    runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest, job_name: str,
) -> None:
    store = runtime.store
    native = runtime.build(_keys(runtime)[-1], provisional=True)
    _assert_public_readers(runtime, request, 'before_canonical_control')
    # This is a one-minute activation/proof protocol control, never a complete day.
    # Every copied market value is unchanged; only its canonical provenance differs.
    record = replace(
        native, partition=replace(native.partition, provisional=False), build_id=uuid4(),
        generation=native.generation + 1,
        component_hashes=tuple((key.removesuffix('_latest'), digest)
                               for key, digest in native.component_hashes),
    )
    bindings = {'native': native.build_id, 'canonical': record.build_id}
    for component in runtime.spec.components:
        if not component.provisional:
            columns = ','.join(column.name for column in component.columns)
            store.execute(
                f'INSERT INTO {store.component_table(component.key)} '
                f'SELECT source_date,partition_key,revision,%(canonical)s,{columns} '
                f'FROM {store.component_table(component.key + "_latest")} '
                'WHERE build_id=%(native)s', bindings,
            )
    store.execute(
        'INSERT INTO origo.source_component_log SELECT source_key,partition_key,0,revision,'
        "%(canonical)s,replaceOne(component,'_latest',''),row_count,content_hash,"
        'source_content_hash,evidence_json,artifact_ref,dagster_run_id,completed_at '
        'FROM origo.source_component_log WHERE build_id=%(native)s', bindings,
    )
    store.insert_activation(record, runtime.run_id)
    assert store.snapshot().records == (record,)
    for phase in ('canonical_precedes_overlapping_provisional', 'provisional_physically_absent'):
        if phase == 'provisional_physically_absent':
            # Deliberate physical-absence control rejects a reader using only the
            # identical provisional copy instead of the accepted canonical arm.
            for component in runtime.spec.components:
                if component.provisional:
                    store.execute(
                        f'ALTER TABLE {store.component_table(component.key)} DELETE '
                        'WHERE build_id=%(native)s', bindings, settings={'mutations_sync': 2},
                    )
        for component, count in zip(BOOK_COMPONENT_KEYS, (600, 60, 1, 1), strict=True):
            assert store.execute(
                f'SELECT count() FROM {store.table(runtime.spec.key + "_" + component + "_current")}'
            ) == [(count,)]
        _assert_public_readers(runtime, request, phase)
    proofs = store.execute(
        'SELECT component,row_count,content_hash FROM origo.source_component_log '
        'WHERE source_key=%(source)s AND build_id=%(build)s',
        {'source': runtime.spec.key, 'build': record.build_id},
    )
    assert {key: count for key, count, _digest in proofs} == dict(
        zip(BOOK_COMPONENT_KEYS, (600, 60, 1, 1), strict=True)
    )
    assert dict(record.component_hashes) == {key: digest for key, _count, digest in proofs}

    # Narrow only the partition-calendar protocol to this actual captured minute.
    # The real completion validator and all its storage queries remain unchanged.
    def recorded_minute(self: LocalBookCanonical, key: str) -> Partition:
        assert key == record.partition.key
        return record.partition

    monkeypatch.setattr(LocalBookCanonical, 'partition', recorded_minute)
    with DagsterInstance.ephemeral() as instance:
        parent = str(uuid4())
        instance.add_backfill(PartitionBackfill(
            backfill_id=parent, status=BulkActionStatus.REQUESTED, from_failure=False, tags={},
            backfill_timestamp=datetime.now(UTC).timestamp(), partition_names=[record.partition.key],
            partition_set_origin=RemotePartitionSetOrigin(
                repository_origin=_repository_origin(), partition_set_name=job_name + '_partition_set',
            ),
        ))
        run = create_run_for_test(instance, job_name=job_name, tags={'dagster/backfill': parent})
        context = cast(AssetExecutionContext, SimpleNamespace(
            instance=instance, run=run, partition_keys=[record.partition.key],
            log=logging.getLogger(__name__),
        ))
        result: dict[str, object] = {
            'partition_key': record.partition.key, 'revision': record.revision,
            'build_id': str(record.build_id), 'generation': record.generation,
        }
        assert publication_ready(runtime.spec, context, result) is True
        assert store.execute(
            'SELECT partition_key,revision,build_id,generation,dagster_run_id '
            'FROM origo.source_backfill_log WHERE source_key=%(source)s AND backfill_id=%(parent)s',
            {'source': runtime.spec.key, 'parent': parent},
        ) == [(record.partition.key, record.revision, record.build_id, record.generation, run.run_id)]
        for field, fault in (('revision', 'deliberate-revision-fault'),
                             ('build_id', str(uuid4())), ('generation', record.generation + 1)):
            with pytest.raises(Failure, match='BACKFILL_RECEIPT_INVALID'):
                publication_ready(runtime.spec, context, {**result, field: fault})
            assert store.record(record.partition) == record

        saved = store.execute(
            "SELECT * FROM origo.source_component_log WHERE build_id=%(build)s AND component='depth200_1m'",
            {'build': record.build_id},
        )
        store.execute(
            "ALTER TABLE origo.source_component_log DELETE WHERE build_id=%(build)s AND component='depth200_1m'",
            {'build': record.build_id}, settings={'mutations_sync': 2},
        )
        with pytest.raises(Failure, match='BACKFILL_RECEIPT_INVALID'):
            publication_ready(runtime.spec, context, result)
        store.execute('INSERT INTO origo.source_component_log VALUES', saved)
        persisted = SourceStore.execute
        attempts: list[str] = []

        def unavailable_receipt(
            self: SourceStore, sql: str, params: object | None = None,
            settings: Mapping[str, object] | None = None,
        ) -> list[Row]:
            if sql.startswith('INSERT INTO origo.source_backfill_log'):
                attempts.append(sql)
                raise OSError('Deliberate receipt persistence protocol fault.')
            return persisted(self, sql, params, settings)

        with monkeypatch.context() as control:
            control.setattr(SourceStore, 'execute', unavailable_receipt)
            with pytest.raises(OSError, match='receipt persistence'):
                publication_ready(runtime.spec, context, result)
        assert len(attempts) == 1
        assert publication_ready(runtime.spec, context, result) is True

    unrelated = _keys(runtime)[0]
    assert unrelated != record.partition.key
    runtime.failures.record(operation='canonical', partition=unrelated, scope='PARTITION',
                            error_code='DELIBERATE_UNRELATED_COMPLETION_CONTROL')
    runtime.failures.record(operation='backfill', scope='NONE', error_code='RUN_FAILED')
    before = store.execute(
        "SELECT * FROM origo.source_failure_log WHERE operation='canonical' AND partition_key=%(key)s",
        {'key': unrelated},
    )
    completed = execute_source(runtime.spec, 'complete', SourceRunConfig(), run_id=runtime.run_id)
    assert completed['healthy'] is False and completed['unresolved_failures'] == 1
    assert store.execute(
        "SELECT * FROM origo.source_failure_log WHERE operation='canonical' AND partition_key=%(key)s",
        {'key': unrelated},
    ) == before
    assert store.execute(
        "SELECT argMax(event_type,event_time) FROM origo.source_failure_log WHERE operation='backfill'"
    ) == [('RECOVERED',)]
    assert store.record(record.partition) == record


def test_fresh_book_minute_does_not_hide_a_provisional_gap(book_runtime: SourceRuntime) -> None:
    runtime = book_runtime
    record = runtime.build(_keys(runtime)[-1], provisional=True)
    now = record.partition.end + timedelta(seconds=30)
    query = law._Queries(runtime.store.client)
    edges = law._edge(query, 'origo', runtime.spec.key, now)
    proofs = law._proofs(query, 'origo', runtime.spec.key, str(record.build_id))
    result = law._r1(query, 'origo', runtime.spec, now, edges, proofs)
    assert result['status'] == 'FAIL' and result['reason'] == 'reader_book_minutes_missing'
    assert result['evidence']['age_seconds'] == 30
    assert result['evidence']['covered_minutes'] == 1
    missing = result['evidence']['missing_minutes']
    assert isinstance(missing, int) and missing > 0
    assert [result['evidence']['rows_' + key] for key in BOOK_COMPONENT_KEYS] == [600, 60, 1, 1]


def test_book_tail_proofs_include_distinct_minute_builds(book_runtime: SourceRuntime) -> None:
    runtime = book_runtime
    keys = _keys(runtime)
    first = runtime.build(keys[0], provisional=True)
    last = runtime.build(keys[-1], provisional=True)
    assert first.build_id != last.build_id
    query = law._Queries(runtime.store.client)
    proofs = law._proofs(query, 'origo', runtime.spec.key, str(last.build_id), first.partition.start)
    assert {proof.build for proof in proofs if proof.provisional} == {str(first.build_id), str(last.build_id)}
    now = last.partition.end + timedelta(seconds=30)
    result = law._r1(query, 'origo', runtime.spec, now, law._edge(query, 'origo', runtime.spec.key, now), proofs)
    assert result['evidence']['covered_minutes'] == 2



def _assert_declared_history_migration_preserves_existing_generation(runtime: SourceRuntime) -> None:
    declared = BINANCE_SPOT_BOOK_SPEC if runtime.spec.key == 'binance_spot_book' else BINANCE_PERP_BOOK_SPEC
    retained = runtime.store.records()
    previous = declared.partitions.previous_starts[0]
    runtime.store.execute(
        'ALTER TABLE origo.source_anchor_log UPDATE anchor=%(anchor)s WHERE source_key=%(source)s',
        {'anchor': previous, 'source': runtime.spec.key}, settings={'mutations_sync': 2},
    )
    restored = replace(runtime, spec=declared, store=SourceStore(runtime.store.client, 'origo', declared))
    restored.setup()
    restored.setup()
    assert restored.store.records() == retained
    assert restored.store.anchor() == datetime(2026, 10, 4, tzinfo=UTC)
    evidence = restored.store.execute(
        "SELECT evidence_json FROM origo.source_observation_log WHERE source_key=%(source)s AND partition_key=''",
        {'source': runtime.spec.key},
    )
    assert len(evidence) == 1
    assert json.loads(str(evidence[0][0])) == {
        'previous_anchor': previous.isoformat(), 'anchor': declared.partitions.start.isoformat(),
    }
    # An undeclared anchor remains a hard failure; the retirement does not loosen migrations.
    restored.store.execute(
        'ALTER TABLE origo.source_anchor_log UPDATE anchor=%(anchor)s WHERE source_key=%(source)s',
        {'anchor': previous - timedelta(hours=1), 'source': runtime.spec.key},
        settings={'mutations_sync': 2},
    )
    with pytest.raises(RuntimeError, match='immutable'):
        restored.setup()
    assert restored.store.records() == retained


def _assert_book_reconciliation_uses_native_daily_status(
    runtime: SourceRuntime, monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = build_source_bundle(runtime.spec)
    definitions = Definitions(
        assets=bundle.assets, jobs=bundle.jobs, sensors=bundle.sensors, schedules=bundle.schedules,
    )
    seen: list[object] = []
    original = DagsterInstance.get_status_by_partition

    def status(self: DagsterInstance, asset_key: object, partition_keys: object, partitions_def: object) -> object:
        seen.append(partitions_def)
        return original(self, asset_key, partition_keys, partitions_def)

    monkeypatch.setattr(DagsterInstance, 'get_status_by_partition', status)
    sensor = next(item for item in bundle.sensors if item.name == f'{runtime.spec.key}_reconciliation_sensor')
    with DagsterInstance.ephemeral() as instance:
        context = build_sensor_context(instance=instance, repository_def=definitions.get_repository_def())
        tick = sensor.evaluate_tick(context)
    assert len(seen) == 1 and isinstance(seen[0], DailyPartitionsDefinition)
    assert seen[0].start == runtime.spec.partitions.start
    assert all(item.partition_key is None for item in tick.run_requests)
