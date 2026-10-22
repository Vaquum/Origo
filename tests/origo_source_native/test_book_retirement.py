"""Retirement controls use no fabricated market data."""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from dagster import DagsterInstance
from dagster._core.scheduler.instigation import InstigatorStatus

from origo.assets.create_origo_database import ClickHouseSettings
from origo.sources.columnar import binary_hash
from origo.sources.contracts import Partition, StateRecord
from origo.sources.failures import FailureLog
from origo.sources.lifecycle import SourceRuntime
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from .test_book_sources import _keys, book_runtime as book_runtime

_module_spec = importlib.util.spec_from_file_location(
    'retire_cryptohftdata', Path(__file__).resolve().parents[2] / 'tools/retire_cryptohftdata.py',
)
assert _module_spec is not None and _module_spec.loader is not None
retirement = importlib.util.module_from_spec(_module_spec)
sys.modules[_module_spec.name] = retirement
_module_spec.loader.exec_module(retirement)


def _stopped() -> list[SimpleNamespace]:
    return [
        SimpleNamespace(instigator_name=f'{source}_{role}_{kind}', status=InstigatorStatus.STOPPED)
        for source in retirement.SOURCES
        for kind, roles in (('schedule', ('canonical', 'audit')),
                            ('sensor', ('reconciliation', 'failure')))
        for role in roles
    ]


@pytest.mark.parametrize('fault', ['adapter', 'automation', 'run', 'backfill'])
def test_retirement_refuses_nonlocal_or_nonquiescent_books(
    monkeypatch: pytest.MonkeyPatch, fault: str,
) -> None:
    """Each independent writer-admission fault must prevent the cleanup boundary."""
    specs = tuple(next(spec for spec in SOURCE_REGISTRY if spec.key == key)
                  for key in retirement.SOURCES)
    instance = MagicMock(spec=DagsterInstance)
    instance.all_instigator_state.return_value = _stopped()
    instance.get_runs.return_value = []
    monkeypatch.setattr(retirement, 'backfill_active', lambda *_args: fault == 'backfill')
    if fault == 'adapter':
        # Deliberately invalid adapter is a protocol fault, not a replacement source.
        specs = (replace(specs[0], canonical=object()), specs[1])
    elif fault == 'automation':
        instance.all_instigator_state.return_value[0].status = InstigatorStatus.RUNNING
    elif fault == 'run':
        instance.get_runs.return_value = [SimpleNamespace(
            tags={'origo_source_key': retirement.SOURCES[0]},
            job_name='deliberate_pending_book_run', run_id='deliberate-control', asset_selection=(),
        )]
    with pytest.raises(RuntimeError):
        retirement.require_quiescent(instance, specs)


@pytest.mark.parametrize('apply', [False, True])
def test_retirement_dry_run_and_single_mutation_keep_protection_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, apply: bool,
) -> None:
    """A dry-run never writes; apply must gate, mutate once and verify pinned natives."""
    evidence = tmp_path / 'retirement.jsonl'
    monkeypatch.setattr('sys.argv', ['retirement', '--evidence', str(evidence),
                                    *(['--apply'] if apply else [])])
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    client = MagicMock()
    client.execute.return_value = [(0,)]  # Mutation postcondition, no market rows.
    instance = MagicMock(spec=DagsterInstance)
    manager = MagicMock()
    manager.__enter__.return_value = instance
    monkeypatch.setattr(retirement.DagsterInstance, 'get', lambda: manager)
    monkeypatch.setattr(retirement, 'get_clickhouse_settings', lambda: ClickHouseSettings(
        'localhost', 9000, 'default', 'deliberate-control', 'origo',
    ))
    monkeypatch.setattr(retirement, 'make_clickhouse_client', lambda _settings: client)
    stages: list[str] = []
    monkeypatch.setattr(retirement, 'require_quiescent', lambda *_args: stages.append('quiescent'))
    monkeypatch.setattr(retirement, 'require_no_mutations', lambda *_args: stages.append('idle'))
    deletion = retirement.Deletion('origo.source_activation_log', retirement.VENDOR, {}, 1)
    monkeypatch.setattr(retirement, 'next_deletion', lambda _stores: deletion)
    monkeypatch.setattr(retirement, 'vendor_manifest', lambda _stores:
                        retirement.VendorManifest((), (), (), ()))
    monkeypatch.setattr(retirement, 'protected_fingerprints', lambda _stores: ())
    monkeypatch.setattr(retirement, 'verify_protected', lambda *_args: stages.append('verified'))
    retirement.main()
    mutations = [call for call in client.execute.call_args_list
                 if call.args[0].startswith('ALTER TABLE')]
    assert len(mutations) == int(apply)
    frames = [json.loads(line) for line in evidence.read_text().splitlines()]
    assert [frame['phase'] for frame in frames] == ['before', *(['after'] if apply else [])]
    assert evidence.stat().st_mode & 0o077 == 0
    assert stages == (['quiescent', 'idle', 'quiescent', 'idle', 'idle', 'verified']
                      if apply else ['quiescent', 'idle'])
    if apply:
        assert mutations[0].kwargs['settings']['mutations_sync'] == 2


def test_retirement_visibility_precedes_proofs_and_never_selects_latest_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remaining activation must be removed before a raw or proof candidate."""
    stores = tuple(retirement.SourceStore(MagicMock(), 'origo', spec)
                   for spec in SOURCE_REGISTRY if spec.key in retirement.SOURCES)
    selected: list[str] = []

    def candidate(
        _store: retirement.SourceStore, table: str, predicate: str, _column: str | None,
    ) -> retirement.Deletion | None:
        selected.append(table)
        return retirement.Deletion(table, predicate, {}, 1)

    monkeypatch.setattr(retirement, '_candidate', candidate)
    deletion = retirement.next_deletion(stores)
    assert deletion is not None and deletion.table == 'origo.source_activation_log'
    assert selected == [deletion.table]
    assert 'NOT provisional' in deletion.predicate
    assert "startsWith(ifNull(revision,''),'cryptohftdata-v1:')" in deletion.predicate
    assert "startsWith(ifNull(revision,''),'cryptohftdata-v2:')" in deletion.predicate


def test_vendor_scratch_lookup_stays_below_query_limit_with_many_build_identities() -> None:
    """UUID protocol controls reject a scratch lookup exceeding ClickHouse's query limit."""
    spec = next(spec for spec in SOURCE_REGISTRY if spec.key == retirement.SOURCES[0])
    # Deliberate identity-only controls; no market values or acceptance claims.
    builds = [(spec.key, '', '', UUID(int=index + 1)) for index in range(10000)]
    names = tuple('source_build_' + row[3].hex for row in builds)
    assert len(str(names).encode()) > 262144
    client = MagicMock()
    client.execute.side_effect = [builds, [], [], []]
    store = SourceStore(client, 'origo', spec)

    manifest = retirement.vendor_manifest((store,))

    query, params = client.execute.call_args_list[-1].args
    assert 'system.databases' in query
    assert params is None
    assert len(query.encode()) < 262144
    assert len(manifest.builds) == len(builds)
    assert manifest.scratch_databases == ()


def test_retirement_preserves_native_and_unrelated_data(book_runtime: SourceRuntime) -> None:
    """Real captured values survive; only deliberately vendor-labelled provenance is removed."""
    runtime = book_runtime
    store = runtime.store
    native = runtime.build(_keys(runtime)[-1], provisional=True)
    hour = native.partition.start.replace(minute=0)
    key = hour.strftime('%Y-%m-%dT%HZ')
    vendor = 'cryptohftdata-v2:' + native.revision.removeprefix('local-book-v1:')
    vendor_build, local_build = uuid4(), uuid4()
    bindings = {'native': native.build_id, 'hour': key, 'vendor': vendor,
                'vendor_build': vendor_build, 'start': hour, 'end': hour + timedelta(hours=1)}
    # Copy unchanged captured values. The provider label is an explicit provenance
    # fault control; these rows do not claim to be genuine provider acceptance.
    local_counts: dict[str, tuple[object, str]] = {}
    for component in runtime.spec.components:
        if not component.provisional:
            table = store.component_table(component.key)
            columns = ','.join(column.name for column in component.columns)
            for revision, build in ((vendor, vendor_build), (native.revision, local_build)):
                store.execute(
                    f'INSERT INTO {table} SELECT source_date,%(hour)s,%(revision)s,%(build)s,'
                    f'{columns} FROM {store.component_table(component.key + "_latest")} '
                    'WHERE build_id=%(native)s',
                    {**bindings, 'revision': revision, 'build': build},
                )
            params = {'build': local_build}
            count = store.execute(f'SELECT count() FROM {table} WHERE build_id=%(build)s', params)[0][0]
            digest = binary_hash(store.client, component, table, predicate='build_id=%(build)s',
                                 params=params, schema_version=runtime.spec.schema_version)
            local_counts[component.key] = count, digest
    store.execute(
        'INSERT INTO origo.source_build_log SELECT source_key,%(hour)s,0,%(start)s,%(end)s,'
        '%(vendor)s,%(vendor_build)s,started_at FROM origo.source_build_log '
        'WHERE build_id=%(native)s', bindings,
    )
    store.execute(
        'INSERT INTO origo.source_component_log SELECT source_key,%(hour)s,0,%(vendor)s,'
        "%(vendor_build)s,replaceOne(component,'_latest',''),row_count,content_hash,"
        'source_content_hash,evidence_json,artifact_ref,dagster_run_id,completed_at '
        'FROM origo.source_component_log WHERE build_id=%(native)s', bindings,
    )
    store.insert_activation(StateRecord(
        Partition(key, hour, hour + timedelta(hours=1)), 1, vendor, vendor_build,
        tuple((name.removesuffix('_latest'), digest) for name, digest in native.component_hashes),
    ), str(uuid4()))
    runtime.failures.record(operation='canonical', partition=key, revision=vendor,
                            build_id=vendor_build, scope='PARTITION', error_code='DELIBERATE_VENDOR_CONTROL')
    runtime.failures.record(operation='provisional', partition=native.partition.key,
                            revision=native.revision, build_id=native.build_id, scope='PARTITION',
                            error_code='DELIBERATE_NATIVE_CONTROL')
    # A real Dagit bridge uses the originating failure ID but its own observer run.
    events = store.execute(
        'SELECT event_id,event_time,error_code FROM origo.source_failure_log '
        'WHERE source_key=%(source)s ORDER BY error_code', {'source': runtime.spec.key},
    )
    observer = str(uuid4())
    receipts = [
        (event, runtime.spec.key, f'{runtime.spec.key}:dagit:{event}', 0,
         'LOGGED', observer, occurred)
        for event, occurred, _code in events
    ]
    native_receipt = next(receipt for receipt, event in zip(receipts, events, strict=True)
                          if event[2] == 'DELIBERATE_NATIVE_CONTROL')
    store.execute('INSERT INTO origo.source_run_log VALUES', receipts)
    other = SourceStore(store.client, store.database, SOURCE_REGISTRY[0])
    FailureLog(other, runtime.lock_root, str(uuid4())).record(
        operation='canonical', partition=key, revision=vendor, build_id=vendor_build,
        scope='PARTITION', error_code='DELIBERATE_UNRELATED_CONTROL',
    )
    with pytest.raises(RuntimeError, match='only the two declared'):
        retirement.next_deletion((other,))
    scratch_names = ('source_build_' + vendor_build.hex, 'source_build_' + local_build.hex)
    for name in scratch_names:
        store.execute(f'CREATE DATABASE {name}')
    try:
        ownership = retirement.vendor_manifest((store,))
        assert {row[3] for row in ownership.builds} == {vendor_build}
        assert {row[1] for row in ownership.runs} >= {runtime.run_id, observer}
        assert {row[1] for row in ownership.events} == {
            row[0] for row in receipts if row[0] != native_receipt[0]
        }
        assert ownership.scratch_databases == (scratch_names[0],)
        with pytest.raises(RuntimeError, match=scratch_names[0]):
            retirement.require_no_vendor_scratch(ownership)
    finally:
        for name in scratch_names:
            store.execute(f'DROP DATABASE {name} SYNC')
    pins = retirement.protected_fingerprints((store,))
    deletion = retirement.next_deletion((store,))
    assert deletion is not None and deletion.table.endswith('.source_activation_log')
    mutations: list[str] = []
    for _ in range(30):
        if deletion is None:
            break
        mutations.append(deletion.table)
        store.execute(deletion.statement, deletion.params, settings={'mutations_sync': 2})
        retirement.verify_protected((store,), pins)
        deletion = retirement.next_deletion((store,))
    assert deletion is None
    assert all('_latest_' not in table for table in mutations)
    assert store.records() == (native,)
    assert store.execute(
        'SELECT error_code FROM origo.source_failure_log ORDER BY error_code'
    ) == [('DELIBERATE_NATIVE_CONTROL',), ('DELIBERATE_UNRELATED_CONTROL',)]
    assert store.execute(
        'SELECT * FROM origo.source_run_log WHERE source_key=%(source)s',
        {'source': runtime.spec.key},
    ) == [native_receipt]
    for component in runtime.spec.components:
        if not component.provisional:
            table = store.component_table(component.key)
            params = {'build': local_build}
            count = store.execute(f'SELECT count() FROM {table} WHERE build_id=%(build)s', params)[0][0]
            digest = binary_hash(store.client, component, table, predicate='build_id=%(build)s',
                                 params=params, schema_version=runtime.spec.schema_version)
            assert (count, digest) == local_counts[component.key]
            assert store.execute(f'SELECT count() FROM {table} WHERE {retirement.VENDOR}') == [(0,)]
