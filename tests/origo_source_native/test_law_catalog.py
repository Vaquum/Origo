from __future__ import annotations

import inspect
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from origo.law_catalog import (
    GATE_FAMILIES,
    LawCatalog,
    ProjectionObservation,
    build_catalog,
    certification_evaluations,
    code_location,
    import_gate_events,
    observe_publications,
    projection_gate_evaluations,
    source_failure_evaluations,
    unknown_observations,
)
from origo.sources import registry
from origo.sources.adapters.binance_provisional import (
    PROVISIONAL_CATCHUP_LOOKBACK_HOURS,
    PROVISIONAL_CATCHUP_MINUTES,
    PROVISIONAL_PAGE_CAP,
)
from origo.sources.capacity import CAPACITY_FREE_INODE_DENOMINATOR, CAPACITY_TOTAL_RESERVE_TENTHS
from origo.sources.contracts import RolloutStage, Row, SourceError
from origo.sources.dagit import (
    HEALTH_IDLE_INTERVAL_SECONDS,
    HEALTH_MIN_INTERVAL_SECONDS,
    HEALTH_RECONCILIATION_BATCH_SIZE,
    HEALTH_RECONCILIATION_RETRY_DELAYS,
)
from origo.sources.lifecycle import SourceRuntime
from origo.sources.locking import source_lock
from origo.workers.depth import DEPTH_SPECS
from origo.workers.provisional import PROVISIONAL_MAX_WORKERS

from .test_current_partition_recovery import real_minutes as real_minutes

SHA = 'ca888afed9bc1681ceebe4c9cfd0502538e2a2d2'
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def catalog() -> LawCatalog:
    return build_catalog(SHA)


def test_source_catalog_matches_all_authoritative_components_and_consumers(
    catalog: LawCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = {source['id']: source for source in catalog['sources']}
    assert set(sources) == {spec.key for spec in registry.SOURCE_REGISTRY} | {
        spec.projection_table_name for spec in DEPTH_SPECS
    }
    for spec in registry.SOURCE_REGISTRY:
        source = sources[spec.key]
        assert source['rollout_stage'] == spec.rollout_stage.value
        nodes = {node['id']: node for node in source['projections']}
        assert set(nodes) == {f'{spec.key}:{component.key}' for component in spec.components} | {
            f'{spec.key}:consumer:{consumer.key}' for consumer in spec.consumers
        }
        for component in spec.components:
            node = nodes[f'{spec.key}:{component.key}']
            assert node['lane'] == ('provisional' if component.provisional else 'canonical')
            assert node['current_target'] == (
                f'{spec.key}:{component.current_target}' if component.current_target else None
            )
        assert all(
            nodes[f'{spec.key}:consumer:{item.key}']['lane'] == 'consumer'
            for item in spec.consumers
        )
    for spec in DEPTH_SPECS:
        nodes = sources[spec.projection_table_name]['projections']
        assert {node['name'] for node in nodes} == {
            spec.snapshot_table_name,
            spec.projection_table_name,
            spec.series,
        }
    # Declaration-only mutation verifies owner derivation; no market observations are invented.
    spec = registry.SOURCE_REGISTRY[0]
    added = replace(spec.components[0], key='catalog_probe')
    changed = replace(spec, rollout_stage=RolloutStage.CANARY, components=(*spec.components, added))
    monkeypatch.setattr(registry, 'SOURCE_REGISTRY', (changed, *registry.SOURCE_REGISTRY[1:]))
    updated = build_catalog(SHA)
    assert updated['version'] != catalog['version']
    source = next(source for source in updated['sources'] if source['id'] == spec.key)
    assert source['rollout_stage'] == 'CANARY'
    assert f'{spec.key}:catalog_probe' in {node['id'] for node in source['projections']}


def test_gate_catalog_has_owner_bound_meaning_thresholds_and_code(catalog: LawCatalog) -> None:
    gates = {gate['id']: gate for gate in catalog['gates']}
    assert len(gates) == len(catalog['gates'])
    for family in GATE_FAMILIES:
        assert any(
            key == family or key.startswith(family + '.') or key.startswith(family + ':')
            for key in gates
        ), family
    for gate in gates.values():
        assert (
            gate['purpose']
            and gate['governed_action']
            and gate['condition']
            and gate['evidence_source']
        )
        code = gate['code']
        assert code is not None, gate['id']
        assert code['deployed_sha'] == SHA
        path = ROOT / code['path']
        assert 1 <= code['line'] <= len(path.read_text().splitlines())
        assert (
            code['url']
            == f'https://github.com/Vaquum/Origo/blob/{SHA}/{code["path"]}#L{code["line"]}'
        )
        assert not gate['id'].startswith(('ci.', 'merge.'))
    for spec in registry.SOURCE_REGISTRY:
        key = spec.key
        assert gates[f'worker.minute_admission.catchup:{key}']['thresholds'] == {
            'lookback_hours': PROVISIONAL_CATCHUP_LOOKBACK_HOURS,
            'catchup_minutes': PROVISIONAL_CATCHUP_MINUTES,
        }
        assert (
            gates[f'worker.minute_admission.concurrent:{key}']['thresholds']['workers']
            == PROVISIONAL_MAX_WORKERS
        )
        assert (
            gates[f'source.capacity.byte_reserve:{key}']['thresholds']['reserve_tenths']
            == CAPACITY_TOTAL_RESERVE_TENTHS
        )
        assert (
            gates[f'source.capacity.inode_reserve:{key}']['thresholds']['denominator']
            == CAPACITY_FREE_INODE_DENOMINATOR
        )
        assert gates[f'sensor.reconciliation.cadence:{key}']['thresholds'] == {
            'minimum_seconds': HEALTH_MIN_INTERVAL_SECONDS,
            'idle_seconds': HEALTH_IDLE_INTERVAL_SECONDS,
        }
        assert gates[f'sensor.reconciliation.backoff:{key}']['thresholds'] == {
            f'attempt_{index + 1}_seconds': delay
            for index, delay in enumerate(HEALTH_RECONCILIATION_RETRY_DELAYS)
        }
        assert gates[f'sensor.reconciliation.batch:{key}']['thresholds'] == {
            'partitions': HEALTH_RECONCILIATION_BATCH_SIZE,
        }
        assert gates[f'provider.response_completeness.page_cap:{key}']['thresholds'] == {
            'pages': PROVISIONAL_PAGE_CAP,
        }
        assert (
            gates[f'sensor.retry.budget:{key}']['thresholds']['retry_count']
            == spec.orchestration.retry_count
        )
    for spec in registry.SOURCE_REGISTRY:
        for family in (
            'provider.response_completeness.empty_minute',
            'provider.response_completeness.request_contract',
            'sensor.retry.terminal_state',
            'sensor.reconciliation.native_run',
            'sensor.reconciliation.terminal_verdict',
        ):
            assert f'{family}:{spec.key}' in gates
        for consumer in spec.consumers:
            identity = f'publication.current.canonical_drift:{spec.key}:consumer:{consumer.key}'
            location = gates[identity]['code']
            assert location is not None
            assert location['path'] == 'origo/sources/profiles/consumer_base.py'
            ready = gates[f'publication.canonical_readiness:{spec.key}:consumer:{consumer.key}']
            assert 'unresolved partition failure' in ready['condition']
    location = gates['orchestration.admission.redundant_launch']['code']
    assert location is not None and location['path'] == 'origo/orchestration/launcher.py'
    for spec in DEPTH_SPECS:
        from origo.law import D1_DELIVERY_GRACE_SECONDS

        assert (
            gates[f'law.D1:{spec.projection_table_name}']['thresholds']['delivery_grace_seconds']
            == D1_DELIVERY_GRACE_SECONDS
        )
    assert gates['orchestration.admission.global']['thresholds']['limit'] == 19
    assert len([key for key in gates if key.startswith('orchestration.admission.tag:')]) == 5
    assert build_catalog(SHA)['version'] == catalog['version']
    assert code_location(source_lock, SHA)['path'] == 'origo/sources/locking.py'
    assert code_location(source_lock, SHA)['line'] == inspect.getsourcelines(source_lock)[1]
    assert code_location(source_lock, 'latest') is None
    assert code_location(object(), SHA) is None


def test_definition_versions_ignore_deployment_and_source_locations(
    catalog: LawCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo import law_catalog

    versions = {gate['id']: gate['definition_version'] for gate in catalog['gates']}
    deployed = build_catalog('f' * 40)
    assert {gate['id']: gate['definition_version'] for gate in deployed['gates']} == versions
    assert deployed['version'] != catalog['version']
    assert all(
        gate['code'] is not None and gate['code']['deployed_sha'] == 'f' * 40
        for gate in deployed['gates']
    )
    original = law_catalog.code_location

    def moved(owner: object, sha: str) -> law_catalog.CodeLocation | None:
        location = original(owner, sha)
        if location is not None:
            location['line'] += 1
            location['url'] = location['url'].partition('#L')[0] + f'#L{location["line"]}'
        return location

    monkeypatch.setattr(law_catalog, 'code_location', moved)
    shifted = build_catalog(SHA)
    assert {gate['id']: gate['definition_version'] for gate in shifted['gates']} == versions
    assert shifted['version'] != catalog['version']


def test_core_definition_versions_include_predicate_helpers_and_ignore_unrelated_code(
    catalog: LawCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo import law

    original = inspect.getsource
    core = {
        gate['id']: gate['definition_version']
        for gate in catalog['gates']
        if gate['id'].startswith('law.')
    }
    owner_source = original(law)
    changed = owner_source.replace("'status': status", "'status': 'UNKNOWN'", 1)
    assert changed != owner_source  # Change the real helper's predicate logic, not market data.

    def source(owner: ModuleType) -> str:
        if owner is law:
            return changed
        return original(owner)

    monkeypatch.setattr(inspect, 'getsource', source)
    revised = build_catalog(SHA)
    assert all(
        gate['definition_version'] != core[gate['id']]
        for gate in revised['gates']
        if gate['id'].startswith('law.')
    )

    def unrelated(owner: ModuleType) -> str:
        value = original(owner)
        if isinstance(owner, ModuleType) and owner.__name__ == 'origo.workers.monitor':
            return value + '\nUNRELATED_DESCRIPTOR_TEST = True\n'
        return '\n# Location/comment changes carry no predicate meaning.\n' + value

    monkeypatch.setattr(inspect, 'getsource', unrelated)
    redeployed = build_catalog(SHA)
    assert {
        gate['id']: gate['definition_version']
        for gate in redeployed['gates']
        if gate['id'].startswith('law.')
    } == core


def test_unrecorded_guards_and_projection_nodes_never_invent_health(catalog: LawCatalog) -> None:
    now = datetime.now(UTC)
    observations = unknown_observations(catalog, now)
    assert len(observations) == sum(len(source['projections']) for source in catalog['sources'])
    assert all(item['status'] == 'UNKNOWN' and item['evidence_id'] is None for item in observations)
    assert all(
        item['evidence_at'] is None and item['data_through'] is None for item in observations
    )
    unversioned = build_catalog('unknown')
    assert all(gate['code'] is None for gate in unversioned['gates'])
    assert all(
        node['code'] is None for source in unversioned['sources'] for node in source['projections']
    )


def test_schema_import_does_not_load_source_configuration() -> None:
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            "import sys; import origo.law_catalog; assert 'origo.sources.registry' not in sys.modules; assert 'clickhouse_driver' not in sys.modules",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('configured_home', [False, True])
def test_installed_package_uses_instance_config_and_package_relative_code(
    tmp_path: Path,
    configured_home: bool,
) -> None:
    from origo import law_catalog

    installed = tmp_path / 'site-packages'
    package = installed / 'origo'
    shutil.copytree(
        Path(law_catalog.__file__).parent, package, ignore=shutil.ignore_patterns('__pycache__')
    )
    environment = dict(os.environ)
    environment.pop('DAGSTER_HOME', None)
    working_directory = ROOT
    if configured_home:
        instance = tmp_path / 'instance'
        instance.mkdir()
        shutil.copyfile(ROOT / 'dagster.yaml', instance / 'dagster.yaml')
        environment['DAGSTER_HOME'] = str(instance)
        working_directory = tmp_path
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            'import sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); '
            'from origo import law_catalog; from origo.sources.locking import source_lock; '
            'import yaml; '
            "assert Path(law_catalog.__file__).parent == Path(sys.argv[1]) / 'origo'; "
            'catalog = law_catalog.build_catalog(sys.argv[2]); '
            "gate = next(g for g in catalog['gates'] if g['id'] == 'orchestration.admission.global'); "
            "assert gate['thresholds']['limit'] == 19; "
            "assert law_catalog.code_location(source_lock, sys.argv[2])['path'] == 'origo/sources/locking.py'; "
            'assert law_catalog.code_location(yaml.safe_load, sys.argv[2]) is None',
            str(installed),
            SHA,
        ],
        cwd=working_directory,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_failure_protocol_maps_only_attributable_guards(catalog: LawCatalog) -> None:
    # Isolated failure-protocol inputs, never market rows or production health history.
    values = dict(
        source_key=registry.SOURCE_REGISTRY[0].key,
        event_id='recorded-event',
        event_time='2026-09-23T10:00:00+00:00',
        consumer='mount',
    )
    events = source_failure_evaluations(
        catalog, **values, error_code='RENDER_DEFERRED', event_type='FAILED'
    )
    assert len(events) == 1
    assert events[0]['outcome'] == 'EXPECTED_WAIT'
    assert events[0]['evidence_id'] == 'recorded-event'
    assert events[0].get('deployed_sha') == SHA
    unversioned = source_failure_evaluations(
        build_catalog(''), **values, error_code='RENDER_DEFERRED', event_type='FAILED'
    )
    assert 'deployed_sha' not in unversioned[0]
    assert events[0]['gate_id'].endswith(':consumer:mount')
    assert (
        source_failure_evaluations(
            catalog, **values, error_code='RENDER_DEFERRED', event_type='RECOVERED'
        )
        == []
    )
    assert (
        source_failure_evaluations(
            catalog, **values, error_code='RuntimeError', event_type='FAILED'
        )
        == []
    )
    assert (
        source_failure_evaluations(
            catalog, **values, error_code='PROVIDER_RATE_CIRCUIT', event_type='FAILED'
        )
        == []
    )
    archive = source_failure_evaluations(
        catalog,
        **{**values, 'consumer': None},
        error_code='ARCHIVE_ROWS_INVALID',
        event_type='FAILED',
    )
    assert len(archive) == 1 and archive[0]['outcome'] == 'FAIL'
    assert '.archive_rows:' in archive[0]['gate_id']
    credential = source_failure_evaluations(
        catalog,
        **{**values, 'consumer': None},
        error_code='PROVIDER_CREDENTIAL_MISSING',
        event_type='FAILED',
    )
    assert len(credential) == 1 and '.request_contract:' in credential[0]['gate_id']
    circuit = source_failure_evaluations(
        catalog,
        **values,
        error_code='PROVIDER_RATE_CIRCUIT',
        event_type='FAILED',
        evidence={'host': 'fapi.binance.com'},
    )
    assert len(circuit) == 1 and circuit[0]['gate_id'].endswith(':fapi.binance.com')


def test_certification_protocol_never_calls_pending_approved(catalog: LawCatalog) -> None:
    # Certification-envelope fault test, not a fabricated certified market build.
    source = registry.SOURCE_REGISTRY[0].key
    values = dict(
        source_key=source,
        event_id='recorded-event',
        recorded_at='2026-09-23T10:00:00+00:00',
        check_results=json.dumps({'official_revision': 'recorded', 'components': {'raw': 'hash'}}),
        evidence={'revision': 'recorded'},
    )
    events = certification_evaluations(catalog, **values, review_state='PENDING')
    assert (
        next(event for event in events if '.review_state:' in event['gate_id'])['outcome']
        == 'EXPECTED_WAIT'
    )
    assert all(event['evaluated_at'] == values['recorded_at'] for event in events)
    approved = certification_evaluations(catalog, **values, review_state='APPROVED')
    assert (
        next(event for event in approved if '.review_state:' in event['gate_id'])['outcome']
        == 'PASS'
    )
    invalid = certification_evaluations(
        catalog, **{**values, 'check_results': '{}'}, review_state='unknown'
    )
    assert all(event['outcome'] == 'UNKNOWN' for event in invalid)


def test_historical_import_executes_real_bounded_query(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    catalog: LawCatalog,
) -> None:
    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.sources.lifecycle import SourceRuntime
    from origo.sources.storage import SourceStore

    client = make_clickhouse_client(get_clickhouse_settings())
    spec = registry.SOURCE_REGISTRY[0]
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', 'worker:catalog-query-test'
    )
    try:
        runtime.setup()
        now = datetime.now(UTC)
        # A real empty store must not turn absence into a successful gate result.
        events, cursor = import_gate_events(
            client, 'origo', catalog, now, known_since=now - timedelta(minutes=1)
        )
        assert events == [] and cursor is None
        # Exercise actual lock contention and its real persisted failure receipt.
        with source_lock(runtime.lock_root, spec.key, 'consumer_mount'):
            with pytest.raises(SourceError, match='Source lock is already held'):
                runtime.publish('mount', str(tmp_path / spec.key / 'mount'))
        later = datetime.now(UTC)
        events, cursor = import_gate_events(client, 'origo', catalog, later, known_since=now)
        assert len(events) == 1 and events[0]['gate_id'] == 'locks.contention.source'
        assert events[0]['outcome'] == 'EXPECTED_WAIT'
        assert datetime.fromisoformat(events[0]['evaluated_at']) <= later
        repeated, next_cursor = import_gate_events(
            client, 'origo', catalog, later, known_since=now, cursor=cursor
        )
        assert repeated == [] and next_cursor == cursor
        old, _ = import_gate_events(client, 'origo', catalog, later, known_since=later)
        assert old == []
        with pytest.raises(ValueError, match='database'):
            import_gate_events(client, 'origo;DROP', catalog, now, known_since=now)
    finally:
        client.disconnect()


def test_component_gate_events_retain_real_proof_identity(
    real_minutes: tuple[SourceRuntime, tuple[str, ...], dict[str, tuple[Row, ...]], int],
    catalog: LawCatalog,
) -> None:
    runtime, keys, _, _ = real_minutes
    record = runtime.build(keys[0], provisional=True)
    rows = runtime.store.execute(
        'SELECT component, content_hash, completed_at FROM origo.source_component_log '
        'WHERE build_id=%(build)s',
        {'build': record.build_id},
    )
    observations: list[ProjectionObservation] = []
    for component, digest, completed_at in rows:
        assert isinstance(completed_at, datetime)
        stamp = completed_at.replace(tzinfo=UTC).isoformat()
        observations.append(
            {
                'id': f'{runtime.spec.key}:{component}',
                'status': 'STALE',
                'observed_at': datetime.now(UTC).isoformat(),
                'evidence_at': stamp,
                'evidence_id': f'{record.build_id}:{component}:{digest}',
                'data_through': record.partition.end.isoformat(),
                'reason': 'validated_activation',
                'gate_ids': [],
                'dagit_url': None,
            }
        )
    events = projection_gate_evaluations(catalog, observations)
    assert len(events) == len(rows) * 3
    by_id = {item['evidence_id']: item for item in observations}
    for event in events:
        assert event['outcome'] == 'PASS'
        assert event.get('deployed_sha') == SHA
        assert event['evaluated_at'] == by_id[event['evidence_id']]['evidence_at']
        assert event['evaluated_at'] != by_id[event['evidence_id']]['observed_at']
        assert event['gate_id'].startswith('source.component_integrity.')
    observations[0]['reason'] = 'component_proof_missing'
    assert len(projection_gate_evaluations(catalog, observations)) == len(events) - 3


def test_publication_observation_checks_real_token_and_artifacts(
    real_minutes: tuple[SourceRuntime, tuple[str, ...], dict[str, tuple[Row, ...]], int],
    catalog: LawCatalog,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.sources.storage import SourceStore

    runtime, keys, _, _ = real_minutes
    live_spec = replace(runtime.spec, rollout_stage=RolloutStage.LIVE)
    runtime = replace(
        runtime,
        spec=live_spec,
        store=SourceStore(runtime.store.client, runtime.store.database, live_spec),
    )
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    root = tmp_path / 'publications'
    record = runtime.build(keys[0][:10])
    runtime.publish('mount', str(root / live_spec.key / 'mount'))
    manifest = root / live_spec.key / 'mount' / 'latest.json'
    published = json.loads(manifest.read_text())

    def observe() -> dict[str, ProjectionObservation]:
        return {
            item['id']: item
            for item in observe_publications(
                runtime.store.client,
                runtime.store.database,
                catalog,
                datetime.now(UTC),
                root,
                deadline=time.monotonic() + 5,
            )
        }

    node = f'{live_spec.key}:consumer:mount'
    actual = observe()[node]
    if not published['files']:
        # The genuine 2017 day precedes this consumer's 2020 export floor.
        assert actual['status'] == 'UNKNOWN'
        assert actual['reason'] == 'publication_evidence_invalid'
        return
    assert actual['status'] == 'CURRENT'
    assert actual['reason'] == 'publication_state_and_files'
    assert actual['evidence_id'] == published['version']
    # Real activation-generation change preserves the data/end, but invalidates pinned currency.
    runtime._activate(replace(record, generation=record.generation + 1), record.generation)
    changed = observe()[node]
    assert changed['data_through'] == actual['data_through']
    assert changed['status'] == 'STALE' and changed['reason'] == 'publication_state_changed'
    artifact = Path(published['files'][0]['path'])
    artifact.unlink()
    missing = observe()[node]
    assert missing['status'] == 'FAILED' and missing['reason'] == 'publication_artifact_missing'
    assert observe()[f'{live_spec.key}:consumer:huggingface']['status'] == 'UNKNOWN'
    with pytest.raises(TimeoutError, match='budget'):
        observe_publications(
            runtime.store.client,
            runtime.store.database,
            catalog,
            datetime.now(UTC),
            root,
            deadline=time.monotonic() - 1,
        )
