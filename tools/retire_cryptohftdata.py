"""Retire vendor book rows, one synchronous monthly mutation per invocation.

Run inside the deployed Dagster container after stopping book schedules/sensors and
draining book runs/backfills. Dry-run is the default. Each --apply appends durable
before/after evidence and deletes at most one table/month. Live collectors and
provisional workers must keep running; this tool never takes their heavy fence.
Query settings bound foreground reads, not background mutation resource use.

After the database phase returns no candidate, --dagster audits the two canonical
assets. --dagster --apply wipes their history only when every source fact is a
vendor hourly fact. Native daily facts, observations or unknown facts refuse it.
Separate provisional/feed assets and raw tables are preserved.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from dagster import (
    AssetKey,
    DagsterEventType,
    DagsterInstance,
    DagsterRunStatus,
    EventRecordsFilter,
    RunsFilter,
)
from dagster._core.scheduler.instigation import InstigatorStatus

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.maintenance.event_storage import OrigoSqliteEventLogStorage
from origo.orchestration.policy import OUTSTANDING
from origo.sources.adapters.book_local import LocalBookCanonical
from origo.sources.columnar import binary_hash
from origo.sources.contracts import Client, RevisionedSourceSpec, RolloutStage, Row
from origo.sources.locking import source_lock
from origo.sources.prepare import backfill_active
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore

SOURCES = ('binance_spot_book', 'binance_perp_book')
VENDOR = "(startsWith(ifNull(revision,''),'cryptohftdata-v1:') OR startsWith(ifNull(revision,''),'cryptohftdata-v2:'))"
HOURLY = "match(ifNull(partition_key,''),'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}Z$')"
OPERATIONS = "('canonical','discovery','component','cleanup','audit','repair','rollback','quarantine','integrity','certification')"
READ_SETTINGS = {'max_threads': 1, 'max_memory_usage': 268435456, 'max_execution_time': 10}


class RetirementClient:
    def __init__(self, client: Client) -> None:
        self.client = client

    def execute(
        self, query: str, params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[Row]:
        return self.client.execute(query, params, settings={**(settings or {}), **READ_SETTINGS})

    def disconnect(self) -> None:
        self.client.disconnect()


@dataclass(frozen=True)
class Deletion:
    table: str
    predicate: str
    params: dict[str, object]
    rows: int

    @property
    def statement(self) -> str:
        return f'ALTER TABLE {self.table} DELETE WHERE {self.predicate}'


@dataclass(frozen=True)
class VendorManifest:
    builds: tuple[Row, ...]
    runs: tuple[Row, ...]
    events: tuple[Row, ...]
    scratch_databases: tuple[str, ...]


@dataclass(frozen=True)
class CanonicalHistory:
    asset: str
    counts: dict[str, int]
    partitions: tuple[str, ...]
    sha256: str


def require_quiescent(instance: DagsterInstance, specs: tuple[RevisionedSourceSpec, ...]) -> None:
    if tuple(spec.key for spec in specs) != SOURCES or any(
        not isinstance(spec.canonical, LocalBookCanonical) for spec in specs
    ):
        raise RuntimeError('Run the retirement from the deployed local-book image.')
    names = {
        f'{source}_{role}_{kind}'
        for source in SOURCES
        for kind, roles in (('schedule', ('canonical', 'audit')),
                            ('sensor', ('reconciliation', 'failure')))
        for role in roles
    }
    states = {state.instigator_name: state.status for state in instance.all_instigator_state()}
    if any(states.get(name) != InstigatorStatus.STOPPED for name in names):
        raise RuntimeError('Every book schedule and reconciliation/failure sensor must be STOPPED.')
    for run in instance.get_runs(RunsFilter(statuses=[DagsterRunStatus.NOT_STARTED, *OUTSTANDING])):
        if (run.tags.get('origo_source_key') in SOURCES
                or any(source in run.job_name for source in SOURCES)
                or any(source in '/'.join(key.path)
                       for key in (run.asset_selection or ()) for source in SOURCES)):
            raise RuntimeError(f'Book run remains nonterminal: {run.run_id}')
    if any(backfill_active(instance, spec) for spec in specs):
        raise RuntimeError('Book native backfills must be terminal before retirement.')


def require_no_mutations(store: SourceStore) -> None:
    if store.execute(
        'SELECT table,mutation_id FROM system.mutations '
        'WHERE database=%(database)s AND NOT is_done LIMIT 1',
        {'database': store.database},
    ):
        raise RuntimeError('An unfinished database mutation exists; inspect it before continuing.')


def predicates(store: SourceStore) -> tuple[tuple[str, str, str | None], ...]:
    """Keep identity-bearing logs until their dependent records have been removed."""
    builds = ' UNION DISTINCT '.join(
        f'SELECT build_id FROM {store.table(name)} '
        f'WHERE source_key=%(source)s AND {VENDOR}'
        for name in ('source_build_log', 'source_component_log', 'source_backfill_log')
    )
    failure = f'({VENDOR} OR ({HOURLY} AND operation IN {OPERATIONS}))'
    failure_events = (
        f'SELECT event_id FROM {store.table("source_failure_log")} '
        f'WHERE source_key=%(source)s AND {failure}'
    )
    runs = ' UNION DISTINCT '.join(
        f'SELECT dagster_run_id FROM {store.table(name)} '
        f'WHERE source_key=%(source)s AND {condition} AND dagster_run_id!=\'\''
        for name, condition in (
            ('source_component_log', VENDOR), ('source_backfill_log', VENDOR),
            ('source_failure_log', failure),
        )
    )
    scoped = 'source_key=%(source)s AND '
    return (
        ('source_activation_log', scoped + f'NOT provisional AND {VENDOR}', 'partition_start'),
        ('source_capacity_log', scoped + f'dagster_run_id IN ({runs})', None),
        ('source_run_log', scoped + f"(event_id IN ({failure_events}) OR (status!='LOGGED' AND dagster_run_id IN ({runs})) OR match(event_key,'^binance_(spot|perp)_book:canonical:[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}Z:cryptohftdata-v[12]:'))", 'recorded_at'),
        ('source_cleanup_log', scoped + f'build_id IN ({builds})', None),
        ('source_failure_log', scoped + failure, 'event_time'),
        ('source_observation_log', scoped + HOURLY, 'observed_at'),
        ('source_discovery_log', scoped + HOURLY, None),
        ('source_certification_log', scoped + VENDOR, 'recorded_at'),
        ('source_backfill_log', scoped + VENDOR, None),
        ('source_component_log', scoped + f'NOT provisional AND {VENDOR}', 'completed_at'),
        ('source_build_log', scoped + f'NOT provisional AND {VENDOR}', 'partition_start'),
    )


def _candidate(
    store: SourceStore, table: str, predicate: str, column: str | None,
) -> Deletion | None:
    params: dict[str, object] = {'source': store.spec.key}
    if column is None:
        rows = store.execute(f'SELECT count() FROM {table} WHERE {predicate}', params)
        count = int(str(rows[0][0]))
    else:
        rows = store.execute(
            f'SELECT toYYYYMM({column}) AS month,count() FROM {table} '
            f'WHERE {predicate} GROUP BY month ORDER BY month LIMIT 1', params,
        )
        count = int(str(rows[0][1])) if rows else 0
        if rows:
            params['month'] = int(str(rows[0][0]))
            predicate += f' AND toYYYYMM({column})=%(month)s'
    return Deletion(table, predicate, params, count) if count else None


def next_deletion(stores: tuple[SourceStore, ...]) -> Deletion | None:
    if any(store.spec.key not in SOURCES or store.spec.names.prefix != store.spec.key
           or tuple(component.key for component in store.spec.components
                    if not component.provisional) != BOOK_COMPONENT_KEYS for store in stores):
        raise RuntimeError('Retirement accepts only the two declared book table namespaces.')
    # Remove all vendor visibility before deleting any proof or physical output.
    for index in range(11):
        for store in stores:
            name, predicate, column = predicates(store)[index]
            candidate = _candidate(store, store.table(name), predicate, column)
            if candidate is not None:
                return candidate
    for store in stores:
        for component in store.spec.components:
            if not component.provisional:
                table = store.component_table(component.key)
                months = store.execute(
                    'SELECT DISTINCT partition FROM system.parts WHERE active '
                    'AND database=%(database)s AND table=%(table)s ORDER BY partition',
                    {'database': store.database, 'table': table.split('.')[1]},
                )
                for (month,) in months:
                    if not re.fullmatch(r'[0-9]{6}', str(month)):
                        raise RuntimeError(f'Unexpected physical partition in {table}.')
                    predicate = VENDOR + ' AND toYYYYMM(source_date)=%(month)s'
                    params: dict[str, object] = {'month': int(str(month))}
                    count = store.execute(
                        f'SELECT count() FROM {table} WHERE {predicate}', params,
                    )[0][0]
                    if count:
                        return Deletion(table, predicate, params, int(str(count)))
    return None


def vendor_manifest(stores: tuple[SourceStore, ...]) -> VendorManifest:
    """Retain provider ownership before its identity-bearing rows can be erased."""
    builds: list[Row] = []
    runs: list[Row] = []
    events: list[Row] = []
    for store in stores:
        params = {'source': store.spec.key}
        builds.extend(store.execute(' UNION DISTINCT '.join(
            f'SELECT source_key,partition_key,ifNull(revision,\'\'),build_id '
            f'FROM {store.table(name)} WHERE source_key=%(source)s AND {VENDOR} '
            'AND build_id IS NOT NULL'
            for name in ('source_activation_log', 'source_build_log', 'source_component_log',
                         'source_backfill_log', 'source_failure_log', 'source_certification_log')
        ), params))
        selected = {name: predicate for name, predicate, _column in predicates(store)}
        runs.extend(store.execute(' UNION DISTINCT '.join(
            f'SELECT source_key,dagster_run_id FROM {store.table(name)} '
            f'WHERE {selected[name]} AND dagster_run_id!=\'\''
            for name in ('source_activation_log', 'source_component_log', 'source_backfill_log',
                         'source_failure_log', 'source_run_log', 'source_cleanup_log',
                         'source_certification_log')
        ), params))
        events.extend(store.execute(' UNION DISTINCT '.join(
            f'SELECT source_key,event_id FROM {store.table(name)} WHERE {selected[name]}'
            for name in ('source_failure_log', 'source_run_log')
        ), params))
    names: set[str] = set()
    for row in builds:
        if len(row) != 4 or not isinstance(row[3], UUID):
            raise TypeError('Vendor scratch ownership requires a concrete recorded build UUID.')
        names.add('source_build_' + row[3].hex)
    scratch = stores[0].execute(
        "SELECT name FROM system.databases WHERE match(name,'^source_build_[0-9a-f]{32}$') "
        'ORDER BY name',
    ) if names else []
    return VendorManifest(tuple(builds), tuple(runs), tuple(events),
                          tuple(str(row[0]) for row in scratch if str(row[0]) in names))


def require_no_vendor_scratch(manifest: VendorManifest) -> None:
    if manifest.scratch_databases:
        raise RuntimeError('Vendor-owned scratch databases require exact controlled cleanup: '
                           + ', '.join(manifest.scratch_databases))


def protected_fingerprints(stores: tuple[SourceStore, ...]) -> tuple[dict[str, object], ...]:
    """Pin immutable accepted native generations; concurrent newer minutes may append."""
    pins: list[dict[str, object]] = []
    for store in stores:
        rows = store.execute(
            f'SELECT partition_key,revision,build_id,provisional,partition_start,partition_end FROM '
            f'{store.table("source_activation_log")} WHERE source_key=%(source)s '
            f'AND provisional=1 AND NOT {VENDOR} '
            'ORDER BY partition_start DESC,generation DESC LIMIT 1',
            {'source': store.spec.key},
        )
        if not rows:
            raise RuntimeError(f'No protected accepted generation for {store.spec.key}.')
        partition, revision, build, provisional, start, end = rows[0]
        if (provisional != 1 or not isinstance(start, datetime) or not isinstance(end, datetime)
                or not timedelta(0) < end - start <= timedelta(minutes=1)):
            raise RuntimeError('Protection must pin at most one accepted provisional minute.')
        params = {'partition': str(partition), 'revision': str(revision), 'build': build,
                  'date': str(partition)[:10]}
        if store.spec.key in SOURCES and not str(revision).startswith('local-book-v1:'):
            raise RuntimeError('Book protection must pin an independent local generation.')
        for component in store.spec.components:
            if component.provisional == bool(provisional) and component.activation_group is None:
                table = store.component_table(component.key)
                predicate = ('source_date=toDate(%(date)s) AND partition_key=%(partition)s '
                             'AND revision=%(revision)s AND build_id=%(build)s')
                count = store.execute(f'SELECT count() FROM {table} WHERE {predicate}', params)[0][0]
                digest = binary_hash(store.client, component, table, predicate=predicate,
                                     params=params, schema_version=store.spec.schema_version)
                pins.append({'source': store.spec.key, 'table': table, 'predicate': predicate,
                             'params': params, 'rows': count, 'sha256': digest,
                             'component': component.key})
    return tuple(pins)


def verify_protected(stores: tuple[SourceStore, ...], pins: tuple[dict[str, object], ...]) -> None:
    for pin in pins:
        store = next(item for item in stores if item.spec.key == pin['source'])
        component = next(item for item in store.spec.components if item.key == pin['component'])
        params = pin['params']
        if not isinstance(params, dict):
            raise TypeError('Protected generation bindings must be a mapping.')
        predicate = str(pin['predicate'])
        count = store.execute(f'SELECT count() FROM {pin["table"]} WHERE {predicate}', params)[0][0]
        digest = binary_hash(store.client, component, str(pin['table']), predicate=predicate,
                             params=params, schema_version=store.spec.schema_version)
        if count != pin['rows'] or digest != pin['sha256']:
            raise RuntimeError(f'Protected generation changed: {pin["table"]}')


def dagster_inventory(instance: DagsterInstance) -> tuple[CanonicalHistory, ...]:
    """Prove complete ownership before the supported whole-canonical-asset wipe."""
    storage = instance.event_log_storage
    if not isinstance(storage, OrigoSqliteEventLogStorage):
        raise RuntimeError('Dagster retirement requires the deployed Origo event storage.')
    inventory: list[CanonicalHistory] = []
    for source in SOURCES:
        asset = AssetKey(f'build_{source}_canonical_revision_origo')
        with storage.index_connection() as database:
            facts = database.exec_driver_sql(
                'SELECT dagster_event_type,count(*),sum(CASE WHEN partition IS NULL '
                'THEN 1 ELSE 0 END) FROM event_logs WHERE asset_key=? GROUP BY dagster_event_type',
                (asset.to_string(),),
            ).fetchall()
        counts = {str(kind): int(count) for kind, count, _nulls in facts}
        # ASSET_WIPED is the supported API's own receipt, permitting safe reruns.
        markers = {'FRESHNESS_STATE_CHANGE', 'ASSET_WIPED'}
        if (set(counts) - markers - {'ASSET_MATERIALIZATION', 'ASSET_MATERIALIZATION_PLANNED'}
                or any(int(count) != int(nulls) for kind, count, nulls in facts
                       if kind in markers)):
            raise RuntimeError(f'Canonical asset has observations or unknown facts: {source}')
        digest = hashlib.sha256()
        partitions: set[str] = set()
        for kind in (DagsterEventType.ASSET_MATERIALIZATION,
                     DagsterEventType.ASSET_MATERIALIZATION_PLANNED):
            before = None
            scanned = 0
            while True:
                page = storage.get_event_records(
                    EventRecordsFilter(kind, asset_key=asset, before_cursor=before), limit=500,
                )
                for item in page:
                    event = item.event_log_entry.dagster_event
                    partition = event.partition if event is not None else None
                    if (not isinstance(partition, str)
                            or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}Z', partition)):
                        raise RuntimeError(f'Canonical asset contains native/daily/unknown facts: {source}')
                    datetime.strptime(partition, '%Y-%m-%dT%HZ')
                    revisions: list[str] = []
                    if kind == DagsterEventType.ASSET_MATERIALIZATION:
                        materialization = item.event_log_entry.asset_materialization
                        if materialization is None:
                            raise RuntimeError('Canonical materialization payload is absent.')
                        for key in ('source_state', 'source_result'):
                            if key in materialization.metadata:
                                state = getattr(materialization.metadata[key], 'value', None)
                                revision = state.get('revision') if isinstance(state, dict) else None
                                if not isinstance(revision, str) or not revision.startswith(
                                    ('cryptohftdata-v1:', 'cryptohftdata-v2:')
                                ):
                                    raise RuntimeError(f'Canonical provenance is native or unknown: {source}')
                                revisions.append(revision)
                        if not revisions:
                            raise RuntimeError(f'Canonical provenance is absent: {source}')
                        partitions.add(partition)
                    digest.update(json.dumps([item.storage_id, kind.value, partition, revisions]).encode())
                scanned += len(page)
                if len(page) < 500:
                    break
                before = page[-1].storage_id
            if scanned != counts.get(kind.value, 0):
                raise RuntimeError(f'Canonical history changed or could not be fully decoded: {source}')
        if partitions != instance.get_materialized_partitions(asset):
            raise RuntimeError(f'Canonical materialization inventory changed: {source}')
        inventory.append(CanonicalHistory(asset.to_user_string(), counts,
                                          tuple(sorted(partitions)), digest.hexdigest()))
    return tuple(inventory)


def wipe_vendor_assets(
    instance: DagsterInstance, expected: tuple[CanonicalHistory, ...],
) -> tuple[CanonicalHistory, ...]:
    current = dagster_inventory(instance)
    if current != expected:
        raise RuntimeError('Canonical ownership changed after the retirement inventory.')
    facts = {'ASSET_MATERIALIZATION', 'ASSET_MATERIALIZATION_PLANNED'}
    targets = [AssetKey(item.asset) for item in current if facts.intersection(item.counts)]
    instance.wipe_assets(targets)
    remaining = dagster_inventory(instance)
    if any(facts.intersection(item.counts) for item in remaining):
        raise RuntimeError('Canonical vendor facts remain after the supported wipe.')
    return remaining


def record(path: Path, value: dict[str, object]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, 'a') as stream:
        if os.fstat(stream.fileno()).st_mode & 0o077:
            raise RuntimeError('Retirement evidence must have private permissions.')
        stream.write(json.dumps(value, default=str, sort_keys=True) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--dagster', action='store_true')
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    specs = tuple(next(spec for spec in SOURCE_REGISTRY if spec.key == key) for key in SOURCES)
    settings = get_clickhouse_settings()
    client = RetirementClient(make_clickhouse_client(settings))
    root = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    if not root.is_absolute() or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', settings.database):
        raise ValueError('Retirement requires the absolute shared lock mount and database identifier.')
    try:
        with DagsterInstance.get() as instance, source_lock(root, 'cryptohftdata_retirement', 'mutation'):
            require_quiescent(instance, specs)
            book_stores = tuple(SourceStore(client, settings.database, spec) for spec in specs)
            protected = tuple(SourceStore(client, settings.database, spec) for spec in SOURCE_REGISTRY
                              if spec.rollout_stage != RolloutStage.DORMANT)
            require_no_mutations(book_stores[0])
            deletion = next_deletion(book_stores)
            if args.dagster and deletion is not None:
                raise RuntimeError('Complete vendor database retirement before --dagster.')
            canonical = dagster_inventory(instance) if args.dagster else ()
            pins = protected_fingerprints(protected)
            ownership = vendor_manifest(book_stores)
            identity = str(uuid4())
            report: dict[str, object] = {
                'id': identity, 'at': datetime.now(UTC).isoformat(), 'apply': args.apply,
                'statement': deletion.statement if deletion else None,
                'params': deletion.params if deletion else {},
                'selected_rows': deletion.rows if deletion else 0, 'protected': pins,
                'vendor_ownership': asdict(ownership),
                'operation': 'dagster' if args.dagster else 'rows',
                'canonical_assets': tuple(asdict(item) for item in canonical),
            }
            record(args.evidence, {**report, 'phase': 'before'})
            print(json.dumps(report, default=str, sort_keys=True))
            require_no_vendor_scratch(ownership)
            if args.dagster:
                if args.apply:
                    try:
                        require_quiescent(instance, specs)
                        require_no_mutations(book_stores[0])
                        if next_deletion(book_stores) is not None:
                            raise RuntimeError('Vendor database retirement changed before the wipe.')
                        after = wipe_vendor_assets(instance, canonical)
                        verify_protected(protected, pins)
                    except Exception as error:
                        record(args.evidence, {'id': identity, 'phase': 'failed',
                                               'error_type': type(error).__name__,
                                               'retirement_or_protection_unverified': True})
                        raise
                    record(args.evidence, {'id': identity, 'phase': 'after',
                                           'canonical_assets': tuple(asdict(item) for item in after),
                                           'protected': pins,
                                           'protected_unchanged': True})
                    print(json.dumps({'id': identity,
                                      'canonical_assets': tuple(asdict(item) for item in after),
                                      'protected_unchanged': True}, default=str))
                return
            if args.apply and deletion is not None:
                try:
                    require_quiescent(instance, specs)
                    require_no_mutations(book_stores[0])
                    book_stores[0].execute(deletion.statement, deletion.params,
                                           settings={'mutations_sync': 2})
                    require_no_mutations(book_stores[0])
                    verify_protected(protected, pins)
                    remaining = book_stores[0].execute(
                        f'SELECT count() FROM {deletion.table} WHERE {deletion.predicate}', deletion.params,
                    )[0][0]
                    if remaining:
                        raise RuntimeError('The selected vendor rows remain after mutation.')
                except Exception as error:
                    record(args.evidence, {'id': identity, 'phase': 'failed',
                                           'error_type': type(error).__name__,
                                           'mutation_or_protection_unverified': True})
                    raise
                record(args.evidence, {'id': identity, 'phase': 'after', 'protected_unchanged': True,
                                       'selected_rows_remaining': 0, 'protected': pins})
                print(json.dumps({'id': identity, 'deleted_rows': deletion.rows,
                                  'protected_unchanged': True, 'next_invocation_required': True}))
    finally:
        client.disconnect()


if __name__ == '__main__':
    main()
