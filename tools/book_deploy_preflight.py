"""Refuse daily book deployment while retired hourly authority remains. Read only."""

from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

from dagster import AssetKey, DagsterInstance

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.maintenance.event_storage import OrigoSqliteEventLogStorage
from origo.maintenance.sqlite import connection
from origo.sources.contracts import Client, identifier
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore

_specification = importlib.util.spec_from_file_location(
    'book_retirement_preflight', Path(__file__).with_name('retire_cryptohftdata.py'),
)
if _specification is None or _specification.loader is None:
    raise RuntimeError('Cannot load the retirement predicates for deployment preflight.')
retirement = importlib.util.module_from_spec(_specification)
sys.modules[_specification.name] = retirement
_specification.loader.exec_module(retirement)


def require_retired_history(instance: DagsterInstance, client: Client, database: str) -> None:
    """Every retired metadata family must be absent before bootstrap can run."""
    database = identifier(database)
    settings = {**retirement.READ_SETTINGS, 'readonly': 1}
    tables = {
        str(row[0]) for row in client.execute(
            'SELECT name FROM system.tables WHERE database=%(database)s '
            "AND startsWith(name,'source_')",
            {'database': database}, settings=settings,
        )
    }
    revision_tables = {
        'source_activation_log', 'source_build_log', 'source_component_log',
        'source_backfill_log', 'source_certification_log', 'source_failure_log',
    }
    canonical_tables = revision_tables - {'source_failure_log'}
    for source in retirement.SOURCES:
        spec = next(spec for spec in SOURCE_REGISTRY if spec.key == source)
        store = SourceStore(client, database, spec)
        for name, predicate, _column in retirement.predicates(store):
            if name not in tables:
                continue
            if name in revision_tables:
                # Prefix ownership is sufficient even for malformed provisional metadata.
                predicate = f'({predicate}) OR (source_key=%(source)s AND {retirement.VENDOR})'
            if name in canonical_tables:
                canonical = 'NOT provisional AND ' if name in {
                    'source_activation_log', 'source_build_log', 'source_component_log',
                } else ''
                predicate += f' OR (source_key=%(source)s AND {canonical}{retirement.HOURLY})'
            if client.execute(
                f'SELECT 1 FROM {store.table(name)} WHERE {predicate} LIMIT 1',
                {'source': source}, settings=settings,
            ):
                raise RuntimeError(f'Retired book metadata remains: {source}/{name}')
    storage = instance.event_log_storage
    if not isinstance(storage, OrigoSqliteEventLogStorage):
        raise RuntimeError('Book deployment preflight requires the deployed Origo event storage.')
    path = Path(storage.path_for_shard('index'))
    if path.exists():
        with connection(path, time.monotonic() + 10, 1) as index:
            for source in retirement.SOURCES:
                asset = AssetKey(f'build_{source}_canonical_revision_origo')
                if index.execute(
                    'SELECT 1 FROM event_logs WHERE asset_key=? AND partition GLOB '
                    "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]Z' LIMIT 1",
                    (asset.to_string(),),
                ).fetchone() is not None:
                    raise RuntimeError(f'Retired hourly canonical Dagster facts remain: {source}')


def main() -> None:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        with DagsterInstance.get() as instance:
            require_retired_history(instance, client, settings.database)
    finally:
        client.disconnect()
    print('Retired hourly book metadata is absent; daily bootstrap may proceed.', flush=True)


if __name__ == '__main__':
    main()
