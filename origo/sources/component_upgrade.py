"""Attach newly enabled projections to an already accepted retained build."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from .cleanup import preserve_primary_failure
from .contracts import (
    BuildContext,
    ComponentSpec,
    Partition,
    Revision,
    SourceError,
    StateRecord,
    failure_code,
    failure_message,
)
from .failures import FailureLog
from .storage import SourceStore


class ComponentUpgradeError(SourceError):
    """An additive projection failed while its previously accepted products remain active."""


def _bindings(store: SourceStore, record: StateRecord) -> dict[str, object]:
    return {
        'source': store.spec.key,
        'source_date': record.partition.start.date(),
        'partition': record.partition.key,
        'revision': record.revision,
        'build': record.build_id,
    }


_PREDICATE = (
    'source_date=%(source_date)s AND partition_key=%(partition)s '
    'AND revision=%(revision)s AND build_id=%(build)s'
)
_PROOF_PREDICATE = (
    'source_key=%(source)s AND partition_key=%(partition)s '
    'AND revision=%(revision)s AND build_id=%(build)s AND component=%(component)s'
)


def _retained_proof(
    store: SourceStore, record: StateRecord, component: ComponentSpec
) -> tuple[int, str] | None:
    """Reuse completed additions; remove only interrupted, never-activated writes."""
    params = {**_bindings(store, record), 'component': component.key}
    proofs = store.execute(
        f'SELECT row_count, content_hash FROM {store.table("source_component_log")} '
        f'WHERE {_PROOF_PREDICATE}',
        params,
    )
    actual: tuple[int, str] | None = None
    if len(proofs) == 1:
        try:
            actual = store.validate_component(
                component,
                store.component_table(component.key),
                record.partition,
                predicate=_PREDICATE,
                params=params,
                legacy_hash=not str(proofs[0][1]).startswith('v2:'),
            )
        except SourceError as error:
            if error.code != 'COMPONENT_CONTENT_INVALID':
                raise
            actual = None
        if actual is not None and proofs == [actual]:
            return actual
    activated = store.execute(
        f'SELECT count() FROM {store.table("source_activation_log")} '
        'WHERE source_key=%(source)s AND partition_key=%(partition)s '
        'AND revision=%(revision)s AND build_id=%(build)s '
        "AND arrayExists(item -> item[1]=%(component)s, "
        "JSONExtract(component_hashes, 'Array(Array(String))'))",
        params,
    )
    if activated != [(0,)]:
        raise SourceError(
            'RETAINED_CONTENT_INVALID',
            f'Previously activated {component.key} cannot be overwritten by an additive upgrade.',
        )
    if store.execute(
        f'SELECT 1 FROM {store.component_table(component.key)} WHERE {_PREDICATE} LIMIT 1',
        params,
    ):
        store.execute(
            f'ALTER TABLE {store.component_table(component.key)} DELETE WHERE {_PREDICATE}',
            params,
            settings={'mutations_sync': 2},
        )
    if proofs:
        store.execute(
            f'ALTER TABLE {store.table("source_component_log")} DELETE WHERE {_PROOF_PREDICATE}',
            params,
            settings={'mutations_sync': 2},
        )
    return None


def _attach(
    store: SourceStore,
    record: StateRecord,
    run_id: str,
    progress: Callable[[Partition, UUID, str], None],
) -> StateRecord:
    missing = store.missing_components(record)
    if not missing:
        return record
    params = _bindings(store, record)
    raw = next(
        component
        for component in store.accepted_components(record)
        if component.key == ('raw_latest' if record.partition.provisional else 'raw')
    )
    raw_proofs = store.execute(
        'SELECT row_count, source_content_hash, evidence_json '
        f'FROM {store.table("source_component_log")} WHERE {_PROOF_PREDICATE}',
        {**params, 'component': raw.key},
    )
    if len(raw_proofs) != 1:
        raise SourceError('RETAINED_CONTENT_INVALID', 'Retained raw evidence is not unique.')
    raw_count, source_hash, evidence = raw_proofs[0]
    if isinstance(raw_count, bool) or not isinstance(raw_count, int):
        raise TypeError('Retained raw evidence must have an integer row count.')
    revision = Revision(record.revision, str(source_hash), str(evidence), raw_count, lambda: iter(()))
    database = 'source_build_' + record.build_id.hex
    context = BuildContext(store.client, database, record.partition, revision, record.build_id)
    hashes = list(record.component_hashes)
    store.execute(f'DROP DATABASE IF EXISTS {database} SYNC')
    store.execute(f'CREATE DATABASE {database}')
    with preserve_primary_failure(
        'component upgrade staging', lambda: store.execute(f'DROP DATABASE {database} SYNC')
    ):
        store.execute(
            f'CREATE VIEW {context.table(raw.key)} AS SELECT '
            + ', '.join(column.name for column in raw.columns)
            + f' FROM {store.component_table(raw.key)} WHERE {_PREDICATE}',
            params,
        )
        for component in missing:
            retained = _retained_proof(store, record, component)
            if retained is None:
                columns = ', '.join(
                    f'{column.name} {column.sql_type}' for column in component.columns
                )
                store.execute(
                    f'CREATE TABLE {context.table(component.key)} ({columns}) '
                    f'ENGINE=MergeTree ORDER BY ({", ".join(component.primary_key)})'
                )
                component.build(context)
                count, digest = store.validate_component(
                    component, context.table(component.key), record.partition
                )
                if raw_count and count == 0:
                    raise SourceError(
                        'COMPONENT_CONTENT_INVALID',
                        'An eligible required projection is empty for a non-empty retained revision.',
                    )
                store.execute(
                    f'INSERT INTO {store.component_table(component.key)} '
                    'SELECT %(source_date)s, %(partition)s, %(revision)s, %(build)s, * '
                    f'FROM {context.table(component.key)}',
                    params,
                )
                retained = store.validate_component(
                    component,
                    store.component_table(component.key),
                    record.partition,
                    predicate=_PREDICATE,
                    params=params,
                )
                if retained != (count, digest):
                    raise SourceError(
                        'COMPONENT_CONTENT_INVALID', 'Retained projection differs from its staged proof.'
                    )
                values = [(
                    store.spec.key, record.partition.key, int(record.partition.provisional),
                    record.revision, record.build_id, component.key, count, digest,
                    revision.content_hash, revision.evidence_json,
                    store.component_table(component.key), run_id, datetime.now(UTC),
                )]
                try:
                    store.execute(
                        f'INSERT INTO {store.table("source_component_log")} VALUES', values
                    )
                except Exception as error:
                    found = store.execute(
                        f'SELECT row_count, content_hash FROM {store.table("source_component_log")} '
                        f'WHERE {_PROOF_PREDICATE}',
                        {**params, 'component': component.key},
                    )
                    if found != [retained]:
                        raise RuntimeError('Projection evidence insert was not uniquely committed.') from error
            found = store.execute(
                f'SELECT row_count, content_hash FROM {store.table("source_component_log")} '
                f'WHERE {_PROOF_PREDICATE}',
                {**params, 'component': component.key},
            )
            if found != [retained]:
                raise SourceError('RETAINED_CONTENT_INVALID', 'Projection evidence is not unique.')
            hashes.append((component.key, retained[1]))
            progress(record.partition, record.build_id, component.key)
        upgraded = StateRecord(
            record.partition, record.generation + 1, record.revision, record.build_id, tuple(hashes)
        )
        store.accepted_components(upgraded)
        if store.generation(record.partition) != record.generation:
            raise SourceError('GENERATION_CHANGED', 'Activation advanced during component upgrade.')
        # The caller checked old contents under this fence; each new retained addition
        # and its unique evidence was checked above. Do not rehash the raw day again.
        store.insert_activation(upgraded, run_id)
        return upgraded


def attach_components(
    store: SourceStore,
    record: StateRecord,
    *,
    failure_log: FailureLog,
    run_id: str,
    progress: Callable[[Partition, UUID, str], None],
) -> StateRecord:
    """Caller owns the partition/maintenance fences and has validated existing products."""
    try:
        upgraded = _attach(store, record, run_id, progress)
        failure_log.recover(operation='component_upgrade', partition=record.partition.key)
        return upgraded
    except Exception as error:
        failure_log.record(
            operation='component_upgrade', scope='NONE',
            partition=record.partition.key, revision=record.revision, build_id=record.build_id,
            error_code=failure_code(error), message=failure_message(error),
        )
        raise ComponentUpgradeError(
            'COMPONENT_UPGRADE_FAILED',
            f'Additive projection upgrade failed: {failure_code(error)}.',
        ) from error
