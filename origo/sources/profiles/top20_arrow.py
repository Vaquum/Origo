from __future__ import annotations

import hashlib
import io
import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast, runtime_checkable

import polars as pl

from ..adapters.render_book import atomic_write, object_mapping
from ..contracts import (
    Client,
    ComponentSpec,
    Partition,
    Row,
    Snapshot,
    SnapshotReader,
    SourceError,
    identifier,
)
from ..hashing import content_hash
from ..locking import source_lock
from ..publication import file_sha256_matches
from .top20 import top20_components

SOURCE = 'binance_spot_top20_v1'
ARROW_MAX_BYTES = 16 * 1024**2
ARROW_STALE_AFTER = timedelta(seconds=180)


@runtime_checkable
class _BookReader(Protocol):
    def rows(self, component: str, snapshot: Snapshot) -> list[Row]: ...

    def snapshot(self, *, canonical_only: bool = True) -> Snapshot: ...

    def component_table(self, component: str) -> str: ...

    def validate_component(
        self, component: ComponentSpec, table: str, partition: Partition, *,
        predicate: str = '1', params: object | None = None, legacy_hash: bool = False,
    ) -> tuple[int, str]: ...


def _window(snapshot: Snapshot, start: datetime, end: datetime) -> Snapshot:
    return Snapshot(snapshot.token, tuple(
        record for record in snapshot.records
        if record.partition.end > start and record.partition.start < end
    ))


def _input(snapshot: Snapshot, start: datetime, end: datetime) -> str:
    rows: list[Row] = [(SOURCE, start, end)]
    for record in sorted(_window(snapshot, start, end).records, key=lambda r: r.partition.key):
        component = 'depth20_latest' if record.partition.provisional else 'depth20'
        rows.append((record.partition.key, dict(record.component_hashes)[component]))
    return content_hash(rows, schema_version=1)


def current_input_token(client: Client, database: str, start_us: int, end_us: int) -> str:
    if end_us - start_us != 1800_000_000 or end_us % 60_000_000:
        raise ValueError('Top20 Arrow must declare its thirty-minute UTC window.')
    start, end = (datetime.fromtimestamp(value // 1_000_000, UTC) for value in (start_us, end_us))
    selected = client.execute(
        f"""SELECT partition_key,provisional,component_hashes
        FROM {identifier(database)}.source_current_partitions
        WHERE source_key=%(source)s AND partition_key >= %(prefix)s
          AND partition_end > %(start)s AND partition_start < %(end)s ORDER BY partition_key""",
        {'source': SOURCE, 'prefix': start.strftime('%Y-%m-%dT%H'),
         'start': start.replace(tzinfo=None), 'end': end.replace(tzinfo=None)},
        settings={'readonly': 1, 'max_threads': 1, 'max_execution_time': 10},
    )
    rows: list[Row] = [(SOURCE, start, end)]
    for key, provisional, encoded in selected:
        hashes = dict((pair[0], pair[1]) for pair in cast(list[list[str]], json.loads(str(encoded))))
        component = 'depth20_latest' if int(str(provisional)) else 'depth20'
        rows.append((key, hashes[component]))
    return content_hash(rows, schema_version=1)


def _frame(reader: _BookReader, snapshot: Snapshot, start: datetime, end: datetime) -> pl.DataFrame:
    rows: list[Row] = []
    components = {c.key: c for c in top20_components()}
    for record in _window(snapshot, start, end).records:
        component = 'depth20_latest' if record.partition.provisional else 'depth20'
        params = {'source_date': record.partition.start.date(), 'partition': record.partition.key,
                  'revision': record.revision, 'build': record.build_id}
        count, digest = reader.validate_component(
            components[component], reader.component_table(component), record.partition,
            predicate='source_date=%(source_date)s AND partition_key=%(partition)s '
                      'AND revision=%(revision)s AND build_id=%(build)s', params=params,
        )
        if (digest != dict(record.component_hashes)[component]
                or count > int((record.partition.end - record.partition.start).total_seconds())):
            raise SourceError('RETAINED_CONTENT_INVALID', 'Arrow input contradicts its accepted raw proof.')
        selected = Snapshot(snapshot.token, (record,))
        rows.extend((*row, not record.partition.provisional) for row in reader.rows(component, selected))
    columns = components['depth20'].columns
    overrides: dict[str, pl.DataType | type[pl.DataType]] = {
        column.name: pl.UInt64 for column in columns if column.sql_type == 'UInt64'
    }
    overrides.update({column.name: pl.String for column in columns if column.sql_type == 'String'})
    overrides['datetime'] = pl.Datetime('us', 'UTC')
    overrides.update({side: pl.List(pl.Array(pl.Float64, 2)) for side in ('bids', 'asks')})
    frame = pl.DataFrame(rows, schema=[c.name for c in columns] + ['canonical'],
                         schema_overrides=overrides, orient='row').filter(
        pl.col('exchange_time_us').is_between(int(start.timestamp()) * 1_000_000,
                                            int(end.timestamp()) * 1_000_000, closed='left')
    )
    if frame.select((pl.col('exchange_time_us') // 1_000_000).n_unique()).item() != frame.height:
        raise SourceError('BOOK_PUBLICATION_INPUT', 'Arrow requires unique actual observed seconds.')
    return frame.with_columns(
        (pl.col('exchange_time_us') * 1000).alias('ts'),
        (pl.col('exchange_time_us') // 1000).alias('source_timestamp_ms'),
        *(pl.col(side).list.eval(pl.element().arr.to_struct(fields=['price', 'qty']))
          .list.to_array(20).alias(side) for side in ('bids', 'asks')),
    ).select(
        'ts', 'source_timestamp_ms', 'exchange_time_us', 'observed_at_us', 'last_update_id',
        'collector', 'session', 'sequence', 'bids', 'asks', 'original_snapshot', 'canonical',
    ).sort('ts')


def publish_top20_arrow(
    reader: SnapshotReader, snapshot: Snapshot, destination: str, *, allow_full: bool = False,
) -> None:
    if not isinstance(reader, _BookReader) or not snapshot.records:
        raise SourceError('BOOK_PUBLICATION_INPUT', 'Arrow requires a pinned source-store snapshot.')
    root = Path(destination)
    target = Path(os.environ.get('LOCAL_ARROW_DIR', '/opt/arrow')) / SOURCE / 'latest.arrow'
    if not root.is_absolute() or not target.is_absolute():
        raise ValueError('Book publication requires absolute mounted paths.')
    end = max(record.partition.end for record in snapshot.records)
    start = end - timedelta(minutes=30)
    consumed = _input(snapshot, start, end)
    manifest = root / 'latest.json'
    locks = Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks'))
    with source_lock(locks, SOURCE, 'heavy', shared=True, wait=True):
        previous: Mapping[str, object] = (
            object_mapping(json.loads(manifest.read_bytes())) if manifest.exists() else {}
        )
        reused = (previous.get('input_token') == consumed
                  and isinstance(previous.get('sha256'), str)
                  and file_sha256_matches(target, str(previous['sha256']), ARROW_MAX_BYTES))
        payload: bytes | None = None
        if reused:
            digest = str(previous['sha256'])
            row_count = previous['rows']
            if type(row_count) is not int or not 0 <= row_count <= 1800:
                raise SourceError('BOOK_PUBLICATION_INPUT', 'Published Arrow row count is invalid.')
        else:
            frame = _frame(reader, snapshot, start, end)
            buffer = io.BytesIO()
            frame.rechunk().write_ipc(buffer, compression='uncompressed')
            payload = buffer.getvalue()
            if len(payload) > ARROW_MAX_BYTES:
                raise SourceError('BOOK_PUBLICATION_BOUND', 'Arrow exceeded its file verification bound.')
            digest, row_count = hashlib.sha256(payload).hexdigest(), frame.height
        canonical = reader.snapshot(canonical_only=True)
        original = Snapshot('', tuple(r for r in snapshot.records if not r.partition.provisional))
        if _input(canonical, start, end) != _input(original, start, end):
            raise SourceError('GENERATION_CHANGED', 'Canonical Arrow inputs changed during rendering.')
        if payload is not None:
            atomic_write(target, payload)
        document = {
            'format': 1, 'source_key': SOURCE, 'kind': 'arrow', 'depth': 20,
            'state_token': canonical.token, 'pinned_token': snapshot.token, 'input_token': consumed,
            'version': consumed, 'sha256': digest, 'rows': row_count,
            'expected_seconds': 1800, 'missing_seconds': 1800 - row_count,
            'window_start_us': int(start.timestamp()) * 1_000_000,
            'window_end_us': int(end.timestamp()) * 1_000_000,
            'active_through': end.isoformat(),
            'files': [{'path': str(target), 'sha256': digest}],
        }
        atomic_write(manifest, json.dumps(document, sort_keys=True, separators=(',', ':')).encode())
