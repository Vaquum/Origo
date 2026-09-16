"""Native bulk transport and versioned binary component validation."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from importlib import import_module
from typing import Protocol, cast

import numpy as np
from dagster import get_dagster_logger

from origo.assets.create_origo_database import get_clickhouse_settings

from .arrow_types import ArrowColumn, ArrowTable
from .contracts import Client, ComponentSpec, Row

QUERY_THREADS = 1
QUERY_MEMORY_BYTES = 2 * 1024**3
INSERT_BATCH_ROWS = 1048576


class ArrowClient(Protocol):
    def query_arrow(self, query: str) -> ArrowTable: ...
    def insert_arrow(self, table: str, arrow_table: ArrowTable) -> object: ...
    def close(self) -> None: ...


@contextmanager
def arrow_client() -> Iterator[ArrowClient]:
    settings = get_clickhouse_settings()
    factory = getattr(import_module('clickhouse_connect'), 'get_client')
    client = cast(
        ArrowClient,
        factory(
            host=settings.host,
            port=int(os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')),
            username=settings.user,
            password=settings.password,
            settings={'max_threads': QUERY_THREADS, 'max_memory_usage': QUERY_MEMORY_BYTES},
        ),
    )
    try:
        yield client
    finally:
        client.close()


class NativeColumnClient(Protocol):
    def execute(self, query: str, data: list[object], *, columnar: bool) -> int: ...
    def disconnect(self) -> None: ...


@contextmanager
def native_column_client() -> Iterator[NativeColumnClient]:
    settings = get_clickhouse_settings()
    factory = getattr(import_module('clickhouse_driver'), 'Client')
    client = cast(
        NativeColumnClient,
        factory(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            compression='lz4',
            settings={
                'use_numpy': True,
                # NumPy array_split rounds the block count down and rebalances rows.
                # Half the cap keeps even a rebalanced final block below the cap.
                'insert_block_size': INSERT_BATCH_ROWS // 2,
                'max_threads': QUERY_THREADS,
                'max_memory_usage': QUERY_MEMORY_BYTES,
            },
        ),
    )
    try:
        yield client
    finally:
        client.disconnect()


def insert_arrow(table: str, data: ArrowTable) -> None:
    columns: list[object] = []
    for name in data.column_names:
        values = np.asarray(cast(ArrowColumn, data[name]).to_numpy(zero_copy_only=False))
        if values.dtype.kind == 'M':
            # Both raw contracts use UTC DateTime64(6). Native integer ticks avoid
            # timezone conversion and the driver's per-row DatetimeIndex splitting.
            if np.datetime_data(values.dtype) != ('us', 1):
                raise ValueError('Native raw timestamps must use UTC microsecond ticks.')
            values = values.view(np.int64)
        columns.append(values)
    started = time.perf_counter()
    with native_column_client() as client:
        inserted = client.execute(
            f'INSERT INTO {table} ({", ".join(data.column_names)}) VALUES', columns, columnar=True
        )
    if inserted != data.num_rows:
        raise RuntimeError('Native bulk insert did not acknowledge every input row.')
    get_dagster_logger('origo.sources').info(
        'phase=bulk_insert table=%s rows=%s seconds=%.3f',
        table,
        inserted,
        time.perf_counter() - started,
    )


HASH_CHUNK_ROWS = 1048576


def binary_hash(
    client: Client,
    component: ComponentSpec,
    table: str,
    *,
    predicate: str = '1',
    params: Mapping[str, object] | None = None,
    schema_version: int = 1,
) -> str:
    """SHA256 tree over fixed ordered RowBinary chunks, computed inside ClickHouse.

    Only chunk digests cross the wire. Column schema, chunk sizes and row counts
    are bound into the root; negative floating-point zero is normalized.
    Retained v1 proofs continue using their original encoding.
    """
    columns = ', '.join(
        f'if({c.name}=0, toFloat64(0), {c.name})' if c.sql_type == 'Float64' else c.name
        for c in component.columns
    )
    ordering = ', '.join(component.primary_key)
    header = json.dumps([(c.name, c.sql_type) for c in component.columns], separators=(',', ':'))
    digest = hashlib.sha256(b'origo-source-rowbinary-chunks-v2\n' + header.encode() + b'\n')
    digest.update(schema_version.to_bytes(8, 'big'))
    digest.update(HASH_CHUNK_ROWS.to_bytes(8, 'big'))
    bindings = dict(params or {})
    seek = '1'
    chunk = 0
    while True:
        count, chunk_hash, last = client.execute(
            f"""SELECT count(), hex(SHA256(arrayStringConcat(arrayMap(item -> item.2,
                arraySort(item -> item.1, groupArray((tuple({ordering}), encoded))))))),
                max(tuple({ordering}))
            FROM (
                SELECT {ordering}, formatRow('RowBinary', {columns}) AS encoded
                FROM {table} WHERE ({predicate}) AND ({seek})
                ORDER BY {ordering} LIMIT {HASH_CHUNK_ROWS}
            )""",
            bindings,
            settings={'read_in_order_use_buffering': 0},
        )[0]
        size = int(str(count))
        if not size:
            break
        digest.update(chunk.to_bytes(8, 'big'))
        digest.update(size.to_bytes(8, 'big'))
        digest.update(bytes.fromhex(str(chunk_hash)))
        if size < HASH_CHUNK_ROWS:
            break
        if not isinstance(last, tuple):
            raise TypeError('Component hash cursor must be a tuple.')
        cursor = cast(tuple[object, ...], last)
        if len(cursor) != len(component.primary_key):
            raise TypeError('Component hash cursor must contain every primary key column.')
        casts: list[str] = []
        for name, value in zip(component.primary_key, cursor, strict=True):
            parameter = 'source_hash_after_' + name
            # Driver datetime parameters otherwise discard subsecond cursor precision.
            bindings[parameter] = (
                value.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(value, datetime) else value
            )
            sql_type = next(c.sql_type for c in component.columns if c.name == name)
            casts.append(f'CAST(%({parameter})s AS {sql_type})')
        # ClickHouse 25.3 does not prune our parameterized tuple comparison.
        # Expand the same lexicographic order so pages skip prior index granules.
        seek = ' OR '.join(
            '('
            + ' AND '.join(
                [
                    f'{prior}={bound}'
                    for prior, bound in zip(
                        component.primary_key[:index], casts[:index], strict=True
                    )
                ]
                + [f'{name}>{casts[index]}']
            )
            + ')'
            for index, name in enumerate(component.primary_key)
        )
        chunk += 1
    return 'v2:' + digest.hexdigest()


def validation_query(component: ComponentSpec, table: str, predicate: str) -> str:
    finite = (
        ' AND '.join(f'isFinite({c.name})' for c in component.columns if c.sql_type == 'Float64')
        or '1'
    )
    return (
        f'SELECT count(), uniqExact(tuple({", ".join(component.primary_key)})), '
        f'min({component.time_column}), max({component.time_column}), countIf(NOT ({finite})) '
        f'FROM {table} WHERE {predicate}'
    )


class BoundedClient:
    def __init__(self, client: Client) -> None:
        self.client = client

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        return self.client.execute(
            query,
            params,
            settings={
                'max_threads': QUERY_THREADS,
                'max_memory_usage': QUERY_MEMORY_BYTES,
                **(settings or {}),
            },
        )

    def disconnect(self) -> None:
        self.client.disconnect()
