"""Native bulk transport and versioned binary component validation."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from importlib import import_module
from typing import Protocol, cast

from origo.assets.create_origo_database import get_clickhouse_settings

from .arrow_types import ArrowTable
from .contracts import Client, ComponentSpec, Row

QUERY_THREADS = 1
QUERY_MEMORY_BYTES = 2 * 1024**3


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


def insert_arrow(table: str, data: ArrowTable) -> None:
    with arrow_client() as client:
        client.insert_arrow(table, data.combine_chunks())


HASH_CHUNK_ROWS = 65536


def binary_hash(
    client: Client,
    component: ComponentSpec,
    table: str,
    *,
    predicate: str = '1',
    params: Mapping[str, object] | None = None,
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
    query = f"""SELECT chunk, count(), hex(SHA256(arrayStringConcat(
        arrayMap(item -> item.2, arraySort(item -> item.1, groupArray((ordinal, encoded)))))))
        FROM (
            SELECT row_number() OVER (ORDER BY {', '.join(component.primary_key)}) - 1 AS ordinal,
                intDiv(ordinal, {HASH_CHUNK_ROWS}) AS chunk,
                formatRow('RowBinary', {columns}) AS encoded
            FROM {table} WHERE {predicate}
        ) GROUP BY chunk ORDER BY chunk"""
    header = json.dumps([(c.name, c.sql_type) for c in component.columns], separators=(',', ':'))
    digest = hashlib.sha256(b'origo-source-rowbinary-chunks-v2\n' + header.encode() + b'\n')
    digest.update(HASH_CHUNK_ROWS.to_bytes(8, 'big'))
    for chunk, count, chunk_hash in client.execute(query, params):
        digest.update(int(str(chunk)).to_bytes(8, 'big'))
        digest.update(int(str(count)).to_bytes(8, 'big'))
        digest.update(bytes.fromhex(str(chunk_hash)))
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
