"""Versioned lossless JSON payloads; indexed identity/status columns remain native SQL."""

import hashlib
import sqlite3
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

from sqlalchemy import Connection

_MAGIC = b'ORIGO\x00JSON1'
_MAX_BYTES = 16 * 1024**2


def encode_json(value: str) -> bytes:
    raw = value.encode('utf-8')
    if len(raw) > _MAX_BYTES:
        raise ValueError('Dagster JSON payload exceeds the supported compression bound.')
    return (
        _MAGIC + len(raw).to_bytes(4, 'big') + hashlib.sha256(raw).digest() + zlib.compress(raw, 6)
    )


def decode_json(value: bytes) -> str:
    if not value.startswith(_MAGIC) or len(value) < len(_MAGIC) + 36:
        raise ValueError('Unsupported compressed Dagster JSON format.')
    header = len(_MAGIC)
    size = int.from_bytes(value[header : header + 4], 'big')
    if size > _MAX_BYTES:
        raise ValueError('Compressed Dagster JSON exceeds its read bound.')
    decoder = zlib.decompressobj()
    raw = decoder.decompress(value[header + 36 :], size + 1)
    if (
        len(raw) != size
        or not decoder.eof
        or decoder.unused_data
        or hashlib.sha256(raw).digest() != value[header + 4 : header + 36]
    ):
        raise ValueError('Compressed Dagster JSON checksum or length mismatch.')
    return raw.decode('utf-8')


def decoded_values(
    cursor: sqlite3.Cursor, values: tuple[object, ...] | sqlite3.Row
) -> tuple[object, ...]:
    return tuple(
        decode_json(value) if isinstance(value, bytes) and value.startswith(_MAGIC) else value
        for value in values
    )


def decoded_row(cursor: sqlite3.Cursor, values: tuple[object, ...] | sqlite3.Row) -> sqlite3.Row:
    return sqlite3.Row(cursor, decoded_values(cursor, values))


@contextmanager
def json_rows(database: Connection) -> Iterator[None]:
    driver = database.connection.driver_connection
    if not isinstance(driver, sqlite3.Connection):
        raise TypeError('Compressed provenance requires the pinned SQLite driver.')
    previous = driver.row_factory
    driver.row_factory = decoded_values
    try:
        yield
    finally:
        driver.row_factory = previous


def compress_run_json(
    database: Connection, table: Literal['runs', 'event_logs'], run_id: str
) -> int:
    column = 'run_body' if table == 'runs' else 'event'
    rows = database.exec_driver_sql(
        f"SELECT id,{column} FROM {table} WHERE run_id=? AND typeof({column})='text' ORDER BY id LIMIT 500",
        (run_id,),
    ).fetchall()
    for row in rows:
        raw = row[1]
        if not isinstance(raw, str):
            raise TypeError('Expected a serialized Dagster JSON string.')
        database.exec_driver_sql(
            f'UPDATE {table} SET {column}=? WHERE id=? AND {column}=?',
            (encode_json(raw), row[0], raw),
        )
    return len(rows)
