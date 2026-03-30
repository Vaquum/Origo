from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from uuid import uuid4

import polars as pl
from clickhouse_driver import Client as ClickhouseClient

from origo.events.backfill_state import (
    PartitionSourceProof,
    SourceIdentityMaterial,
    build_partition_source_proof,
    build_partition_source_proof_from_precomputed,
)
from origo.events.envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.quarantine import NoopStreamQuarantineRegistry
from origo.events.runtime_audit import get_canonical_runtime_audit_log
from origo.events.storage import CANONICAL_EVENT_LOG_READ_TABLE
from origo.events.writer import (
    CanonicalEventWriteInput,
    CanonicalEventWriter,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)
from origo_control_plane.backfill.runtime_contract import FastInsertMode

_SOURCE_ID = 'okx'
_STREAM_ID = 'okx_spot_trades'
_INSTRUMENT_ID = 'BTC-USDT'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_WRITE_EVENTS_BATCH_SIZE = 10_000
_FAST_INSERT_MODE_DEFAULT = 'writer'
_FAST_INSERT_MODE_ASSUME_NEW_PARTITION = 'assume_new_partition'
_INSERT_CHUNK_SIZE = 100_000
_CANONICAL_EVENT_NAMESPACE_HEX = 'f1d1ef1795ce4f9fbd81f9958cdf8ee5'
_EXPECTED_CSV_HEADER = (
    'instrument_name',
    'trade_id',
    'side',
    'price',
    'size',
    'created_time',
)


def _parse_int(value: str, *, label: str) -> int:
    candidate = value.strip()
    if candidate == '':
        raise RuntimeError(f'{label} must be non-empty integer')
    try:
        return int(candidate)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be integer, got {value!r}') from exc


def _parse_decimal_text(value: str, *, label: str) -> str:
    candidate = value.strip()
    if candidate == '':
        raise RuntimeError(f'{label} must be non-empty decimal text')
    try:
        parsed = Decimal(candidate)
    except InvalidOperation as exc:
        raise RuntimeError(f'{label} must be valid decimal text, got {value!r}') from exc
    if parsed <= 0:
        raise RuntimeError(f'{label} must be positive, got {value!r}')
    return candidate


def _parse_timestamp_ms(value: str, *, label: str) -> tuple[int, datetime]:
    timestamp = _parse_int(value, label=label)
    if len(value.strip()) != 13:
        raise RuntimeError(f'{label} must be 13-digit epoch milliseconds, got {value!r}')
    event_time_utc = datetime.fromtimestamp(timestamp / 1000.0, tz=UTC)
    return timestamp, event_time_utc


def _compute_quote_quantity_text(*, price_text: str, size_text: str) -> str:
    price = Decimal(price_text)
    size = Decimal(size_text)
    return format((price * size).quantize(Decimal('0.00000001')), '.8f')


@dataclass(frozen=True)
class OKXSpotTradeEvent:
    instrument_name: str
    trade_id: int
    side: str
    price_text: str
    size_text: str
    quote_quantity_text: str
    timestamp: int
    event_time_utc: datetime

    @property
    def partition_id(self) -> str:
        return self.event_time_utc.strftime('%Y-%m-%d')

    def to_payload(self) -> dict[str, object]:
        return {
            'instrument_name': self.instrument_name,
            'trade_id': self.trade_id,
            'side': self.side,
            'price': self.price_text,
            'size': self.size_text,
            'timestamp': self.timestamp,
        }

    def to_integrity_tuple(self) -> tuple[str, int, str, float, float, float, int, datetime]:
        return (
            self.instrument_name,
            self.trade_id,
            self.side,
            float(Decimal(self.price_text)),
            float(Decimal(self.size_text)),
            float(Decimal(self.quote_quantity_text)),
            self.timestamp,
            self.event_time_utc,
        )

    def duplicate_identity_tuple(self) -> tuple[int, str, str, str, int]:
        return (
            self.trade_id,
            self.side,
            self.price_text,
            self.size_text,
            self.timestamp,
        )


@dataclass(frozen=True)
class OKXDeduplicatedTradeEvents:
    events: list[OKXSpotTradeEvent]
    raw_row_count: int
    exact_duplicate_row_count: int


@dataclass(frozen=True)
class OKXDeduplicatedTradeFrame:
    frame: pl.DataFrame
    raw_row_count: int
    exact_duplicate_row_count: int


def parse_okx_spot_trade_csv_frame(csv_content: bytes) -> pl.DataFrame:
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    header = next(reader, None)
    if header is None:
        raise RuntimeError('OKX CSV payload is empty')
    if tuple(header) != _EXPECTED_CSV_HEADER:
        raise RuntimeError(
            f'OKX CSV header mismatch: expected={_EXPECTED_CSV_HEADER} got={tuple(header)}'
        )

    raw_frame = pl.read_csv(
        io.BytesIO(csv_content),
        has_header=True,
        schema_overrides={
            'instrument_name': pl.Utf8,
            'trade_id': pl.Utf8,
            'side': pl.Utf8,
            'price': pl.Utf8,
            'size': pl.Utf8,
            'created_time': pl.Utf8,
        },
        truncate_ragged_lines=False,
    ).with_row_index(name='_line_number', offset=2)
    if raw_frame.height == 0:
        raise RuntimeError('OKX CSV payload produced zero spot trade events')

    normalized = raw_frame.with_columns(
        pl.col('instrument_name')
        .cast(pl.Utf8)
        .str.strip_chars()
        .alias('_instrument_name'),
        pl.col('trade_id').cast(pl.Utf8).str.strip_chars().alias('_trade_id_text'),
        pl.col('side').cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias('_side'),
        pl.col('price').cast(pl.Utf8).str.strip_chars().alias('_price_text'),
        pl.col('size').cast(pl.Utf8).str.strip_chars().alias('_size_text'),
        pl.col('created_time')
        .cast(pl.Utf8)
        .str.strip_chars()
        .alias('_timestamp_text'),
    ).with_columns(
        pl.col('_trade_id_text').cast(pl.Int64, strict=False).alias('_trade_id_int'),
        pl.col('_timestamp_text').cast(pl.Int64, strict=False).alias('_timestamp_ms'),
        pl.col('_price_text').cast(pl.Float64, strict=False).alias('_price_float'),
        pl.col('_size_text').cast(pl.Float64, strict=False).alias('_size_float'),
    )

    invalid_instrument = normalized.filter(pl.col('_instrument_name') != _INSTRUMENT_ID)
    if invalid_instrument.height > 0:
        line_number = int(invalid_instrument.get_column('_line_number').item(0))
        value = str(invalid_instrument.get_column('_instrument_name').item(0))
        raise RuntimeError(
            f'OKX CSV instrument_name mismatch at line={line_number}: '
            f'expected={_INSTRUMENT_ID} got={value!r}'
        )

    invalid_trade_id = normalized.filter(pl.col('_trade_id_int').is_null())
    if invalid_trade_id.height > 0:
        line_number = int(invalid_trade_id.get_column('_line_number').item(0))
        value = str(invalid_trade_id.get_column('_trade_id_text').item(0))
        raise RuntimeError(
            f'OKX row {line_number} trade_id must be integer, got {value!r}'
        )

    invalid_side = normalized.filter(~pl.col('_side').is_in(['buy', 'sell']))
    if invalid_side.height > 0:
        line_number = int(invalid_side.get_column('_line_number').item(0))
        value = str(invalid_side.get_column('_side').item(0))
        raise RuntimeError(
            f'OKX CSV side must be buy/sell at line={line_number}, got={value!r}'
        )

    for column_name, label in (
        ('_price_text', 'price'),
        ('_size_text', 'size'),
    ):
        invalid_decimal = normalized.filter(
            (pl.col(column_name) == '')
            | ~pl.col(column_name).str.contains(r'^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$')
            | pl.col(column_name).cast(pl.Float64, strict=False).is_null()
            | (pl.col(column_name).cast(pl.Float64, strict=False) <= 0)
        )
        if invalid_decimal.height > 0:
            line_number = int(invalid_decimal.get_column('_line_number').item(0))
            value = str(invalid_decimal.get_column(column_name).item(0))
            raise RuntimeError(
                f'OKX row {line_number} {label} must be valid positive decimal text, '
                f'got {value!r}'
            )

    invalid_timestamp = normalized.filter(
        pl.col('_timestamp_ms').is_null()
        | (pl.col('_timestamp_text').str.len_chars() != 13)
        | (pl.col('_timestamp_ms') <= 0)
    )
    if invalid_timestamp.height > 0:
        line_number = int(invalid_timestamp.get_column('_line_number').item(0))
        value = str(invalid_timestamp.get_column('_timestamp_text').item(0))
        raise RuntimeError(
            f'OKX row {line_number} created_time must be 13-digit epoch milliseconds, '
            f'got {value!r}'
        )

    return normalized.select(
        pl.from_epoch(pl.col('_timestamp_ms'), time_unit='ms')
        .dt.strftime('%Y-%m-%d')
        .alias('partition_id'),
        pl.col('_instrument_name').alias('instrument_name'),
        pl.col('_trade_id_int').alias('trade_id'),
        pl.col('_side').alias('side'),
        pl.col('_price_text').alias('price'),
        pl.col('_size_text').alias('size'),
        (pl.col('_price_float') * pl.col('_size_float')).alias('quote_quantity'),
        pl.col('_timestamp_ms').alias('timestamp'),
        pl.from_epoch(pl.col('_timestamp_ms'), time_unit='ms')
        .dt.replace_time_zone('UTC')
        .alias('datetime'),
    )


def deduplicate_okx_exact_duplicate_frame_or_raise(
    frame: pl.DataFrame,
) -> OKXDeduplicatedTradeFrame:
    if frame.height == 0:
        raise RuntimeError('OKX frame deduplication requires non-empty frame')

    conflicts = frame.group_by('trade_id').agg(
        pl.len().alias('_row_count'),
        pl.struct('side', 'price', 'size', 'timestamp')
        .n_unique()
        .alias('_identity_count'),
    ).filter(pl.col('_identity_count') > 1)
    if conflicts.height > 0:
        trade_id = int(conflicts.get_column('trade_id').item(0))
        raise RuntimeError(
            'OKX source contains conflicting duplicate trade_id payloads: '
            f'trade_id={trade_id}'
        )

    deduplicated_frame = frame.unique(
        subset=['trade_id'],
        keep='first',
        maintain_order=True,
    )
    return OKXDeduplicatedTradeFrame(
        frame=deduplicated_frame,
        raw_row_count=frame.height,
        exact_duplicate_row_count=frame.height - deduplicated_frame.height,
    )


def create_staged_okx_spot_trade_csv_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    frame: pl.DataFrame,
) -> str:
    stage_table = f'__origo_okx_stage_{uuid4().hex}'
    client.execute(
        f'''
        CREATE TABLE {database}.{stage_table}
        (
            instrument_name String,
            trade_id Int64,
            side String,
            price_text String,
            size_text String,
            timestamp_ms Int64
        )
        ENGINE = Memory
        '''
    )
    try:
        insert_sql = (
            f'INSERT INTO {database}.{stage_table} '
            '('
            'instrument_name,'
            'trade_id,'
            'side,'
            'price_text,'
            'size_text,'
            'timestamp_ms'
            ') VALUES'
        )
        for chunk in frame.iter_slices(n_rows=_INSERT_CHUNK_SIZE):
            client.execute(
                insert_sql,
                [
                    chunk.get_column('instrument_name').cast(pl.Utf8).to_list(),
                    chunk.get_column('trade_id').cast(pl.Int64).to_list(),
                    chunk.get_column('side').cast(pl.Utf8).to_list(),
                    chunk.get_column('price').cast(pl.Utf8).to_list(),
                    chunk.get_column('size').cast(pl.Utf8).to_list(),
                    chunk.get_column('timestamp').cast(pl.Int64).to_list(),
                ],
                columnar=True,
            )
    except Exception:
        client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')
        raise
    return stage_table


def drop_staged_okx_spot_trade_csv_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
) -> None:
    client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')


def _okx_stage_event_projection_sql(*, database: str, stage_table: str) -> str:
    return f'''
        SELECT
            trade_id_int,
            toString(trade_id_int) AS source_offset,
            timestamp_ms,
            payload_json,
            lower(hex(SHA256(payload_json))) AS payload_sha256_raw,
            toString(toUUID(concat(
                substring(event_id_hex, 1, 8), '-',
                substring(event_id_hex, 9, 4), '-',
                substring(event_id_hex, 13, 4), '-',
                substring(event_id_hex, 17, 4), '-',
                substring(event_id_hex, 21, 12)
            ))) AS event_id_text,
            toUUID(concat(
                substring(event_id_hex, 1, 8), '-',
                substring(event_id_hex, 9, 4), '-',
                substring(event_id_hex, 13, 4), '-',
                substring(event_id_hex, 17, 4), '-',
                substring(event_id_hex, 21, 12)
            )) AS event_id_uuid
        FROM
        (
            SELECT
                trade_id_int,
                timestamp_ms,
                concat(
                    '{{"instrument_name":"', instrument_name_normalized,
                    '","price":"', price_text_normalized,
                    '","side":"', side_normalized,
                    '","size":"', size_text_normalized,
                    '","timestamp":', toString(timestamp_ms),
                    ',"trade_id":', toString(trade_id_int),
                    '}}'
                ) AS payload_json,
                lower(hex(uuid_bytes)) AS event_id_hex
            FROM
            (
                SELECT
                    trade_id_int,
                    timestamp_ms,
                    instrument_name_normalized,
                    side_normalized,
                    price_text_normalized,
                    size_text_normalized,
                    concat(
                        substring(sha1_digest, 1, 6),
                        reinterpretAsString(reinterpretAsUInt8(
                            bitOr(bitAnd(reinterpretAsUInt8(substring(sha1_digest, 7, 1)), 15), 80)
                        )),
                        substring(sha1_digest, 8, 1),
                        reinterpretAsString(reinterpretAsUInt8(
                            bitOr(bitAnd(reinterpretAsUInt8(substring(sha1_digest, 9, 1)), 63), 128)
                        )),
                        substring(sha1_digest, 10, 7)
                    ) AS uuid_bytes
                FROM
                (
                    SELECT
                        trimBoth(instrument_name) AS instrument_name_normalized,
                        trade_id AS trade_id_int,
                        lower(trimBoth(side)) AS side_normalized,
                        trimBoth(price_text) AS price_text_normalized,
                        trimBoth(size_text) AS size_text_normalized,
                        timestamp_ms,
                        SHA1(concat(
                            unhex(%(canonical_event_namespace_hex)s),
                            concat(%(idempotency_prefix)s, toString(trade_id))
                        )) AS sha1_digest
                    FROM {database}.{stage_table}
                )
            )
        )
    '''


def build_okx_partition_source_proof_from_stage_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
    canonical_partition_id: str,
    source_file_url: str,
    source_filename: str,
    zip_sha256: str,
    csv_sha256: str,
    content_md5_b64: str,
    raw_row_count: int,
    deduplicated_exact_duplicate_rows: int,
) -> PartitionSourceProof:
    rows = client.execute(
        f'''
        SELECT
            source_row_count,
            first_offset_or_equivalent,
            last_offset_or_equivalent,
            source_offset_digest_sha256,
            source_identity_digest_sha256
        FROM
        (
            SELECT
                row_count AS source_row_count,
                toString(tupleElement(materials[1], 2)) AS first_offset_or_equivalent,
                toString(tupleElement(materials[length(materials)], 2)) AS last_offset_or_equivalent,
                lower(hex(SHA256(concat(
                    arrayStringConcat(
                        arrayMap(item -> tupleElement(item, 2), materials),
                        '\\n'
                    ),
                    '\\n'
                )))) AS source_offset_digest_sha256,
                lower(hex(SHA256(concat(
                    arrayStringConcat(
                        arrayMap(
                            item -> concat(
                                tupleElement(item, 2),
                                '|',
                                tupleElement(item, 3),
                                '|',
                                tupleElement(item, 4)
                            ),
                            materials
                        ),
                        '\\n'
                    ),
                    '\\n'
                )))) AS source_identity_digest_sha256
            FROM
            (
                SELECT
                    count() AS row_count,
                    arraySort(
                        groupArray(
                            (
                                trade_id_int,
                                source_offset,
                                event_id_text,
                                payload_sha256_raw
                            )
                        )
                    ) AS materials
                FROM
                (
                    {_okx_stage_event_projection_sql(database=database, stage_table=stage_table)}
                )
            )
        )
        ''',
        {
            'canonical_event_namespace_hex': _CANONICAL_EVENT_NAMESPACE_HEX,
            'idempotency_prefix': (
                f'{CANONICAL_EVENT_ENVELOPE_VERSION}|'
                f'{_SOURCE_ID}|{_STREAM_ID}|{canonical_partition_id}|'
            ),
        },
    )
    if rows == []:
        raise RuntimeError(
            'OKX staged source proof query returned no rows '
            f'for partition_id={canonical_partition_id}'
        )
    row = rows[0]
    return build_partition_source_proof_from_precomputed(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=_STREAM_ID,
            partition_id=canonical_partition_id,
        ),
        offset_ordering='numeric_monotonic',
        source_artifact_identity={
            'source_file_url': source_file_url,
            'source_filename': source_filename,
            'zip_sha256': zip_sha256,
            'csv_sha256': csv_sha256,
            'content_md5_b64': content_md5_b64,
            'raw_csv_row_count': raw_row_count,
            'deduplicated_exact_duplicate_rows': deduplicated_exact_duplicate_rows,
        },
        source_row_count=int(row[0]),
        first_offset_or_equivalent=str(row[1]),
        last_offset_or_equivalent=str(row[2]),
        source_offset_digest_sha256=str(row[3]),
        source_identity_digest_sha256=str(row[4]),
        allow_empty_partition=False,
        allow_duplicate_offsets=False,
    )


def write_staged_okx_spot_trade_csv_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
    partition_id: str,
    row_count: int,
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
    if partition_id.strip() == '':
        raise RuntimeError('partition_id must be non-empty')
    if row_count <= 0:
        raise RuntimeError(
            'OKX staged canonical insert requires row_count > 0, '
            f'got={row_count}'
        )
    existing_rows = client.execute(
        f'''
        SELECT 1
        FROM {database}.{CANONICAL_EVENT_LOG_READ_TABLE}
        WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id = %(partition_id)s
        LIMIT 1
        ''',
        {
            'source_id': _SOURCE_ID,
            'stream_id': _STREAM_ID,
            'partition_id': partition_id,
        },
    )
    if existing_rows != []:
        raise RuntimeError(
            'OKX staged canonical insert requires empty target partition; '
            f'partition already has data source={_SOURCE_ID} stream={_STREAM_ID} '
            f'partition_id={partition_id}'
        )
    client.execute(
        f'''
        INSERT INTO {database}.canonical_event_log
        (
            envelope_version,
            event_id,
            source_id,
            stream_id,
            partition_id,
            source_offset_or_equivalent,
            source_event_time_utc,
            ingested_at_utc,
            payload_content_type,
            payload_encoding,
            payload_raw,
            payload_sha256_raw,
            payload_json
        )
        SELECT
            toUInt16({CANONICAL_EVENT_ENVELOPE_VERSION}) AS envelope_version,
            event_id_uuid AS event_id,
            %(source_id)s AS source_id,
            %(stream_id)s AS stream_id,
            %(partition_id)s AS partition_id,
            source_offset,
            fromUnixTimestamp64Milli(timestamp_ms, 'UTC') AS source_event_time_utc,
            %(ingested_at_utc)s AS ingested_at_utc,
            %(payload_content_type)s AS payload_content_type,
            %(payload_encoding)s AS payload_encoding,
            payload_json AS payload_raw,
            payload_sha256_raw,
            payload_json
        FROM
        (
            {_okx_stage_event_projection_sql(database=database, stage_table=stage_table)}
        )
        ''',
        {
            'source_id': _SOURCE_ID,
            'stream_id': _STREAM_ID,
            'partition_id': partition_id,
            'ingested_at_utc': ingested_at_utc,
            'payload_content_type': _PAYLOAD_CONTENT_TYPE,
            'payload_encoding': _PAYLOAD_ENCODING,
            'canonical_event_namespace_hex': _CANONICAL_EVENT_NAMESPACE_HEX,
            'idempotency_prefix': (
                f'{CANONICAL_EVENT_ENVELOPE_VERSION}|'
                f'{_SOURCE_ID}|{_STREAM_ID}|{partition_id}|'
            ),
        },
    )
    bounds = client.execute(
        f'''
        SELECT min(trade_id), max(trade_id)
        FROM {database}.{stage_table}
        '''
    )
    if bounds == []:
        raise RuntimeError(
            'OKX staged canonical insert could not load stage bounds '
            f'for partition_id={partition_id}'
        )
    min_offset = str(int(bounds[0][0]))
    max_offset = str(int(bounds[0][1]))
    get_canonical_runtime_audit_log().append_ingest_batch_event(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=_STREAM_ID,
            partition_id=partition_id,
        ),
        event_type='canonical_ingest_batch',
        run_id=run_id,
        batch_event_count=row_count,
        inserted_count=row_count,
        duplicate_count=0,
        first_source_offset_or_equivalent=min_offset,
        last_source_offset_or_equivalent=max_offset,
        first_event_id=str(
            canonical_event_id_from_key(
                canonical_event_idempotency_key(
                    source_id=_SOURCE_ID,
                    stream_id=_STREAM_ID,
                    partition_id=partition_id,
                    source_offset_or_equivalent=min_offset,
                )
            )
        ),
        last_event_id=str(
            canonical_event_id_from_key(
                canonical_event_idempotency_key(
                    source_id=_SOURCE_ID,
                    stream_id=_STREAM_ID,
                    partition_id=partition_id,
                    source_offset_or_equivalent=max_offset,
                )
            )
        ),
    )
    return {
        'rows_processed': row_count,
        'rows_inserted': row_count,
        'rows_duplicate': 0,
    }


def parse_okx_spot_trade_csv(csv_content: bytes) -> list[OKXSpotTradeEvent]:
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    header = next(reader, None)
    if header is None:
        raise RuntimeError('OKX CSV payload is empty')
    if tuple(header) != _EXPECTED_CSV_HEADER:
        raise RuntimeError(
            f'OKX CSV header mismatch: expected={_EXPECTED_CSV_HEADER} got={tuple(header)}'
        )

    events: list[OKXSpotTradeEvent] = []
    for row_number, row in enumerate(reader, start=2):
        if len(row) != 6:
            raise RuntimeError(
                f'OKX CSV row has unexpected column count at line={row_number}: '
                f'expected=6 got={len(row)}'
            )
        instrument_name = row[0].strip()
        if instrument_name != _INSTRUMENT_ID:
            raise RuntimeError(
                f'OKX CSV instrument_name mismatch at line={row_number}: '
                f'expected={_INSTRUMENT_ID} got={instrument_name!r}'
            )
        trade_id = _parse_int(row[1], label=f'OKX row {row_number} trade_id')
        side = row[2].strip().lower()
        if side not in {'buy', 'sell'}:
            raise RuntimeError(
                f'OKX CSV side must be buy/sell at line={row_number}, got={row[2]!r}'
            )
        price_text = _parse_decimal_text(row[3], label=f'OKX row {row_number} price')
        size_text = _parse_decimal_text(row[4], label=f'OKX row {row_number} size')
        timestamp, event_time_utc = _parse_timestamp_ms(
            row[5], label=f'OKX row {row_number} created_time'
        )
        quote_quantity_text = _compute_quote_quantity_text(
            price_text=price_text,
            size_text=size_text,
        )
        events.append(
            OKXSpotTradeEvent(
                instrument_name=instrument_name,
                trade_id=trade_id,
                side=side,
                price_text=price_text,
                size_text=size_text,
                quote_quantity_text=quote_quantity_text,
                timestamp=timestamp,
                event_time_utc=event_time_utc,
            )
        )

    if events == []:
        raise RuntimeError('OKX CSV payload produced zero spot trade events')
    return events


def deduplicate_okx_exact_duplicate_events_or_raise(
    events: list[OKXSpotTradeEvent],
) -> OKXDeduplicatedTradeEvents:
    deduplicated_events: list[OKXSpotTradeEvent] = []
    seen_by_trade_id: dict[int, tuple[int, str, str, str, int]] = {}
    exact_duplicate_row_count = 0

    for event in events:
        identity = event.duplicate_identity_tuple()
        previous_identity = seen_by_trade_id.get(event.trade_id)
        if previous_identity is None:
            seen_by_trade_id[event.trade_id] = identity
            deduplicated_events.append(event)
            continue
        if previous_identity != identity:
            raise RuntimeError(
                'OKX source contains conflicting duplicate trade_id payloads: '
                f'trade_id={event.trade_id}'
            )
        exact_duplicate_row_count += 1

    return OKXDeduplicatedTradeEvents(
        events=deduplicated_events,
        raw_row_count=len(events),
        exact_duplicate_row_count=exact_duplicate_row_count,
    )


def build_okx_partition_source_proof(
    *,
    canonical_partition_id: str,
    events: list[OKXSpotTradeEvent],
    source_file_url: str,
    source_filename: str,
    zip_sha256: str,
    csv_sha256: str,
    content_md5_b64: str,
) -> PartitionSourceProof:
    deduplicated = deduplicate_okx_exact_duplicate_events_or_raise(events)
    materials: list[SourceIdentityMaterial] = []
    for event in deduplicated.events:
        payload_json = json.dumps(
            event.to_payload(),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        source_offset = str(event.trade_id)
        materials.append(
            SourceIdentityMaterial(
                source_offset_or_equivalent=source_offset,
                event_id=str(
                    canonical_event_id_from_key(
                        canonical_event_idempotency_key(
                            source_id=_SOURCE_ID,
                            stream_id=_STREAM_ID,
                            partition_id=canonical_partition_id,
                            source_offset_or_equivalent=source_offset,
                        )
                    )
                ),
                payload_sha256_raw=hashlib.sha256(
                    payload_json.encode(_PAYLOAD_ENCODING)
                ).hexdigest(),
            )
        )
    return build_partition_source_proof(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=_STREAM_ID,
            partition_id=canonical_partition_id,
        ),
        offset_ordering='numeric_monotonic',
        source_artifact_identity={
            'source_file_url': source_file_url,
            'source_filename': source_filename,
            'zip_sha256': zip_sha256,
            'csv_sha256': csv_sha256,
            'content_md5_b64': content_md5_b64,
            'raw_csv_row_count': deduplicated.raw_row_count,
            'deduplicated_exact_duplicate_rows': deduplicated.exact_duplicate_row_count,
        },
        materials=materials,
        allow_empty_partition=False,
        allow_duplicate_offsets=False,
    )


def write_okx_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[OKXSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
    canonical_partition_id: str | None = None,
    fast_insert_mode: FastInsertMode = _FAST_INSERT_MODE_DEFAULT,
) -> dict[str, int]:
    deduplicated = deduplicate_okx_exact_duplicate_events_or_raise(events)
    resolved_partition_id: str | None = None
    if canonical_partition_id is not None:
        normalized_partition_id = canonical_partition_id.strip()
        if normalized_partition_id == '':
            raise RuntimeError('canonical_partition_id must be non-empty when provided')
        resolved_partition_id = normalized_partition_id

    canonical_events: list[dict[str, object]] = []
    for event in deduplicated.events:
        canonical_events.append(
            {
                'partition_id': resolved_partition_id or event.partition_id,
                'source_offset_or_equivalent': str(event.trade_id),
                'source_event_time_utc': event.event_time_utc,
                'payload': event.to_payload(),
            }
        )
    return _write_events_to_canonical(
        client=client,
        database=database,
        events=canonical_events,
        run_id=run_id,
        ingested_at_utc=ingested_at_utc,
        fast_insert_mode=fast_insert_mode,
    ) | {
        'raw_row_count': deduplicated.raw_row_count,
        'deduplicated_exact_duplicate_rows': deduplicated.exact_duplicate_row_count,
    }


def _write_events_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[dict[str, object]],
    run_id: str | None,
    ingested_at_utc: datetime,
    fast_insert_mode: FastInsertMode = _FAST_INSERT_MODE_DEFAULT,
) -> dict[str, int]:
    if fast_insert_mode not in {
        _FAST_INSERT_MODE_DEFAULT,
        _FAST_INSERT_MODE_ASSUME_NEW_PARTITION,
    }:
        raise RuntimeError(
            'fast_insert_mode must be one of '
            f'[{_FAST_INSERT_MODE_DEFAULT}, {_FAST_INSERT_MODE_ASSUME_NEW_PARTITION}], '
            f'got={fast_insert_mode!r}'
        )

    if (
        fast_insert_mode == _FAST_INSERT_MODE_ASSUME_NEW_PARTITION
        and events != []
    ):
        partition_ids = {str(event['partition_id']) for event in events}
        if len(partition_ids) != 1:
            raise RuntimeError(
                'OKX fast canonical insert requires exactly one partition_id '
                f'per batch, got={sorted(partition_ids)}'
            )
        partition_id = next(iter(partition_ids))
        existing_rows = client.execute(
            f'''
            SELECT 1
            FROM {database}.{CANONICAL_EVENT_LOG_READ_TABLE}
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id = %(partition_id)s
            LIMIT 1
            ''',
            {
                'source_id': _SOURCE_ID,
                'stream_id': _STREAM_ID,
                'partition_id': partition_id,
            },
        )
        if existing_rows != []:
            raise RuntimeError(
                'OKX fast canonical insert requires empty target partition; '
                f'partition already has data source={_SOURCE_ID} stream={_STREAM_ID} '
                f'partition_id={partition_id}'
            )

        insert_rows: list[tuple[object, ...]] = []
        first_offset: str | None = None
        last_offset: str | None = None
        first_event_id: str | None = None
        last_event_id: str | None = None
        for event in events:
            source_offset = str(event['source_offset_or_equivalent'])
            source_event_time_utc = event['source_event_time_utc']
            if not isinstance(source_event_time_utc, datetime):
                raise RuntimeError('source_event_time_utc must be datetime')
            payload = event['payload']
            if not isinstance(payload, dict):
                raise RuntimeError('payload must be dict')
            payload_json = json.dumps(
                payload,
                sort_keys=True,
                separators=(',', ':'),
                ensure_ascii=True,
            )
            payload_raw = payload_json.encode(_PAYLOAD_ENCODING)
            payload_sha256_raw = hashlib.sha256(payload_raw).hexdigest()
            event_id = canonical_event_id_from_key(
                canonical_event_idempotency_key(
                    source_id=_SOURCE_ID,
                    stream_id=_STREAM_ID,
                    partition_id=partition_id,
                    source_offset_or_equivalent=source_offset,
                )
            )
            if first_offset is None:
                first_offset = source_offset
                first_event_id = str(event_id)
            last_offset = source_offset
            last_event_id = str(event_id)
            insert_rows.append(
                (
                    CANONICAL_EVENT_ENVELOPE_VERSION,
                    event_id,
                    _SOURCE_ID,
                    _STREAM_ID,
                    partition_id,
                    source_offset,
                    source_event_time_utc.astimezone(UTC),
                    ingested_at_utc.astimezone(UTC),
                    _PAYLOAD_CONTENT_TYPE,
                    _PAYLOAD_ENCODING,
                    payload_raw,
                    payload_sha256_raw,
                    payload_json,
                )
            )

        for start in range(0, len(insert_rows), _INSERT_CHUNK_SIZE):
            chunk = insert_rows[start : start + _INSERT_CHUNK_SIZE]
            client.execute(
                f'''
                INSERT INTO {database}.canonical_event_log
                (
                    envelope_version,
                    event_id,
                    source_id,
                    stream_id,
                    partition_id,
                    source_offset_or_equivalent,
                    source_event_time_utc,
                    ingested_at_utc,
                    payload_content_type,
                    payload_encoding,
                    payload_raw,
                    payload_sha256_raw,
                    payload_json
                )
                VALUES
                ''',
                chunk,
            )

        if first_offset is None or last_offset is None:
            raise RuntimeError('OKX fast canonical insert produced no rows')
        if first_event_id is None or last_event_id is None:
            raise RuntimeError('OKX fast canonical insert missing event IDs')

        get_canonical_runtime_audit_log().append_ingest_batch_event(
            stream_key=CanonicalStreamKey(
                source_id=_SOURCE_ID,
                stream_id=_STREAM_ID,
                partition_id=partition_id,
            ),
            event_type='canonical_ingest_batch',
            run_id=run_id,
            batch_event_count=len(insert_rows),
            inserted_count=len(insert_rows),
            duplicate_count=0,
            first_source_offset_or_equivalent=first_offset,
            last_source_offset_or_equivalent=last_offset,
            first_event_id=first_event_id,
            last_event_id=last_event_id,
        )
        return {
            'rows_processed': len(events),
            'rows_inserted': len(insert_rows),
            'rows_duplicate': 0,
        }

    writer = CanonicalEventWriter(
        client=client,
        database=database,
        quarantine_registry=NoopStreamQuarantineRegistry(),
    )

    inserted = 0
    duplicate = 0
    write_inputs: list[CanonicalEventWriteInput] = []

    def flush_batch() -> None:
        nonlocal inserted, duplicate
        if write_inputs == []:
            return
        results = writer.write_events(write_inputs)
        write_inputs.clear()
        for result in results:
            if result.status == 'inserted':
                inserted += 1
            elif result.status == 'duplicate':
                duplicate += 1
            else:
                raise RuntimeError(
                    f'Unexpected canonical writer status: {result.status}'
                )

    for event in events:
        partition_id = str(event['partition_id'])
        source_offset = str(event['source_offset_or_equivalent'])
        source_event_time_utc = event['source_event_time_utc']
        if not isinstance(source_event_time_utc, datetime):
            raise RuntimeError('source_event_time_utc must be datetime')
        payload = event['payload']
        if not isinstance(payload, dict):
            raise RuntimeError('payload must be dict')
        payload_json = json.dumps(
            payload,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        payload_raw = payload_json.encode(_PAYLOAD_ENCODING)
        write_inputs.append(
            CanonicalEventWriteInput(
                source_id=_SOURCE_ID,
                stream_id=_STREAM_ID,
                partition_id=partition_id,
                source_offset_or_equivalent=source_offset,
                source_event_time_utc=source_event_time_utc,
                ingested_at_utc=ingested_at_utc,
                payload_content_type=_PAYLOAD_CONTENT_TYPE,
                payload_encoding=_PAYLOAD_ENCODING,
                payload_raw=payload_raw,
                run_id=run_id,
            )
        )
        if len(write_inputs) >= _WRITE_EVENTS_BATCH_SIZE:
            flush_batch()

    flush_batch()

    return {
        'rows_processed': len(events),
        'rows_inserted': inserted,
        'rows_duplicate': duplicate,
    }
