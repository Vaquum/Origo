from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import polars as pl
from clickhouse_connect import get_client as get_clickhouse_http_client
from clickhouse_driver import Client as ClickhouseClient

from origo.events.backfill_state import (
    PartitionSourceProof,
    build_partition_source_proof_from_precomputed,
)
from origo.events.envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.quarantine import NoopStreamQuarantineRegistry
from origo.events.runtime_audit import get_canonical_runtime_audit_log
from origo.events.writer import (
    CanonicalEventWriteInput,
    CanonicalEventWriter,
    canonical_event_id_from_key,
)
from origo_control_plane.backfill.runtime_contract import FastInsertMode
from origo_control_plane.config import resolve_clickhouse_http_settings

_SOURCE_ID = 'binance'
_STREAM_ID = 'binance_spot_trades'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_FAST_INSERT_MODE_DEFAULT = 'writer'
_FAST_INSERT_MODE_ASSUME_NEW_PARTITION = 'assume_new_partition'
_INSERT_CHUNK_SIZE = 100000
_CANONICAL_EVENT_NAMESPACE_HEX = 'f1d1ef1795ce4f9fbd81f9958cdf8ee5'


def _build_clickhouse_http_client_or_raise() -> Any:
    settings = resolve_clickhouse_http_settings()
    return get_clickhouse_http_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=settings.database,
    )


@dataclass(frozen=True)
class BinanceSpotTradeEvent:
    partition_id: str
    trade_id: int
    price_text: str
    quantity_text: str
    quote_quantity_text: str
    timestamp_ms: int
    is_buyer_maker: bool
    is_best_match: bool

    def to_payload(self) -> dict[str, object]:
        return {
            'trade_id': self.trade_id,
            'price': self.price_text,
            'qty': self.quantity_text,
            'quote_qty': self.quote_quantity_text,
            'is_buyer_maker': self.is_buyer_maker,
            'is_best_match': self.is_best_match,
        }

    def to_integrity_tuple(self) -> tuple[int, str, str, str, int, bool, bool]:
        return (
            self.trade_id,
            self.price_text,
            self.quantity_text,
            self.quote_quantity_text,
            self.timestamp_ms,
            self.is_buyer_maker,
            self.is_best_match,
        )


def _partition_bounds_epoch_ms(*, partition_id: str) -> tuple[int, int]:
    try:
        day_start_utc = datetime.fromisoformat(partition_id).replace(tzinfo=UTC)
    except ValueError as exc:
        raise RuntimeError(
            f'partition_id must be ISO date (YYYY-MM-DD), got {partition_id!r}'
        ) from exc
    day_end_utc = day_start_utc + timedelta(days=1)
    return int(day_start_utc.timestamp() * 1000), int(day_end_utc.timestamp() * 1000)


def _payload_json_for_event(event: BinanceSpotTradeEvent) -> str:
    return _payload_json_from_fields(
        trade_id=event.trade_id,
        price_text=event.price_text,
        quantity_text=event.quantity_text,
        quote_quantity_text=event.quote_quantity_text,
        is_buyer_maker=event.is_buyer_maker,
        is_best_match=event.is_best_match,
    )


def _payload_json_from_fields(
    *,
    trade_id: int,
    price_text: str,
    quantity_text: str,
    quote_quantity_text: str,
    is_buyer_maker: bool,
    is_best_match: bool,
) -> str:
    is_best_match_text = 'true' if is_best_match else 'false'
    is_buyer_maker_text = 'true' if is_buyer_maker else 'false'
    # Keys are intentionally sorted to match canonical JSON ordering.
    return (
        '{"is_best_match":'
        + is_best_match_text
        + ',"is_buyer_maker":'
        + is_buyer_maker_text
        + ',"price":"'
        + price_text
        + '","qty":"'
        + quantity_text
        + '","quote_qty":"'
        + quote_quantity_text
        + '","trade_id":'
        + str(trade_id)
        + '}'
    )


def build_binance_partition_source_proof(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
    partition_id: str,
    file_url: str,
    checksum_url: str,
    zip_sha256: str,
    csv_sha256: str,
    csv_filename: str,
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
                    {_binance_stage_event_projection_sql(database=database, stage_table=stage_table)}
                )
            )
        )
        ''',
        {
            'canonical_event_namespace_hex': _CANONICAL_EVENT_NAMESPACE_HEX,
            'idempotency_prefix': (
                f'{CANONICAL_EVENT_ENVELOPE_VERSION}|'
                f'{_SOURCE_ID}|{_STREAM_ID}|{partition_id}|'
            ),
        },
    )
    if rows == []:
        raise RuntimeError(
            'Binance staged source proof query returned no rows '
            f'for partition_id={partition_id}'
        )
    row = rows[0]
    return build_partition_source_proof_from_precomputed(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=_STREAM_ID,
            partition_id=partition_id,
        ),
        offset_ordering='numeric',
        source_artifact_identity={
            'file_url': file_url,
            'checksum_url': checksum_url,
            'zip_sha256': zip_sha256,
            'csv_sha256': csv_sha256,
            'csv_filename': csv_filename,
        },
        source_row_count=int(row[0]),
        first_offset_or_equivalent=str(row[1]),
        last_offset_or_equivalent=str(row[2]),
        source_offset_digest_sha256=str(row[3]),
        source_identity_digest_sha256=str(row[4]),
        allow_empty_partition=False,
    )


def _first_row_text_or_raise(csv_content: bytes) -> str:
    first_newline = csv_content.find(b'\n')
    first_line_bytes = (
        csv_content if first_newline == -1 else csv_content[:first_newline]
    )
    if first_line_bytes == b'':
        raise RuntimeError('CSV payload is empty')
    return first_line_bytes.decode('utf-8')


def _normalized_binance_csv_payload_for_stage_insert_or_raise(
    csv_content: bytes,
) -> bytes:
    first_line = _first_row_text_or_raise(csv_content)
    first_cell = first_line.split(',', 1)[0].strip().lstrip('\ufeff').lower()
    has_header = first_cell in {'id', 'trade_id'}
    if not has_header:
        return csv_content
    first_newline = csv_content.find(b'\n')
    if first_newline == -1:
        raise RuntimeError('CSV payload contains header row but no data rows')
    normalized_payload = csv_content[first_newline + 1 :]
    if normalized_payload == b'':
        raise RuntimeError('CSV payload contains header row but no data rows')
    return normalized_payload


def create_staged_binance_spot_trade_csv_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    csv_content: bytes,
) -> str:
    stage_table = f'__origo_binance_stage_{uuid4().hex}'
    client.execute(
        f'''
        CREATE TABLE {database}.{stage_table}
        (
            trade_id String,
            price_text String,
            quantity_text String,
            quote_quantity_text String,
            timestamp_raw String,
            is_buyer_maker_raw String,
            is_best_match_raw String
        )
        ENGINE = Memory
        '''
    )
    http_client: Any | None = None
    try:
        http_client = _build_clickhouse_http_client_or_raise()
        if http_client is None:
            raise RuntimeError('Failed to build ClickHouse HTTP client')
        http_client.raw_insert(
            table=f'{database}.{stage_table}',
            insert_block=_normalized_binance_csv_payload_for_stage_insert_or_raise(
                csv_content
            ),
            fmt='CSV',
        )
    except Exception:
        client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')
        raise
    finally:
        if http_client is not None:
            http_client.close()
    return stage_table


def drop_staged_binance_spot_trade_csv_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
) -> None:
    client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')


def _binance_stage_event_projection_sql(*, database: str, stage_table: str) -> str:
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
                    '{{"is_best_match":',
                    if(is_best_match, 'true', 'false'),
                    ',"is_buyer_maker":',
                    if(is_buyer_maker, 'true', 'false'),
                    ',"price":"', price_text_normalized,
                    '","qty":"', quantity_text_normalized,
                    '","quote_qty":"', quote_quantity_text_normalized,
                    '","trade_id":', toString(trade_id_int),
                    '}}'
                ) AS payload_json,
                lower(hex(uuid_bytes)) AS event_id_hex
            FROM
            (
                SELECT
                    trade_id_int,
                    timestamp_ms,
                    price_text_normalized,
                    quantity_text_normalized,
                    quote_quantity_text_normalized,
                    is_buyer_maker,
                    is_best_match,
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
                        toInt64(trimBoth(trade_id)) AS trade_id_int,
                        trimBoth(price_text) AS price_text_normalized,
                        trimBoth(quantity_text) AS quantity_text_normalized,
                        trimBoth(quote_quantity_text) AS quote_quantity_text_normalized,
                        if(
                            length(trimBoth(timestamp_raw)) = 13,
                            toInt64(trimBoth(timestamp_raw)),
                            intDiv(toInt64(trimBoth(timestamp_raw)), 1000)
                        ) AS timestamp_ms,
                        lower(trimBoth(is_buyer_maker_raw)) IN ('true', '1') AS is_buyer_maker,
                        lower(trimBoth(is_best_match_raw)) IN ('true', '1') AS is_best_match,
                        SHA1(concat(
                            unhex(%(canonical_event_namespace_hex)s),
                            concat(%(idempotency_prefix)s, trimBoth(trade_id))
                        )) AS sha1_digest
                    FROM {database}.{stage_table}
                )
            )
        )
    '''


def _bool_series_from_text(
    frame: pl.DataFrame,
    *,
    column_name: str,
) -> pl.Series:
    normalized_name = f'_{column_name}_normalized'
    parsed_name = f'_{column_name}_parsed'
    parsed_frame = frame.with_columns(
        pl.col(column_name)
        .str.strip_chars()
        .str.to_lowercase()
        .alias(normalized_name)
    ).with_columns(
        pl.when(pl.col(normalized_name).is_in(['true', '1']))
        .then(pl.lit(True))
        .when(pl.col(normalized_name).is_in(['false', '0']))
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
        .alias(parsed_name)
    )
    invalid = parsed_frame.filter(pl.col(parsed_name).is_null())
    if invalid.height > 0:
        row_number = int(invalid.get_column('_row_number').item(0))
        raw_value = invalid.get_column(column_name).item(0)
        raise RuntimeError(
            f'CSV row {row_number} {column_name} must be one of true|false|1|0, '
            f'got {raw_value!r}'
        )
    return parsed_frame.get_column(parsed_name)


def parse_binance_spot_trade_csv_frame(
    csv_content: bytes, *, partition_id: str | None = None
) -> pl.DataFrame:
    first_line = _first_row_text_or_raise(csv_content)
    first_cell = first_line.split(',', 1)[0].strip().lstrip('\ufeff').lower()
    has_header = first_cell in {'id', 'trade_id'}
    raw_frame = pl.read_csv(
        io.BytesIO(csv_content),
        has_header=False,
        skip_rows=1 if has_header else 0,
        new_columns=[
            'trade_id',
            'price_text',
            'quantity_text',
            'quote_quantity_text',
            'timestamp_raw',
            'is_buyer_maker_raw',
            'is_best_match_raw',
        ],
        schema_overrides={
            'trade_id': pl.Utf8,
            'price_text': pl.Utf8,
            'quantity_text': pl.Utf8,
            'quote_quantity_text': pl.Utf8,
            'timestamp_raw': pl.Utf8,
            'is_buyer_maker_raw': pl.Utf8,
            'is_best_match_raw': pl.Utf8,
        },
        truncate_ragged_lines=False,
    ).with_row_index(name='_row_number', offset=1)

    if raw_frame.height == 0:
        raise RuntimeError('CSV payload produced zero spot trade events')

    cast_frame = raw_frame.with_columns(
        pl.col('trade_id').str.strip_chars().alias('trade_id'),
        pl.col('price_text').str.strip_chars().alias('price_text'),
        pl.col('quantity_text').str.strip_chars().alias('quantity_text'),
        pl.col('quote_quantity_text').str.strip_chars().alias('quote_quantity_text'),
        pl.col('timestamp_raw').str.strip_chars().alias('timestamp_raw'),
    ).with_columns(
        pl.col('trade_id').cast(pl.Int64, strict=False).alias('_trade_id_int'),
        pl.col('timestamp_raw').cast(pl.Int64, strict=False).alias('_timestamp_int'),
        pl.col('timestamp_raw').str.len_chars().alias('_timestamp_len'),
    )

    invalid_trade_id = cast_frame.filter(pl.col('_trade_id_int').is_null())
    if invalid_trade_id.height > 0:
        row_number = int(invalid_trade_id.get_column('_row_number').item(0))
        raw_value = invalid_trade_id.get_column('trade_id').item(0)
        raise RuntimeError(f'CSV row {row_number} trade_id must be integer, got {raw_value!r}')
    invalid_negative_trade_id = cast_frame.filter(pl.col('_trade_id_int') < 0)
    if invalid_negative_trade_id.height > 0:
        row_number = int(invalid_negative_trade_id.get_column('_row_number').item(0))
        raw_value = int(invalid_negative_trade_id.get_column('_trade_id_int').item(0))
        raise RuntimeError(
            f'CSV row {row_number} trade_id must be >= 0, got {raw_value}'
        )

    for column_name, label in (
        ('price_text', 'price'),
        ('quantity_text', 'quantity'),
        ('quote_quantity_text', 'quote_quantity'),
    ):
        invalid_empty = cast_frame.filter(pl.col(column_name) == '')
        if invalid_empty.height > 0:
            row_number = int(invalid_empty.get_column('_row_number').item(0))
            raise RuntimeError(f'CSV row {row_number} {label} must be non-empty decimal text')
        invalid_no_digits = cast_frame.filter(~pl.col(column_name).str.contains(r'\d'))
        if invalid_no_digits.height > 0:
            row_number = int(invalid_no_digits.get_column('_row_number').item(0))
            raw_value = invalid_no_digits.get_column(column_name).item(0)
            raise RuntimeError(
                f'CSV row {row_number} {label} must contain digits, got {raw_value!r}'
            )
        invalid_chars = cast_frame.filter(
            ~pl.col(column_name).str.contains(r'^[+\-.eE0-9]+$')
        )
        if invalid_chars.height > 0:
            row_number = int(invalid_chars.get_column('_row_number').item(0))
            raw_value = invalid_chars.get_column(column_name).item(0)
            raise RuntimeError(
                f'CSV row {row_number} {label} contains invalid decimal characters, '
                f'got {raw_value!r}'
            )

    invalid_timestamp_value = cast_frame.filter(pl.col('_timestamp_int').is_null())
    if invalid_timestamp_value.height > 0:
        row_number = int(invalid_timestamp_value.get_column('_row_number').item(0))
        raw_value = invalid_timestamp_value.get_column('timestamp_raw').item(0)
        raise RuntimeError(
            f'CSV row {row_number} timestamp must be integer, got {raw_value!r}'
        )

    cast_frame = cast_frame.with_columns(
        pl.when(pl.col('_timestamp_len') == 13)
        .then(pl.col('_timestamp_int'))
        .when(pl.col('_timestamp_len') == 16)
        .then(pl.col('_timestamp_int') // 1000)
        .otherwise(pl.lit(None, dtype=pl.Int64))
        .alias('timestamp_ms')
    )
    invalid_timestamp_length = cast_frame.filter(pl.col('timestamp_ms').is_null())
    if invalid_timestamp_length.height > 0:
        row_number = int(invalid_timestamp_length.get_column('_row_number').item(0))
        raw_value = int(invalid_timestamp_length.get_column('_timestamp_int').item(0))
        raise RuntimeError(
            'CSV row timestamp must be epoch milliseconds or microseconds, '
            f'got {raw_value}'
        )

    day_start_epoch_ms: int | None = None
    day_end_epoch_ms: int | None = None
    if partition_id is not None:
        day_start_epoch_ms, day_end_epoch_ms = _partition_bounds_epoch_ms(
            partition_id=partition_id
        )
        invalid_partition_timestamp = cast_frame.filter(
            (pl.col('timestamp_ms') < day_start_epoch_ms)
            | (pl.col('timestamp_ms') >= day_end_epoch_ms)
        )
        if invalid_partition_timestamp.height > 0:
            row_number = int(invalid_partition_timestamp.get_column('_row_number').item(0))
            timestamp_ms = int(
                invalid_partition_timestamp.get_column('timestamp_ms').item(0)
            )
            raise RuntimeError(
                'CSV row timestamp must fall inside partition UTC day window, '
                f'row={row_number}, partition_id={partition_id}, '
                f'timestamp_ms={timestamp_ms}'
            )

    is_buyer_maker = _bool_series_from_text(
        cast_frame.select('_row_number', 'is_buyer_maker_raw'),
        column_name='is_buyer_maker_raw',
    )
    is_best_match = _bool_series_from_text(
        cast_frame.select('_row_number', 'is_best_match_raw'),
        column_name='is_best_match_raw',
    )

    if partition_id is None:
        partition_series = (
            cast_frame.select(
                pl.from_epoch('timestamp_ms', time_unit='ms')
                .dt.strftime('%Y-%m-%d')
                .alias('partition_id')
            )
            .get_column('partition_id')
            .cast(pl.Utf8)
        )
    else:
        partition_series = pl.Series(
            'partition_id',
            [partition_id] * cast_frame.height,
            dtype=pl.Utf8,
        )

    return pl.DataFrame(
        {
            'partition_id': partition_series,
            'trade_id': cast_frame.get_column('_trade_id_int').cast(pl.Int64),
            'price_text': cast_frame.get_column('price_text').cast(pl.Utf8),
            'quantity_text': cast_frame.get_column('quantity_text').cast(pl.Utf8),
            'quote_quantity_text': cast_frame.get_column('quote_quantity_text').cast(
                pl.Utf8
            ),
            'timestamp_ms': cast_frame.get_column('timestamp_ms').cast(pl.Int64),
            'is_buyer_maker': is_buyer_maker.cast(pl.Boolean),
            'is_best_match': is_best_match.cast(pl.Boolean),
        }
    )


def parse_binance_spot_trade_csv(
    csv_content: bytes, *, partition_id: str | None = None
) -> list[BinanceSpotTradeEvent]:
    frame = parse_binance_spot_trade_csv_frame(
        csv_content,
        partition_id=partition_id,
    )
    if frame.height == 0:
        raise RuntimeError('CSV payload produced zero spot trade events')
    return [
        BinanceSpotTradeEvent(
            partition_id=str(partition_value),
            trade_id=int(trade_id),
            price_text=str(price_text),
            quantity_text=str(quantity_text),
            quote_quantity_text=str(quote_quantity_text),
            timestamp_ms=int(timestamp_ms),
            is_buyer_maker=bool(is_buyer_maker),
            is_best_match=bool(is_best_match),
        )
        for partition_value, trade_id, price_text, quantity_text, quote_quantity_text, timestamp_ms, is_buyer_maker, is_best_match in frame.iter_rows()
    ]


def _write_events_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str | None,
    events: list[BinanceSpotTradeEvent],
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

    if fast_insert_mode == _FAST_INSERT_MODE_ASSUME_NEW_PARTITION and events != []:
        resolved_partition_id = partition_id
        if resolved_partition_id is None:
            event_partition_ids = {event.partition_id for event in events}
            if len(event_partition_ids) != 1:
                raise RuntimeError(
                    'Binance fast canonical insert requires exactly one partition_id '
                    f'per batch, got={sorted(event_partition_ids)}'
                )
            resolved_partition_id = next(iter(event_partition_ids))
        if resolved_partition_id.strip() == '':
            raise RuntimeError('partition_id must be non-empty')
        existing_rows = client.execute(
            f'''
            SELECT 1
            FROM {database}.canonical_event_log
            WHERE source_id = %(source_id)s
              AND stream_id = %(stream_id)s
              AND partition_id = %(partition_id)s
            LIMIT 1
            ''',
            {
                'source_id': _SOURCE_ID,
                'stream_id': _STREAM_ID,
                'partition_id': resolved_partition_id,
            },
        )
        if existing_rows != []:
            raise RuntimeError(
                'Binance fast canonical insert requires empty target partition; '
                f'partition already has data source={_SOURCE_ID} stream={_STREAM_ID} '
                f'partition_id={resolved_partition_id}'
            )

        ingested_at_utc_ms = int(ingested_at_utc.astimezone(UTC).timestamp() * 1000)
        idempotency_key_prefix = (
            f'{CANONICAL_EVENT_ENVELOPE_VERSION}|{_SOURCE_ID}|{_STREAM_ID}|'
            f'{resolved_partition_id}|'
        )
        envelope_versions: list[int] = []
        event_ids: list[object] = []
        source_ids: list[str] = []
        stream_ids: list[str] = []
        partition_ids: list[str] = []
        source_offsets: list[str] = []
        source_event_times: list[int] = []
        ingested_times: list[int] = []
        payload_content_types: list[str] = []
        payload_encodings: list[str] = []
        payload_raws: list[bytes] = []
        payload_sha256_raws: list[str] = []
        payload_jsons: list[str] = []
        inserted_count = 0
        first_offset: str | None = None
        last_offset: str | None = None
        first_event_id: str | None = None
        last_event_id: str | None = None

        def _flush_chunk() -> None:
            nonlocal inserted_count
            if event_ids == []:
                return
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
                [
                    envelope_versions,
                    event_ids,
                    source_ids,
                    stream_ids,
                    partition_ids,
                    source_offsets,
                    source_event_times,
                    ingested_times,
                    payload_content_types,
                    payload_encodings,
                    payload_raws,
                    payload_sha256_raws,
                    payload_jsons,
                ],
                columnar=True,
            )
            inserted_count += len(event_ids)
            envelope_versions.clear()
            event_ids.clear()
            source_ids.clear()
            stream_ids.clear()
            partition_ids.clear()
            source_offsets.clear()
            source_event_times.clear()
            ingested_times.clear()
            payload_content_types.clear()
            payload_encodings.clear()
            payload_raws.clear()
            payload_sha256_raws.clear()
            payload_jsons.clear()

        for event in events:
            source_offset = str(event.trade_id)
            payload_json = _payload_json_for_event(event)
            payload_raw = payload_json.encode(_PAYLOAD_ENCODING)
            payload_sha256_raw = hashlib.sha256(payload_raw).hexdigest()
            event_id = canonical_event_id_from_key(
                idempotency_key_prefix + source_offset
            )
            if first_offset is None:
                first_offset = source_offset
                first_event_id = str(event_id)
            last_offset = source_offset
            last_event_id = str(event_id)
            envelope_versions.append(CANONICAL_EVENT_ENVELOPE_VERSION)
            event_ids.append(event_id)
            source_ids.append(_SOURCE_ID)
            stream_ids.append(_STREAM_ID)
            partition_ids.append(resolved_partition_id)
            source_offsets.append(source_offset)
            source_event_times.append(event.timestamp_ms)
            ingested_times.append(ingested_at_utc_ms)
            payload_content_types.append(_PAYLOAD_CONTENT_TYPE)
            payload_encodings.append(_PAYLOAD_ENCODING)
            payload_raws.append(payload_raw)
            payload_sha256_raws.append(payload_sha256_raw)
            payload_jsons.append(payload_json)
            if len(event_ids) >= _INSERT_CHUNK_SIZE:
                _flush_chunk()
        _flush_chunk()

        if first_offset is None or last_offset is None:
            raise RuntimeError('Binance fast canonical insert produced no rows')
        if first_event_id is None or last_event_id is None:
            raise RuntimeError('Binance fast canonical insert missing event IDs')

        get_canonical_runtime_audit_log().append_ingest_batch_event(
            stream_key=CanonicalStreamKey(
                source_id=_SOURCE_ID,
                stream_id=_STREAM_ID,
                partition_id=resolved_partition_id,
            ),
            event_type='canonical_ingest_batch',
            run_id=run_id,
            batch_event_count=len(events),
            inserted_count=inserted_count,
            duplicate_count=0,
            first_source_offset_or_equivalent=first_offset,
            last_source_offset_or_equivalent=last_offset,
            first_event_id=first_event_id,
            last_event_id=last_event_id,
        )

        return {
            'rows_processed': len(events),
            'rows_inserted': inserted_count,
            'rows_duplicate': 0,
        }

    writer = CanonicalEventWriter(
        client=client,
        database=database,
        quarantine_registry=NoopStreamQuarantineRegistry(),
    )
    write_inputs: list[CanonicalEventWriteInput] = []
    for event in events:
        resolved_partition_id = partition_id if partition_id is not None else event.partition_id
        if resolved_partition_id.strip() == '':
            raise RuntimeError('partition_id must be non-empty')
        source_offset = str(event.trade_id)
        source_event_time_utc = datetime.fromtimestamp(event.timestamp_ms / 1000.0, tz=UTC)
        payload_json = _payload_json_for_event(event)
        payload_raw = payload_json.encode(_PAYLOAD_ENCODING)
        payload_sha256_raw = hashlib.sha256(payload_raw).hexdigest()
        write_inputs.append(
            CanonicalEventWriteInput(
                source_id=_SOURCE_ID,
                stream_id=_STREAM_ID,
                partition_id=resolved_partition_id,
                source_offset_or_equivalent=source_offset,
                source_event_time_utc=source_event_time_utc,
                ingested_at_utc=ingested_at_utc,
                payload_content_type=_PAYLOAD_CONTENT_TYPE,
                payload_encoding=_PAYLOAD_ENCODING,
                payload_raw=payload_raw,
                payload_json_precanonical=payload_json,
                payload_sha256_raw_precomputed=payload_sha256_raw,
                run_id=run_id,
            )
        )
    results = writer.write_events(write_inputs)

    inserted = 0
    duplicate = 0
    for result in results:
        if result.status == 'inserted':
            inserted += 1
        elif result.status == 'duplicate':
            duplicate += 1
        else:
            raise RuntimeError(f'Unexpected canonical writer status: {result.status}')

    return {
        'rows_processed': len(events),
        'rows_inserted': inserted,
        'rows_duplicate': duplicate,
    }


def _insert_binance_frame_via_clickhouse_transform_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    resolved_partition_id: str,
    frame: pl.DataFrame,
    ingested_at_utc: datetime,
) -> int:
    row_count = frame.height
    if row_count == 0:
        raise RuntimeError(
            'Binance ClickHouse transform insert requires non-empty frame'
        )

    stage_table = f'__origo_binance_stage_{uuid4().hex}'
    trade_ids = frame.get_column('trade_id').to_list()
    price_texts = frame.get_column('price_text').to_list()
    quantity_texts = frame.get_column('quantity_text').to_list()
    quote_quantity_texts = frame.get_column('quote_quantity_text').to_list()
    timestamp_ms_values = frame.get_column('timestamp_ms').to_list()
    is_buyer_maker_values = frame.get_column('is_buyer_maker').to_list()
    is_best_match_values = frame.get_column('is_best_match').to_list()

    client.execute(
        f'''
        CREATE TABLE {database}.{stage_table}
        (
            trade_id Int64,
            price_text String,
            quantity_text String,
            quote_quantity_text String,
            timestamp_ms Int64,
            is_buyer_maker Bool,
            is_best_match Bool
        )
        ENGINE = Memory
        '''
    )
    try:
        client.execute(
            f'''
            INSERT INTO {database}.{stage_table}
            (
                trade_id,
                price_text,
                quantity_text,
                quote_quantity_text,
                timestamp_ms,
                is_buyer_maker,
                is_best_match
            )
            VALUES
            ''',
            [
                trade_ids,
                price_texts,
                quantity_texts,
                quote_quantity_texts,
                timestamp_ms_values,
                is_buyer_maker_values,
                is_best_match_values,
            ],
            columnar=True,
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
                toUUID(concat(
                    substring(event_id_hex, 1, 8), '-',
                    substring(event_id_hex, 9, 4), '-',
                    substring(event_id_hex, 13, 4), '-',
                    substring(event_id_hex, 17, 4), '-',
                    substring(event_id_hex, 21, 12)
                )) AS event_id,
                %(source_id)s AS source_id,
                %(stream_id)s AS stream_id,
                %(partition_id)s AS partition_id,
                source_offset,
                fromUnixTimestamp64Milli(timestamp_ms, 'UTC') AS source_event_time_utc,
                %(ingested_at_utc)s AS ingested_at_utc,
                %(payload_content_type)s AS payload_content_type,
                %(payload_encoding)s AS payload_encoding,
                payload_json AS payload_raw,
                lower(hex(SHA256(payload_json))) AS payload_sha256_raw,
                payload_json
            FROM
            (
                SELECT
                    timestamp_ms,
                    toString(trade_id) AS source_offset,
                    concat(
                        '{{"is_best_match":',
                        if(is_best_match, 'true', 'false'),
                        ',"is_buyer_maker":',
                        if(is_buyer_maker, 'true', 'false'),
                        ',"price":"', price_text,
                        '","qty":"', quantity_text,
                        '","quote_qty":"', quote_quantity_text,
                        '","trade_id":', toString(trade_id),
                        '}}'
                    ) AS payload_json,
                    lower(hex(uuid_bytes)) AS event_id_hex
                FROM
                (
                    SELECT
                        trade_id,
                        timestamp_ms,
                        price_text,
                        quantity_text,
                        quote_quantity_text,
                        is_buyer_maker,
                        is_best_match,
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
                            trade_id,
                            timestamp_ms,
                            price_text,
                            quantity_text,
                            quote_quantity_text,
                            is_buyer_maker,
                            is_best_match,
                            SHA1(concat(
                                unhex(%(canonical_event_namespace_hex)s),
                                concat(
                                    %(idempotency_prefix)s,
                                    toString(trade_id)
                                )
                            )) AS sha1_digest
                        FROM {database}.{stage_table}
                    )
                )
            )
            ''',
            {
                'source_id': _SOURCE_ID,
                'stream_id': _STREAM_ID,
                'partition_id': resolved_partition_id,
                'ingested_at_utc': ingested_at_utc,
                'payload_content_type': _PAYLOAD_CONTENT_TYPE,
                'payload_encoding': _PAYLOAD_ENCODING,
                'canonical_event_namespace_hex': _CANONICAL_EVENT_NAMESPACE_HEX,
                'idempotency_prefix': (
                    f'{CANONICAL_EVENT_ENVELOPE_VERSION}|'
                    f'{_SOURCE_ID}|{_STREAM_ID}|{resolved_partition_id}|'
                ),
            },
        )
    finally:
        client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')

    return row_count


def write_staged_binance_spot_trade_csv_to_canonical(
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
            'Binance staged canonical insert requires row_count > 0, '
            f'got={row_count}'
        )
    existing_rows = client.execute(
        f'''
        SELECT 1
        FROM {database}.canonical_event_log
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
            'Binance staged canonical insert requires empty target partition; '
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
            {_binance_stage_event_projection_sql(database=database, stage_table=stage_table)}
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
    first_offset = client.execute(
        f'''
        SELECT min(toInt64(trimBoth(trade_id))), max(toInt64(trimBoth(trade_id)))
        FROM {database}.{stage_table}
        '''
    )
    if first_offset == []:
        raise RuntimeError(
            'Binance staged canonical insert could not load stage bounds '
            f'for partition_id={partition_id}'
        )
    min_offset = int(first_offset[0][0])
    max_offset = int(first_offset[0][1])
    idempotency_key_prefix = (
        f'{CANONICAL_EVENT_ENVELOPE_VERSION}|{_SOURCE_ID}|{_STREAM_ID}|'
        f'{partition_id}|'
    )
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
        first_source_offset_or_equivalent=str(min_offset),
        last_source_offset_or_equivalent=str(max_offset),
        first_event_id=str(
            canonical_event_id_from_key(idempotency_key_prefix + str(min_offset))
        ),
        last_event_id=str(
            canonical_event_id_from_key(idempotency_key_prefix + str(max_offset))
        ),
    )
    return {
        'rows_processed': row_count,
        'rows_inserted': row_count,
        'rows_duplicate': 0,
    }


def write_binance_spot_trade_frame_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str | None = None,
    frame: pl.DataFrame,
    run_id: str | None,
    ingested_at_utc: datetime,
    fast_insert_mode: FastInsertMode = _FAST_INSERT_MODE_DEFAULT,
) -> dict[str, int]:
    if frame.height == 0:
        raise RuntimeError('Binance canonical frame writer requires non-empty frame')

    required_columns = {
        'partition_id',
        'trade_id',
        'price_text',
        'quantity_text',
        'quote_quantity_text',
        'timestamp_ms',
        'is_buyer_maker',
        'is_best_match',
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns != []:
        raise RuntimeError(
            f'Binance canonical frame writer missing required columns: {missing_columns}'
        )
    expected_dtypes = {
        'partition_id': pl.Utf8,
        'trade_id': pl.Int64,
        'price_text': pl.Utf8,
        'quantity_text': pl.Utf8,
        'quote_quantity_text': pl.Utf8,
        'timestamp_ms': pl.Int64,
        'is_buyer_maker': pl.Boolean,
        'is_best_match': pl.Boolean,
    }
    for column_name, expected_dtype in expected_dtypes.items():
        actual_dtype = frame.schema.get(column_name)
        if actual_dtype != expected_dtype:
            raise RuntimeError(
                'Binance canonical frame writer dtype mismatch for '
                f'column={column_name}: expected={expected_dtype}, got={actual_dtype}'
            )

    if fast_insert_mode not in {
        _FAST_INSERT_MODE_DEFAULT,
        _FAST_INSERT_MODE_ASSUME_NEW_PARTITION,
    }:
        raise RuntimeError(
            'fast_insert_mode must be one of '
            f'[{_FAST_INSERT_MODE_DEFAULT}, {_FAST_INSERT_MODE_ASSUME_NEW_PARTITION}], '
            f'got={fast_insert_mode!r}'
        )

    partition_column = frame.get_column('partition_id')
    if partition_id is None:
        unique_partition_values = sorted(
            str(value) for value in partition_column.unique().to_list()
        )
        if len(unique_partition_values) != 1:
            raise RuntimeError(
                'Binance frame writer requires exactly one partition_id when no explicit '
                f'partition_id argument is provided, got={unique_partition_values}'
            )
        resolved_partition_id = unique_partition_values[0]
    else:
        resolved_partition_id = partition_id
        if resolved_partition_id.strip() == '':
            raise RuntimeError('partition_id must be non-empty')
        mismatch_frame = frame.filter(pl.col('partition_id') != resolved_partition_id).head(
            1
        )
        if mismatch_frame.height > 0:
            observed_value = mismatch_frame.get_column('partition_id').item(0)
            raise RuntimeError(
                'Binance frame writer partition mismatch: '
                f'expected partition_id={resolved_partition_id}, '
                f'observed_sample={observed_value!r}'
            )

    if fast_insert_mode == _FAST_INSERT_MODE_DEFAULT:
        events = [
            BinanceSpotTradeEvent(
                partition_id=resolved_partition_id,
                trade_id=int(trade_id),
                price_text=str(price_text),
                quantity_text=str(quantity_text),
                quote_quantity_text=str(quote_quantity_text),
                timestamp_ms=int(timestamp_ms),
                is_buyer_maker=bool(is_buyer_maker),
                is_best_match=bool(is_best_match),
            )
            for trade_id, price_text, quantity_text, quote_quantity_text, timestamp_ms, is_buyer_maker, is_best_match in frame.select(
                'trade_id',
                'price_text',
                'quantity_text',
                'quote_quantity_text',
                'timestamp_ms',
                'is_buyer_maker',
                'is_best_match',
            ).iter_rows()
        ]
        return _write_events_to_canonical(
            client=client,
            database=database,
            partition_id=resolved_partition_id,
            events=events,
            run_id=run_id,
            ingested_at_utc=ingested_at_utc,
            fast_insert_mode=fast_insert_mode,
        )

    existing_rows = client.execute(
        f'''
        SELECT 1
        FROM {database}.canonical_event_log
        WHERE source_id = %(source_id)s
          AND stream_id = %(stream_id)s
          AND partition_id = %(partition_id)s
        LIMIT 1
        ''',
        {
            'source_id': _SOURCE_ID,
            'stream_id': _STREAM_ID,
            'partition_id': resolved_partition_id,
        },
    )
    if existing_rows != []:
        raise RuntimeError(
            'Binance fast canonical insert requires empty target partition; '
            f'partition already has data source={_SOURCE_ID} stream={_STREAM_ID} '
            f'partition_id={resolved_partition_id}'
        )

    trade_ids = frame.get_column('trade_id').to_list()
    if trade_ids == []:
        raise RuntimeError('Binance fast canonical frame insert produced no rows')
    source_offsets = [str(int(trade_id)) for trade_id in trade_ids]
    row_count = len(source_offsets)

    inserted_count = _insert_binance_frame_via_clickhouse_transform_or_raise(
        client=client,
        database=database,
        resolved_partition_id=resolved_partition_id,
        frame=frame,
        ingested_at_utc=ingested_at_utc,
    )

    idempotency_key_prefix = (
        f'{CANONICAL_EVENT_ENVELOPE_VERSION}|{_SOURCE_ID}|{_STREAM_ID}|'
        f'{resolved_partition_id}|'
    )
    first_offset = source_offsets[0]
    last_offset = source_offsets[-1]
    first_event_id = str(canonical_event_id_from_key(idempotency_key_prefix + first_offset))
    last_event_id = str(canonical_event_id_from_key(idempotency_key_prefix + last_offset))
    get_canonical_runtime_audit_log().append_ingest_batch_event(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=_STREAM_ID,
            partition_id=resolved_partition_id,
        ),
        event_type='canonical_ingest_batch',
        run_id=run_id,
        batch_event_count=row_count,
        inserted_count=inserted_count,
        duplicate_count=0,
        first_source_offset_or_equivalent=first_offset,
        last_source_offset_or_equivalent=last_offset,
        first_event_id=first_event_id,
        last_event_id=last_event_id,
    )
    return {
        'rows_processed': row_count,
        'rows_inserted': inserted_count,
        'rows_duplicate': 0,
    }


def write_binance_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str | None = None,
    events: list[BinanceSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
    fast_insert_mode: FastInsertMode = _FAST_INSERT_MODE_DEFAULT,
) -> dict[str, int]:
    return _write_events_to_canonical(
        client=client,
        database=database,
        partition_id=partition_id,
        events=events,
        run_id=run_id,
        ingested_at_utc=ingested_at_utc,
        fast_insert_mode=fast_insert_mode,
    )
