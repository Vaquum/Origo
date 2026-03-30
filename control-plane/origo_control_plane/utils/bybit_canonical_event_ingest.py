from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from uuid import UUID, uuid4

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

_SOURCE_ID = 'bybit'
_STREAM_ID = 'bybit_spot_trades'
_SYMBOL = 'BTCUSDT'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_WRITE_EVENTS_BATCH_SIZE = 10_000
_FAST_INSERT_MODE_DEFAULT = 'writer'
_FAST_INSERT_MODE_ASSUME_NEW_PARTITION = 'assume_new_partition'
_INSERT_CHUNK_SIZE = 100_000
_CANONICAL_EVENT_NAMESPACE_HEX = 'f1d1ef1795ce4f9fbd81f9958cdf8ee5'
_EXPECTED_CSV_HEADER = (
    'timestamp',
    'symbol',
    'side',
    'size',
    'price',
    'tickDirection',
    'trdMatchID',
    'grossValue',
    'homeNotional',
    'foreignNotional',
)
_EXPECTED_CSV_HEADER_WITH_RPI = (*_EXPECTED_CSV_HEADER, 'RPI')


def _read_bybit_csv_header_or_raise(csv_content: bytes) -> tuple[str, ...]:
    first_line = io.BytesIO(csv_content).readline()
    if first_line == b'':
        raise RuntimeError('Bybit CSV payload is empty')
    decoded_line = first_line.decode('utf-8-sig').rstrip('\r\n')
    header = next(csv.reader([decoded_line]), None)
    if header is None:
        raise RuntimeError('Bybit CSV payload is empty')
    normalized_header = tuple(cell.strip() for cell in header)
    if normalized_header not in {
        _EXPECTED_CSV_HEADER,
        _EXPECTED_CSV_HEADER_WITH_RPI,
    }:
        raise RuntimeError(
            'Bybit CSV header mismatch: '
            f'expected one of [{_EXPECTED_CSV_HEADER}, {_EXPECTED_CSV_HEADER_WITH_RPI}] '
            f'got={normalized_header}'
        )
    return normalized_header


def parse_bybit_timestamp_ms_or_raise(*, raw_value: str, row_index: int) -> int:
    candidate = raw_value.strip()
    try:
        timestamp_seconds = Decimal(candidate)
    except InvalidOperation as exc:
        raise RuntimeError(
            f'Bybit CSV timestamp is invalid at line={row_index}: {raw_value}'
        ) from exc
    timestamp_ms = int(
        (timestamp_seconds * Decimal(1000)).to_integral_value(
            rounding=ROUND_HALF_UP
        )
    )
    if timestamp_ms <= 0:
        raise RuntimeError(
            f'Bybit CSV timestamp must be positive at line={row_index}, got={raw_value}'
        )
    return timestamp_ms


def _parse_decimal_text(
    raw_value: str,
    *,
    label: str,
    allow_zero: bool = False,
) -> str:
    candidate = raw_value.strip()
    if candidate == '':
        raise RuntimeError(f'{label} must be non-empty decimal text')
    try:
        parsed = Decimal(candidate)
    except InvalidOperation as exc:
        raise RuntimeError(f'{label} must be valid decimal text, got {raw_value!r}') from exc
    if allow_zero:
        if parsed < 0:
            raise RuntimeError(f'{label} must be non-negative, got {raw_value!r}')
    elif parsed <= 0:
        raise RuntimeError(f'{label} must be positive, got {raw_value!r}')
    return candidate


def _normalize_side(raw_value: str, *, row_index: int) -> str:
    side = raw_value.strip().lower()
    if side not in {'buy', 'sell'}:
        raise RuntimeError(
            f'Bybit CSV side must be Buy/Sell at line={row_index}, got={raw_value}'
        )
    return side


def _parse_trade_id_from_match_id(*, trd_match_id: str, row_index: int) -> int:
    if trd_match_id.startswith('m-'):
        raw_trade_id = trd_match_id[2:]
        if raw_trade_id == '' or not raw_trade_id.isdigit():
            raise RuntimeError(
                'Bybit CSV trdMatchID suffix must be numeric '
                f'at line={row_index}, got={trd_match_id!r}'
            )
        trade_id = int(raw_trade_id)
        if trade_id <= 0:
            raise RuntimeError(
                f'Bybit CSV trade_id must be positive at line={row_index}, got={trade_id}'
            )
        return trade_id

    try:
        parsed_uuid = UUID(trd_match_id)
    except ValueError as exc:
        raise RuntimeError(
            'Bybit CSV trdMatchID must be m-<digits> or canonical UUID '
            f'at line={row_index}, got={trd_match_id!r}'
        ) from exc
    if str(parsed_uuid) != trd_match_id.lower():
        raise RuntimeError(
            'Bybit CSV trdMatchID UUID must use canonical hyphenated form '
            f'at line={row_index}, got={trd_match_id!r}'
        )
    return row_index - 1


@dataclass(frozen=True)
class BybitSpotTradeEvent:
    symbol: str
    trade_id: int
    trd_match_id: str
    side: str
    price_text: str
    size_text: str
    quote_quantity_text: str
    timestamp: int
    event_time_utc: datetime
    tick_direction: str
    gross_value_text: str
    home_notional_text: str
    foreign_notional_text: str

    @property
    def partition_id(self) -> str:
        return self.event_time_utc.strftime('%Y-%m-%d')

    def to_payload(self) -> dict[str, object]:
        return {
            'symbol': self.symbol,
            'trade_id': self.trade_id,
            'trd_match_id': self.trd_match_id,
            'side': self.side,
            'price': self.price_text,
            'size': self.size_text,
            'quote_quantity': self.quote_quantity_text,
            'timestamp': self.timestamp,
            'tick_direction': self.tick_direction,
            'gross_value': self.gross_value_text,
            'home_notional': self.home_notional_text,
            'foreign_notional': self.foreign_notional_text,
        }

    def to_integrity_tuple(
        self,
    ) -> tuple[str, int, str, str, float, float, float, int, datetime, str, float, float, float]:
        return (
            self.symbol,
            self.trade_id,
            self.trd_match_id,
            self.side,
            float(Decimal(self.price_text)),
            float(Decimal(self.size_text)),
            float(Decimal(self.quote_quantity_text)),
            self.timestamp,
            self.event_time_utc,
            self.tick_direction,
            float(Decimal(self.gross_value_text)),
            float(Decimal(self.home_notional_text)),
            float(Decimal(self.foreign_notional_text)),
        )


def parse_bybit_spot_trade_csv_frame(
    *,
    csv_content: bytes,
    date_str: str,
    day_start_ts_utc_ms: int,
    day_end_ts_utc_ms: int,
) -> pl.DataFrame:
    header = _read_bybit_csv_header_or_raise(csv_content)
    has_rpi = header == _EXPECTED_CSV_HEADER_WITH_RPI
    raw_frame = pl.read_csv(
        io.BytesIO(csv_content),
        has_header=True,
        schema_overrides={
            'timestamp': pl.Utf8,
            'symbol': pl.Utf8,
            'side': pl.Utf8,
            'size': pl.Utf8,
            'price': pl.Utf8,
            'tickDirection': pl.Utf8,
            'trdMatchID': pl.Utf8,
            'grossValue': pl.Utf8,
            'homeNotional': pl.Utf8,
            'foreignNotional': pl.Utf8,
            'RPI': pl.Utf8,
        },
        truncate_ragged_lines=False,
    ).with_row_index(name='_line_number', offset=2)
    if raw_frame.height == 0:
        raise RuntimeError('Bybit CSV payload produced zero spot trade events')

    normalized = raw_frame.with_columns(
        pl.col('timestamp').cast(pl.Utf8).str.strip_chars().alias('_timestamp_text'),
        pl.col('symbol').cast(pl.Utf8).str.strip_chars().alias('_symbol'),
        pl.col('side').cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias('_side'),
        pl.col('size').cast(pl.Utf8).str.strip_chars().alias('_size_text'),
        pl.col('price').cast(pl.Utf8).str.strip_chars().alias('_price_text'),
        pl.col('tickDirection').cast(pl.Utf8).str.strip_chars().alias('_tick_direction'),
        pl.col('trdMatchID').cast(pl.Utf8).str.strip_chars().alias('_trd_match_id'),
        pl.col('grossValue').cast(pl.Utf8).str.strip_chars().alias('_gross_value_text'),
        pl.col('homeNotional').cast(pl.Utf8).str.strip_chars().alias('_home_notional_text'),
        pl.col('foreignNotional')
        .cast(pl.Utf8)
        .str.strip_chars()
        .alias('_foreign_notional_text'),
    )
    if has_rpi:
        normalized = normalized.with_columns(
            pl.col('RPI').cast(pl.Utf8).str.strip_chars().alias('_rpi_text')
        )

    normalized = normalized.with_columns(
        pl.col('_timestamp_text').str.split_exact('.', 1).alias('_timestamp_parts'),
        pl.col('_trd_match_id')
        .str.to_lowercase()
        .alias('_trd_match_id_lower'),
    ).with_columns(
        pl.col('_timestamp_parts').struct.field('field_0').alias('_timestamp_seconds_text'),
        pl.coalesce(
            pl.col('_timestamp_parts').struct.field('field_1'),
            pl.lit('', dtype=pl.Utf8),
        ).alias('_timestamp_fraction_text'),
    ).with_columns(
        pl.col('_timestamp_seconds_text')
        .cast(pl.Int64, strict=False)
        .alias('_timestamp_seconds_int'),
        pl.concat_str(
            [pl.col('_timestamp_fraction_text'), pl.lit('0000', dtype=pl.Utf8)]
        )
        .str.slice(0, 4)
        .alias('_timestamp_fraction_padded'),
        (
            pl.col('_trd_match_id').str.starts_with('m-')
            & pl.col('_trd_match_id').str.slice(2).str.contains(r'^\d+$')
        ).alias('_is_numeric_match_id'),
        pl.col('_trd_match_id_lower')
        .str.contains(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
        .alias('_is_uuid_match_id'),
    ).with_columns(
        (
            pl.col('_timestamp_seconds_int') * 1000
            + pl.col('_timestamp_fraction_padded').str.slice(0, 3).cast(pl.Int64)
            + pl.when(
                pl.col('_timestamp_fraction_padded').str.slice(3, 1).cast(pl.Int64)
                >= 5
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        ).alias('_timestamp_ms'),
        pl.when(pl.col('_is_numeric_match_id'))
        .then(pl.col('_trd_match_id').str.slice(2).cast(pl.Int64, strict=False))
        .when(pl.col('_is_uuid_match_id'))
        .then(pl.col('_line_number') - 1)
        .otherwise(pl.lit(None, dtype=pl.Int64))
        .alias('_trade_id_int'),
    )

    invalid_timestamp = normalized.filter(
        ~pl.col('_timestamp_text').str.contains(r'^\d+(\.\d+)?$')
        | pl.col('_timestamp_seconds_int').is_null()
        | (pl.col('_timestamp_ms') <= 0)
    )
    if invalid_timestamp.height > 0:
        line_number = int(invalid_timestamp.get_column('_line_number').item(0))
        raw_value = str(invalid_timestamp.get_column('_timestamp_text').item(0))
        raise RuntimeError(
            f'Bybit CSV timestamp is invalid at line={line_number}: {raw_value}'
        )

    outside_window = normalized.filter(
        (pl.col('_timestamp_ms') < day_start_ts_utc_ms)
        | (pl.col('_timestamp_ms') >= day_end_ts_utc_ms)
    )
    if outside_window.height > 0:
        line_number = int(outside_window.get_column('_line_number').item(0))
        timestamp_ms = int(outside_window.get_column('_timestamp_ms').item(0))
        raise RuntimeError(
            'Bybit CSV timestamp is outside requested UTC day window '
            f'at line={line_number}, date={date_str}, '
            f'day_start_ts_utc_ms={day_start_ts_utc_ms}, '
            f'day_end_ts_utc_ms={day_end_ts_utc_ms}, '
            f'timestamp_ms={timestamp_ms}'
        )

    invalid_symbol = normalized.filter(pl.col('_symbol') != _SYMBOL)
    if invalid_symbol.height > 0:
        line_number = int(invalid_symbol.get_column('_line_number').item(0))
        symbol = str(invalid_symbol.get_column('_symbol').item(0))
        raise RuntimeError(
            f'Bybit CSV symbol mismatch at line={line_number}: '
            f'expected={_SYMBOL} got={symbol}'
        )

    invalid_side = normalized.filter(~pl.col('_side').is_in(['buy', 'sell']))
    if invalid_side.height > 0:
        line_number = int(invalid_side.get_column('_line_number').item(0))
        raw_value = str(invalid_side.get_column('_side').item(0))
        raise RuntimeError(
            f'Bybit CSV side must be Buy/Sell at line={line_number}, got={raw_value}'
        )

    for column_name, label, allow_zero in (
        ('_size_text', 'size', True),
        ('_price_text', 'price', False),
        ('_gross_value_text', 'grossValue', False),
        ('_home_notional_text', 'homeNotional', True),
        ('_foreign_notional_text', 'foreignNotional', False),
    ):
        invalid_decimal = normalized.filter(
            (pl.col(column_name) == '')
            | ~pl.col(column_name).str.contains(r'^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$')
            | pl.col(column_name).cast(pl.Float64, strict=False).is_null()
            | (
                pl.col(column_name).cast(pl.Float64, strict=False) < 0
                if allow_zero
                else pl.col(column_name).cast(pl.Float64, strict=False) <= 0
            )
        )
        if invalid_decimal.height > 0:
            line_number = int(invalid_decimal.get_column('_line_number').item(0))
            raw_value = str(invalid_decimal.get_column(column_name).item(0))
            requirement = 'non-negative' if allow_zero else 'positive'
            raise RuntimeError(
                f'Bybit row {line_number} {label} must be {requirement} decimal text, '
                f'got {raw_value!r}'
            )

    invalid_tick_direction = normalized.filter(pl.col('_tick_direction') == '')
    if invalid_tick_direction.height > 0:
        line_number = int(invalid_tick_direction.get_column('_line_number').item(0))
        raise RuntimeError(
            f'Bybit CSV tickDirection must be non-empty at line={line_number}'
        )

    invalid_match_id = normalized.filter(
        (pl.col('_trd_match_id') == '')
        | (~pl.col('_is_numeric_match_id') & ~pl.col('_is_uuid_match_id'))
        | (pl.col('_trade_id_int').is_null())
        | (pl.col('_trade_id_int') <= 0)
    )
    if invalid_match_id.height > 0:
        line_number = int(invalid_match_id.get_column('_line_number').item(0))
        raw_value = str(invalid_match_id.get_column('_trd_match_id').item(0))
        raise RuntimeError(
            'Bybit CSV trdMatchID must be m-<digits> or canonical UUID '
            f'at line={line_number}, got={raw_value!r}'
        )

    if has_rpi:
        invalid_rpi = normalized.filter(
            (pl.col('_rpi_text') != '') & ~pl.col('_rpi_text').is_in(['true', 'false', '0', '1'])
        )
        if invalid_rpi.height > 0:
            line_number = int(invalid_rpi.get_column('_line_number').item(0))
            raw_value = str(invalid_rpi.get_column('_rpi_text').item(0))
            raise RuntimeError(
                f'Bybit CSV RPI must be empty or boolean-like at line={line_number}, '
                f'got={raw_value!r}'
            )

    return normalized.select(
        pl.lit(date_str).alias('partition_id'),
        pl.col('_symbol').alias('symbol'),
        pl.col('_trade_id_int').alias('trade_id'),
        pl.col('_trd_match_id').alias('trd_match_id'),
        pl.col('_side').alias('side'),
        pl.col('_price_text').alias('price'),
        pl.col('_size_text').alias('size'),
        pl.col('_foreign_notional_text').alias('quote_quantity'),
        pl.col('_timestamp_ms').alias('timestamp'),
        pl.from_epoch(pl.col('_timestamp_ms'), time_unit='ms')
        .dt.replace_time_zone('UTC')
        .alias('datetime'),
        pl.col('_tick_direction').alias('tick_direction'),
        pl.col('_gross_value_text').alias('gross_value'),
        pl.col('_home_notional_text').alias('home_notional'),
        pl.col('_foreign_notional_text').alias('foreign_notional'),
    )


def create_staged_bybit_spot_trade_csv_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    frame: pl.DataFrame,
) -> str:
    stage_table = f'__origo_bybit_stage_{uuid4().hex}'
    client.execute(
        f'''
        CREATE TABLE {database}.{stage_table}
        (
            symbol String,
            trade_id Int64,
            trd_match_id String,
            side String,
            price_text String,
            size_text String,
            quote_quantity_text String,
            timestamp_ms Int64,
            tick_direction String,
            gross_value_text String,
            home_notional_text String,
            foreign_notional_text String
        )
        ENGINE = Memory
        '''
    )
    try:
        insert_sql = (
            f'INSERT INTO {database}.{stage_table} '
            '('
            'symbol,'
            'trade_id,'
            'trd_match_id,'
            'side,'
            'price_text,'
            'size_text,'
            'quote_quantity_text,'
            'timestamp_ms,'
            'tick_direction,'
            'gross_value_text,'
            'home_notional_text,'
            'foreign_notional_text'
            ') VALUES'
        )
        for chunk in frame.iter_slices(n_rows=_INSERT_CHUNK_SIZE):
            client.execute(
                insert_sql,
                [
                    chunk.get_column('symbol').cast(pl.Utf8).to_list(),
                    chunk.get_column('trade_id').cast(pl.Int64).to_list(),
                    chunk.get_column('trd_match_id').cast(pl.Utf8).to_list(),
                    chunk.get_column('side').cast(pl.Utf8).to_list(),
                    chunk.get_column('price').cast(pl.Utf8).to_list(),
                    chunk.get_column('size').cast(pl.Utf8).to_list(),
                    chunk.get_column('quote_quantity').cast(pl.Utf8).to_list(),
                    chunk.get_column('timestamp').cast(pl.Int64).to_list(),
                    chunk.get_column('tick_direction').cast(pl.Utf8).to_list(),
                    chunk.get_column('gross_value').cast(pl.Utf8).to_list(),
                    chunk.get_column('home_notional').cast(pl.Utf8).to_list(),
                    chunk.get_column('foreign_notional').cast(pl.Utf8).to_list(),
                ],
                columnar=True,
            )
    except Exception:
        client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')
        raise
    return stage_table


def drop_staged_bybit_spot_trade_csv_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
) -> None:
    client.execute(f'DROP TABLE IF EXISTS {database}.{stage_table}')


def _bybit_stage_event_projection_sql(*, database: str, stage_table: str) -> str:
    return f'''
        SELECT
            trd_match_id_normalized AS source_offset,
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
                trd_match_id_normalized,
                timestamp_ms,
                concat(
                    '{{"foreign_notional":"', foreign_notional_text_normalized,
                    '","gross_value":"', gross_value_text_normalized,
                    '","home_notional":"', home_notional_text_normalized,
                    '","price":"', price_text_normalized,
                    '","quote_quantity":"', quote_quantity_text_normalized,
                    '","side":"', side_normalized,
                    '","size":"', size_text_normalized,
                    '","symbol":"', symbol_normalized,
                    '","tick_direction":"', tick_direction_normalized,
                    '","timestamp":', toString(timestamp_ms),
                    ',"trd_match_id":"', trd_match_id_normalized,
                    '","trade_id":', toString(trade_id_int),
                    '}}'
                ) AS payload_json,
                lower(hex(uuid_bytes)) AS event_id_hex
            FROM
            (
                SELECT
                    symbol_normalized,
                    trade_id_int,
                    trd_match_id_normalized,
                    side_normalized,
                    price_text_normalized,
                    size_text_normalized,
                    quote_quantity_text_normalized,
                    timestamp_ms,
                    tick_direction_normalized,
                    gross_value_text_normalized,
                    home_notional_text_normalized,
                    foreign_notional_text_normalized,
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
                        trimBoth(symbol) AS symbol_normalized,
                        trade_id AS trade_id_int,
                        trimBoth(trd_match_id) AS trd_match_id_normalized,
                        lower(trimBoth(side)) AS side_normalized,
                        trimBoth(price_text) AS price_text_normalized,
                        trimBoth(size_text) AS size_text_normalized,
                        trimBoth(quote_quantity_text) AS quote_quantity_text_normalized,
                        timestamp_ms,
                        trimBoth(tick_direction) AS tick_direction_normalized,
                        trimBoth(gross_value_text) AS gross_value_text_normalized,
                        trimBoth(home_notional_text) AS home_notional_text_normalized,
                        trimBoth(foreign_notional_text) AS foreign_notional_text_normalized,
                        SHA1(concat(
                            unhex(%(canonical_event_namespace_hex)s),
                            concat(%(idempotency_prefix)s, trimBoth(trd_match_id))
                        )) AS sha1_digest
                    FROM {database}.{stage_table}
                )
            )
        )
    '''


def build_bybit_partition_source_proof_from_stage_or_raise(
    *,
    client: ClickhouseClient,
    database: str,
    stage_table: str,
    partition_id: str,
    source_file_url: str,
    source_filename: str,
    gzip_sha256: str,
    csv_sha256: str,
    source_etag: str,
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
                tupleElement(materials[1], 1) AS first_offset_or_equivalent,
                tupleElement(materials[length(materials)], 1) AS last_offset_or_equivalent,
                lower(hex(SHA256(concat(
                    arrayStringConcat(
                        arrayMap(item -> tupleElement(item, 1), materials),
                        '\\n'
                    ),
                    '\\n'
                )))) AS source_offset_digest_sha256,
                lower(hex(SHA256(concat(
                    arrayStringConcat(
                        arrayMap(
                            item -> concat(
                                tupleElement(item, 1),
                                '|',
                                tupleElement(item, 2),
                                '|',
                                tupleElement(item, 3)
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
                                source_offset,
                                event_id_text,
                                payload_sha256_raw
                            )
                        )
                    ) AS materials
                FROM
                (
                    {_bybit_stage_event_projection_sql(database=database, stage_table=stage_table)}
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
            'Bybit staged source proof query returned no rows '
            f'for partition_id={partition_id}'
        )
    row = rows[0]
    return build_partition_source_proof_from_precomputed(
        stream_key=CanonicalStreamKey(
            source_id=_SOURCE_ID,
            stream_id=_STREAM_ID,
            partition_id=partition_id,
        ),
        offset_ordering='lexicographic',
        source_artifact_identity={
            'source_file_url': source_file_url,
            'source_filename': source_filename,
            'gzip_sha256': gzip_sha256,
            'csv_sha256': csv_sha256,
            'source_etag': source_etag,
        },
        source_row_count=int(row[0]),
        first_offset_or_equivalent=str(row[1]),
        last_offset_or_equivalent=str(row[2]),
        source_offset_digest_sha256=str(row[3]),
        source_identity_digest_sha256=str(row[4]),
        allow_empty_partition=False,
        allow_duplicate_offsets=True,
    )


def write_staged_bybit_spot_trade_csv_to_canonical(
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
            'Bybit staged canonical insert requires row_count > 0, '
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
            'Bybit staged canonical insert requires empty target partition; '
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
            {_bybit_stage_event_projection_sql(database=database, stage_table=stage_table)}
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
        SELECT min(trimBoth(trd_match_id)), max(trimBoth(trd_match_id))
        FROM {database}.{stage_table}
        '''
    )
    if bounds == []:
        raise RuntimeError(
            'Bybit staged canonical insert could not load stage bounds '
            f'for partition_id={partition_id}'
        )
    min_offset = str(bounds[0][0])
    max_offset = str(bounds[0][1])
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


def parse_bybit_spot_trade_csv(
    *,
    csv_content: bytes,
    date_str: str,
    day_start_ts_utc_ms: int,
    day_end_ts_utc_ms: int,
) -> list[BybitSpotTradeEvent]:
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())

    header = _read_bybit_csv_header_or_raise(csv_content)
    next(reader, None)
    expected_column_count = len(header)

    events: list[BybitSpotTradeEvent] = []
    for row_index, row in enumerate(reader, start=2):
        if len(row) != expected_column_count:
            raise RuntimeError(
                f'Bybit CSV row has unexpected column count at line={row_index}: '
                f'expected={expected_column_count} got={len(row)}'
            )
        timestamp_ms = parse_bybit_timestamp_ms_or_raise(
            raw_value=row[0],
            row_index=row_index,
        )
        if timestamp_ms < day_start_ts_utc_ms or timestamp_ms >= day_end_ts_utc_ms:
            raise RuntimeError(
                'Bybit CSV timestamp is outside requested UTC day window '
                f'at line={row_index}, date={date_str}, '
                f'day_start_ts_utc_ms={day_start_ts_utc_ms}, '
                f'day_end_ts_utc_ms={day_end_ts_utc_ms}, '
                f'timestamp_ms={timestamp_ms}'
            )
        symbol = row[1].strip()
        if symbol != _SYMBOL:
            raise RuntimeError(
                f'Bybit CSV symbol mismatch at line={row_index}: '
                f'expected={_SYMBOL} got={symbol}'
            )
        side = _normalize_side(row[2], row_index=row_index)
        size_text = _parse_decimal_text(
            row[3],
            label=f'Bybit row {row_index} size',
            allow_zero=True,
        )
        price_text = _parse_decimal_text(
            row[4],
            label=f'Bybit row {row_index} price',
        )
        tick_direction = row[5].strip()
        if tick_direction == '':
            raise RuntimeError(
                f'Bybit CSV tickDirection must be non-empty at line={row_index}'
            )
        trd_match_id = row[6].strip()
        if trd_match_id == '':
            raise RuntimeError(
                f'Bybit CSV trdMatchID must be non-empty at line={row_index}'
            )
        trade_id = _parse_trade_id_from_match_id(
            trd_match_id=trd_match_id,
            row_index=row_index,
        )
        gross_value_text = _parse_decimal_text(
            row[7],
            label=f'Bybit row {row_index} grossValue',
        )
        home_notional_text = _parse_decimal_text(
            row[8],
            label=f'Bybit row {row_index} homeNotional',
            allow_zero=True,
        )
        foreign_notional_text = _parse_decimal_text(
            row[9],
            label=f'Bybit row {row_index} foreignNotional',
        )
        if expected_column_count == len(_EXPECTED_CSV_HEADER_WITH_RPI):
            rpi_text = row[10].strip()
            if rpi_text != '' and rpi_text.lower() not in {'true', 'false', '0', '1'}:
                raise RuntimeError(
                    f'Bybit CSV RPI must be empty or boolean-like at line={row_index}, '
                    f'got={rpi_text!r}'
                )
        event_time_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC)
        quote_quantity_text = foreign_notional_text
        events.append(
            BybitSpotTradeEvent(
                symbol=symbol,
                trade_id=trade_id,
                trd_match_id=trd_match_id,
                side=side,
                price_text=price_text,
                size_text=size_text,
                quote_quantity_text=quote_quantity_text,
                timestamp=timestamp_ms,
                event_time_utc=event_time_utc,
                tick_direction=tick_direction,
                gross_value_text=gross_value_text,
                home_notional_text=home_notional_text,
                foreign_notional_text=foreign_notional_text,
            )
        )
    if events == []:
        raise RuntimeError('Bybit CSV payload produced zero spot trade events')
    return events


def build_bybit_partition_source_proof(
    *,
    partition_id: str,
    events: list[BybitSpotTradeEvent],
    source_file_url: str,
    source_filename: str,
    gzip_sha256: str,
    csv_sha256: str,
    source_etag: str,
) -> PartitionSourceProof:
    materials: list[SourceIdentityMaterial] = []
    for event in events:
        payload_json = json.dumps(
            event.to_payload(),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        source_offset = event.trd_match_id
        materials.append(
            SourceIdentityMaterial(
                source_offset_or_equivalent=source_offset,
                event_id=str(
                    canonical_event_id_from_key(
                        canonical_event_idempotency_key(
                            source_id=_SOURCE_ID,
                            stream_id=_STREAM_ID,
                            partition_id=partition_id,
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
            partition_id=partition_id,
        ),
        offset_ordering='lexicographic',
        source_artifact_identity={
            'source_file_url': source_file_url,
            'source_filename': source_filename,
            'gzip_sha256': gzip_sha256,
            'csv_sha256': csv_sha256,
            'source_etag': source_etag,
        },
        materials=materials,
        allow_empty_partition=False,
        allow_duplicate_offsets=True,
    )


def write_bybit_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[BybitSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
    fast_insert_mode: FastInsertMode = _FAST_INSERT_MODE_DEFAULT,
) -> dict[str, int]:
    canonical_events: list[dict[str, object]] = []
    for event in events:
        canonical_events.append(
            {
                'partition_id': event.partition_id,
                'source_offset_or_equivalent': event.trd_match_id,
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
    )


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
                'Bybit fast canonical insert requires exactly one partition_id '
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
                'Bybit fast canonical insert requires empty target partition; '
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
            raise RuntimeError('Bybit fast canonical insert produced no rows')
        if first_event_id is None or last_event_id is None:
            raise RuntimeError('Bybit fast canonical insert missing event IDs')

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
