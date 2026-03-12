from __future__ import annotations

import csv
import hashlib
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import chain

from clickhouse_driver import Client as ClickhouseClient

from origo.events.envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.runtime_audit import get_canonical_runtime_audit_log
from origo.events.writer import (
    CanonicalEventWriteInput,
    CanonicalEventWriter,
    canonical_event_id_from_key,
)

_SOURCE_ID = 'binance'
_STREAM_ID = 'binance_spot_trades'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_FAST_INSERT_MODE_ENV = 'ORIGO_CANONICAL_FAST_INSERT_MODE'
_FAST_INSERT_MODE_DEFAULT = 'writer'
_FAST_INSERT_MODE_ASSUME_NEW_PARTITION = 'assume_new_partition'
_INSERT_CHUNK_SIZE = 100000


def _parse_bool(value: str, *, label: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {'true', '1'}:
        return True
    if normalized in {'false', '0'}:
        return False
    raise RuntimeError(f'{label} must be one of true|false|1|0, got {value!r}')


def _parse_decimal_text(value: str, *, label: str) -> str:
    candidate = value.strip()
    if candidate == '':
        raise RuntimeError(f'{label} must be non-empty decimal text')
    if not any(character.isdigit() for character in candidate):
        raise RuntimeError(f'{label} must contain digits, got {value!r}')
    if any(character not in '+-.eE0123456789' for character in candidate):
        raise RuntimeError(f'{label} contains invalid decimal characters, got {value!r}')
    return candidate


def _parse_int(value: str, *, label: str) -> int:
    candidate = value.strip()
    try:
        return int(candidate)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be integer, got {value!r}') from exc


def _parse_timestamp_to_epoch_ms(value: str, *, label: str) -> int:
    timestamp = _parse_int(value, label=label)
    timestamp_length = len(value.strip())
    if timestamp_length == 13:
        return timestamp
    if timestamp_length == 16:
        return timestamp // 1000
    raise RuntimeError(
        f'{label} must be epoch milliseconds or microseconds, got {timestamp}'
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


def _iter_csv_rows(csv_content: bytes) -> Iterator[list[str]]:
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    first_row = next(reader, None)
    if first_row is None:
        raise RuntimeError('CSV payload is empty')

    first_cell = first_row[0].strip().lower() if first_row else ''
    if first_cell in {'id', 'trade_id'}:
        rows: Iterator[list[str]] = reader
    else:
        rows = chain([first_row], reader)
    for row in rows:
        if row != []:
            yield row


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
    is_best_match = 'true' if event.is_best_match else 'false'
    is_buyer_maker = 'true' if event.is_buyer_maker else 'false'
    # Keys are intentionally sorted to match canonical JSON ordering.
    return (
        '{"is_best_match":'
        + is_best_match
        + ',"is_buyer_maker":'
        + is_buyer_maker
        + ',"price":"'
        + event.price_text
        + '","qty":"'
        + event.quantity_text
        + '","quote_qty":"'
        + event.quote_quantity_text
        + '","trade_id":'
        + str(event.trade_id)
        + '}'
    )


def parse_binance_spot_trade_csv(
    csv_content: bytes, *, partition_id: str | None = None
) -> list[BinanceSpotTradeEvent]:
    day_start_epoch_ms: int | None = None
    day_end_epoch_ms: int | None = None
    if partition_id is not None:
        day_start_epoch_ms, day_end_epoch_ms = _partition_bounds_epoch_ms(
            partition_id=partition_id
        )
    rows = _iter_csv_rows(csv_content)
    events: list[BinanceSpotTradeEvent] = []
    for row_number, row in enumerate(rows, start=1):
        if len(row) < 7:
            raise RuntimeError(
                f'CSV row {row_number} must contain at least 7 columns, got {len(row)}'
            )
        timestamp_ms = _parse_timestamp_to_epoch_ms(
            row[4],
            label=f'CSV row {row_number} timestamp',
        )
        event_partition_id = partition_id
        if day_start_epoch_ms is not None and day_end_epoch_ms is not None:
            if timestamp_ms < day_start_epoch_ms or timestamp_ms >= day_end_epoch_ms:
                raise RuntimeError(
                    'CSV row timestamp must fall inside partition UTC day window, '
                    f'row={row_number}, partition_id={partition_id}, '
                    f'timestamp_ms={timestamp_ms}'
                )
        else:
            event_partition_id = datetime.fromtimestamp(
                timestamp_ms / 1000.0, tz=UTC
            ).strftime('%Y-%m-%d')
        if event_partition_id is None:
            raise RuntimeError('Parsed Binance row must resolve partition_id')
        events.append(
            BinanceSpotTradeEvent(
                partition_id=event_partition_id,
                trade_id=_parse_int(row[0], label=f'CSV row {row_number} trade_id'),
                price_text=_parse_decimal_text(
                    row[1], label=f'CSV row {row_number} price'
                ),
                quantity_text=_parse_decimal_text(
                    row[2], label=f'CSV row {row_number} quantity'
                ),
                quote_quantity_text=_parse_decimal_text(
                    row[3], label=f'CSV row {row_number} quote_quantity'
                ),
                timestamp_ms=timestamp_ms,
                is_buyer_maker=_parse_bool(
                    row[5],
                    label=f'CSV row {row_number} is_buyer_maker',
                ),
                is_best_match=_parse_bool(
                    row[6],
                    label=f'CSV row {row_number} is_best_match',
                ),
            )
        )
    if events == []:
        raise RuntimeError('CSV payload produced zero spot trade events')
    return events


def _write_events_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str | None,
    events: list[BinanceSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
    fast_insert_mode = os.environ.get(
        _FAST_INSERT_MODE_ENV,
        _FAST_INSERT_MODE_DEFAULT,
    ).strip().lower()
    if fast_insert_mode not in {
        _FAST_INSERT_MODE_DEFAULT,
        _FAST_INSERT_MODE_ASSUME_NEW_PARTITION,
    }:
        raise RuntimeError(
            f'{_FAST_INSERT_MODE_ENV} must be one of '
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

    writer = CanonicalEventWriter(client=client, database=database)
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


def write_binance_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    partition_id: str | None = None,
    events: list[BinanceSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
    return _write_events_to_canonical(
        client=client,
        database=database,
        partition_id=partition_id,
        events=events,
        run_id=run_id,
        ingested_at_utc=ingested_at_utc,
    )
