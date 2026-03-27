from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from clickhouse_driver import Client as ClickhouseClient

from origo.events.backfill_state import (
    PartitionSourceProof,
    SourceIdentityMaterial,
    build_partition_source_proof,
)
from origo.events.envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.quarantine import NoopStreamQuarantineRegistry
from origo.events.runtime_audit import get_canonical_runtime_audit_log
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
