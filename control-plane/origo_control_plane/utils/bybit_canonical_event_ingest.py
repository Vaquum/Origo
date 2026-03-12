from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from clickhouse_driver import Client as ClickhouseClient

from origo.events.envelope import CANONICAL_EVENT_ENVELOPE_VERSION
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.runtime_audit import get_canonical_runtime_audit_log
from origo.events.writer import (
    CanonicalEventWriteInput,
    CanonicalEventWriter,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)

_SOURCE_ID = 'bybit'
_STREAM_ID = 'bybit_spot_trades'
_SYMBOL = 'BTCUSDT'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_WRITE_EVENTS_BATCH_SIZE = 10_000
_FAST_INSERT_MODE_ENV = 'ORIGO_CANONICAL_FAST_INSERT_MODE'
_FAST_INSERT_MODE_DEFAULT = 'writer'
_FAST_INSERT_MODE_ASSUME_NEW_PARTITION = 'assume_new_partition'
_INSERT_CHUNK_SIZE = 100_000
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


def _parse_decimal_text(raw_value: str, *, label: str) -> str:
    candidate = raw_value.strip()
    if candidate == '':
        raise RuntimeError(f'{label} must be non-empty decimal text')
    try:
        parsed = Decimal(candidate)
    except InvalidOperation as exc:
        raise RuntimeError(f'{label} must be valid decimal text, got {raw_value!r}') from exc
    if parsed <= 0:
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
    if not trd_match_id.startswith('m-'):
        raise RuntimeError(
            'Bybit CSV trdMatchID must use m-<digits> format '
            f'at line={row_index}, got={trd_match_id!r}'
        )
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


def parse_bybit_spot_trade_csv(
    *,
    csv_content: bytes,
    date_str: str,
    day_start_ts_utc_ms: int,
    day_end_ts_utc_ms: int,
) -> list[BybitSpotTradeEvent]:
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())

    header = next(reader, None)
    if header is None:
        raise RuntimeError('Bybit CSV payload is empty')
    if tuple(header) != _EXPECTED_CSV_HEADER:
        raise RuntimeError(
            'Bybit CSV header mismatch: '
            f'expected={_EXPECTED_CSV_HEADER} got={tuple(header)}'
        )

    events: list[BybitSpotTradeEvent] = []
    for row_index, row in enumerate(reader, start=2):
        if len(row) != 10:
            raise RuntimeError(
                f'Bybit CSV row has unexpected column count at line={row_index}: '
                f'expected=10 got={len(row)}'
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
        )
        foreign_notional_text = _parse_decimal_text(
            row[9],
            label=f'Bybit row {row_index} foreignNotional',
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


def write_bybit_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[BybitSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
    canonical_events: list[dict[str, object]] = []
    for event in events:
        canonical_events.append(
            {
                'partition_id': event.partition_id,
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
    )


def _write_events_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[dict[str, object]],
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

    writer = CanonicalEventWriter(client=client, database=database)

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
