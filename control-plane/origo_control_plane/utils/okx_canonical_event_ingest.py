from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from clickhouse_driver import Client as ClickhouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter

_SOURCE_ID = 'okx'
_STREAM_ID = 'okx_spot_trades'
_INSTRUMENT_ID = 'BTC-USDT'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'
_WRITE_EVENTS_BATCH_SIZE = 10_000
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


def write_okx_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[OKXSpotTradeEvent],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
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
        payload_json = json.dumps(
            event.to_payload(),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        payload_raw = payload_json.encode(_PAYLOAD_ENCODING)
        write_inputs.append(
            CanonicalEventWriteInput(
                source_id=_SOURCE_ID,
                stream_id=_STREAM_ID,
                partition_id=event.partition_id,
                source_offset_or_equivalent=str(event.trade_id),
                source_event_time_utc=event.event_time_utc,
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
