from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from itertools import chain

from clickhouse_driver import Client as ClickhouseClient

from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter

_SOURCE_ID = 'binance'
_STREAM_ID = 'binance_spot_trades'
_PAYLOAD_CONTENT_TYPE = 'application/json'
_PAYLOAD_ENCODING = 'utf-8'


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
    try:
        Decimal(candidate)
    except InvalidOperation as exc:
        raise RuntimeError(f'{label} must be valid decimal text, got {value!r}') from exc
    return candidate


def _parse_int(value: str, *, label: str) -> int:
    candidate = value.strip()
    try:
        return int(candidate)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be integer, got {value!r}') from exc


def _parse_timestamp_to_utc(value: str, *, label: str) -> tuple[int, datetime]:
    timestamp = _parse_int(value, label=label)
    timestamp_length = len(value.strip())
    if timestamp_length == 13:
        event_time_utc = datetime.fromtimestamp(timestamp / 1000.0, tz=UTC)
    elif timestamp_length == 16:
        event_time_utc = datetime.fromtimestamp(timestamp / 1000000.0, tz=UTC)
    else:
        raise RuntimeError(
            f'{label} must be epoch milliseconds or microseconds, got {timestamp}'
        )
    return timestamp, event_time_utc


@dataclass(frozen=True)
class BinanceSpotTradeEvent:
    trade_id: int
    price_text: str
    quantity_text: str
    quote_quantity_text: str
    timestamp: int
    event_time_utc: datetime
    is_buyer_maker: bool
    is_best_match: bool

    @property
    def partition_id(self) -> str:
        return self.event_time_utc.strftime('%Y-%m-%d')

    def to_payload(self) -> dict[str, object]:
        return {
            'trade_id': self.trade_id,
            'price': self.price_text,
            'qty': self.quantity_text,
            'quote_qty': self.quote_quantity_text,
            'is_buyer_maker': self.is_buyer_maker,
            'is_best_match': self.is_best_match,
        }

    def to_integrity_tuple(self) -> tuple[int, float, float, float, int, bool, bool, datetime]:
        return (
            self.trade_id,
            float(Decimal(self.price_text)),
            float(Decimal(self.quantity_text)),
            float(Decimal(self.quote_quantity_text)),
            self.timestamp,
            self.is_buyer_maker,
            self.is_best_match,
            self.event_time_utc,
        )


def _iter_csv_rows(csv_content: bytes) -> list[list[str]]:
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    first_row = next(reader, None)
    if first_row is None:
        raise RuntimeError('CSV payload is empty')

    first_cell = first_row[0].strip().lower() if first_row else ''
    if first_cell in {'id', 'trade_id'}:
        rows = reader
    else:
        rows = chain([first_row], reader)
    return [row for row in rows if row != []]


def parse_binance_spot_trade_csv(csv_content: bytes) -> list[BinanceSpotTradeEvent]:
    rows = _iter_csv_rows(csv_content)
    events: list[BinanceSpotTradeEvent] = []
    for row_number, row in enumerate(rows, start=1):
        if len(row) < 7:
            raise RuntimeError(
                f'CSV row {row_number} must contain at least 7 columns, got {len(row)}'
            )
        timestamp, event_time_utc = _parse_timestamp_to_utc(
            row[4],
            label=f'CSV row {row_number} timestamp',
        )
        events.append(
            BinanceSpotTradeEvent(
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
                timestamp=timestamp,
                event_time_utc=event_time_utc,
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
    events: list[dict[str, object]],
    run_id: str | None,
    ingested_at_utc: datetime,
) -> dict[str, int]:
    writer = CanonicalEventWriter(client=client, database=database)

    inserted = 0
    duplicate = 0
    for event in events:
        partition_id = str(event['partition_id'])
        source_offset = str(event['source_offset_or_equivalent'])
        source_event_time_utc = event['source_event_time_utc']
        if not isinstance(source_event_time_utc, datetime):
            raise RuntimeError('source_event_time_utc must be datetime')
        payload = event['payload']
        if not isinstance(payload, dict):
            raise RuntimeError('payload must be dict')
        payload_raw = json.dumps(
            payload,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        ).encode(_PAYLOAD_ENCODING)
        result = writer.write_event(
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
        if result.status == 'inserted':
            inserted += 1
        else:
            duplicate += 1

    return {
        'rows_processed': len(events),
        'rows_inserted': inserted,
        'rows_duplicate': duplicate,
    }


def write_binance_spot_trades_to_canonical(
    *,
    client: ClickhouseClient,
    database: str,
    events: list[BinanceSpotTradeEvent],
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
