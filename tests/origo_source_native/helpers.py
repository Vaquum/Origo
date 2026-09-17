from __future__ import annotations

import csv
import hashlib
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import UUID


REPO_ROOT = Path(__file__).resolve().parents[2]
BINANCE_FIXTURE_ROOT = REPO_ROOT / 'tests' / 'fixtures' / 'binance'
ORIGO_DATABASE = 'origo'
SEED_REVISION = 'seed'
SEED_BUILD_ID = UUID(int=1)


def seeded_columns(day: str) -> str:
    """Provenance columns that attach seeded rows to the activated partition of ``day``."""
    return (
        f"toDate('{day}') AS source_date, '{day}' AS partition_key, "
        f"'{SEED_REVISION}' AS revision, toUUID('{SEED_BUILD_ID}') AS build_id"
    )


def seed_spot_source(day: str) -> None:
    """Prepare the spot source schema and activate one canonical partition for seeded rows.

    Rows inserted into the component tables with ``seeded_columns(day)`` then appear in
    the source's current views, which the briefing queries read.
    """
    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
    from origo.sources.contracts import StateRecord
    from origo.sources.storage import SourceStore

    spec = BINANCE_SPOT_TRADES_SPEC
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        store = SourceStore(client, ORIGO_DATABASE, spec)
        store.setup(
            anchor=datetime.combine(
                spec.partitions.first_day, datetime.min.time(), timezone.utc
            )
        )
        store.insert_activation(
            StateRecord(spec.canonical.partition(day), 1, SEED_REVISION, SEED_BUILD_ID, ()),
            'seed',
        )
    finally:
        client.disconnect()
BINANCE_FUTURES_DATASET_SOURCE = 'binance_futures'


def _futures_zip_path(date_str: str) -> Path:
    return (
        BINANCE_FIXTURE_ROOT
        / 'futures'
        / 'daily'
        / 'trades'
        / 'BTCUSDT'
        / f'BTCUSDT-trades-{date_str}.zip'
    )


def _load_zip_bytes(path: Path) -> bytes:
    return path.read_bytes()


def _load_csv_bytes(path: Path) -> bytes:
    with zipfile.ZipFile(BytesIO(_load_zip_bytes(path))) as archive:
        with archive.open(archive.namelist()[0]) as csv_file:
            return csv_file.read()


def _datetime_from_timestamp(raw_timestamp: str, *, allowed_lengths: set[int]) -> datetime:
    timestamp = int(raw_timestamp)
    timestamp_length = len(raw_timestamp)

    if timestamp_length == 13 and 13 in allowed_lengths:
        return datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).replace(tzinfo=None)

    if timestamp_length == 16 and 16 in allowed_lengths:
        return datetime.fromtimestamp(timestamp / 1000000.0, tz=timezone.utc).replace(
            tzinfo=None
        )

    raise ValueError(
        f'Unsupported fixture timestamp length {timestamp_length} for value {raw_timestamp}'
    )


def load_expected_futures_trade_rows(
    date_str: str,
    *,
    limit: int | None = None,
) -> list[tuple[Any, ...]]:
    csv_bytes = _load_csv_bytes(_futures_zip_path(date_str))
    reader = csv.reader(csv_bytes.decode('utf-8').splitlines())
    rows: list[tuple[Any, ...]] = []

    first_row = next(reader, None)
    if first_row is None:
        raise ValueError('Empty futures fixture archive')

    pending_rows = []
    if first_row != ['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']:
        pending_rows.append(first_row)

    for row in pending_rows:
        dt = _datetime_from_timestamp(row[4], allowed_lengths={13})
        rows.append(
            (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                int(row[4]),
                1 if row[5].lower() == 'true' else 0,
                dt,
            )
        )
        if limit is not None and len(rows) >= limit:
            return rows

    for row in reader:
        dt = _datetime_from_timestamp(row[4], allowed_lengths={13})
        rows.append(
            (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                int(row[4]),
                1 if row[5].lower() == 'true' else 0,
                dt,
            )
        )
        if limit is not None and len(rows) >= limit:
            return rows

    return rows


def load_expected_futures_trade_count(date_str: str) -> int:
    csv_bytes = _load_csv_bytes(_futures_zip_path(date_str))
    reader = csv.reader(csv_bytes.decode('utf-8').splitlines())
    first_row = next(reader, None)
    if first_row is None:
        raise ValueError('Empty futures fixture archive')

    count = 0 if first_row == ['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker'] else 1
    for _ in reader:
        count += 1
    return count


def load_expected_futures_ledger_payload(date_str: str) -> dict[str, Any]:
    zip_path = _futures_zip_path(date_str)
    zip_bytes = _load_zip_bytes(zip_path)
    csv_bytes = _load_csv_bytes(zip_path)

    return {
        'source_date': date_str,
        'source_file': f'BTCUSDT-trades-{date_str}.zip',
        'zip_checksum': hashlib.sha256(zip_bytes).hexdigest(),
        'csv_checksum': hashlib.sha256(csv_bytes).hexdigest(),
        'source_row_count': load_expected_futures_trade_count(date_str),
    }
