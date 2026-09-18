from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
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
