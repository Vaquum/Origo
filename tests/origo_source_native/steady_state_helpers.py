"""Authentic metadata replay helpers for steady-state regressions."""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

from origo.sources.contracts import Client, Row
from origo.sources.lifecycle import SourceRuntime
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore

ROOT = Path(__file__).resolve().parents[1] / 'fixtures/steady_state'


def metadata_rows(table: str) -> list[dict[str, object]]:
    manifest = json.loads((ROOT / 'provenance.json').read_text())
    entry = manifest['tables'][table]
    packed = (ROOT / entry['file']).read_bytes()
    assert hashlib.sha256(packed).hexdigest() == entry['sha256']
    body = gzip.decompress(packed)
    assert hashlib.sha256(body).hexdigest() == entry['uncompressed_sha256']
    rows = [json.loads(line) for line in body.splitlines()]
    assert len(rows) == entry['rows']
    return cast(list[dict[str, object]], rows)


def restore_metadata(client: Client, database: str, lock_root: Path) -> tuple[SourceStore, ...]:
    anchors = {
        str(row['source_key']): datetime.fromisoformat(str(row['anchor'])).replace(tzinfo=UTC)
        for row in metadata_rows('source_anchor_log')
    }
    stores = tuple(SourceStore(client, database, spec) for spec in SOURCE_REGISTRY)
    for store in stores:
        SourceRuntime(store.spec, store, lock_root, str(uuid4())).setup(
            anchor=anchors[store.spec.key]
        )
    timestamps = {'partition_start', 'partition_end', 'activated_at', 'completed_at'}
    integers = {'provisional', 'generation', 'row_count'}
    for table in ('source_activation_log', 'source_component_log'):
        rows = metadata_rows(table)
        columns = tuple(rows[0])
        values: list[Row] = []
        for row in rows:
            items: list[object] = []
            for key in columns:
                value = row[key]
                if key in timestamps:
                    value = datetime.fromisoformat(str(value)).replace(tzinfo=UTC)
                elif key in integers:
                    value = int(str(value))
                elif key == 'build_id':
                    value = UUID(str(value))
                items.append(value)
            values.append(tuple(items))
        partition_column = columns.index(
            'partition_start' if table == 'source_activation_log' else 'completed_at'
        )
        months: dict[str, list[Row]] = {}
        for value in values:
            months.setdefault(str(value[partition_column])[:7], []).append(value)
        for month in sorted(months):
            batch = months[month]
            for offset in range(0, len(batch), 5000):
                client.execute(
                    f'INSERT INTO {database}.{table} ({", ".join(columns)}) VALUES',
                    batch[offset : offset + 5000],
                )
    return stores
