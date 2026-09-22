from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily as daily
from origo.sources.adapters.binance_spot_rest import BinanceSpotProvisional
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Partition, Revision, RolloutStage, Row
from origo.sources.hashing import content_hash
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

from .test_binance_daily_source_adapter import archive_response


@pytest.fixture(params=['2017-08-17', '2020-01-01'])
def real_minutes(
    request: pytest.FixtureRequest,
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[SourceRuntime, tuple[str, ...], dict[str, tuple[Row, ...]], int]]:
    monkeypatch.setattr(daily, 'get_response', archive_response)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    day = spec.canonical.partition(str(request.param))
    archive = spec.canonical.fetch(day)
    by_minute: dict[datetime, list[Row]] = {}
    for row in archive.rows():
        instant = row[-1]
        assert isinstance(instant, datetime)
        minute = instant.replace(second=0, microsecond=0)
        by_minute.setdefault(minute, []).append(row)
    anchor = next(
        minute for minute in sorted(by_minute)
        if all(minute + timedelta(minutes=i) in by_minute for i in range(3))
    )
    rows = {
        (anchor + timedelta(minutes=i)).strftime('%Y-%m-%dT%H:%M:%SZ'):
        tuple(by_minute[anchor + timedelta(minutes=i)])
        for i in range(3)
    }

    def captured(
        self: BinanceSpotProvisional,
        partition: Partition,
        previous_evidence: str | None = None,
    ) -> Revision:
        # Replay complete minutes from the checksum-verified official archive;
        # these are unchanged source rows, not manufactured REST responses.
        minute_rows = rows[partition.key]
        digest = content_hash(minute_rows, schema_version=spec.schema_version)
        return Revision(digest, digest, archive.evidence_json, len(minute_rows), lambda: minute_rows)

    monkeypatch.setattr(BinanceSpotProvisional, 'fetch', captured)
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup(anchor=anchor)
        yield runtime, tuple(rows), rows, archive.row_count
    finally:
        client.disconnect()


def _current(store: SourceStore) -> list[Row]:
    return store.execute(
        'SELECT partition_key, provisional FROM origo.source_current_partitions '
        'WHERE source_key=%(source)s ORDER BY partition_start, provisional',
        {'source': store.spec.key},
    )


def test_real_minutes_clip_at_gap_then_canonical_day_supersedes_them(
    real_minutes: tuple[SourceRuntime, tuple[str, ...], dict[str, tuple[Row, ...]], int],
) -> None:
    runtime, keys, rows, archive_count = real_minutes
    store = runtime.store
    runtime.build(keys[2], provisional=True)
    assert _current(store) == []
    runtime.build(keys[0], provisional=True)
    assert _current(store) == [(keys[0], 1)]
    assert store.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
        (len(rows[keys[0]]),)
    ]
    runtime.build(keys[1], provisional=True)
    assert _current(store) == [(key, 1) for key in keys]
    assert store.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
        (sum(map(len, rows.values())),)
    ]
    assert store.execute('SELECT count() FROM origo.binance_spot_trades_time_current') == [(3,)]
    day = keys[0][:10]
    runtime.build(day)
    assert _current(store) == [(day, 0)]
    assert store.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [
        (archive_count,)
    ]
    assert [record.partition.key for record in store.records()] == [day]
    # The January fixture starts at midnight: the canonical day must win even
    # when its first provisional minute has the exact same partition_start.
    if day == '2020-01-01':
        assert keys[0] == '2020-01-01T00:00:00Z'


def test_preparation_replaces_existing_view_without_changing_dependent_views(
    real_minutes: tuple[SourceRuntime, tuple[str, ...], dict[str, tuple[Row, ...]], int],
) -> None:
    runtime, keys, _, _ = real_minutes
    store = runtime.store
    runtime.build(keys[0], provisional=True)
    names = [f'{store.spec.names.prefix}_{component.key}_current' for component in store.spec.components]
    names.extend(name for name, _ in store.spec.aliases)
    counts = {name: store.execute(f'SELECT count() FROM origo.{name}') for name in names}
    columns = store.execute('DESCRIBE TABLE origo.source_active_partitions')
    store.execute(
        'CREATE OR REPLACE VIEW origo.source_current_partitions AS '
        'SELECT * FROM origo.source_active_partitions WHERE 0'
    )
    assert store.execute('SELECT count() FROM origo.binance_spot_trades_raw_current') == [(0,)]
    for _ in range(2):
        runtime.setup(anchor=store.anchor())
        assert store.execute('DESCRIBE TABLE origo.source_current_partitions') == columns
        assert _current(store) == [(keys[0], 1)]
        assert {name: store.execute(f'SELECT count() FROM origo.{name}') for name in names} == counts
        assert store.execute('SELECT count() FROM origo.source_activation_log') == [(1,)]
