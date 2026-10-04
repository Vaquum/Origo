from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from origo import law
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.book_spool import Market
from origo.sources.adapters.book_vendor import CryptoHFTBookHourly, hour_partition
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.contracts import Partition, Revision
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore
from origo.sources.profiles.book import BOOK_COMPONENT_KEYS
from origo.law_catalog import build_catalog

from .test_book_vendor import (
    REAL_HOUR,
    original_vendor_archive as original_vendor_archive,
    original_vendor_revision as original_vendor_revision,
)


class RecordedVendorHour(CryptoHFTBookHourly):
    def __init__(self, market: str, revision: Revision) -> None:
        super().__init__(cast(Market, market))
        object.__setattr__(self, 'recorded_revision', revision)

    recorded_revision: Revision

    def discover(self, partition: Partition) -> str:
        assert partition.key == REAL_HOUR
        return self.recorded_revision.key

    def fetch(self, partition: Partition) -> Revision:
        assert partition.key == REAL_HOUR
        return self.recorded_revision

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        assert partition.key == REAL_HOUR and revision.key == self.recorded_revision.key


@pytest.fixture()
def authoritative_book_runtime(
    original_vendor_revision: tuple[str, Revision], origo_test_env: dict[str, str], tmp_path: Path
) -> Iterator[SourceRuntime]:
    market, revision = original_vendor_revision
    declared = BINANCE_SPOT_BOOK_SPEC if market == 'spot' else BINANCE_PERP_BOOK_SPEC
    spec = replace(declared, canonical=RecordedVendorHour(market, revision))
    client = make_clickhouse_client(get_clickhouse_settings())
    runtime = SourceRuntime(
        spec, SourceStore(client, 'origo', spec), tmp_path / 'locks', str(uuid4())
    )
    try:
        runtime.setup()
        yield runtime
    finally:
        client.disconnect()


def _predicate(report: law.LawReport, source: str, key: law.LawPredicate) -> law.PredicateReport:
    return next(feed for feed in report['feeds'] if feed['source_key'] == source)['predicates'][key]


def test_canary_books_have_the_same_reader_law_contract(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    now = hour_partition(REAL_HOUR).end + timedelta(seconds=30)
    empty = law.evaluate(runtime.store.client, 'origo', now)
    assert _predicate(empty, runtime.spec.key, 'R1')['reason'] == 'reader_empty'
    assert _predicate(empty, runtime.spec.key, 'C1')['status'] == 'NOT_DUE'
    runtime.build(REAL_HOUR)
    report = law.evaluate(runtime.store.client, 'origo', now)
    assert runtime.spec.rollout_stage.value == 'CANARY'
    assert {
        key
        for key in next(f for f in report['feeds'] if f['source_key'] == runtime.spec.key)[
            'predicates'
        ]
    } == {'R1', 'C1', 'C2'}
    r1 = _predicate(report, runtime.spec.key, 'R1')
    assert r1['status'] == 'PASS' and r1['reason'] == 'reader_current'
    assert [r1['evidence']['rows_' + key] for key in BOOK_COMPONENT_KEYS] == [600, 60, 1, 1]
    assert _predicate(report, runtime.spec.key, 'C1')['status'] == 'PASS'
    c2 = _predicate(report, runtime.spec.key, 'C2')
    assert (
        c2['status'] == 'FAIL'
        and c2['evidence']['expected_hours'] == 9
        and c2['evidence']['missing_hours'] == 9
    )
    projections = [p for p in report['projections'] if p['id'].startswith(runtime.spec.key + ':')]
    assert len(projections) == 8
    assert all(
        p['status'] == 'CURRENT' and p['evidence_id']
        for p in projections
        if not p['id'].endswith('_latest')
    )
    assert all(p['status'] == 'UNKNOWN' for p in projections if p['id'].endswith('_latest'))


def test_hourly_authority_deadline_and_history_are_independent_of_reader_liveness(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    now = hour_partition(REAL_HOUR).end + timedelta(minutes=16)
    report = law.evaluate(runtime.store.client, 'origo', now)
    c1 = _predicate(report, runtime.spec.key, 'C1')
    assert c1['status'] == 'FAIL' and c1['reason'] == 'canonical_hour_missing'
    assert c1['evidence']['hour'] == REAL_HOUR
    assert c1['evidence']['deadline'] == '2026-10-04T10:15:00+00:00'
    assert _predicate(report, runtime.spec.key, 'C2')['status'] == 'FAIL'


def test_hourly_authority_uses_exact_source_component_proofs(
    authoritative_book_runtime: SourceRuntime,
) -> None:
    runtime = authoritative_book_runtime
    record = runtime.build(REAL_HOUR)
    runtime.store.execute(
        "ALTER TABLE origo.source_component_log DELETE WHERE source_key=%(source)s AND component='depth200_1m' AND build_id=%(build)s SETTINGS mutations_sync=1",
        {'source': runtime.spec.key, 'build': record.build_id},
    )
    report = law.evaluate(
        runtime.store.client, 'origo', record.partition.end + timedelta(seconds=30)
    )
    assert _predicate(report, runtime.spec.key, 'R1')['status'] == 'UNKNOWN'
    assert _predicate(report, runtime.spec.key, 'C1')['status'] == 'UNKNOWN'


def test_book_sources_are_required_hourly_reader_laws() -> None:
    catalog = build_catalog('6ac86db9fbe11e53e0fa3fb4470fc7fd8f78bb70')
    gates = {g['id']: g for g in catalog['gates']}
    for source in ('binance_spot_book', 'binance_perp_book'):
        for predicate in ('R1', 'C1', 'C2'):
            key = f'law.{predicate}:{source}'
            assert key in catalog['law_gate_ids'] and gates[key]['cadence'] == 'periodic'
        assert gates[f'law.C1:{source}']['thresholds'] == {
            'delivery_grace_seconds': 900,
            'canonical_interval': 'hour',
        }
