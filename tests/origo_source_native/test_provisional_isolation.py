from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.contracts import (
    WORKER_HEARTBEAT_ENV,
    Client,
    Partition,
    RevisionedSourceSpec,
    RolloutStage,
)
from origo.sources.storage import SourceStore
from origo.workers import provisional
from origo.workers.dagster_reader import DagsterReader
from origo.workers.receipts import (
    ensure_monitoring_tables,
    failed_attempts,
    reconcile_died_receipts,
)
from origo.workers.report import Reporter
from origo.workers.runtime import TickOutcome, touch_heartbeat, utc_now

from .helpers import ORIGO_DATABASE
from .test_provisional_worker import ANCHOR, KEY, NOW, RECEIPTS, Query
from .test_provisional_worker import spot as spot


def _feed(
    spec: RevisionedSourceSpec, tmp_path: Path, clock: Callable[[], datetime] = utc_now,
) -> provisional.ProvisionalFeed:
    reader = Mock(spec=DagsterReader)
    reader.backfill_owns_publication.return_value = True
    return provisional.ProvisionalFeed(
        [spec], publication_root=tmp_path / 'publication', clock=clock,
        reporter=cast(Reporter, Mock(spec=Reporter)), dagster=cast(DagsterReader, reader),
    )


def test_source_selector_accepts_only_enabled_provisional_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    enabled = provisional.selected_specs({})
    assert len(enabled) == 4
    for spec in enabled:
        assert provisional.selected_specs({'ORIGO_PROVISIONAL_SOURCE': spec.key}) == (spec,)
    for source in ('', 'unknown'):
        with pytest.raises(ValueError, match='Unknown or disabled provisional source'):
            provisional.build_feed({'ORIGO_PROVISIONAL_SOURCE': source})
    for disabled in (
        replace(enabled[0], rollout_stage=RolloutStage.DORMANT),
        replace(enabled[0], provisional=None),
    ):
        monkeypatch.setattr(provisional, 'SOURCE_REGISTRY', (disabled,))
        with pytest.raises(ValueError, match='Unknown or disabled provisional source'):
            provisional.build_feed({'ORIGO_PROVISIONAL_SOURCE': disabled.key})


def test_healthcheck_requires_its_own_source_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = provisional.selected_specs({})
    monkeypatch.setenv('ORIGO_HEARTBEAT_DIR', str(tmp_path))
    monkeypatch.setenv('ORIGO_PROVISIONAL_SOURCE', sources[0].key)
    touch_heartbeat(tmp_path / 'provisional.heartbeat')
    touch_heartbeat(tmp_path / f'provisional_{sources[1].key}.heartbeat')
    assert provisional.main(['--check']) == 1
    touch_heartbeat(tmp_path / f'provisional_{sources[0].key}.heartbeat')
    assert provisional.main(['--check']) == 0
    monkeypatch.delenv('ORIGO_PROVISIONAL_SOURCE')
    assert provisional.main(['--check']) == 0


def test_main_exports_selected_heartbeat_before_source_initialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = provisional.selected_specs({})[0]
    monkeypatch.setenv('ORIGO_PROVISIONAL_SOURCE', spec.key)
    monkeypatch.setenv('ORIGO_HEARTBEAT_DIR', str(tmp_path))
    monkeypatch.setenv(WORKER_HEARTBEAT_ENV, str(tmp_path / 'retired.heartbeat'))
    expected = tmp_path / f'provisional_{spec.key}.heartbeat'
    feed = _feed(spec, tmp_path)
    calls: list[datetime] = []

    def tick(now: datetime) -> TickOutcome:
        calls.append(now)
        return TickOutcome('provisional', now, (), ())

    def build(environ: dict[str, str], *, heartbeat: Path) -> provisional.ProvisionalFeed:
        assert heartbeat == expected
        assert environ[WORKER_HEARTBEAT_ENV] == str(expected)
        return feed

    monkeypatch.setattr(feed, 'tick', tick)
    monkeypatch.setattr(provisional, 'build_feed', build)
    assert provisional.main(['--once']) == 0
    assert len(calls) == 1


def test_selected_worker_replays_only_its_source_and_preserves_receipt_identity(
    spot: tuple[RevisionedSourceSpec, object], tmp_path: Path, query_origo: Query,
) -> None:
    spec, requests = spot
    feed = provisional.build_feed({
        'ORIGO_PROVISIONAL_SOURCE': spec.key,
        'ORIGO_SOURCE_PUBLICATION_ROOT': str(tmp_path / 'publication'),
    })
    dagster, reporter = Mock(spec=DagsterReader), Mock(spec=Reporter)
    dagster.backfill_owns_publication.return_value = True
    feed.dagster = cast(DagsterReader, dagster)
    feed.reporter = cast(Reporter, reporter)
    assert tuple(item.key for item in feed.specs) == (spec.key,)
    outcome = feed.tick(NOW)
    assert outcome.feed == 'provisional'
    assert outcome.processed == (f'{spec.key}:{KEY}',) and outcome.failed == ()
    assert not requests
    receipts = query_origo(RECEIPTS)
    assert [(row[0], row[1]) for row in receipts] == [(spec.key, 'STARTED'), (spec.key, 'OK')]
    assert isinstance(receipts[1][2], int) and receipts[1][2] > 1000


def test_newest_candidate_is_admitted_before_historical_catchup(
    spot: tuple[RevisionedSourceSpec, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, _ = spot
    feed = _feed(spec, tmp_path)
    now = NOW + timedelta(minutes=10)

    def completed(
        store: SourceStore, selected: RevisionedSourceSpec, partition: Partition, tick: datetime,
    ) -> tuple[str, None]:
        return f'{selected.key}:{partition.key}', None

    monkeypatch.setattr(feed, '_build_one', completed)
    assert spec.provisional is not None
    candidates = spec.provisional.candidates(now, ANCHOR, ())
    outcome = feed.tick(now)
    assert len(outcome.processed) > 1 and outcome.failed == ()
    assert outcome.processed == tuple(f'{spec.key}:{partition.key}' for partition in candidates)
    assert candidates[0].start > candidates[1].start


def test_source_reconciliation_preserves_other_live_workers_and_retry_history(
    origo_test_env: dict[str, str],
) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        source, other = (spec.key for spec in provisional.selected_specs({})[:2])
        now = datetime.now(UTC)
        old = now - timedelta(hours=1)
        abandoned = (source, f'{source}:mount', f'{source}:tick')
        running = (other, f'{other}:mount', f'{other}:tick', f'{source}_other')
        rows = [
            ('provisional', series, old, 'STARTED', '', old)
            for series in abandoned + running
        ]
        rows += [
            ('provisional', source, old, 'FAILED', 'RuntimeError', old + timedelta(seconds=1)),
            ('provisional', source, old, 'STARTED', '', old + timedelta(seconds=2)),
            ('provisional', source, now, 'STARTED', '', old),
            ('provisional', source, now, 'OK', '', old + timedelta(seconds=1)),
        ]
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.worker_minute_log '
            '(feed, series, minute, status, error_code, recorded_at) VALUES', rows,
        )
        assert reconcile_died_receipts(
            client, ORIGO_DATABASE, feed='provisional', now=now, source_keys=(source,),
        ) == 3
        failures = client.execute(
            f'SELECT series FROM {ORIGO_DATABASE}.worker_minute_log '
            "WHERE error_code = 'WORKER_DIED' ORDER BY series"
        )
        assert failures == [(series,) for series in sorted(abandoned)]
        attempts, last_failed = failed_attempts(
            client, ORIGO_DATABASE, feed='provisional', series=source, minute=old,
        )
        assert attempts == 2 and last_failed is not None
        for series in running:
            assert failed_attempts(
                client, ORIGO_DATABASE, feed='provisional', series=series, minute=old,
            ) == (0, None)
        assert reconcile_died_receipts(
            client, ORIGO_DATABASE, feed='provisional', now=now, source_keys=(source,),
        ) == 0
    finally:
        client.disconnect()


@pytest.mark.parametrize('mount', [False, True])
def test_required_work_retries_huge_failure_counts_with_capped_backoff(
    spot: tuple[RevisionedSourceSpec, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    mount: bool,
) -> None:
    spec, _ = spot
    last_failed = datetime.now(UTC)
    clock = [last_failed + timedelta(seconds=spec.orchestration.retry_delay - 1)]
    feed = _feed(spec, tmp_path, clock=lambda: clock[0])
    if mount:
        assert feed.tick(NOW).processed == (f'{spec.key}:{KEY}',)
        reader = Mock(spec=DagsterReader)
        reader.backfill_owns_publication.return_value = False
        reader.publication_owns_consumer.return_value = False
        feed.dagster = cast(DagsterReader, reader)

    def failures(
        client: Client, database: str, *, feed: str, series: str,
        minute: datetime | None = None, token: str | None = None,
    ) -> tuple[int, datetime]:
        assert series == (f'{spec.key}:mount' if mount else spec.key)
        assert (token is not None) == mount
        return 10**100, last_failed

    def published(*args: object, **kwargs: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(provisional, 'failed_attempts', failures)
    if mount:
        monkeypatch.setattr(provisional, 'execute_source', published)
    assert feed.tick(NOW).processed == ()
    clock[0] += timedelta(seconds=1)
    outcome = feed.tick(NOW)
    assert outcome.failed == ()
    assert outcome.processed == (f'{spec.key}:mount' if mount else f'{spec.key}:{KEY}',)
