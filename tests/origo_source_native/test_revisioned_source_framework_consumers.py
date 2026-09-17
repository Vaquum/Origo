from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query.binance_spot_kline_rollups import dollar_month, time_month
from origo.sources import publication
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import Partition, RolloutStage, Snapshot, StateRecord
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles import spot_consumers
from origo.sources.profiles.formulas.huggingface_dollar import get_binance_spot_dollar_klines
from origo.sources.profiles.formulas.huggingface_time import (
    get_binance_spot_klines_from_1m_projection,
)
from origo.sources.profiles.formulas.spot_series import SPECS
from origo.sources.profiles.spot_consumers import HUGGINGFACE_DATASETS
from origo.sources.storage import SourceStore
from origo.utils.arrow_store import build_series_frame

from .test_binance_daily_source_adapter import archive_response


class FakeHfApi:
    calls: list[tuple[str, dict[str, object]]] = []

    def __init__(self, token: str) -> None:
        assert token == 'test-token'

    def create_repo(self, **kwargs: object) -> None:
        self.calls.append(('create_repo', kwargs))

    def upload_folder(self, **kwargs: object) -> None:
        self.calls.append(('upload_folder', kwargs))


def test_spot_consumers_publish_public_identities_from_one_pinned_state(
    origo_test_env: dict[str, str],
    binance_fixture_server_root_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = '2020-01-01'
    monkeypatch.setenv(
        'BINANCE_SPOT_DAILY_TRADES_BASE_URL',
        binance_fixture_server_root_url + '/spot/daily/trades/revisioned/',
    )
    monkeypatch.setattr(daily, 'get_response', archive_response)
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('HF_TOKEN', 'test-token')
    FakeHfApi.calls = []
    monkeypatch.setattr(spot_consumers, 'HfApi', FakeHfApi)
    spec = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.LIVE)
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, 'origo', spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    try:
        runtime.setup(anchor=datetime(2020, 1, 1, tzinfo=UTC))
        record = runtime.build(day)
        # The legacy table names are views over the built day, so the original monthly
        # readers produce the expected files from the same canonical rows.
        expected_root = tmp_path / 'legacy-parquet'
        for series in SPECS:
            if series.family == 'time':
                frame = time_month(interval_minutes=series.size, year=2020, month=1)
            else:
                frame = dollar_month(ratio=series.size, year=2020, month=1)
            target = expected_root / series.sub_path / '2020/01.parquet'
            target.parent.mkdir(parents=True, exist_ok=True)
            frame.write_parquet(target)
        source_root = tmp_path / spec.key

        # The mount consumer owns the public Parquet mirror and Arrow bar store.
        mount = source_root / 'mount'
        snapshot = runtime.publish('mount', str(mount))
        manifest = json.loads((mount / 'latest.json').read_text())
        assert manifest['state_token'] == snapshot.token == manifest['pinned_token']
        assert list(manifest['month_tokens']) == ['2020-01']
        assert len(manifest['files']) == 24
        for entry in manifest['files']:
            assert Path(entry['path']).is_absolute() and Path(entry['path']).is_file()
        for series in SPECS:
            month = tmp_path / 'parquet' / series.sub_path / '2020/01.parquet'
            assert_frame_equal(
                pl.read_parquet(month),
                pl.read_parquet(expected_root / series.sub_path / '2020/01.parquet'),
            )
            assert_frame_equal(
                pl.read_ipc(tmp_path / 'arrow' / series.name / 'latest.arrow'),
                build_series_frame(series, expected_root).df,
            )
        assert publication.publication_current(
            spec, 'mount', store.snapshot().token, root=tmp_path, pinned=True
        )

        # A render whose months are unchanged reuses every file and rebuilds no series.
        written = {
            entry['path']: Path(entry['path']).stat().st_mtime_ns for entry in manifest['files']
        }
        with monkeypatch.context() as patch:
            patch.setattr(publication, 'publication_current', lambda *a, **k: False)
            runtime.publish('mount', str(mount))
        repeated = json.loads((mount / 'latest.json').read_text())
        assert {e['path']: Path(e['path']).stat().st_mtime_ns for e in repeated['files']} == written

        # A provisional interval after the canonical day refreshes only its month, which is
        # January again here, and the manifest follows the pinned state.
        start = datetime(2020, 1, 2, tzinfo=UTC)
        store.insert_activation(
            StateRecord(
                Partition(
                    start.strftime('%Y-%m-%dT%H:%M:%SZ'), start, start + timedelta(minutes=1), True
                ),
                1,
                'provisional',
                uuid4(),
                (),
            ),
            'provisional-refresh',
        )
        pinned = store.snapshot()
        assert pinned.token != snapshot.token
        runtime.publish('mount', str(mount))
        refreshed = json.loads((mount / 'latest.json').read_text())
        assert refreshed['pinned_token'] == pinned.token
        assert refreshed['state_token'] == snapshot.token
        assert refreshed['month_tokens']['2020-01'] != manifest['month_tokens']['2020-01']

        # The huggingface consumer uploads the twelve public datasets from the canonical state.
        huggingface = source_root / 'huggingface'
        canonical = runtime.publish('huggingface', str(huggingface))
        manifest = json.loads((huggingface / 'latest.json').read_text())
        assert manifest['state_token'] == canonical.token == snapshot.token
        assert manifest['export_end_date'] == day
        assert len(manifest['uploads']) == 12 and len(manifest['files']) == 12
        version = huggingface / 'versions' / manifest['version']
        uploads = {upload['series']: upload for upload in manifest['uploads']}
        for series in SPECS:
            repo_id, _env, prefix, _resolution = HUGGINGFACE_DATASETS[series.name]
            upload = uploads[series.name]
            assert upload['repo_id'] == repo_id
            assert upload['file_name'] == f'{prefix}20200101.parquet'
            actual = pl.read_parquet(version / series.name / upload['file_name'])
            if series.family == 'time':
                expected = get_binance_spot_klines_from_1m_projection(
                    kline_size_seconds=series.size * 60,
                    start_date_limit='2020-01-01',
                    end_date_limit='2020-01-02 00:00:00',
                    table_name='binance_spot_klines',
                    database_name='origo',
                )
            else:
                expected = get_binance_spot_dollar_klines(
                    dollar_size=float(series.size * 1000000),
                    start_date_limit='2020-01-01',
                    end_date_limit='2020-01-02 00:00:00',
                    table_name='binance_spot_dollar_klines',
                    database_name='origo',
                )
            assert_frame_equal(actual, expected)
            assert (version / series.name / 'README.md').is_file()
            assert (
                json.loads((version / series.name / 'latest.json').read_text())['file_name']
                == (upload['file_name'])
            )
        assert [call for call, _ in FakeHfApi.calls] == ['create_repo', 'upload_folder'] * 12
        assert {kwargs['repo_id'] for call, kwargs in FakeHfApi.calls} == {
            repo for repo, _, _, _ in HUGGINGFACE_DATASETS.values()
        }
        assert all(
            kwargs['delete_patterns'] == [f'{HUGGINGFACE_DATASETS[s.name][2]}*.parquet']
            for s, (call, kwargs) in zip(
                SPECS, [c for c in FakeHfApi.calls if c[0] == 'upload_folder'], strict=True
            )
        )

        # A canonical change during a render discards the render, records the failure and
        # leaves the public roots untouched: a month removed beforehand stays absent, no
        # series flips and nothing staged remains.
        before = (mount / 'latest.json').read_bytes()
        removed = tmp_path / 'parquet' / SPECS[0].sub_path / '2020/01.parquet'
        removed.unlink()
        targets = {s.name: (tmp_path / 'arrow' / s.name / 'latest.arrow').resolve() for s in SPECS}
        original = store.snapshot
        reads = 0

        def advance_before_commit(*, canonical_only: bool = False) -> Snapshot:
            nonlocal reads
            reads += 1
            # Publication reads the canonical state, then pins; the renderer's pre-commit
            # recheck is the third read, where a canonical change must be observed.
            if reads >= 3:
                runtime.rollback(
                    record, operator='test', reason='Real-build software rollback during render'
                )
            return original(canonical_only=canonical_only)

        with monkeypatch.context() as patch:
            patch.setattr(store, 'snapshot', advance_before_commit)
            patch.setattr(publication, 'publication_current', lambda *a, **k: False)
            with pytest.raises(RuntimeError, match='state changed'):
                runtime.publish('mount', str(mount))
        assert (mount / 'latest.json').read_bytes() == before
        assert not removed.exists()
        assert {s.name: (tmp_path / 'arrow' / s.name / 'latest.arrow').resolve() for s in SPECS} == targets
        assert not list((tmp_path / 'parquet').glob('.staging-*'))
        assert store.generation(record.partition) == 2
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' AND blocking_scope='CONSUMER' AND event_type='FAILED'"
        ) == [(1,)]
        runtime.publish('mount', str(mount))
        assert removed.is_file() and not list((tmp_path / 'parquet').glob('.staging-*'))
        assert (
            json.loads((mount / 'latest.json').read_text())['state_token']
            == original(canonical_only=True).token
        )
        assert client.execute(
            "SELECT count() FROM origo.source_failure_log WHERE operation='consumer' AND event_type='RECOVERED'"
        ) == [(1,)]
    finally:
        client.disconnect()


def test_public_consumers_require_the_live_stage() -> None:
    canary = replace(BINANCE_SPOT_TRADES_SPEC, rollout_stage=RolloutStage.CANARY)
    with pytest.raises(RuntimeError, match='LIVE'):
        canary.require_enabled('publish', public=True)
    assert all(consumer.public for consumer in BINANCE_SPOT_TRADES_SPEC.consumers)
