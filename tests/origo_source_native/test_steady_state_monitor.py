"""SS-11: the monitor observes delivered data, keeps unresolved findings red, cannot pass on
missing evidence, and pages its authorities completely before advancing a cursor.

Operational fixtures only: the replayed production metadata under
``tests/fixtures/steady_state`` (no market rows), receipt rows written to a real
ClickHouse, real files with real digests, and a local stand-in for Dagit and Resend that
records every request. No mail leaves the process.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.definitions import MONITOR_CHECK_NAMES
from origo.sources.contracts import Client, Partition
from origo.steady_state.coverage import coverage_from_intervals
from origo.steady_state.monitoring import (
    PaginationOverflow,
    delivered_end,
    depth_state,
    parse_manifest,
    read_pages,
    source_age,
    verify_files,
    verify_uploads,
)
from origo.steady_state.policy import load_inventory
from origo.workers.dagster_reader import DagsterReader
from origo.workers.monitor import CHECK_NAMES, DELIVERY_LAG_SECONDS, Monitor
from origo.workers.receipts import ensure_monitoring_tables, record_receipt
from origo.workers.report import Reporter
from origo.workers.runtime import heartbeat_path, touch_heartbeat

from .helpers import ORIGO_DATABASE
from .steady_state_helpers import metadata_rows, restore_metadata
from .test_monitor import _Recorder, _settings, _url, recorder  # noqa: F401 - fixture

INVENTORY = load_inventory()
RECEIPT_COLUMNS = (
    'feed,series,minute,rows,sha256,duration_ms,status,error_code,error,worker_host,recorded_at'
)


def _real_intervals(source: str) -> tuple[tuple[Partition, ...], datetime]:
    """The replayed activation intervals of one source and its anchor."""
    latest: dict[tuple[str, bool], dict[str, object]] = {}
    for row in metadata_rows('source_activation_log'):
        if row['source_key'] != source:
            continue
        key = (str(row['partition_key']), bool(row['provisional']))
        if key not in latest or int(str(row['generation'])) > int(str(latest[key]['generation'])):
            latest[key] = row
    intervals = tuple(
        Partition(
            str(row['partition_key']),
            datetime.fromisoformat(str(row['partition_start'])).replace(tzinfo=UTC),
            datetime.fromisoformat(str(row['partition_end'])).replace(tzinfo=UTC),
            bool(row['provisional']),
        )
        for row in latest.values()
    )
    anchor = next(
        datetime.fromisoformat(str(row['anchor'])).replace(tzinfo=UTC)
        for row in metadata_rows('source_anchor_log')
        if row['source_key'] == source
    )
    return intervals, anchor


def _write(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _mount_manifest(
    root: Path, parquet_root: Path, arrow_root: Path, source: str, through: datetime, *,
    month_tokens: dict[str, str], version: str = 'v1',
) -> None:
    """A mount manifest naming real files with their real digests under the mirror roots,
    one month file per pinned series and month plus one Arrow version per series."""
    files: list[dict[str, object]] = []
    for item in INVENTORY.sources[source].series:
        name = item.name
        for month in month_tokens:
            target = parquet_root / item.sub_path / month[:4] / f'{month[5:]}.parquet'
            digest = _write(target, f'{name} {month} {month_tokens[month]}\n'.encode())
            files.append(
                {'path': str(target), 'row_count': 1, 'sha256': digest, 'series': name, 'month': month}
            )
        payload = f'{name} arrow {version}\n'.encode()
        digest = hashlib.sha256(payload).hexdigest()
        arrow = arrow_root / name / f'{name}.{digest[:16]}.arrow'
        _write(arrow, payload)
        files.append({'path': str(arrow), 'row_count': 1, 'sha256': digest, 'series': name, 'kind': 'arrow'})
    manifest = {
        'source_key': source,
        'state_token': 'state',
        'pinned_token': version,
        'active_through': through.isoformat(),
        'kind': 'mount',
        'month_tokens': month_tokens,
        'files': files,
        'version': version,
    }
    path = root / source / 'mount' / 'latest.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, sort_keys=True))


def _huggingface_manifest(
    root: Path, source: str, through: datetime, *, series: tuple[str, ...], kind: str,
    version: str = 'hf1', uploads: bool,
) -> dict[str, str]:
    """A canonical-only manifest with one real file per series; returns series -> sha256."""
    digests: dict[str, str] = {}
    files: list[dict[str, object]] = []
    listed: list[dict[str, object]] = []
    base = root / source / kind / 'versions' / version
    for name in series:
        file_name = f'{name}_kline_to_{through:%Y%m%d}.parquet'
        digests[name] = _write(base / name / file_name, f'{name} {version}\n'.encode())
        files.append({'path': f'{name}/{file_name}', 'row_count': 1, 'sha256': digests[name]})
        if uploads:
            listed.append(
                {'series': name, 'repo_id': f'vaquum/{name}', 'file_name': file_name,
                 'row_count': 1, 'sha256': digests[name]}
            )
    manifest = {
        'source_key': source,
        'state_token': 'state',
        'pinned_token': version,
        'active_through': through.isoformat(),
        'kind': kind,
        'uploads': listed,
        'files': files,
        'version': version,
    }
    path = root / source / kind / 'latest.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, sort_keys=True))
    return digests


def _depth_store(arrow_root: Path, series: str, minute: datetime, *, chunks: int) -> None:
    directory = arrow_root / series
    for offset in range(chunks):
        start = minute - timedelta(minutes=offset)
        _write(directory / start.strftime('chunks/%Y/%m/%d/%H/%Y%m%dT%H%M%SZ.arrow'), b'chunk')
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'latest.json').write_text(
        json.dumps({'series': series, 'source_partition_key': minute.strftime('%Y-%m-%dT%H:%M:%SZ')})
    )


def _series(source: str) -> tuple[str, ...]:
    return tuple(item.name for item in INVENTORY.sources[source].series)


def _emails(server: _Recorder) -> list[dict[str, object]]:
    return [body for path, body in server.posts if path == '/emails']


def _checks(server: _Recorder) -> list[dict[str, object]]:
    return [body for path, body in server.posts if path == '/report_asset_check/origo_monitor']


def _monitor(
    server: _Recorder, tmp_path: Path, client: Client, *, remote: dict[str, str] | None = None
) -> Monitor:
    remote = remote if remote is not None else {}

    def verifier(repo_id: str, file_name: str) -> str | None:
        return remote.get(f'{repo_id}/{file_name}')

    return Monitor(
        dagster=DagsterReader(_url(server), timeout_seconds=2.0),
        client=client,
        database=ORIGO_DATABASE,
        heartbeat_dir=tmp_path / 'heartbeats',
        probes=(),
        settings=_settings(server),
        reporter=Reporter(_url(server), timeout_seconds=2.0),
        cursor_path=tmp_path / 'monitor.cursor.json',
        publication_root=tmp_path / 'shadow',
        remote_verifier=verifier,
        arrow_root=tmp_path / 'arrow',
        parquet_root=tmp_path / 'parquet',
    )


def _failed_rows(
    feed: str, series: str, first_minute: datetime, stamps: list[datetime]
) -> list[tuple[object, ...]]:
    return [
        (
            feed, series, (first_minute - timedelta(minutes=index)).replace(tzinfo=None), 0, '', 1,
            'FAILED', 'PROVIDER_HTTP_503', f'unit {index}', 'host', stamp.replace(tzinfo=None),
        )
        for index, stamp in enumerate(stamps)
    ]


def test_durable_findings_missing_evidence_and_cursor_pagination(
    recorder: _Recorder, tmp_path: Path, origo_test_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    recorder.graphql['Failures'] = {'data': {'runsOrError': {'__typename': 'Runs', 'results': []}}}
    recorder.graphql['Backfills'] = {
        'data': {'partitionBackfillsOrError': {'__typename': 'PartitionBackfills', 'results': []}}
    }
    recorder.graphql['Runs'] = {'data': {'runsOrError': {'__typename': 'Runs', 'results': []}}}
    client = cast(Client, make_clickhouse_client(get_clickhouse_settings()))
    try:
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        restore_metadata(client, ORIGO_DATABASE, tmp_path / 'locks')
        # Only the provisional worker is alive here; the depth heartbeat never existed.
        touch_heartbeat(heartbeat_path(tmp_path / 'heartbeats', 'provisional'))
        (tmp_path / 'parquet').mkdir()
        now = datetime.now(UTC)
        # The spot mount publishes through the newest accepted end N, as production did.
        spot_intervals, spot_anchor = _real_intervals('binance_spot_trades')
        spot = coverage_from_intervals(spot_anchor, spot_intervals, now)
        months = {'2026-08': 'aug', '2026-09': 'sep'}
        _mount_manifest(
            tmp_path / 'shadow', tmp_path / 'parquet', tmp_path / 'arrow', 'binance_spot_trades',
            spot.newest_end, month_tokens=months,
        )
        digests = _huggingface_manifest(
            tmp_path / 'shadow', 'binance_spot_trades', spot.canonical_end,
            series=_series('binance_spot_trades'), kind='huggingface', uploads=True,
        )
        remote = {f'vaquum/{name}/{name}_kline_to_{spot.canonical_end:%Y%m%d}.parquet': digest
                  for name, digest in digests.items()}
        # depth20 committed its Arrow for the last completed minute; depth200 has nothing.
        due = now.replace(second=0, microsecond=0)
        _depth_store(tmp_path / 'arrow', 'depth20_snapshots', due - timedelta(minutes=1), chunks=29)
        # One source's coverage is unreadable: its anchor row is gone.
        client.execute(
            f"ALTER TABLE {ORIGO_DATABASE}.source_anchor_log DELETE WHERE source_key = "
            "'binance_perp_aggtrades' SETTINGS mutations_sync = 2"
        )
        # 2,600 failed depth units older than the delivery lag: six seconds of 100 rows, then
        # 900 rows stamped on one and the same second astride the first page boundary, then
        # 1,000 rows ten milliseconds apart, then 100 rows on one second at the very end.
        base = now - timedelta(minutes=10)
        stamps = [base + timedelta(seconds=index // 100) for index in range(600)]
        stamps += [base + timedelta(seconds=6)] * 900
        stamps += [base + timedelta(seconds=10, milliseconds=10 * index) for index in range(1000)]
        stamps += [base + timedelta(seconds=30)] * 100
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.worker_minute_log ({RECEIPT_COLUMNS}) VALUES',
            _failed_rows('depth', 'depth20_snapshots', due - timedelta(hours=3), stamps),
        )
        # A provisional minute failed, but the replayed coverage certifies that minute.
        covered_minute = spot_intervals[0].start
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.worker_minute_log ({RECEIPT_COLUMNS}) VALUES',
            _failed_rows('provisional', 'binance_spot_trades', covered_minute, [base]),
        )
        monitor = _monitor(recorder, tmp_path, client, remote=remote)

        first = monitor.tick(now)
        keys = set(first.failed)
        # Missing evidence is red, not an empty healthy result.
        assert {'heartbeat_missing:depth', 'depth_missing:depth200'} <= keys
        assert 'source_unreadable:binance_perp_aggtrades' in keys
        assert 'detector_failed:coverage' not in keys
        for source in ('binance_perp_trades', 'binance_spot_aggtrades', 'binance_perp_aggtrades'):
            assert f'publication_missing:{source}:mount' in keys
        assert 'publication_missing:binance_perp_aggtrades:huggingface_shadow' in keys
        # The source and its mount publisher froze together: both are measured against the
        # clock, and P is capped by the certified frontier F, not the manifest's claim.
        assert 'source_stale:binance_spot_trades' in keys
        assert 'publication_stale:binance_spot_trades:mount' in keys
        data = next(post for post in _checks(recorder) if post['check_name'] == 'data_current')
        metadata = cast(dict[str, object], data['metadata'])
        sources = cast(dict[str, dict[str, object]], metadata['sources'])
        assert sources['binance_spot_trades']['F'] == spot.contiguous_end.isoformat()
        assert sources['binance_spot_trades']['N'] == spot.newest_end.isoformat()
        assert 'binance_perp_aggtrades' not in sources
        for source in ('binance_perp_trades', 'binance_spot_aggtrades'):
            intervals, anchor = _real_intervals(source)
            expected = coverage_from_intervals(anchor, intervals, now)
            assert sources[source]['F'] == expected.contiguous_end.isoformat()
            assert f'source_stale:{source}' in keys
        publication = next(
            post for post in _checks(recorder) if post['check_name'] == 'publication_current'
        )
        consumers = cast(
            dict[str, dict[str, object]],
            cast(dict[str, object], publication['metadata'])['consumers'],
        )
        mount = consumers['binance_spot_trades:mount']
        assert mount['P'] == spot.contiguous_end.isoformat()
        assert mount['files_missing'] == 0 and mount['files_mismatched'] == 0
        assert mount['files_hashed'] == 24 and mount['files_reused'] == 12
        huggingface = consumers['binance_spot_trades:huggingface']
        assert huggingface['remote_problems'] == 0 and huggingface['files_hashed'] == 12
        assert 'publication_remote_unverified:binance_spot_trades:huggingface' not in keys
        assert 'publication_files_invalid:binance_spot_trades:huggingface' not in keys
        # depth20 is current; its chunks exist for every closed minute inside retention.
        depth = cast(dict[str, dict[str, object]], metadata['depth'])
        assert depth['depth20']['missing_closed_minutes'] == 0
        assert 'depth_stale:depth20' not in keys and 'depth_gap:depth20' not in keys
        # Every failed receipt was read across page boundaries and equal stamps.
        assert 'receipts_read_incomplete' not in keys
        assert 'detector_failed:workers' not in keys
        cursor = json.loads((tmp_path / 'monitor.cursor.json').read_text())
        assert len(cursor['open_units']) == 2600
        assert 'receipt_failed:depth:depth20_snapshots' in keys
        # The covered provisional minute resolved through certified coverage, not time.
        assert 'receipt_failed:provisional:binance_spot_trades' not in keys
        assert cursor['receipts_after'] == (now - timedelta(seconds=DELIVERY_LAG_SECONDS)).isoformat()
        # Dagit is written before the one e-mail, and the e-mail carries every new key.
        paths = [path for path, _ in recorder.posts]
        assert paths.index('/emails') == len(CHECK_NAMES)
        assert len(_emails(recorder)) == 1
        text = str(_emails(recorder)[0]['text'])
        assert '2600 unresolved unit(s)' in text
        assert 'Dagit check evaluations: written.' in text

        # A quiet tick inside the cooldown: nothing new arrives, nothing resolves, and every
        # unresolved condition is still a failed check without a second e-mail.
        second = monitor.tick(now + timedelta(minutes=1))
        assert set(second.failed) == keys
        assert len(_emails(recorder)) == 1
        workers = [post for post in _checks(recorder) if post['check_name'] == 'workers_alive']
        assert [post['passed'] for post in workers] == [False, False]
        # Files whose identity did not change are reused, not rehashed.
        publication = [
            post for post in _checks(recorder) if post['check_name'] == 'publication_current'
        ][-1]
        consumers = cast(
            dict[str, dict[str, object]],
            cast(dict[str, object], publication['metadata'])['consumers'],
        )
        assert consumers['binance_spot_trades:mount']['files_hashed'] == 0
        assert consumers['binance_spot_trades:mount']['files_reused'] == 36
        assert consumers['binance_spot_trades:huggingface']['files_hashed'] == 0

        # A late fact: stamped before the second tick's clock but delivered after that tick
        # read, inside the delivery lag the window keeps behind the clock. An OK receipt
        # resolves one unit; the other 2,600 stay red.
        late = now + timedelta(seconds=30)
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.worker_minute_log ({RECEIPT_COLUMNS}) VALUES',
            _failed_rows('depth', 'depth200_snapshots', due - timedelta(minutes=2), [late]),
        )
        record_receipt(
            client, ORIGO_DATABASE, feed='depth', series='depth20_snapshots',
            minute=due - timedelta(hours=3), rows=40, sha256='ab', duration_ms=9, status='OK',
        )
        third = monitor.tick(now + timedelta(minutes=2, seconds=30))
        assert 'receipt_failed:depth:depth200_snapshots' in third.failed
        cursor = json.loads((tmp_path / 'monitor.cursor.json').read_text())
        assert len(cursor['open_units']) == 2600
        assert f'depth|depth20_snapshots|{(due - timedelta(hours=3)).isoformat()}' not in cursor['open_units']
        assert len(_emails(recorder)) == 2
        assert 'receipt_failed:depth:depth200_snapshots' in str(_emails(recorder)[1]['text'])
        assert 'receipt_failed:depth:depth20_snapshots' not in str(_emails(recorder)[1]['text'])

        # More than a page of rows on one second cannot be read completely: the detector
        # says so and its cursor does not move past the unread rows.
        stuck = now + timedelta(minutes=3)
        client.execute(
            f'INSERT INTO {ORIGO_DATABASE}.worker_minute_log ({RECEIPT_COLUMNS}) VALUES',
            _failed_rows('depth', 'depth200_snapshots', due - timedelta(days=1), [stuck] * 1000),
        )
        fourth = monitor.tick(stuck + timedelta(seconds=DELIVERY_LAG_SECONDS + 30))
        assert 'detector_failed:workers' in fourth.failed
        assert PaginationOverflow.__name__ in str(_emails(recorder)[-1]['text'])
        assert json.loads((tmp_path / 'monitor.cursor.json').read_text())['receipts_after'] == (
            cursor['receipts_after']
        )
        assert 'heartbeat_missing:depth' in fourth.failed
    finally:
        client.disconnect()


def test_wall_clock_ages_use_the_contiguous_frontier_from_real_intervals(tmp_path: Path) -> None:
    intervals, anchor = _real_intervals('binance_spot_trades')
    at_frontier = coverage_from_intervals(anchor, intervals, datetime(2026, 9, 21, 4, 22, 30, tzinfo=UTC))
    current = source_age('binance_spot_trades', at_frontier)
    assert current.lag_seconds == 0
    assert current.due == current.contiguous_end == datetime(2026, 9, 21, 4, 22, tzinfo=UTC)
    later = coverage_from_intervals(anchor, intervals, datetime(2026, 9, 21, 13, tzinfo=UTC))
    stale = source_age('binance_spot_trades', later)
    assert stale.contiguous_end == datetime(2026, 9, 21, 4, 22, tzinfo=UTC)
    assert stale.newest_end == datetime(2026, 9, 21, 12, 24, tzinfo=UTC)
    assert stale.lag_seconds == 8 * 3600 + 38 * 60
    assert stale.canonical_end == datetime(2026, 9, 21, tzinfo=UTC)
    assert stale.missing_minutes == 474 and stale.oldest_gap_age_seconds == stale.lag_seconds
    # A manifest through N delivers no more than F; a canonical-only one no more than C.
    manifest = parse_manifest(
        json.dumps({
            'kind': 'mount', 'version': 'v', 'state_token': 's',
            'active_through': stale.newest_end.isoformat(), 'files': [],
        })
    )
    assert delivered_end(manifest, stale, canonical_only=False) == stale.contiguous_end
    assert delivered_end(manifest, stale, canonical_only=True) == stale.canonical_end
    # Depth: the committed minute against the clock and the closed minutes without chunks.
    due = datetime(2026, 9, 21, 13, tzinfo=UTC)
    _depth_store(tmp_path / 'arrow', 'depth20_snapshots', due - timedelta(minutes=5), chunks=24)
    state = depth_state(
        tmp_path / 'arrow', 'depth20_snapshots', manifest_name='latest.json',
        chunk_pattern='chunks/%Y/%m/%d/%H/%Y%m%dT%H%M%SZ.arrow', retention_minutes=30, due=due,
        close_to_arrow_seconds=180,
    )
    assert state.lag_seconds(due) == 240 and state.latest_chunk_present
    assert state.missing_minutes == (due - timedelta(minutes=4),)
    absent = depth_state(
        tmp_path / 'arrow', 'depth200_snapshots', manifest_name='latest.json',
        chunk_pattern='chunks/%Y/%m/%d/%H/%Y%m%dT%H%M%SZ.arrow', retention_minutes=30, due=due,
        close_to_arrow_seconds=180,
    )
    assert not absent.manifest_present and absent.lag_seconds(due) is None


def test_file_evidence_is_incremental_and_budgeted(tmp_path: Path) -> None:
    through = datetime(2026, 9, 21, 12, tzinfo=UTC)
    months = {'2026-08': 'aug', '2026-09': 'sep'}
    series = _series('binance_perp_trades')
    _mount_manifest(
        tmp_path / 'shadow', tmp_path / 'parquet', tmp_path / 'arrow', 'binance_perp_trades', through,
        month_tokens=months,
    )
    root = tmp_path / 'shadow' / 'binance_perp_trades' / 'mount'
    manifest = parse_manifest((root / 'latest.json').read_text())
    assert manifest.series == frozenset(series) and len(manifest.files) == 36
    state: dict[str, object] = {}
    first = verify_files(manifest, root, state)
    assert not first.invalid and first.hashed == 24 and first.reused == 12 and first.deferred == 0
    assert state['months'] == months
    again = verify_files(manifest, root, state)
    assert again.hashed == 0 and again.reused == 36
    # The current month's token changes: only that month is rehashed.
    changed = dict(months, **{'2026-09': 'sep2'})
    _mount_manifest(
        tmp_path / 'shadow', tmp_path / 'parquet', tmp_path / 'arrow', 'binance_perp_trades', through,
        month_tokens=changed,
    )
    manifest = parse_manifest((root / 'latest.json').read_text())
    third = verify_files(manifest, root, state)
    assert third.hashed == 12 and third.reused == 24 and state['months'] == changed
    # A tampered file is a mismatch even though its month token is unchanged after a reset.
    target = Path(manifest.files[0].path)
    target.write_bytes(b'corrupted')
    tampered = verify_files(manifest, root, {})
    assert tampered.mismatched == [str(target)] and tampered.hashed == 23
    target.unlink()
    assert verify_files(manifest, root, {}).missing == [str(target)]
    # An Arrow version whose name does not carry the manifest's digest is a mismatch.
    arrow = next(entry for entry in manifest.files if entry.arrow)
    renamed = Path(arrow.path).with_name(f'{arrow.series}.{"0" * 16}.arrow')
    os.replace(arrow.path, renamed)
    doctored = json.loads((root / 'latest.json').read_text())
    for entry in doctored['files']:
        if entry.get('kind') == 'arrow' and entry['series'] == arrow.series:
            entry['path'] = str(renamed)
    (root / 'latest.json').write_text(json.dumps(doctored))
    assert str(renamed) in verify_files(parse_manifest((root / 'latest.json').read_text()), root, {}).mismatched
    # The byte budget defers what it cannot hash this tick instead of assuming it.
    budgeted = verify_files(manifest, root, {}, budget_bytes=0)
    assert budgeted.deferred == 23 and budgeted.hashed == 0


def test_remote_publication_is_verified_once_per_version(tmp_path: Path) -> None:
    through = datetime(2026, 9, 21, tzinfo=UTC)
    series = _series('binance_spot_trades')
    digests = _huggingface_manifest(
        tmp_path / 'shadow', 'binance_spot_trades', through, series=series, kind='huggingface', uploads=True,
    )
    root = tmp_path / 'shadow' / 'binance_spot_trades' / 'huggingface'
    manifest = parse_manifest((root / 'latest.json').read_text())
    hub = {f'vaquum/{name}/{name}_kline_to_{through:%Y%m%d}.parquet': digest for name, digest in digests.items()}
    calls: list[str] = []

    def verifier(repo_id: str, file_name: str) -> str | None:
        calls.append(f'{repo_id}/{file_name}')
        return hub.get(f'{repo_id}/{file_name}')

    now = datetime(2026, 9, 21, 1, tzinfo=UTC)
    state: dict[str, object] = {}
    assert verify_uploads(manifest, frozenset(series), state, verifier=verifier, now=now) == ()
    assert len(calls) == 12
    assert verify_uploads(manifest, frozenset(series), state, verifier=verifier, now=now + timedelta(days=2)) == ()
    assert len(calls) == 12
    # A file the Hub does not hold, or holds with other bytes, is a problem; a failed
    # attempt is retried only after the retry interval.
    hub[f'vaquum/time_1m/time_1m_kline_to_{through:%Y%m%d}.parquet'] = 'f' * 64
    del hub[f'vaquum/dollar_1M/dollar_1M_kline_to_{through:%Y%m%d}.parquet']
    fresh: dict[str, object] = {}
    problems = verify_uploads(manifest, frozenset(series), fresh, verifier=verifier, now=now)
    assert len(problems) == 2 and any('absent' in item for item in problems)
    assert any('differs' in item for item in problems)
    assert verify_uploads(manifest, frozenset(series), fresh, verifier=verifier, now=now + timedelta(minutes=30)) == problems
    assert len(calls) == 24
    verify_uploads(manifest, frozenset(series), fresh, verifier=verifier, now=now + timedelta(hours=2))
    assert len(calls) == 36
    # An upload list short of a required series is a problem before the Hub is asked.
    shadow = _huggingface_manifest(
        tmp_path / 'shadow', 'binance_perp_aggtrades', through, series=_series('binance_perp_aggtrades'),
        kind='huggingface_shadow', uploads=False,
    )
    assert len(shadow) == 12
    local = parse_manifest((tmp_path / 'shadow' / 'binance_perp_aggtrades' / 'huggingface_shadow' / 'latest.json').read_text())
    missing = verify_uploads(local, frozenset(_series('binance_perp_aggtrades')), {}, verifier=verifier, now=now)
    assert len(missing) == 12 and len(calls) == 36


def test_read_pages_never_skips_a_boundary_second() -> None:
    base = datetime(2026, 9, 21, 12, tzinfo=UTC)
    rows = [(base + timedelta(milliseconds=250 * index), index) for index in range(2600)]
    rows[1000:1900] = [(base + timedelta(seconds=250), 10_000 + index) for index in range(900)]
    reads: list[tuple[datetime, datetime]] = []

    def reader(since: datetime, until: datetime) -> list[tuple[datetime, int]]:
        # The driver binds at second precision: bounds are truncated like ClickHouse sees them.
        reads.append((since, until))
        lower, upper = since.replace(microsecond=0), until.replace(microsecond=0)
        return sorted(
            (row for row in rows if lower < row[0] <= upper), key=lambda row: (row[0], row[1])
        )[:1000]

    result = read_pages(
        reader, lambda row: row[0], lambda row: row[1], base - timedelta(seconds=1), base + timedelta(hours=1),
    )
    assert result.complete and len(result.rows) == 2600
    assert {row[1] for row in result.rows} == {row[1] for row in rows}
    partial = read_pages(
        reader, lambda row: row[0], lambda row: row[1], base - timedelta(seconds=1),
        base + timedelta(hours=1), max_pages=1,
    )
    assert not partial.complete and partial.read_through < rows[-1][0]
    assert all(row[0] <= partial.read_through + timedelta(seconds=1) for row in partial.rows)
    rows[1000:1900] = [(base + timedelta(seconds=250), 10_000 + index) for index in range(1000)]
    with pytest.raises(PaginationOverflow):
        read_pages(reader, lambda row: row[0], lambda row: row[1], base - timedelta(seconds=1), base + timedelta(hours=1))


def test_monitor_checks_are_pinned_in_definitions_and_inventory() -> None:
    """The seven checks the monitor writes must be the checks Dagit declares and the
    inventory pins; otherwise ``data_current`` is written into a pane that never shows it."""
    assert 'data_current' in CHECK_NAMES
    assert sorted(CHECK_NAMES) == sorted(MONITOR_CHECK_NAMES)
    assert sorted(CHECK_NAMES) == sorted(INVENTORY.monitor_checks)


def test_tick_records_its_own_evaluation_time(recorder: _Recorder, tmp_path: Path) -> None:
    class _SlowClient:
        def execute(self, query: str, params: object | None = None, settings: object | None = None) -> list[tuple[object, ...]]:
            time.sleep(0.05)
            return []

        def disconnect(self) -> None:
            return None

    recorder.graphql['Failures'] = {'data': {'runsOrError': {'__typename': 'Runs', 'results': []}}}
    monitor = _monitor(recorder, tmp_path, cast(Client, _SlowClient()))
    monitor.bounds = type(monitor.bounds)(**{**monitor.bounds.__dict__, 'evaluation_seconds': 0.01})
    outcome = monitor.tick(datetime.now(UTC))
    assert 'monitor_period_exceeded' in outcome.failed
    data = next(post for post in _checks(recorder) if post['check_name'] == 'data_current')
    assert cast(dict[str, object], data['metadata'])['evaluation_seconds'] > 0.01
