from __future__ import annotations

import base64
import gzip
import hashlib
import json
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_perp_spool as spool
from origo.sources.adapters.binance_daily import Response
from origo.sources.adapters.binance_perp_daily import BinancePerpDaily, perp_csv_rows
from origo.sources.adapters.binance_perp_rest import BinancePerpProvisional
from origo.sources.binance_perp_trades import BINANCE_PERP_TRADES_SPEC
from origo.sources.contracts import Partition, Revision, SourceError
from origo.sources.hashing import content_hash
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore

_FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures/binance/futures/recent_trades/2026-09-21'
_KEY = '2026-09-21T15:11:00Z'


@pytest.fixture(scope='module')
def responses() -> tuple[tuple[datetime, list[dict[str, object]]], ...]:
    provenance = json.loads((_FIXTURE / 'provenance.json').read_text())
    bundle = (_FIXTURE / 'responses.jsonl.gz').read_bytes()
    assert hashlib.sha256(bundle).hexdigest() == provenance['sha256']
    result: list[tuple[datetime, list[dict[str, object]]]] = []
    for line in gzip.decompress(bundle).splitlines():
        record = json.loads(line)
        body = base64.b64decode(record['body_base64'])
        assert hashlib.sha256(body).hexdigest() == record['body_sha256']
        assert record['url'].endswith('/fapi/v1/trades') and record['status'] == 200
        result.append((datetime.fromisoformat(record['completed_at']), json.loads(body)))
    return tuple(result)


def _append(root: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...]) -> None:
    for received_at, rows in responses:
        spool.append_capture(root, rows, received_at=received_at)


def _partition(key: str = _KEY) -> Partition:
    return BinancePerpProvisional().partition(key)


def _actual_rows(responses: tuple[tuple[datetime, list[dict[str, object]]], ...]) -> tuple[dict[str, object], ...]:
    unique = {cast(int, row['id']): row for _, rows in responses for row in rows}
    return tuple(unique[key] for key in sorted(unique))


def _repair_from_actual(root: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...],
                        monkeypatch: pytest.MonkeyPatch, *, cap: int = 500) -> list[int]:
    actual = _actual_rows(responses)
    starts: list[int] = []
    monkeypatch.setenv('BINANCE_API_KEY', 'recorded-response-replay')
    def recorded(url: str, *, params: dict[str, str | int], headers: dict[str, str],
                 weight: int, egress_ip: str) -> Response:
        assert url.endswith('/fapi/v1/historicalTrades')
        assert params['limit'] == 500 and weight == 200 and egress_ip == '37.27.112.144'
        assert headers == {'X-MBX-APIKEY': 'recorded-response-replay'}
        first_id = cast(int, params['fromId'])
        starts.append(first_id)
        # Transport fault replay: actual recorded raw trades, not a historical provenance claim.
        page = [row for row in actual if cast(int, row['id']) >= first_id][:cap]
        return Response(json.dumps(page).encode(), {}, 200, egress_ip=egress_ip)
    monkeypatch.setattr(spool, 'get_response', recorded)
    return starts


def test_only_proven_closed_minutes_can_activate(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    partition = _partition()
    before_right = tuple(response for response in responses if cast(int, response[1][-1]['time']) < int(partition.end.timestamp() * 1000))
    _append(tmp_path, before_right)
    monkeypatch.setattr(spool, 'now_utc', lambda: partition.end + timedelta(seconds=1))
    assert spool.classify_spooled_partition(tmp_path, partition) == 'pending'
    assert spool.read_spooled_revision(tmp_path, partition) is None
    _append(tmp_path, responses[len(before_right):])
    revision = spool.read_spooled_revision(tmp_path, partition)
    assert revision is not None and revision.row_count == 9948 and revision.complete
    proof = json.loads(revision.evidence_json)['capture_proof']
    assert proof['before_start_trade_id'] < proof['first_trade_id']
    assert proof['last_trade_id'] < proof['at_or_after_end_trade_id']
    expected = tuple(spool.historical_row(row) for row in _actual_rows(responses)
                     if partition.start.timestamp() * 1000 <= cast(int, row['time']) < partition.end.timestamp() * 1000)
    assert tuple(revision.rows()) == expected
    assert revision.content_hash == content_hash(expected, schema_version=1)
    assert spool.classify_spooled_partition(tmp_path, _partition('2026-09-21T15:10:00Z')) == 'fallback'
    monkeypatch.setattr(spool, 'now_utc', lambda: responses[-1][0] + timedelta(minutes=3))
    assert spool.classify_spooled_partition(tmp_path, _partition('2026-09-21T15:13:00Z')) == 'fallback'


def test_restart_recovers_only_durable_capture(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...]) -> None:
    _append(tmp_path, responses)
    revision = spool.read_spooled_revision(tmp_path, _partition())
    assert revision is not None
    checkpoint = spool.capture_state(tmp_path)
    code = "import os,sqlite3,sys;c=sqlite3.connect(sys.argv[1]);c.execute('BEGIN IMMEDIATE');c.execute('DELETE FROM trades');c.execute('DELETE FROM state');os._exit(19)"
    crash = subprocess.run([sys.executable, '-c', code, str(tmp_path / 'capture.sqlite3')], check=False)
    assert crash.returncode == 19
    restarted = spool.read_spooled_revision(tmp_path, _partition())
    assert restarted is not None and restarted.content_hash == revision.content_hash
    recovered = spool.capture_state(tmp_path)
    assert recovered is not None and checkpoint is not None
    assert recovered.last_durable_capture_at == checkpoint.last_durable_capture_at
    assert recovered.segment_id == checkpoint.segment_id


def test_bounds_and_cleanup_preserve_unacknowledged_data(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses)
    revision = spool.read_spooled_revision(tmp_path, _partition())
    assert revision is not None
    spool.cleanup_spool(tmp_path)
    still = spool.read_spooled_revision(tmp_path, _partition())
    assert still is not None and still.content_hash == revision.content_hash
    with sqlite3.connect(tmp_path / 'capture.sqlite3') as connection:
        before = connection.execute('SELECT count(*) FROM trades').fetchone()[0]
    day = Partition('2026-09-21', datetime(2026, 9, 21, tzinfo=UTC), datetime(2026, 9, 22, tzinfo=UTC))
    spool.acknowledge_spooled_revision(tmp_path, day)
    spool.acknowledge_spooled_revision(tmp_path, day)
    with sqlite3.connect(tmp_path / 'capture.sqlite3') as connection:
        after = connection.execute('SELECT count(*) FROM trades').fetchone()[0]
    assert 0 < after < before
    durable = spool.capture_state(tmp_path)
    assert durable is not None and durable.segment_id
    (tmp_path / 'partial.bin').write_bytes(b'partial transaction delivery fault')
    monkeypatch.setattr(spool, 'SPOOL_LIMIT_BYTES', spool.spool_bytes(tmp_path))
    with pytest.raises(SourceError, match='capacity'):
        spool.append_capture(tmp_path, responses[-1][1], received_at=responses[-1][0])
    assert (tmp_path / 'partial.bin').exists()
    assert spool.capture_state(tmp_path) is not None


def test_spool_repair_and_canonical_replacement_preserve_outputs(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch, origo_test_env: dict[str, str]) -> None:
    # Isolated source builds use genuine complete-minute excerpts. The canonical
    # adapter is a fixture delivery seam; it does not claim a full-day acquisition.
    dropped = tuple(response for index, response in enumerate(responses) if not 60 <= index < 80)
    _append(tmp_path, dropped)
    starts = _repair_from_actual(tmp_path, responses, monkeypatch)
    for _ in range(20):
        spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
        if spool.classify_spooled_partition(tmp_path, _partition()) == 'ready':
            break
    revision = spool.read_spooled_revision(tmp_path, _partition())
    assert starts and revision is not None and revision.row_count == 9948
    actual = tuple(spool.historical_row(row) for row in _actual_rows(responses)
                   if _partition().start.timestamp() * 1000 <= cast(int, row['time']) < _partition().end.timestamp() * 1000)
    assert tuple(revision.rows()) == actual and revision.key == content_hash(actual, schema_version=1)
    archive_bytes = (_FIXTURE / 'minutes-15-11-15-12.csv').read_bytes()
    archive_provenance = json.loads((_FIXTURE / 'archive.provenance.json').read_text())
    assert hashlib.sha256(archive_bytes).hexdigest() == archive_provenance['extract_sha256']
    canonical_rows = tuple(perp_csv_rows(archive_bytes, BinancePerpDaily().partition('2026-09-21')))
    minute_archive = tuple(row for row in canonical_rows if _partition().start <= cast(datetime, row[-1]) < _partition().end)
    assert tuple(revision.rows()) == minute_archive

    class RecordedMinute(BinancePerpProvisional):
        def fetch(self, partition: Partition, previous_evidence: str | None = None) -> Revision:
            assert partition == _partition()
            digest = content_hash(minute_archive, schema_version=1)
            return Revision(digest, digest, '{}', len(minute_archive), lambda: iter(minute_archive))

    class RecordedArchive(BinancePerpDaily):
        def fetch(self, partition: Partition) -> Revision:
            assert partition.key == '2026-09-21'
            digest = content_hash(canonical_rows, schema_version=1)
            return Revision(digest, digest, json.dumps(archive_provenance), len(canonical_rows), lambda: iter(canonical_rows))

        def revalidate(self, partition: Partition, revision: Revision) -> None:
            assert partition.key == '2026-09-21' and revision.key == content_hash(canonical_rows, schema_version=1)

    import polars as pl
    component_hashes: list[tuple[tuple[str, str], ...]] = []
    mount_hashes: list[dict[str, str]] = []
    coverage: list[tuple[Partition, ...]] = []
    for mode in ('capture', 'whole_minute'):
        database = f'origo_spool_{mode}'
        case_root = tmp_path / mode
        monkeypatch.setenv('CLICKHOUSE_DATABASE', database)
        monkeypatch.setenv('LOCAL_PARQUET_DIR', str(case_root / 'parquet'))
        monkeypatch.setenv('LOCAL_ARROW_DIR', str(case_root / 'arrow'))
        monkeypatch.setenv('ORIGO_PERP_CAPTURE_ROOT', str(tmp_path))
        spec = replace(BINANCE_PERP_TRADES_SPEC, canonical=RecordedArchive(),
                       provisional=BinancePerpProvisional() if mode == 'capture' else RecordedMinute())
        client = make_clickhouse_client(get_clickhouse_settings())
        store = SourceStore(client, database, spec)
        runtime = SourceRuntime(spec, store, case_root / 'locks', str(uuid4()))
        try:
            runtime.setup(anchor=_partition().start)
            record = runtime.build(_KEY, provisional=True)
            component_hashes.append(record.component_hashes)
            coverage.append(store.active_intervals())
            for component in ('raw', 'time', 'dollar'):
                assert client.execute(f'SELECT count() FROM {database}.binance_perp_trades_{component}_current')[0][0] > 0
            destination = case_root / spec.key / 'mount'
            snapshot = runtime.publish('mount', str(destination))
            manifest = json.loads((destination / 'latest.json').read_text())
            assert manifest['state_token'] == snapshot.token
            assert manifest['pinned_token'] == store.snapshot().token
            mount_hashes.append({str(path.relative_to(case_root / 'parquet')): content_hash(tuple(pl.read_parquet(path).rows()), schema_version=1)
                                 for path in (case_root / 'parquet').rglob('*.parquet')})
            assert mount_hashes[-1]
            runtime.build('2026-09-21')
            assert client.execute(f'SELECT count() FROM {database}.binance_perp_trades_raw_current') == [(len(canonical_rows),)]
            canonical_snapshot = runtime.publish('mount', str(destination))
            canonical_manifest = json.loads((destination / 'latest.json').read_text())
            assert canonical_manifest['pinned_token'] == canonical_snapshot.token
            assert canonical_manifest['state_token'] == store.snapshot(canonical_only=True).token
            assert all(not item.partition.provisional for item in store.snapshot().records)
        finally:
            client.execute(f'DROP DATABASE IF EXISTS {database} SYNC')
            client.disconnect()
    assert component_hashes[0] == component_hashes[1]
    assert mount_hashes[0] == mount_hashes[1] and coverage[0] == coverage[1]
    spool.acknowledge_spooled_revision(tmp_path, Partition('2026-09-21', datetime(2026, 9, 21, tzinfo=UTC), datetime(2026, 9, 22, tzinfo=UTC)))
    assert tuple(revision.rows()) == actual  # Revision owns its copied rows during cleanup.
    assert spool.classify_spooled_partition(tmp_path, _partition()) == 'fallback'
    state = spool.capture_state(tmp_path)
    assert state is not None and state.pending_gaps == 0


def test_spool_rejects_wrong_identity_and_open_partitions(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses)
    monkeypatch.setenv('BINANCE_PERP_LATEST_SYMBOL', 'ETHUSDT')
    with pytest.raises(ValueError, match='BTCUSDT'):
        spool.read_spooled_revision(tmp_path, _partition())
    monkeypatch.delenv('BINANCE_PERP_LATEST_SYMBOL')
    with pytest.raises(ValueError, match='closed UTC'):
        spool.read_spooled_revision(tmp_path, Partition(_KEY, _partition().start, _partition().end))
    future = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
    with pytest.raises(ValueError, match='closed UTC'):
        spool.read_spooled_revision(tmp_path, _partition(future.strftime('%Y-%m-%dT%H:%M:%SZ')))
    with sqlite3.connect(tmp_path / 'capture.sqlite3') as connection:
        connection.execute("UPDATE identity SET symbol='ETHUSDT'")
    with pytest.raises(SourceError, match='source or symbol'):
        spool.read_spooled_revision(tmp_path, _partition())
    with sqlite3.connect(tmp_path / 'capture.sqlite3') as connection:
        connection.execute("UPDATE identity SET symbol='BTCUSDT'")
        connection.execute('PRAGMA user_version=99')
    assert spool.classify_spooled_partition(tmp_path, _partition()) == 'fallback'
    assert json.loads((tmp_path / 'quarantine.json').read_text())['code'] == 'CAPTURE_SCHEMA'
    assert (tmp_path / 'capture.sqlite3').exists()


def test_verified_bridge_repairs_only_missing_rows(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    left, right = 60, 80
    _append(tmp_path, responses[:left] + responses[right:])
    assert spool.classify_spooled_partition(tmp_path, _partition()) == 'pending'
    starts = _repair_from_actual(tmp_path, responses, monkeypatch, cap=200)
    for _ in range(20):
        spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
        if spool.classify_spooled_partition(tmp_path, _partition()) == 'ready':
            break
    assert starts[0] == responses[left - 1][1][-1]['id']
    assert all(a < b for a, b in pairwise(starts))
    assert spool.read_spooled_revision(tmp_path, _partition()) is not None
    with sqlite3.connect(tmp_path / 'repair.sqlite3') as connection:
        ids = [row[0] for row in connection.execute('SELECT id FROM trades ORDER BY id')]
    assert ids[0] == responses[left - 1][1][-1]['id']
    assert ids[-1] == responses[right][1][0]['id']
    assert len(ids) < 9948
    state = spool.capture_state(tmp_path)
    assert state is not None and state.pending_gaps == 0


def test_bridge_restart_and_cross_minute_reuse(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses[:30] + responses[140:])
    starts = _repair_from_actual(tmp_path, responses, monkeypatch)
    spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
    assert len(starts) == 7
    assert spool.read_spooled_revision(tmp_path, _partition()) is None
    with sqlite3.connect(tmp_path / 'repair.sqlite3') as connection:
        next_id = connection.execute('SELECT next_id FROM gaps').fetchone()[0]
    spool.repair_spooled_gaps(tmp_path, _partition('2026-09-21T15:12:00Z'), egress_ip='37.27.112.144')
    assert starts[7] == next_id and len(starts) == 14
    for _ in range(20):
        spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
        if spool.classify_spooled_partition(tmp_path, _partition()) == 'ready':
            break
    before = len(starts)
    spool.repair_spooled_gaps(tmp_path, _partition('2026-09-21T15:12:00Z'), egress_ip='37.27.112.144')
    assert len(starts) == before and len(starts) == len(set(starts))
    assert spool.read_spooled_revision(tmp_path, _partition()) is not None
    assert spool.read_spooled_revision(tmp_path, _partition('2026-09-21T15:12:00Z')) is not None


def test_modified_committed_payload_is_quarantined(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...]) -> None:
    _append(tmp_path, responses)
    with sqlite3.connect(tmp_path / 'capture.sqlite3') as connection:
        # Delivery/storage fault: omit an actual recorded row from an immutable page.
        connection.execute('DELETE FROM trades WHERE id=(SELECT min(id) FROM trades WHERE time>=?)', (int(_partition().start.timestamp() * 1000),))
    with pytest.raises(SourceError, match='page hash'):
        spool.read_spooled_revision(tmp_path, _partition())
    assert spool.classify_spooled_partition(tmp_path, _partition()) == 'fallback'
    assert json.loads((tmp_path / 'quarantine.json').read_text())['code'] == 'CAPTURE_PAYLOAD_HASH'


def test_capacity_recovers_after_acknowledged_reclamation(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses)
    ceiling = spool.spool_bytes(tmp_path) + 8 * 1024**2 + 100
    monkeypatch.setattr(spool, 'SPOOL_LIMIT_BYTES', ceiling)
    with pytest.raises(SourceError, match='capacity'):
        spool.append_capture(tmp_path, responses[-1][1], received_at=responses[-1][0])
    spool.acknowledge_spooled_revision(tmp_path, Partition('2026-09-21', datetime(2026, 9, 21, tzinfo=UTC), datetime(2026, 9, 22, tzinfo=UTC)))
    # Repeating an authentic response must remain admissible after physical reclamation.
    result = spool.append_capture(tmp_path, responses[-1][1], received_at=responses[-1][0])
    assert result.spool_bytes < ceiling and not result.advanced


def test_deleted_bridge_page_cannot_seal_minute(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses[:60] + responses[80:])
    _repair_from_actual(tmp_path, responses, monkeypatch, cap=200)
    for _ in range(20):
        spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
        if spool.classify_spooled_partition(tmp_path, _partition()) == 'ready':
            break
    assert spool.read_spooled_revision(tmp_path, _partition()) is not None
    with sqlite3.connect(tmp_path / 'repair.sqlite3') as connection:
        gap, first, last = connection.execute('SELECT gap,first_id,last_id FROM pages ORDER BY first_id LIMIT 1 OFFSET 2').fetchone()
        connection.execute('DELETE FROM pages WHERE gap=? AND first_id=?', (gap, first))
        connection.execute('DELETE FROM trades WHERE gap=? AND id BETWEEN ? AND ?', (gap, first, last))
    with pytest.raises(SourceError, match='missing request range'):
        spool.read_spooled_revision(tmp_path, _partition())
    assert spool.classify_spooled_partition(tmp_path, _partition()) == 'fallback'


def test_failed_and_empty_repairs_retain_attempt_cost(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses[:60] + responses[80:])
    monkeypatch.setenv('BINANCE_API_KEY', 'recorded-response-replay')
    def empty(url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int, egress_ip: str) -> Response:
        return Response(b'[]', {}, 200, egress_ip=egress_ip)
    monkeypatch.setattr(spool, 'get_response', empty)
    spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
    def failed(url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int, egress_ip: str) -> Response:
        raise OSError('injected replay transport interruption')
    monkeypatch.setattr(spool, 'get_response', failed)
    with pytest.raises(OSError, match='injected'):
        spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
    with sqlite3.connect(tmp_path / 'repair.sqlite3') as connection:
        attempts = connection.execute('SELECT params,outcome FROM attempts ORDER BY seq').fetchall()
        assert connection.execute('SELECT count(*) FROM pages').fetchone() == (0,)
    assert len(attempts) == 2 and sum(json.loads(row[0])['weight'] for row in attempts) == 400
    assert json.loads(attempts[0][1])['body_sha256'] == hashlib.sha256(b'[]').hexdigest()
    assert json.loads(attempts[1][1])['error_code'] == 'OSError'


def test_bridge_rejects_time_regression_across_pages(tmp_path: Path, responses: tuple[tuple[datetime, list[dict[str, object]]], ...], monkeypatch: pytest.MonkeyPatch) -> None:
    _append(tmp_path, responses[:60] + responses[80:])
    _repair_from_actual(tmp_path, responses, monkeypatch, cap=200)
    genuine_replay = spool.get_response
    calls = 0
    def reordered_time(url: str, *, params: dict[str, str | int], headers: dict[str, str], weight: int, egress_ip: str) -> Response:
        nonlocal calls
        calls += 1
        response = genuine_replay(url, params=params, headers=headers, weight=weight, egress_ip=egress_ip)
        if calls == 2:
            rows = json.loads(response.body)
            # Field corruption fault: use an earlier genuine timestamp, never a fabricated trade.
            rows[0]['time'] = responses[59][1][-1]['time']
            return Response(json.dumps(rows).encode(), {}, 200, egress_ip=egress_ip)
        return response
    monkeypatch.setattr(spool, 'get_response', reordered_time)
    with pytest.raises(SourceError, match='decreasing_time'):
        spool.repair_spooled_gaps(tmp_path, _partition(), egress_ip='37.27.112.144')
    assert spool.read_spooled_revision(tmp_path, _partition()) is None
