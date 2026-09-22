"""Offline trial boundaries and real-input replay; no Docker or long trial is launched."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

import polars as pl
import pytest
import requests

from origo.sources.adapters import binance_daily, binance_spot_agg_rest
from origo.sources.adapters.binance_spot_rest import BinanceSpotProvisional
from origo.sources.hashing import content_hash
from origo.steady_state import trial
from origo.steady_state.trial_corpus import ArchiveReference, prepare_tape
from origo.steady_state.trial_evidence import derive_capacity
from origo.steady_state.trial_transport import (
    ReplayServer,
    Tape,
    digest,
    install_transport,
    write_json,
)
from origo.steady_state.trial_worker import ArchivePerpAggregate, evaluate

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / 'tests/fixtures/binance'


def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in tuple(os.environ):
        if (
            key.startswith(('CLICKHOUSE_', 'ORIGO_', 'BINANCE_', 'HF_', 'HUGGINGFACE_', 'AWS_'))
            or key
            in (
                'DAGSTER_HOME',
                'DAGSTER_WEBSERVER_URL',
                'LOCAL_PARQUET_DIR',
                'LOCAL_ARROW_DIR',
                'RESEND_API_KEY',
                'DOCKER_HOST',
                'DOCKER_CONTEXT',
            )
            or key.upper().endswith('_PROXY')
        ):
            monkeypatch.delenv(key)


@pytest.mark.parametrize(
    'name',
    [
        'CLICKHOUSE_HOST',
        'HF_TOKEN',
        'BINANCE_API_KEY',
        'ORIGO_SOURCE_CLICKHOUSE_VOLUME_PATH',
        'DAGSTER_HOME',
        'HTTPS_PROXY',
        'DOCKER_HOST',
    ],
)
def test_production_config_rejected_before_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    clean_environment(monkeypatch)
    monkeypatch.setenv(name, 'inherited-value')
    output = tmp_path / 'must-not-exist'
    with pytest.raises(PermissionError, match='refuses'):
        trial.run_trial(tmp_path / 'unread-manifest', output, 1)
    assert not output.exists()


def test_existing_good_evidence_is_never_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clean_environment(monkeypatch)
    good = tmp_path / 'report.json'
    good.write_text('retained verified evidence\n')
    with pytest.raises(FileExistsError):
        trial.run_trial(tmp_path / 'absent', tmp_path, 1)
    assert good.read_text() == 'retained verified evidence\n'


@pytest.mark.parametrize('duration', [0, -1, float('nan'), float('inf'), 21601])
def test_invalid_duration_precedes_resource_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, duration: float
) -> None:
    clean_environment(monkeypatch)
    with pytest.raises(ValueError, match='Duration'):
        trial.validate_request(tmp_path / 'absent', tmp_path / 'new', duration)


def test_real_cached_manifest_validates_without_network_or_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import csv
    import io
    import shutil
    import zipfile
    from collections import Counter

    from origo.steady_state.trial_corpus import archive_url

    clean_environment(monkeypatch)
    cache = tmp_path / 'cache'
    cache.mkdir()
    monkeypatch.setattr(trial, 'CACHE', cache)
    cases = (
        ('binance_spot_trades', 'spot/daily/trades/revisioned/BTCUSDT-trades-2017-08-17.zip'),
        ('binance_perp_trades', 'futures/daily/trades/BTCUSDT/BTCUSDT-trades-2019-09-08.zip'),
        ('binance_spot_aggtrades', 'spot/daily/aggtrades/BTCUSDT/BTCUSDT-aggTrades-2017-08-17.zip'),
        (
            'binance_perp_aggtrades',
            'futures/daily/aggtrades/BTCUSDT/BTCUSDT-aggTrades-2019-12-31.zip',
        ),
    )
    archives = []
    for source, relative in cases:
        original = FIXTURES / relative
        target = cache / original.name
        shutil.copyfile(original, target)
        day = date.fromisoformat(original.name[-14:-4])
        hours: Counter[int] = Counter()
        with zipfile.ZipFile(target) as zipped:
            with zipped.open(zipped.namelist()[0]) as stream:
                for fields in csv.reader(io.TextIOWrapper(stream)):
                    if not fields[0].isdigit():
                        continue
                    stamp = int(fields[5 if source.endswith('aggtrades') else 4])
                    if stamp >= 10**15:
                        stamp //= 1000
                    hour = stamp // 3_600_000
                    if 2 <= datetime.fromtimestamp(hour * 3600, UTC).hour <= 22:
                        hours[hour] += 1
        # A measured busy hour within the admitted short diagnostic window.
        selected = max(hours, key=lambda hour: hours[hour])
        archives.append(
            {
                'source_key': source,
                'date': day.isoformat(),
                'url': archive_url(source, day),
                'cache_path': str(target),
                'sha256': digest(target),
                'captured_at': datetime.now(UTC).isoformat(),
                'capture_kind': 'retained_fixture_copy',
                'busy_hour': {'start': datetime.fromtimestamp(selected * 3600, UTC).isoformat()},
            }
        )
    manifest = tmp_path / 'fixture-manifest.json'
    manifest.write_text(json.dumps({'schema_version': 1, 'archives': archives}))
    assert len(trial.validate_request(manifest, tmp_path / 'new', 1)) == 4
    assert not (tmp_path / 'new').exists()


def test_direct_python311_cli_refuses_production(tmp_path: Path) -> None:
    environment = {'PATH': os.environ.get('PATH', ''), 'CLICKHOUSE_HOST': 'production.invalid'}
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / 'tools/benchmark_steady_state.py'),
            '--fixture-manifest',
            str(tmp_path / 'absent'),
            '--scenario',
            'integrated',
            '--duration-seconds',
            '1',
            '--output',
            str(tmp_path / 'new'),
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert 'inherited CLICKHOUSE_HOST' in result.stderr
    assert not (tmp_path / 'new').exists()


def test_socket_guard_rejects_unowned_loopback_and_external_before_connect() -> None:
    program = """
import socket
from origo.steady_state.trial_transport import restrict_network
restrict_network({12345})
for address in [('127.0.0.1', 54321), ('192.0.2.1', 443)]:
    with socket.socket() as connection:
        try:
            connection.connect(address)
        except PermissionError:
            print('denied')
        else:
            raise AssertionError('unowned destination admitted')
"""
    result = subprocess.run(
        [sys.executable, '-c', program], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ['denied', 'denied']


@pytest.fixture
def official_day(tmp_path: Path) -> tuple[ArchiveReference, Tape]:
    path = FIXTURES / 'spot/daily/trades/revisioned/BTCUSDT-trades-2017-08-17.zip'
    checksum = path.with_suffix('.zip.CHECKSUM').read_text().split()[0]
    assert digest(path) == checksum
    reference = ArchiveReference(
        'binance_spot_trades',
        date(2017, 8, 17),
        path,
        checksum,
        trial.archive_url('binance_spot_trades', date(2017, 8, 17)),
        datetime(2026, 9, 22, tzinfo=UTC),
    )
    return reference, Tape(prepare_tape(reference, tmp_path / 'official.arrow'))


def test_independent_csv_oracle_matches_normalized_real_tape(
    official_day: tuple[ArchiveReference, Tape],
) -> None:
    reference, tape = official_day
    start = datetime(2017, 8, 17, tzinfo=UTC)
    oracle = trial.input_oracle(reference, start, start + timedelta(days=1))
    actual: dict[str, list[tuple[object, ...]]] = {}
    adapter = BinanceSpotProvisional()
    for item in tape.frame.iter_rows(named=True):
        row = adapter.map_row(item)
        stamp = cast(datetime, row[-1]).replace(second=0, microsecond=0).isoformat()
        actual.setdefault(stamp, []).append(
            tuple(float(value) if isinstance(value, Decimal) else value for value in row)
        )
    assert sum(cast(int, item['rows']) for item in oracle) == tape.frame.height
    for item in oracle:
        rows = actual.get(str(item['minute']), [])
        assert item['raw_sha256'] == content_hash(rows, schema_version=1)
    assert tape.document['http_capture'] is False


def test_tape_integrity_and_missing_locator_fail_loud(
    official_day: tuple[ArchiveReference, Tape], tmp_path: Path
) -> None:
    _, tape = official_day
    now = datetime(2017, 8, 17, 12, tzinfo=UTC)
    server = ReplayServer(
        {'binance_spot_trades': tape},
        {'binance_spot_trades': now},
        tmp_path / 'wire.jsonl',
        bind_and_activate=False,
    )
    server.started = time.monotonic()
    try:
        with pytest.raises(LookupError, match='locator archive'):
            server.response(
                'binance_spot_trades',
                '/api/v3/aggTrades',
                {'symbol': 'BTCUSDT', 'limit': '1', 'startTime': str(int(now.timestamp() * 1000))},
            )
        with pytest.raises(ValueError, match='page limit'):
            server.response(
                'binance_spot_trades',
                '/api/v3/historicalTrades',
                {'symbol': 'BTCUSDT', 'limit': '1001', 'fromId': '1'},
            )
    finally:
        server.server_close()
    Path(str(tape.document['tape_path'])).write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='digest'):
        Tape(tape.document)


def test_real_adapter_paging_uses_replay_boundary_and_production_limiter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = FIXTURES / 'spot/rest/aggtrades'
    provenance = json.loads((fixture / 'provenance.json').read_text())
    pages = []
    for request in provenance['requests']:
        path = fixture / request['file']
        assert digest(path) == request['sha256']
        if path.name.startswith('page-'):
            pages.extend(json.loads(path.read_text()))
    frame = pl.DataFrame(pages).unique(subset=['a'], maintain_order=True).sort('a')
    tape_path = tmp_path / 'recorded.arrow'
    frame.write_ipc(tape_path)
    tape = Tape(
        {
            'source_key': 'binance_spot_aggtrades',
            'day': '2026-09-16',
            'tape_path': str(tape_path),
            'tape_sha256': digest(tape_path),
        }
    )
    now = datetime.fromisoformat(provenance['minute_end']) + timedelta(minutes=1)
    server = ReplayServer(
        {'binance_spot_aggtrades': tape},
        {'binance_spot_aggtrades': now},
        tmp_path / 'wire.jsonl',
        bind_and_activate=False,
    )
    server.started = time.monotonic()

    def local_get(
        session: requests.Session,
        url: str,
        *,
        params: dict[str, str | int],
        headers: dict[str, str],
        timeout: tuple[int, int],
        allow_redirects: bool,
    ) -> requests.Response:
        parsed = urlsplit(url)
        assert parsed.hostname == '127.0.0.1' and parsed.port == 12345
        assert not allow_redirects
        source, path = parsed.path.lstrip('/').split('/', 1)
        response = requests.Response()
        response.status_code = 200
        response._content = server.response(
            source, '/' + path, {k: str(v) for k, v in params.items()}
        )
        return response

    monkeypatch.setattr(requests.Session, 'get', local_get)
    # Restore the production seams after this local-only test.
    from origo.sources.adapters import binance_perp_agg_rest, binance_perp_rest, binance_spot_rest
    from origo.workers import trade_capture

    monkeypatch.setattr(binance_daily, '_request', binance_daily._request)
    for module in (
        binance_spot_agg_rest,
        binance_perp_agg_rest,
        binance_perp_rest,
        binance_spot_rest,
        trade_capture,
    ):
        monkeypatch.setattr(module, 'get_response', module.get_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setattr(binance_spot_agg_rest, 'now_utc', lambda: now)
    costs = tmp_path / 'costs.jsonl'
    try:
        install_transport('binance_spot_aggtrades', 12345, costs)
        adapter = binance_spot_agg_rest.BinanceSpotAggProvisional()
        partition = adapter.partition(
            datetime.fromisoformat(provenance['minute_start']).strftime('%Y-%m-%dT%H:%M:%SZ')
        )
        revision = adapter.fetch(partition)
        assert revision.complete
        rows = tuple(revision.rows())
        assert rows
        assert all(partition.start <= cast(datetime, row[-1]) < partition.end for row in rows)
        costs_recorded = [json.loads(line) for line in costs.read_text().splitlines()]
        assert sum(item['weight'] for item in costs_recorded) >= 12
        assert any(item['pace_wait_ms'] > 0 for item in costs_recorded)
        assert all(item['url'].startswith('https://api.binance.com/') for item in costs_recorded)
        assert all(item['http_capture'] is False for item in costs_recorded)
        assert binance_daily.REST_HOST_BUDGETS['api.binance.com'] == (60, 4800)
    finally:
        server.server_close()


def test_archive_perp_aggregate_omits_unsupported_ancillary_field() -> None:
    path = FIXTURES / 'futures/rest/aggtrades/page-00.json'
    row = json.loads(path.read_text())[0]
    expected = __import__(
        'origo.sources.adapters.binance_perp_agg_rest', fromlist=['agg_row']
    ).agg_row(row)
    row.pop('nq')
    assert ArchivePerpAggregate().map_row(row) == expected
    assert 'nq' not in row


def test_empty_work_and_short_duration_cannot_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    # Operational records only: no market rows or fake measured work.
    def no_work(progress: dict[str, object], *, source_keys: tuple[str, ...]) -> dict[str, object]:
        return {
            'normal_freshness_hours_after_recovery': 0,
            'sources': {
                key: {
                    'arrival_minutes': 1,
                    'useful_minutes': 0,
                    'withheld_minutes': 60,
                    'drain_minutes': None,
                    'lost_rows': 0,
                    'duplicate_selected_rows': 0,
                    'hidden_backlog_minutes': 0,
                }
                for key in source_keys
            },
        }

    monkeypatch.setattr('origo.steady_state.trial_worker.derive_capacity', no_work)
    result = evaluate({'samples': [{'elapsed_seconds': 60}]}, 60, ['native_dagster'])
    assert result['acceptance'] == 'FAIL'
    assert 'six_real_hours_required' in cast(list[str], result['failures'])
    with pytest.raises(ValueError, match='Version-1 raw trial progress'):
        derive_capacity({'elapsed_seconds': 21600}, source_keys=trial.SOURCES)


def test_atomic_checkpoint_preserves_previous_on_serialization_failure(tmp_path: Path) -> None:
    target = tmp_path / 'progress.json'
    write_json(target, {'last_good': True})
    invalid: dict[str, object] = {}
    invalid['cycle'] = invalid
    with pytest.raises(ValueError, match='Circular reference'):
        write_json(target, invalid)
    assert json.loads(target.read_text()) == {'last_good': True}


def test_real_oracle_keeps_canonical_credit_out_of_provisional_capacity(
    official_day: tuple[ArchiveReference, Tape],
) -> None:
    reference, _ = official_day
    source = reference.source_key
    origin = datetime(2017, 8, 17, 12, tzinfo=UTC)
    start = origin - timedelta(hours=1)
    oracle = trial.input_oracle(reference, start, origin + timedelta(minutes=1))
    withheld = [(start + timedelta(minutes=index)).isoformat() for index in range(60)]
    observed = datetime(2026, 9, 22, tzinfo=UTC)
    samples = []
    for index in range(2):
        samples.append(
            {
                'observed_at': (observed + timedelta(minutes=index)).isoformat(),
                'elapsed_seconds': index * 60,
                'sources': {
                    source: {
                        'due': (origin + timedelta(minutes=index)).isoformat(),
                        'accepted_minutes': [],
                        'canonical_minutes': withheld if index else [],
                        'missing_minutes': [*withheld, *([origin.isoformat()] if index else [])],
                        'request_weight': 0,
                        'published_through': start.isoformat(),
                    }
                },
            }
        )
    progress = {
        'kind': 'steady_state_trial_progress',
        'schema_version': 1,
        'environment': 'isolated',
        'clock_rate': 1,
        'source_plan': {
            source: {
                'source_time_at_start': origin.isoformat(),
                'withheld': withheld,
                'oracle': oracle,
            }
        },
        'samples': samples,
    }
    derived = derive_capacity(progress, source_keys=[source])
    measured = cast(dict[str, dict[str, object]], derived['sources'])[source]
    assert measured['canonical_replacement_minutes'] == 60
    assert measured['useful_minutes'] == 0
    assert measured['drain_minutes'] is None
    assert derived['normal_freshness_hours_after_recovery'] == 0
