"""Isolated child processes for real feeds, capture, and independent observations."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO

from origo.sources.adapters.binance_perp_agg_daily import BinancePerpAggDaily
from origo.sources.adapters.binance_perp_agg_rest import BinancePerpAggProvisional
from origo.sources.contracts import RevisionedSourceSpec, Row, Snapshot
from origo.steady_state.trial_corpus import SOURCES
from origo.steady_state.trial_evidence import derive_capacity, instant, list_value, object_value
from origo.steady_state.trial_transport import (
    ReplayServer,
    Tape,
    digest,
    install_transport,
    restrict_network,
    write_json,
)

log = logging.getLogger(__name__)


class ArchivePerpAggregate(BinancePerpAggProvisional):
    """Archive rows lack REST's ignored `nq`; use the registered archive row parser.

    Endpoint/parameters/paging/budget and component lifecycle remain unchanged.
    This adaptation is declared in the report; it proves no live REST schema contract.
    """

    def map_row(self, row: Mapping[str, object]) -> Row:
        timestamp, identity = row.get('T'), row.get('a')
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise ValueError('Archive timestamp must be an integer.')
        if isinstance(identity, bool) or not isinstance(identity, int):
            raise ValueError('Archive identity must be an integer.')
        fields = [str(row[key]).lower() for key in ('a', 'p', 'q', 'f', 'l', 'T', 'm')]
        return BinancePerpAggDaily().build_row(
            identity,
            timestamp,
            datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=timestamp),
            fields,
        )


def selected_spec(source: str) -> RevisionedSourceSpec:
    from origo.sources.registry import SOURCE_REGISTRY

    spec = next(item for item in SOURCE_REGISTRY if item.key == source)
    return (
        replace(spec, provisional=ArchivePerpAggregate())
        if source == 'binance_perp_aggtrades'
        else spec
    )


def validate_worker(output: Path) -> set[int]:
    resources = object_value(json.loads((output / 'resources.json').read_text()), 'owned resources')
    configuration = object_value(resources['configuration'], 'owned configuration')
    if os.environ.get('CLICKHOUSE_HOST') != '127.0.0.1':
        raise PermissionError('Trial worker refuses a nonlocal ClickHouse host.')
    if os.environ.get('CLICKHOUSE_PASSWORD') != resources['ownership_label']:
        raise PermissionError('Trial worker requires its owned ClickHouse credential.')
    if os.environ.get('CLICKHOUSE_DATABASE') != 'origo_trial_' + str(resources['ownership_label']):
        raise PermissionError('Trial worker refuses an unowned database.')
    for key, value in configuration.items():
        if key not in ('PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR', 'SSL_CERT_FILE', 'SYSTEMROOT'):
            if os.environ.get(key) != value:
                raise PermissionError(f'Trial worker configuration changed: {key}.')
    for key in (
        'ORIGO_SOURCE_LOCK_DIR',
        'ORIGO_SOURCE_PUBLICATION_ROOT',
        'LOCAL_PARQUET_DIR',
        'LOCAL_ARROW_DIR',
        'ORIGO_TRADE_SPOOL_DIR',
        'DAGSTER_HOME',
    ):
        if not Path(os.environ[key]).resolve().is_relative_to(output.resolve() / 'runtime'):
            raise PermissionError(f'Trial worker path escapes owned runtime: {key}.')
    return {int(os.environ[key]) for key in ('CLICKHOUSE_PORT', 'CLICKHOUSE_HTTP_PORT')}


def wait_clock(output: Path) -> tuple[float, datetime]:
    deadline = time.monotonic() + 180
    while not (output / 'clock.json').is_file():
        if time.monotonic() > deadline:
            raise TimeoutError('Trial admission barrier timed out.')
        time.sleep(0.1)
    clock = object_value(json.loads((output / 'clock.json').read_text()), 'clock')
    return float(str(clock['monotonic_start'])), instant(clock['utc_start'], 'utc_start')


def source_process(output: Path, source: str, port: int, *, capture: bool) -> int:
    ports = validate_worker(output)
    restrict_network(ports | {port})
    install_transport(
        source, port, output / (source + ('-capture' if capture else '') + '-costs.jsonl')
    )
    plan = object_value(json.loads((output / 'plan.json').read_text()), 'plan')
    declaration = object_value(plan[source], source)
    origin = instant(declaration['source_time_at_start'], 'origin')
    spec = selected_spec(source)
    from origo.steady_state.trade_spool import TradeSpool, spool_path

    spool: TradeSpool | None = None
    if capture:
        from origo.sources.adapters.binance_perp_rest import historical_row

        spool = TradeSpool.create(
            spool_path(Path(os.environ['ORIGO_TRADE_SPOOL_DIR']), source, 'BTCUSDT'), historical_row
        )
    else:
        from origo.assets.create_origo_database import (
            get_clickhouse_settings,
            make_clickhouse_client,
        )
        from origo.sources.lifecycle import SourceRuntime
        from origo.sources.storage import SourceStore
        from origo.workers.receipts import ensure_monitoring_tables

        settings = get_clickhouse_settings()
        client = make_clickhouse_client(settings)
        try:
            runtime = SourceRuntime(
                spec,
                SourceStore(client, settings.database, spec),
                Path(os.environ['ORIGO_SOURCE_LOCK_DIR']),
                'trial-setup:' + source,
            )
            runtime.setup(anchor=origin - timedelta(hours=1))
            ensure_monitoring_tables(client, settings.database)
        finally:
            client.disconnect()
    label = source + ('-capture' if capture else '')
    write_json(output / (label + '-ready.json'), {'observed_at': datetime.now(UTC).isoformat()})
    began, _ = wait_clock(output)

    def source_now() -> datetime:
        return origin + timedelta(seconds=time.monotonic() - began)

    from origo.sources.adapters import (
        binance_perp_agg_rest,
        binance_perp_rest,
        binance_spot_agg_rest,
        binance_spot_rest,
    )

    for module in (
        binance_perp_agg_rest,
        binance_perp_rest,
        binance_spot_agg_rest,
        binance_spot_rest,
    ):
        setattr(module, 'now_utc', source_now)
    # A local marker preserves the authenticated repair request shape, never a provider credential.
    os.environ['BINANCE_API_KEY'] = 'isolated-archive-replay-no-provider-credential'
    if capture:
        from origo.workers.trade_capture import POLL_SECONDS, TradeCapture

        if spool is None:
            raise RuntimeError('Capture spool was not initialized.')
        worker = TradeCapture(spool, base_url='https://fapi.binance.com', clock=source_now)
        try:
            while True:
                started = time.monotonic()
                try:
                    outcome = worker.step()
                    with (output / 'capture-outcomes.jsonl').open('a') as stream:
                        stream.write(
                            json.dumps(
                                {
                                    'observed_at': datetime.now(UTC).isoformat(),
                                    'source_time': source_now().isoformat(),
                                    'outcome': str(outcome),
                                    'http_capture': False,
                                }
                            )
                            + '\n'
                        )
                except Exception:
                    log.exception('Archive capture step failed; gap remains explicit')
                time.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))
        finally:
            spool.close()
    from origo.workers.dagster_reader import DagsterReader
    from origo.workers.provisional import ProvisionalFeed
    from origo.workers.report import Reporter

    base = f'http://127.0.0.1:{port}'
    feed = ProvisionalFeed(
        (spec,),
        publication_root=Path(os.environ['ORIGO_SOURCE_PUBLICATION_ROOT']),
        reporter=Reporter(base),
        dagster=DagsterReader(base),
        max_workers=1,
    )
    while True:
        started = time.monotonic()
        outcome = feed.tick(source_now())
        with (output / (source + '-ticks.jsonl')).open('a') as stream:
            stream.write(
                json.dumps(
                    {
                        'observed_at': datetime.now(UTC).isoformat(),
                        'source_time': outcome.minute.isoformat(),
                        'processed': outcome.processed,
                        'failed': outcome.failed,
                        'elapsed_seconds': time.monotonic() - started,
                    }
                )
                + '\n'
            )
        time.sleep(max(0, 60 - (time.monotonic() - started)))


def publication(output: Path, source: str, anchor: datetime) -> dict[str, object]:
    import polars as pl

    from origo.steady_state.policy import load_inventory
    from origo.steady_state.publication import manifest_delivered_through

    root = Path(os.environ['ORIGO_SOURCE_PUBLICATION_ROOT']) / source / 'mount'
    path = root / 'latest.json'
    if not path.is_file():
        return {
            'published_through': anchor.isoformat(),
            'publication_status': 'MISSING',
            'files': [],
        }
    manifest = object_value(json.loads(path.read_text()), 'mount manifest')
    files = [object_value(item, 'file') for item in list_value(manifest['files'], 'files')]
    required = {item.name for item in load_inventory().sources[source].series}
    verified: dict[str, set[str]] = {name: set() for name in required}
    for item in files:
        path = Path(str(item['path']))
        if not path.resolve().is_relative_to(output / 'runtime'):
            raise PermissionError('Published file escapes owned runtime.')
        if digest(path) != item['sha256']:
            raise ValueError('Committed publication file hash differs from its manifest.')
        arrow = item.get('kind') == 'arrow'
        frame = pl.read_ipc(path, memory_map=False) if arrow else pl.read_parquet(path)
        if frame.height != item['row_count']:
            raise ValueError('Committed publication row count differs from its manifest.')
        series = str(item['series'])
        if series not in verified:
            raise ValueError('Publication contains an undeclared series.')
        verified[series].add('arrow' if arrow else 'parquet')
    if any(kinds != {'arrow', 'parquet'} for kinds in verified.values()):
        raise ValueError('Every required mounted series needs verified Parquet and Arrow.')
    endpoint = manifest_delivered_through(manifest)
    return {
        'published_through': endpoint.isoformat(),
        'publication_status': 'VERIFIED',
        'manifest': manifest,
        'series': sorted(required),
    }


class Observer:
    def __init__(self, output: Path, plan: dict[str, object]) -> None:
        from origo.assets.create_origo_database import (
            get_clickhouse_settings,
            make_clickhouse_client,
        )

        self.settings = get_clickhouse_settings()
        self.client = make_clickhouse_client(self.settings)
        self.output, self.plan = output, plan
        self.accepted: dict[str, dict[str, dict[str, object]]] = {source: {} for source in SOURCES}

    def state(self, source: str, elapsed: float) -> dict[str, object]:
        from origo.sources.hashing import content_hash
        from origo.sources.storage import SourceStore

        spec = selected_spec(source)
        store = SourceStore(self.client, self.settings.database, spec)
        declaration = object_value(self.plan[source], source)
        origin = instant(declaration['source_time_at_start'], 'origin')
        anchor = origin - timedelta(hours=1)
        due = (origin + timedelta(seconds=elapsed)).replace(second=0, microsecond=0)
        accepted = self.accepted[source]
        oracle = {
            str(row['minute']): row
            for value in list_value(declaration['oracle'], 'oracle')
            for row in [object_value(value, 'oracle minute')]
        }
        canonical: list[str] = []
        for record in store.records():
            if record.partition.provisional and record.partition.end <= due:
                key = record.partition.start.isoformat()
                cached = accepted.get(key)
                if cached is None or cached['build_id'] != str(record.build_id):
                    expected = {
                        component.key for component in spec.components if component.provisional
                    }
                    if {name for name, _ in record.component_hashes} != expected:
                        raise ValueError('Accepted generation lacks required component evidence.')
                    raw = store.rows('raw_latest', Snapshot('', (record,)))
                    rows = [row[1:] for row in raw]  # minute_start is component metadata.
                    document: dict[str, object] = {
                        'minute': key,
                        'rows': len(rows),
                        'raw_sha256': content_hash(rows, schema_version=1),
                        'build_id': str(record.build_id),
                        'revision': record.revision,
                        'generation': record.generation,
                        'components': record.component_hashes,
                    }
                    target = self.output / 'accepted' / source
                    target.mkdir(parents=True, exist_ok=True)
                    with gzip.open(target / (str(record.build_id) + '.jsonl.gz'), 'wt') as stream:
                        for row in rows:
                            stream.write(json.dumps(row, default=str) + '\n')
                    expected_input = oracle.get(key)
                    if expected_input is None or any(
                        document[field] != expected_input[field] for field in ('rows', 'raw_sha256')
                    ):
                        raise ValueError(
                            'Accepted raw rows differ from the independent CSV oracle.'
                        )
                    if record.revision != expected_input['native_revision_sha256']:
                        raise ValueError('Accepted revision differs from the native CSV oracle.')
                    accepted[key] = document
            elif not record.partition.provisional:
                cursor = max(anchor, record.partition.start)
                while cursor < min(due, record.partition.end):
                    canonical.append(cursor.isoformat())
                    cursor += timedelta(minutes=1)
        missing: list[str] = []
        cursor = anchor
        while cursor < due:
            if cursor.isoformat() not in accepted:
                missing.append(cursor.isoformat())
            cursor += timedelta(minutes=1)
        weight = 0
        for suffix in ('-costs.jsonl', '-capture-costs.jsonl'):
            path = self.output / (source + suffix)
            if path.exists():
                with path.open() as stream:
                    for line in stream:
                        if line.endswith('\n'):
                            cost = object_value(json.loads(line), 'request cost')
                            weight += int(str(cost['weight']))
        return {
            'due': due.isoformat(),
            'accepted_minutes': list(accepted.values()),
            'canonical_minutes': sorted(set(canonical)),
            'missing_minutes': missing,
            'request_weight': weight,
            **publication(self.output, source, anchor),
        }

    def sample(self, elapsed: float, observed_at: datetime) -> dict[str, object]:
        states: dict[str, object] = {}
        for source in SOURCES:
            began = time.monotonic()
            started = datetime.now(UTC)
            try:
                states[source] = {
                    **self.state(source, elapsed),
                    'observation_started_at': started.isoformat(),
                    'observation_finished_at': datetime.now(UTC).isoformat(),
                    'observation_elapsed_seconds': time.monotonic() - began,
                }
            except Exception as error:
                log.exception('Independent observation failed for %s', source)
                states[source] = {'status': 'UNKNOWN', 'error': repr(error)}
        return {
            'elapsed_seconds': elapsed,
            'observed_at': observed_at.isoformat(),
            'sources': states,
        }

    def retain_logs(self) -> None:
        for table in (
            'worker_minute_log',
            'source_failure_log',
            'source_observation_log',
            'source_component_log',
            'source_activation_log',
        ):
            rows = self.client.execute(f'SELECT * FROM {self.settings.database}.{table}')
            with gzip.open(self.output / (table + '.jsonl.gz'), 'wt') as stream:
                for row in rows:
                    stream.write(json.dumps(row, default=str) + '\n')


def evaluate(progress: dict[str, object], duration: float, missing: list[str]) -> dict[str, object]:
    """Capacity may pass independently; incomplete integration cannot claim acceptance."""
    measured = derive_capacity(progress, source_keys=SOURCES)
    from origo.steady_state.policy import load_policy

    policy = load_policy()
    failures: list[str] = []
    samples = list_value(progress['samples'], 'samples')
    elapsed = float(str(object_value(samples[-1], 'sample')['elapsed_seconds']))
    if elapsed < duration or not policy.bound('SS-05', 'trial_hours_min').holds(elapsed / 3600):
        failures.append('six_real_hours_required')
    if not policy.bound('SS-05', 'normal_freshness_hours_after_recovery_min').holds(
        float(str(measured['normal_freshness_hours_after_recovery']))
    ):
        failures.append('one_normal_hour_after_recovery_required')
    for source, raw in object_value(measured['sources'], 'sources').items():
        values = object_value(raw, source)
        arrival = float(str(values['arrival_minutes']))
        ratio = float(str(values['useful_minutes'])) / arrival if arrival > 0 else 0
        stats = {
            'useful_service_to_arrival_ratio_min': ratio,
            'withheld_minutes': values['withheld_minutes'],
            'drain_minutes_max': values['drain_minutes'],
            'lost_rows_max': values['lost_rows'],
            'duplicate_selected_rows_max': values['duplicate_selected_rows'],
            'hidden_backlog_minutes_max': values['hidden_backlog_minutes'],
        }
        for name, value in stats.items():
            if value is None or not policy.bound('SS-05', name).holds(float(str(value))):
                failures.append(source + ':' + name)
    return {
        'capacity_status': 'FAIL' if failures else 'PASS',
        'measurements': measured,
        'failures': failures,
        'missing_requirements': missing,
        'acceptance': 'FAIL' if failures else ('UNKNOWN' if missing else 'PASS'),
        'production_acceptance': 'NOT_EVALUATED',
        'http_capture': False,
    }


def coordinate(output: Path, duration: float) -> int:
    ports = validate_worker(output)
    plan = object_value(json.loads((output / 'plan.json').read_text()), 'plan')
    origins = {
        source: instant(object_value(plan[source], source)['source_time_at_start'], 'origin')
        for source in SOURCES
    }
    tapes = {
        source: Tape(object_value(object_value(plan[source], source)['tape'], 'tape'))
        for source in SOURCES
    }
    locators = {
        source: Tape(
            object_value(object_value(plan[source], source)['locator_tape'], 'locator_tape')
        )
        for source in SOURCES
        if 'locator_tape' in object_value(plan[source], source)
    }
    server = ReplayServer(tapes, origins, output / 'transport.jsonl', locators=locators)
    port = server.server_port
    restrict_network(ports | {port})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    processes: list[subprocess.Popen[bytes]] = []
    streams: list[BinaryIO] = []
    observer: Observer | None = None
    progress: dict[str, object] = {
        'kind': 'steady_state_trial_progress',
        'schema_version': 1,
        'environment': 'isolated',
        'clock_rate': 1,
        'source_plan': plan,
        'samples': [],
    }
    samples: list[dict[str, object]] = []
    progress['samples'] = samples
    missing = [
        'native_dagster_queue_routine_and_maintenance',
        'isolated_fault_cases',
        'full_history_mount_sizes_and_death_during_render',
        'canonical_day_activation_and_consumers',
        'depth_and_dependent_readers',
        'baseline_comparison_on_identical_resources',
        'provider_HTTP_capture_provenance',
        'host_and_container_peak_resource_measurements',
    ]
    for source in SOURCES:
        if not source.endswith('aggtrades'):
            locator = locators.get(source, tapes[source.removesuffix('trades') + 'aggtrades'])
            if locator.document['day'] != tapes[source].document['day']:
                missing.append(source + ':matching_day_aggregate_locator_archive')
    write_json(
        output / 'coverage.json',
        {
            'wired': [
                'SourceRuntime',
                'ProvisionalFeed',
                'production_provider_limiter',
                'TradeCapture',
                'receipts',
                'all_mounted_series',
            ],
            'missing': missing,
            'adaptations': ['perp aggregate archive parser omits REST-only ignored nq'],
            'http_capture': False,
        },
    )
    try:
        labels: list[str] = []
        for source, capture in [*((key, False) for key in SOURCES), ('binance_perp_trades', True)]:
            label = source + ('-capture' if capture else '')
            labels.append(label)
            stream = (output / (label + '.log')).open('wb')
            streams.append(stream)
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        '-m',
                        'origo.steady_state.trial_worker',
                        '--output',
                        str(output),
                        '--source',
                        source,
                        '--port',
                        str(port),
                        *(['--capture'] if capture else []),
                    ],
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                )
            )
        deadline = time.monotonic() + 180
        while not all((output / (label + '-ready.json')).exists() for label in labels):
            if any(process.poll() is not None for process in processes):
                raise RuntimeError('A source process failed before admission; see retained logs.')
            if time.monotonic() > deadline:
                raise TimeoutError('Source setup exceeded its owned-resource deadline.')
            time.sleep(0.1)
        observer = Observer(output, plan)
        initial = observer.sample(0, datetime.now(UTC))
        began = time.monotonic()
        utc_start = datetime.now(UTC)
        initial['observed_at'] = utc_start.isoformat()
        samples.append(initial)
        server.started = began
        write_json(output / 'progress.json', progress)
        write_json(
            output / 'clock.json', {'monotonic_start': began, 'utc_start': utc_start.isoformat()}
        )
        next_sample = min(60.0, duration)
        while True:
            elapsed = time.monotonic() - began
            if elapsed >= next_sample:
                sample = observer.sample(elapsed, datetime.now(UTC))
                samples.append(sample)
                write_json(output / 'progress.json', progress)
                if elapsed >= duration:
                    break
                next_sample = min(duration, next_sample + 60)
            if any(process.poll() is not None for process in processes):
                raise RuntimeError('An admitted source/capture process exited; see retained logs.')
            time.sleep(min(0.25, max(0, next_sample - (time.monotonic() - began))))
        write_json(
            output / 'clock-end.json',
            {
                'utc_end': datetime.now(UTC).isoformat(),
                'monotonic_end': time.monotonic(),
                'elapsed_seconds': time.monotonic() - began,
            },
        )
        report = evaluate(progress, duration, missing)
        write_json(output / 'report.json', report)
        return 0 if report['acceptance'] == 'PASS' else (1 if report['acceptance'] == 'FAIL' else 2)
    except BaseException as error:
        write_json(
            output / 'worker-failure.json',
            {
                'error': repr(error),
                'traceback': traceback.format_exc(),
                'observed_at': datetime.now(UTC).isoformat(),
                'acceptance': 'UNKNOWN',
            },
        )
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for stream in streams:
            stream.close()
        try:
            if observer is not None:
                observer.retain_logs()
        finally:
            if observer is not None:
                observer.client.disconnect()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--duration-seconds', type=float, default=21600)
    parser.add_argument('--source', choices=SOURCES)
    parser.add_argument('--port', type=int)
    parser.add_argument('--capture', action='store_true')
    args = parser.parse_args()

    def stopping(signum: int, frame: object) -> None:
        raise SystemExit(128 + signum)

    if args.source is None:
        signal.signal(signal.SIGTERM, stopping)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    if args.source:
        if args.port is None or (args.capture and args.source != 'binance_perp_trades'):
            parser.error(
                'Source children require the owned replay port; capture is perpetual raw only.'
            )
        return source_process(args.output, args.source, args.port, capture=args.capture)
    return coordinate(args.output, args.duration_seconds)


if __name__ == '__main__':
    raise SystemExit(main())
