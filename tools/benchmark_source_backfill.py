"""Benchmark real cached spot archives against frozen legacy work in an owned ClickHouse.

No production connection is used. Both paths run on the same container and archives;
only the revised path includes independent parity verification and declared file outputs.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import platform
import resource
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from origo.assets.create_origo_database import (
    ClickHouseSettings,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from origo.assets.daily_trades_to_origo import _extract_csv, _parse_trade_rows
from origo.sources.adapters import binance_daily
from origo.sources.archive import archive_session
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC as SPEC
from origo.sources.columnar import BoundedClient, binary_hash
from origo.sources.contracts import BuildContext, CanonicalAdapter, Client, Partition, Revision
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles.spot_parity import _LEGACY
from origo.sources.storage import SourceStore

ROOT = Path(__file__).resolve().parents[1]


def archive_evidence(root: Path, days: list[str]) -> list[dict[str, object]]:
    evidence = []
    for day in days:
        datetime.strptime(day, '%Y-%m-%d')
        name = f'BTCUSDT-trades-{day}.zip'
        path = root / name
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        expected = (root / (name + '.CHECKSUM')).read_text().split()
        if len(expected) != 2 or expected[0] != digest or expected[1].lstrip('*') != name:
            raise ValueError(f'Official checksum mismatch: {name}')
        with zipfile.ZipFile(path) as archive:
            if archive.namelist() != [name.removesuffix('.zip') + '.csv']:
                raise ValueError(f'Unexpected archive members: {name}')
        evidence.append({'day': day, 'sha256': digest, 'bytes': path.stat().st_size})
    return evidence


def throughput(rows: int, seconds: float) -> dict[str, float]:
    if rows <= 0 or seconds <= 0:
        raise ValueError('Throughput requires positive rows and elapsed time.')
    return {'rows_per_second': rows / seconds, 'seconds_per_million': seconds * 1e6 / rows}


def _port() -> int:
    with socket.socket() as server:
        server.bind(('127.0.0.1', 0))
        return int(server.getsockname()[1])


@contextmanager
def clickhouse(memory: str) -> Iterator[dict[str, str]]:
    name = 'origo-benchmark-' + uuid4().hex[:12]
    image = subprocess.run(
        ['docker', 'build', '--quiet', '-f', 'Dockerfile.clickhouse', '.'],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    native, http = _port(), _port()
    subprocess.run(
        [
            'docker',
            'run',
            '--rm',
            '-d',
            '--name',
            name,
            '--memory',
            memory,
            '-p',
            f'127.0.0.1:{native}:9000',
            '-p',
            f'127.0.0.1:{http}:8123',
            '-e',
            'CLICKHOUSE_PASSWORD=benchmark-local',
            image,
        ],
        check=True,
        capture_output=True,
    )
    env = {
        'CLICKHOUSE_HOST': '127.0.0.1',
        'CLICKHOUSE_PORT': str(native),
        'CLICKHOUSE_HTTP_PORT': str(http),
        'CLICKHOUSE_USER': 'default',
        'CLICKHOUSE_PASSWORD': 'benchmark-local',
        'CLICKHOUSE_DATABASE': 'default',
    }
    try:
        os.environ.update(env)
        deadline = time.monotonic() + 60
        while True:
            try:
                client = make_clickhouse_client(get_clickhouse_settings())
                try:
                    client.execute('SELECT 1')
                finally:
                    client.disconnect()
                break
            except (OSError, RuntimeError, EOFError) as error:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Owned ClickHouse did not start.') from error
                time.sleep(0.2)
        yield env
    finally:
        subprocess.run(['docker', 'rm', '-f', name], check=True, capture_output=True)


def _legacy_setup(client: Client, database: str) -> None:
    client.execute(f'CREATE DATABASE {database}')
    settings = replace(get_clickhouse_settings(), database=database)
    for _table, module, function in _LEGACY.values():
        create = cast(
            Callable[[Client, ClickHouseSettings], None],
            getattr(importlib.import_module('origo.assets.' + module), function),
        )
        create(client, settings)


def _legacy_day(client: Client, database: str, day: str, archives: Path) -> dict[str, object]:
    started = time.perf_counter()
    _, body = _extract_csv((archives / f'BTCUSDT-trades-{day}.zip').read_bytes())
    rows = _parse_trade_rows(body)
    client.execute(f'INSERT INTO {database}.binance_daily_spot_trades VALUES', rows)
    ingested = time.perf_counter()
    for component, (table, _module, _create) in _LEGACY.items():
        if component == 'raw':
            continue
        module = importlib.import_module(
            'origo.assets.'
            + (
                'refresh_aligned_1m_exchange_from_binance_spot_origo'
                if component == 'aligned'
                else f'refresh_{table}_origo'
            )
        )
        if component == 'imbalance':
            calculate = cast(
                Callable[[ClickHouseSettings, str], None], module._insert_partition_rows
            )
            calculate(replace(get_clickhouse_settings(), database=database), day)
        else:
            project = cast(Callable[[Client, str, str], None], module._insert_partition_rows)
            project(client, database, day)
    return {
        'day': day,
        'rows': len(rows),
        'ingest_seconds': ingested - started,
        'projection_seconds': time.perf_counter() - ingested,
    }


class TimedAdapter:
    def __init__(self, adapter: CanonicalAdapter, timings: dict[str, float]) -> None:
        self.adapter, self.timings = adapter, timings

    def partition(self, key: str) -> Partition:
        return self.adapter.partition(key)

    def discover(self, partition: Partition) -> str:
        return self.adapter.discover(partition)

    def fetch(self, partition: Partition) -> Revision:
        start = time.perf_counter()
        revision = self.adapter.fetch(partition)
        self.timings['archive_decode_seconds'] = time.perf_counter() - start
        return revision

    def revalidate(self, partition: Partition, revision: Revision) -> None:
        self.adapter.revalidate(partition, revision)


def timed_component(
    key: str, build: Callable[[BuildContext], None], timings: dict[str, float]
) -> Callable[[BuildContext], None]:
    def measured(context: BuildContext) -> None:
        start = time.perf_counter()
        build(context)
        timings[key] = time.perf_counter() - start

    return measured


def worker(args: tuple[str, str, dict[str, str], str, str]) -> dict[str, object]:
    mode, day, env, archives, locks = args
    os.environ.update(env)
    client = BoundedClient(make_clickhouse_client(get_clickhouse_settings()))
    try:
        if mode == 'legacy':
            result = _legacy_day(client, env['CLICKHOUSE_DATABASE'], day, Path(archives))
        else:

            def cached_response(url: str) -> binance_daily.Response:
                return binance_daily.Response(
                    (Path(archives) / url.rsplit('/', 1)[-1]).read_bytes(), {}, 200
                )

            binance_daily.get_response = cached_response
            timings: dict[str, float] = {}
            spec = replace(
                SPEC,
                canonical=TimedAdapter(SPEC.canonical, timings),
                components=tuple(
                    replace(
                        component, build=timed_component(component.key, component.build, timings)
                    )
                    for component in SPEC.components
                ),
            )
            runtime = SourceRuntime(
                spec, SourceStore(client, env['CLICKHOUSE_DATABASE'], spec), Path(locks), day
            )
            with archive_session():
                started = time.perf_counter()
                runtime.build(day)
                built = time.perf_counter()
                _, proof = runtime.verify(day)
                raw = cast(dict[str, object], proof['raw'])
                result = {
                    'day': day,
                    'rows': raw['row_count'],
                    'build_seconds': built - started,
                    'verification_seconds': time.perf_counter() - built,
                    'ingest_seconds': timings['archive_decode_seconds'] + timings['raw'],
                    'projection_seconds': sum(
                        timings[c.key]
                        for c in spec.components
                        if not c.provisional and c.key != 'raw'
                    ),
                    'build_stage_seconds': timings,
                }
        maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result['worker_peak_rss_bytes'] = maximum if sys.platform == 'darwin' else maximum * 1024
        return result
    finally:
        client.disconnect()


def scenario(
    mode: str, workers: int, days: list[str], env: dict[str, str], archives: Path, root: Path
) -> dict[str, object]:
    database = 'origo_benchmark_' + uuid4().hex[:12]
    settings = {**env, 'CLICKHOUSE_DATABASE': database}
    os.environ.update(settings)
    client = BoundedClient(make_clickhouse_client(get_clickhouse_settings()))
    runtime = SourceRuntime(
        SPEC, SourceStore(client, database, SPEC), root / database / 'locks', 'benchmark'
    )
    try:
        if mode == 'legacy':
            _legacy_setup(client, database)
        else:
            runtime.setup(anchor=datetime.strptime(min(days), '%Y-%m-%d').replace(tzinfo=UTC))
        query_start = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')
        started = time.perf_counter()
        with ProcessPoolExecutor(max_workers=workers) as pool:
            measurements = list(
                pool.map(
                    worker,
                    [(mode, day, settings, str(archives), str(runtime.lock_root)) for day in days],
                )
            )
        source_seconds = time.perf_counter() - started
        files: dict[str, float] = {}
        if mode == 'revised':
            for consumer in SPEC.consumers:
                before = time.perf_counter()
                runtime.publish(consumer.key, str(root / database / SPEC.key / consumer.key))
                files[consumer.key] = time.perf_counter() - before
        elapsed = time.perf_counter() - started
        client.execute('SYSTEM FLUSH LOGS')
        peak_query_memory = client.execute(
            "SELECT max(memory_usage) FROM system.query_log WHERE type='QueryFinish' AND query_start_time_microseconds >= toDateTime64(%(start)s,6,'UTC')",
            {'start': query_start},
        )[0][0]
        if mode == 'legacy':
            proofs = {
                day: {
                    component.key: binary_hash(
                        client,
                        component,
                        database + '.' + _LEGACY[component.key][0],
                        predicate=f'toDate({component.time_column})=toDate(%(day)s)',
                        params={'day': day},
                        schema_version=SPEC.schema_version,
                    )
                    for component in SPEC.components
                    if not component.provisional
                }
                for day in days
            }
        else:
            proofs = {
                record.partition.key: dict(record.component_hashes)
                for record in runtime.store.records(canonical_only=True)
            }
        rows = sum(int(str(item['rows'])) for item in measurements)
        return {
            'mode': mode,
            'workers': workers,
            'rows': rows,
            'source_seconds': source_seconds,
            'file_seconds': files,
            'total_seconds': elapsed,
            **throughput(rows, elapsed),
            'source_throughput': throughput(rows, source_seconds),
            'days': measurements,
            'clickhouse': client.execute('SELECT version()')[0][0],
            'clickhouse_peak_query_memory_bytes': peak_query_memory,
            'component_proofs': proofs,
        }
    finally:
        client.execute(f'DROP DATABASE IF EXISTS {database} SYNC')
        client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--archives', type=Path, required=True, help='Official ZIPs and matching CHECKSUM sidecars'
    )
    parser.add_argument('--days', nargs='+', required=True, help='Real archive dates')
    parser.add_argument('--workers', nargs='+', type=int, default=[1, 2, 4])
    parser.add_argument(
        '--modes', nargs='+', choices=['legacy', 'revised'], default=['legacy', 'revised']
    )
    parser.add_argument('--memory', default='6g', help='Owned ClickHouse container memory ceiling')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if len(set(args.days)) != len(args.days) or any(worker <= 0 for worker in args.workers):
        parser.error('Dates must be unique and worker counts positive.')
    report: dict[str, object] = {
        'archives': archive_evidence(args.archives, args.days),
        'scope': 'Cached official archives; worker startup included, Dagster orchestration excluded. Legacy: frozen ingest and six projections. Revised: source build, seven-component independent parity, all declared shadow files. An additional cross-scenario audit of frozen legacy output hashes runs outside the measured interval. No Hugging Face network upload.',
        'git_revision': subprocess.run(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip(),
        'working_tree_dirty': bool(
            subprocess.run(
                ['git', 'status', '--porcelain'],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        ),
        'platform': platform.platform(),
        'docker_cpus': int(
            subprocess.run(
                ['docker', 'info', '--format', '{{.NCPU}}'],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        ),
        'host_cpus': os.cpu_count(),
        'container_memory_limit': args.memory,
        'results': [],
    }
    results: list[dict[str, object]] = []
    report['results'] = results
    with (
        tempfile.TemporaryDirectory(prefix='origo-benchmark-') as directory,
        clickhouse(args.memory) as env,
    ):
        for mode in args.modes:
            for workers in args.workers:
                result = scenario(
                    mode, workers, args.days, env, args.archives.resolve(), Path(directory)
                )
                if results and result['component_proofs'] != results[0]['component_proofs']:
                    raise ValueError('Exact component output changed between benchmark scenarios.')
                results.append(result)
                args.output.write_text(json.dumps(report, indent=2) + '\n')
                print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
