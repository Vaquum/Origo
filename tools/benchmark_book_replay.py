"""Replay pinned provider recordings through all four outputs in local scratch only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import book_vendor as vendor
from origo.sources.adapters.binance_daily import Response
from origo.sources.adapters.book_spool import Market, object_mapping
from origo.sources.binance_perp_book import BINANCE_PERP_BOOK_SPEC
from origo.sources.binance_spot_book import BINANCE_SPOT_BOOK_SPEC
from origo.sources.bundle import SourceRunConfig, _execute_operation
from origo.sources.columnar import BoundedClient
from origo.sources.contracts import Partition
from origo.sources.lifecycle import SourceRuntime
from origo.sources.storage import SourceStore


@dataclass(frozen=True)
class Recording:
    path: Path
    sha256: str
    headers: dict[str, str]


def recordings(path: Path) -> dict[str, Recording]:
    """Read paths, original response headers and mandatory raw-content pins."""
    value = object_mapping(json.loads(path.read_text()))
    result: dict[str, Recording] = {}
    for key, raw in value.items():
        item = object_mapping(raw)
        source = (path.parent / str(item['path'])).resolve()
        header_path = (path.parent / str(item['headers'])).resolve()
        headers = {
            k: str(v) for k, v in object_mapping(json.loads(header_path.read_text())).items()
        }
        if not (headers.get('ETag') or headers.get('etag')):
            raise ValueError(f'Original response ETag missing: {header_path}')
        result[key] = Recording(source, str(item['sha256']), headers)
    return result


def main() -> None:
    process_start, cpu_start = time.perf_counter(), time.process_time()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--manifest',
        type=Path,
        required=True,
        help='JSON hour -> {path,sha256,headers}; paths relative to manifest',
    )
    parser.add_argument('--market', choices=('spot', 'perp'), required=True)
    parser.add_argument('--partition', required=True)
    parser.add_argument('--repeat', type=int, default=3)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if args.repeat < 2:
        parser.error('At least two matched repetitions are required.')
    settings = get_clickhouse_settings()
    if settings.host not in ('127.0.0.1', 'localhost', '::1'):
        raise ValueError('This benchmark requires an owned local ClickHouse.')
    originals = recordings(args.manifest)
    market = cast(Market, args.market)
    spec = BINANCE_SPOT_BOOK_SPEC if market == 'spot' else BINANCE_PERP_BOOK_SPEC
    spec.canonical.partition(args.partition)
    client = BoundedClient(make_clickhouse_client(settings))
    request_counts: dict[str, int] = {'body': 0, 'metadata': 0, 'body_bytes': 0}

    def captured(request_market: Market, partition: Partition, *, metadata: bool) -> Response:
        if request_market != market:
            raise ValueError('Benchmark requested another market.')
        original = originals[partition.key]
        request_counts['metadata' if metadata else 'body'] += 1
        if metadata:
            return Response(b'', original.headers, 200)
        body = original.path.read_bytes()
        if hashlib.sha256(body).hexdigest() != original.sha256:
            raise ValueError(f'Original archive changed: {original.path}')
        request_counts['body_bytes'] += len(body)
        return Response(body, original.headers, 200)

    response = vendor._response
    vendor._response = captured
    results: list[dict[str, object]] = []
    baseline: list[tuple[str, int, str]] | None = None
    try:
        for iteration in range(args.repeat):
            database = 'book_benchmark_' + uuid4().hex
            stages: dict[str, float] = {}
            wall, cpu = time.perf_counter(), time.process_time()
            with tempfile.TemporaryDirectory(prefix='origo-book-benchmark-') as directory:
                root = Path(directory)
                os.environ['ORIGO_SOURCE_LOCK_DIR'] = str(root)
                runtime = SourceRuntime(
                    spec, SourceStore(client, database, spec), root, uuid4().hex
                )
                try:
                    before = time.perf_counter()
                    runtime.setup()
                    stages['setup_s'] = time.perf_counter() - before
                    before = time.perf_counter()
                    result = _execute_operation(
                        runtime, 'canonical',
                        SourceRunConfig(partition_key=args.partition, automatic_capacity=True),
                    )
                    stages['canonical_build_s'] = time.perf_counter() - before
                    before = time.perf_counter()
                    rows = runtime.store.execute(
                        f'SELECT component,row_count,content_hash FROM {database}.source_component_log '
                        'WHERE source_key=%(source)s AND build_id=%(build)s ORDER BY component',
                        {'source': spec.key, 'build': UUID(str(result['build_id']))},
                    )
                    proofs = [(str(k), int(str(n)), str(h)) for k, n, h in rows]
                    stages['output_proof_read_s'] = time.perf_counter() - before
                    if {k: n for k, n, _ in proofs} != {
                        'depth20': 36000,
                        'depth200': 3600,
                        'depth20_1m': 60,
                        'depth200_1m': 60,
                    }:
                        raise ValueError('Incomplete output generation.')
                    if baseline is not None and proofs != baseline:
                        raise ValueError('Repeated identical input changed output hashes.')
                    baseline = proofs
                    result = {
                        'iteration': iteration,
                        'process_cold': iteration == 0,
                        'completed_partitions': 1,
                        'pipeline_s': time.perf_counter() - wall,
                        'process_cpu_s': time.process_time() - cpu,
                        'peak_process_mib_cumulative': resource.getrusage(
                            resource.RUSAGE_SELF
                        ).ru_maxrss
                        / (1024**2 if platform.system() == 'Darwin' else 1024),
                        'stages': stages,
                        'proofs': proofs,
                        'offline_requests_cumulative': dict(request_counts),
                        'load_average': os.getloadavg(),
                    }
                    results.append(result)
                    print(json.dumps(result), flush=True)
                finally:
                    client.execute(f'DROP DATABASE IF EXISTS {database} SYNC')
    finally:
        vendor._response = response
        client.disconnect()
    report = {
        'time_utc': datetime.now(UTC).isoformat(),
        'market': market,
        'partition': args.partition,
        'scope': 'Original local bytes through setup, canonical fetch/replay, all four writes/proofs and activation; OS input cache warm; provider transfer/authentication/pacing and Dagster launch excluded. Peak RSS is cumulative Python process only; raw-source-row throughput not measured. Main totals exclude imports; process startup must be measured by the launcher.',
        'python': platform.python_version(),
        'main_elapsed_s': time.perf_counter() - process_start,
        'main_cpu_s': time.process_time() - cpu_start,
        'input_pins': {k: v.sha256 for k, v in originals.items()},
        'results': results,
    }
    args.report.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
