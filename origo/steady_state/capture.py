"""Bounded, read-only capture of steady-state evidence (PRD-0017 design section 4).

One call records one sample for the current UTC minute bucket ``U(t)`` from the existing
fact stores (ClickHouse receipts/failures/activations, the Dagster webserver, the
publication manifests and the Parquet/Arrow mounts, the worker heartbeats) into an
explicit local evidence directory. Nothing here initialises schemas, launches jobs,
touches cursors or sends alerts; every ClickHouse query is issued through
``ReadOnlyClient`` with ``readonly`` and per-query time/thread/memory/read limits, and a
probe that cannot complete is recorded as ``unknown`` with its reason, never as zero.

Evidence bundle layout (all paths relative to the output directory)::

    manifest.json                  {"schema_version", "kind": "steady_state_evidence",
                                    "files": {relpath: {"sha256", "bytes", "records"}}}
    identity/<sha256>.json         one runtime identity per distinct value seen
    samples/<YYYY-MM-DD>.jsonl     one sample object per line, one per bucket
    activations/<bucket>.jsonl.gz  periodic full activation/observation snapshot
    capture_state.json             hash-reuse cache and probe cadence; not evidence
    trials/*.json, host/*.json     supplied by the isolated trial/deploy tooling and the
                                   operator (schemas in verification.py)

Sample object (``samples/*.jsonl``)::

    {"schema_version": 1, "kind": "steady_state_sample",
     "bucket": <UTC minute>, "observed_start": <UTC>, "observed_end": <UTC>,
     "environment": "production" | "isolated", "identity_sha256": <identity>,
     "sample_id": sha256(bucket|observed_start|host),
     "sources": {<source_key>: SourceProbe}, "consumers": {"<source>:<consumer>": ConsumerProbe},
     "depth": {"depth20": DepthProbe, "depth200": DepthProbe},
     "workers": {<feed>: HeartbeatProbe}, "receipts": ReceiptProbe,
     "container_log": ContainerLogProbe, "blocking": BlockingProbe,
     "dagster": DagsterProbe, "monitor": MonitorProbe, "resources": ResourceProbe,
     "capture": {"elapsed_seconds", "queries", "query_seconds", "query_settings"}}

Every probe is ``{"status": "observed", "observed_at": <UTC>, ...}`` or
``{"status": "unknown", "observed_at": <UTC>, "reason": <text>}``.

SourceProbe: anchor, due, canonical_end, contiguous_end, newest_end, missing_minutes,
oldest_missing, incomplete_partitions, counts{canonical, provisional}, tail_start,
prefix_end (contiguous end of every selected interval ending at or before tail_start) and
tail_intervals [[key, start, end, provisional], ...] (every selected interval ending after
tail_start), from which the verifier recomputes ``F`` without trusting contiguous_end.

ConsumerProbe: manifest{exists, kind, state_token, pinned_token, active_through,
export_end_date, version, files}, series{<name>: {path, manifest_sha256, manifest_rows,
exists, bytes, mtime_ns, sha256_verified, readable, rows}} for the artifact that carries
the consumer's current coverage per series (mount: the newest listed month file plus the
Arrow latest; huggingface*: the versioned snapshot file), and remote{...} for LIVE
Hugging Face consumers at the remote probe cadence.

DepthProbe: minutes{<minute>: {snapshot_rows, projection_rows, chunk_exists, chunk_bytes,
chunk_mtime_ns}} over the worker lookback, and manifest{source_partition_key, version,
rows, updated_at_unix_ns}.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import resource
import socket
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Literal, cast

import polars as pl

from origo.sources.contracts import Client, Row
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.workers.dagster_reader import RUNS_QUERY, DagsterReader, DagsterUnreachable

from .coverage import COVERAGE_QUERY_SETTINGS, contiguous_end, read_coverage
from .policy import (
    Inventory,
    Policy,
    bucket_of,
    canonical_json,
    load_inventory,
    load_policy,
    registry_discrepancies,
    sha256_bytes,
)

log = logging.getLogger('origo.steady_state.capture')

SAMPLE_SCHEMA_VERSION = 1
MANIFEST_KIND = 'steady_state_evidence'
SAMPLE_KIND = 'steady_state_sample'
IDENTITY_KIND = 'steady_state_identity'
ACTIVATION_KIND = 'steady_state_activations'
MAINTENANCE_JOB = 'maintain_operational_metadata_job'
MONITOR_ASSET = 'origo_monitor'
ACTIVATION_SNAPSHOT_INTERVAL = timedelta(hours=6)
REMOTE_PROBE_INTERVAL = timedelta(hours=1)
CANONICAL_EVIDENCE_DAYS = 8
QUERY_SETTINGS: dict[str, object] = {
    'readonly': 1,
    'max_execution_time': 5,
    'max_threads': 2,
    'max_memory_usage': 512 * 1024 * 1024,
    'max_rows_to_read': 5_000_000,
    'max_result_rows': 100_000,
    'max_result_bytes': 32 * 1024 * 1024,
    'result_overflow_mode': 'throw',
    'read_overflow_mode': 'throw',
}
_CAPPED = (
    'max_execution_time',
    'max_threads',
    'max_memory_usage',
    'max_rows_to_read',
    'max_result_rows',
    'max_result_bytes',
)
_READ_VERBS = ('SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'DESC', 'EXISTS', 'EXPLAIN')
NONSECRET_ENV = (
    'CLICKHOUSE_HOST',
    'CLICKHOUSE_PORT',
    'CLICKHOUSE_HTTP_PORT',
    'CLICKHOUSE_DATABASE',
    'DAGSTER_WEBSERVER_URL',
    'LOCAL_PARQUET_DIR',
    'LOCAL_ARROW_DIR',
    'ORIGO_SOURCE_PUBLICATION_ROOT',
    'ORIGO_HEARTBEAT_DIR',
    'ORIGO_SOURCE_LOCK_DIR',
    'ORIGO_ALERT_COOLDOWN_SECONDS',
    'ORIGO_ALERT_QUEUE_THRESHOLD',
    'ORIGO_ALERT_DIGEST_HOUR_UTC',
    'APP_IMAGE',
    'CLICKHOUSE_IMAGE',
    'ORIGO_CODE_SHA',
    'ORIGO_ENVIRONMENT',
)
RECENT_RUNS_QUERY = """query RecentRuns($after: Float!) {
  runsOrError(filter: {updatedAfter: $after}, limit: 500) {
    __typename ... on Runs {
      results { runId status jobName creationTime startTime endTime tags { key value } }
    }
  }
}"""
JOB_SUCCESS_QUERY = """query JobSuccess($job: String!) {
  runsOrError(filter: {pipelineName: $job, statuses: [SUCCESS]}, limit: 1) {
    __typename ... on Runs { results { runId creationTime startTime endTime } }
  }
}"""
CHECK_EXECUTIONS_QUERY = """query CheckExecutions($assetKey: AssetKeyInput!, $checkName: String!) {
  assetCheckExecutions(assetKey: $assetKey, checkName: $checkName, limit: 1) {
    status evaluation { timestamp metadataEntries { label ... on TextMetadataEntry { text }
      ... on IntMetadataEntry { intValue intRepr } ... on FloatMetadataEntry { floatValue } } }
  }
}"""
CONCURRENCY_QUERY = """query Concurrency {
  instance { concurrencyLimits { concurrencyKey slotCount
    claimedSlots { runId stepKey } pendingSteps { runId stepKey enqueuedTimestamp assignedTimestamp } } }
}"""
LATEST_MATERIALIZATION_QUERY = """query Latest($assetKey: AssetKeyInput!) {
  assetOrError(assetKey: $assetKey) { __typename ... on Asset {
    assetMaterializations(limit: 1) { timestamp metadataEntries { label
      ... on TextMetadataEntry { text } ... on IntMetadataEntry { intValue intRepr }
      ... on FloatMetadataEntry { floatValue } } } } }
}"""
Environment = Literal['production', 'isolated']


class ReadOnlyClient:
    """Read queries only, always bounded; a write verb or an unbounded read is refused."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.queries = 0
        self.query_seconds = 0.0
        self.issued: list[dict[str, object]] = []

    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[Row]:
        verb = query.lstrip('( \n\t').split(None, 1)[0].upper() if query.strip() else ''
        if verb not in _READ_VERBS:
            raise PermissionError(
                f'The steady-state observer only reads; refused {verb or "empty"} query.'
            )
        merged: dict[str, object] = dict(QUERY_SETTINGS)
        for name, value in (settings or {}).items():
            if name in _CAPPED and isinstance(value, (int, float)) and not isinstance(value, bool):
                merged[name] = min(float(value), float(cast(float, QUERY_SETTINGS[name])))
                if isinstance(QUERY_SETTINGS[name], int):
                    merged[name] = int(cast(float, merged[name]))
            elif name != 'readonly':
                merged[name] = value
        merged['readonly'] = 1
        self.queries += 1
        self.issued.append({'verb': verb, 'settings': merged})
        started = time.monotonic()
        try:
            return self.client.execute(query, params, settings=merged)
        finally:
            self.query_seconds += time.monotonic() - started

    def disconnect(self) -> None:
        self.client.disconnect()


@dataclass(frozen=True)
class CaptureConfig:
    environment: Environment
    database: str
    publication_root: Path
    parquet_root: Path
    arrow_root: Path
    heartbeat_dir: Path
    lock_dir: Path
    webserver_url: str | None
    code_sha: str
    code_sha_source: str
    nonsecret: dict[str, str]
    remote_probes: bool = True
    activation_snapshots: bool = True

    @classmethod
    def from_environ(
        cls, environ: Mapping[str, str], *, environment: str, remote_probes: bool = True
    ) -> CaptureConfig:
        if environment == 'production':
            label: Environment = 'production'
        elif environment == 'isolated':
            label = 'isolated'
        else:
            raise ValueError('The capture environment must be production or isolated.')
        code_sha, source = _code_identity(environ)
        return cls(
            label,
            environ.get('CLICKHOUSE_DATABASE', 'origo'),
            Path(environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')),
            Path(environ.get('LOCAL_PARQUET_DIR', '/opt/parquet')),
            Path(environ.get('LOCAL_ARROW_DIR', '/opt/arrow')),
            Path(environ.get('ORIGO_HEARTBEAT_DIR', '/opt/origo/heartbeats')),
            Path(environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
            environ.get('DAGSTER_WEBSERVER_URL') or None,
            code_sha,
            source,
            {name: environ[name] for name in NONSECRET_ENV if name in environ},
            remote_probes,
        )


def _code_identity(environ: Mapping[str, str]) -> tuple[str, str]:
    """The deployed code identity from the environment; never the checkout's own HEAD."""
    explicit = environ.get('ORIGO_CODE_SHA', '')
    if explicit:
        return explicit, 'ORIGO_CODE_SHA'
    image = environ.get('APP_IMAGE', '')
    if ':' in image and '/' in image:
        return image.rsplit(':', 1)[1], 'APP_IMAGE'
    return '', 'unavailable'


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError('Expected a datetime.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(str(value))
    return value


def _object(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f'{what} must be an object.')
    return cast(dict[str, object], value)


def _probe(observed_at: datetime, body: Callable[[], dict[str, object]]) -> dict[str, object]:
    """Run one probe; a failure is an explicit unknown carrying its reason, never a zero."""
    try:
        return {'status': 'observed', 'observed_at': _iso(observed_at), **body()}
    except Exception as error:
        detail = f'{type(error).__name__}: {error}'[:300]
        log.warning('probe unknown: %s', detail)
        return {'status': 'unknown', 'observed_at': _iso(observed_at), 'reason': detail}


def _sha256_file(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


class _FileIdentityCache:
    """Full hashing/reading happens once per unchanged (size, mtime_ns) identity."""

    def __init__(self, entries: dict[str, object]) -> None:
        self.entries: dict[str, list[object]] = {
            key: list(cast(list[object], value)) for key, value in entries.items()
        }

    def verify(
        self, path: Path, expected_sha256: str | None, reader: Callable[[Path], int | None]
    ) -> dict[str, object]:
        stat = path.stat()
        identity = [stat.st_size, stat.st_mtime_ns]
        cached = self.entries.get(str(path))
        if (
            cached is not None
            and cached[:2] == identity
            and (expected_sha256 is None or cached[2] == expected_sha256)
        ):
            return {
                'exists': True,
                'bytes': stat.st_size,
                'mtime_ns': stat.st_mtime_ns,
                'sha256_verified': bool(cached[3]),
                'readable': bool(cached[4]),
                'rows': cached[5],
                'reused_identity': True,
            }
        digest = _sha256_file(path)
        verified = expected_sha256 is None or digest == expected_sha256
        rows: int | None = None
        readable = False
        if verified:
            try:
                rows = reader(path)
                readable = True
            except Exception as error:
                log.warning('%s unreadable: %s', path, error)
        self.entries[str(path)] = [*identity, digest, verified, readable, rows]
        return {
            'exists': True,
            'bytes': stat.st_size,
            'mtime_ns': stat.st_mtime_ns,
            'sha256_verified': verified,
            'readable': readable,
            'rows': rows,
            'reused_identity': False,
        }


def _parquet_rows(path: Path) -> int | None:
    pl.read_parquet_schema(path)
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


def _ipc_rows(path: Path) -> int | None:
    pl.read_ipc_schema(path)
    return int(pl.scan_ipc(path).select(pl.len()).collect().item())


@dataclass
class EvidenceWriter:
    """Appends samples and rewrites the manifest so every artifact is hash-bound."""

    root: Path
    state: dict[str, object] = field(default_factory=dict[str, object])

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        state_path = self.root / 'capture_state.json'
        if state_path.exists():
            self.state = _object(json.loads(state_path.read_text()), 'capture_state')

    def file_cache(self) -> _FileIdentityCache:
        return _FileIdentityCache(_object(self.state.get('file_hashes') or {}, 'file_hashes'))

    def last(self, key: str) -> datetime | None:
        value = self.state.get(key)
        return datetime.fromisoformat(str(value)) if isinstance(value, str) else None

    def write_identity(self, identity: dict[str, object]) -> str:
        digest = sha256_bytes(canonical_json(identity))
        path = self.root / 'identity' / f'{digest}.json'
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(canonical_json({**identity, 'runtime_identity_sha256': digest}))
        return digest

    def append_sample(self, sample: dict[str, object]) -> Path:
        bucket = datetime.fromisoformat(str(sample['bucket']))
        path = self.root / 'samples' / f'{bucket:%Y-%m-%d}.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('ab') as handle:
            handle.write(canonical_json(sample))
            handle.flush()
            os.fsync(handle.fileno())
        return path

    def write_activations(self, bucket: datetime, rows: Sequence[dict[str, object]]) -> Path:
        path = self.root / 'activations' / f'{bucket:%Y%m%dT%H%M%SZ}.jsonl.gz'
        path.parent.mkdir(parents=True, exist_ok=True)
        body = b''.join(canonical_json(row) for row in rows)
        path.write_bytes(gzip.compress(body, mtime=0))
        return path

    def write_artifact(self, relative: str, document: dict[str, object]) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_json(document))
        return path

    def save_state(self, cache: _FileIdentityCache, **stamps: datetime) -> None:
        self.state['file_hashes'] = cache.entries
        for key, value in stamps.items():
            self.state[key] = _iso(value)
        (self.root / 'capture_state.json').write_bytes(canonical_json(self.state))

    def write_manifest(self) -> dict[str, object]:
        files: dict[str, object] = {}
        for path in sorted(self.root.rglob('*')):
            relative = path.relative_to(self.root).as_posix()
            if not path.is_file() or relative in ('manifest.json', 'capture_state.json'):
                continue
            payload = path.read_bytes()
            records = None
            if relative.endswith('.jsonl'):
                records = payload.count(b'\n')
            elif relative.endswith('.jsonl.gz'):
                records = gzip.decompress(payload).count(b'\n')
            files[relative] = {
                'sha256': sha256_bytes(payload),
                'bytes': len(payload),
                'records': records,
            }
        manifest: dict[str, object] = {
            'schema_version': SAMPLE_SCHEMA_VERSION,
            'kind': MANIFEST_KIND,
            'written_at': _iso(datetime.now(UTC)),
            'files': files,
        }
        (self.root / 'manifest.json').write_bytes(canonical_json(manifest))
        return manifest


def runtime_identity(
    config: CaptureConfig, policy: Policy, inventory: Inventory, client: ReadOnlyClient
) -> dict[str, object]:
    """Nonsecret identity of the observed runtime; a change mid-window restarts the window."""
    try:
        version = str(client.execute('SELECT version()')[0][0])
    except (OSError, RuntimeError, ValueError, EOFError) as error:
        version = f'unknown: {type(error).__name__}'
    try:
        package = package_version('origo')
    except PackageNotFoundError:
        package = 'unknown: origo is not an installed distribution'
    return {
        'kind': IDENTITY_KIND,
        'schema_version': SAMPLE_SCHEMA_VERSION,
        'environment': config.environment,
        'code_sha': config.code_sha,
        'code_sha_source': config.code_sha_source,
        'package_version': package,
        'policy_sha256': policy.sha256,
        'inventory_sha256': inventory.sha256,
        'clickhouse_version': version,
        'python_version': sys.version.split()[0],
        'nonsecret_config': config.nonsecret,
        'inventory_discrepancies': list(registry_discrepancies(inventory, SOURCE_REGISTRY)),
        'query_settings': QUERY_SETTINGS,
    }


def _source_probe(
    client: ReadOnlyClient, database: str, key: str, now: datetime
) -> dict[str, object]:
    spec = next(item for item in SOURCE_REGISTRY if item.key == key)
    store = SourceStore(cast(Client, client), database, spec)
    coverage = read_coverage(store, now)
    tail_start = coverage.due - timedelta(hours=2)
    prefix = tuple(item for item in coverage.intervals if item.end <= tail_start)
    tail = tuple(item for item in coverage.intervals if item.end > tail_start)
    return {
        'anchor': _iso(coverage.anchor),
        'due': _iso(coverage.due),
        'canonical_end': _iso(coverage.canonical_end),
        'contiguous_end': _iso(coverage.contiguous_end),
        'newest_end': _iso(coverage.newest_end),
        'missing_minutes': coverage.missing_minutes,
        'oldest_missing': _iso(coverage.oldest_missing) if coverage.oldest_missing else None,
        'incomplete_partitions': list(coverage.incomplete_partitions[:20]),
        'incomplete_partition_count': len(coverage.incomplete_partitions),
        'counts': {
            'canonical': sum(1 for item in coverage.intervals if not item.provisional),
            'provisional': sum(1 for item in coverage.intervals if item.provisional),
        },
        'tail_start': _iso(tail_start),
        'prefix_end': _iso(contiguous_end(coverage.anchor, prefix)),
        'tail_intervals': [
            [item.key, _iso(item.start), _iso(item.end), item.provisional] for item in tail
        ],
    }


def _manifest(path: Path) -> dict[str, object]:
    return _object(json.loads(path.read_text()), str(path))


def _consumer_probe(
    config: CaptureConfig,
    inventory: Inventory,
    source_key: str,
    consumer_key: str,
    cache: _FileIdentityCache,
) -> dict[str, object]:
    source = inventory.sources[source_key]
    root = config.publication_root / source_key / consumer_key
    path = root / 'latest.json'
    if not path.exists():
        return {'manifest': {'exists': False, 'path': str(path)}, 'series': {}}
    manifest = _manifest(path)
    files = cast(list[object], manifest.get('files') or [])
    entries = [_object(entry, 'file') for entry in files]
    version = str(manifest.get('version') or '')
    summary: dict[str, object] = {
        'exists': True,
        'path': str(path),
        'mtime_ns': path.stat().st_mtime_ns,
        'kind': manifest.get('kind'),
        'state_token': manifest.get('state_token'),
        'pinned_token': manifest.get('pinned_token'),
        'active_through': manifest.get('active_through'),
        'export_end_date': manifest.get('export_end_date'),
        'version': version,
        'files': len(entries),
        'uploads': len(cast(list[object], manifest.get('uploads') or [])),
    }
    series: dict[str, object] = {}
    for declared in source.series:
        checks: dict[str, object] = {}
        if consumer_key == 'mount':
            months = [
                entry
                for entry in entries
                if entry.get('series') == declared.name and entry.get('kind') != 'arrow'
            ]
            arrow = [
                entry
                for entry in entries
                if entry.get('series') == declared.name and entry.get('kind') == 'arrow'
            ]
            newest = max(months, key=lambda entry: str(entry.get('month', ''))) if months else None
            checks['months_listed'] = len(months)
            checks['newest_month'] = newest.get('month') if newest else None
            checks['month'] = _verify_entry(newest, cache, _parquet_rows, base=None)
            checks['arrow'] = _verify_entry(
                arrow[0] if arrow else None, cache, _ipc_rows, base=None
            )
        else:
            snapshot = [
                entry
                for entry in entries
                if str(entry.get('path', '')).startswith(declared.name + '/')
            ]
            checks['snapshot'] = _verify_entry(
                snapshot[0] if snapshot else None,
                cache,
                _parquet_rows,
                base=root / 'versions' / version,
            )
        series[declared.name] = checks
    return {'manifest': summary, 'series': series}


def _verify_entry(
    entry: dict[str, object] | None,
    cache: _FileIdentityCache,
    reader: Callable[[Path], int | None],
    *,
    base: Path | None,
) -> dict[str, object]:
    if entry is None:
        return {'listed': False, 'exists': False}
    relative = Path(str(entry.get('path', '')))
    location = relative if relative.is_absolute() or base is None else base / relative
    digest = entry.get('sha256')
    result: dict[str, object] = {
        'listed': True,
        'path': str(location),
        'manifest_sha256': digest,
        'manifest_rows': entry.get('row_count'),
    }
    if not location.is_file():
        return {**result, 'exists': False}
    return {
        **result,
        **cache.verify(location, str(digest) if isinstance(digest, str) else None, reader),
    }


def _remote_probe(
    inventory: Inventory, source_key: str, environ: Mapping[str, str]
) -> dict[str, object]:
    from huggingface_hub import HfApi

    api = HfApi()
    repos: dict[str, object] = {}
    for series in inventory.sources[source_key].series:
        repo_id = environ.get(series.repo_id_env, '') if series.repo_id_env else ''
        repo_id = repo_id or series.dataset
        info = api.dataset_info(repo_id)
        names = sorted(
            str(sibling.rfilename)
            for sibling in (info.siblings or [])
            if str(sibling.rfilename).startswith(series.file_prefix)
        )
        repos[series.name] = {
            'repo_id': repo_id,
            'sha': info.sha,
            'last_modified': info.last_modified.isoformat() if info.last_modified else None,
            'files': names,
        }
    return {'repos': repos}


def _depth_probe(
    client: ReadOnlyClient,
    config: CaptureConfig,
    inventory: Inventory,
    key: str,
    now: datetime,
    lookback: int,
) -> dict[str, object]:
    depth = inventory.depth[key]
    last = bucket_of(now) - timedelta(minutes=1)
    first = last - timedelta(minutes=lookback - 1)
    minutes: dict[str, dict[str, object]] = {
        _iso(first + timedelta(minutes=offset)): {'snapshot_rows': 0, 'projection_rows': 0}
        for offset in range(lookback)
    }
    for table, column in (
        (depth.snapshot_table, 'snapshot_rows'),
        (depth.projection_table, 'projection_rows'),
    ):
        rows = client.execute(
            f'SELECT toStartOfMinute(datetime) AS m, count() FROM {config.database}.{table} '
            'WHERE datetime >= %(first)s AND datetime < %(until)s GROUP BY m',
            {'first': _naive(first), 'until': _naive(last + timedelta(minutes=1))},
        )
        for minute, count in rows:
            stamp = _iso(_utc(minute))
            if stamp in minutes:
                minutes[stamp][column] = _int(count)
    directory = config.arrow_root / depth.series
    for stamp, entry in minutes.items():
        minute = datetime.fromisoformat(stamp)
        chunk = directory / minute.strftime(depth.chunk_pattern)
        entry['chunk_exists'] = chunk.is_file()
        if chunk.is_file():
            stat = chunk.stat()
            entry['chunk_bytes'] = stat.st_size
            entry['chunk_mtime_ns'] = stat.st_mtime_ns
    manifest_path = directory / depth.arrow_manifest
    manifest: dict[str, object] = {'exists': manifest_path.is_file()}
    if manifest_path.is_file():
        document = _manifest(manifest_path)
        manifest.update(
            {
                'source_partition_key': document.get('source_partition_key'),
                'version': document.get('version'),
                'rows': document.get('rows'),
                'updated_at_unix_ns': document.get('updated_at_unix_ns'),
                'mtime_ns': manifest_path.stat().st_mtime_ns,
            }
        )
    return {'minutes': minutes, 'manifest': manifest}


def _heartbeat_probe(
    config: CaptureConfig, inventory: Inventory, now: datetime
) -> dict[str, object]:
    beats: dict[str, object] = {}
    for feed, name in inventory.workers.items():
        path = config.heartbeat_dir / name
        if not path.is_file():
            beats[feed] = {
                'status': 'unknown',
                'observed_at': _iso(now),
                'reason': f'{path} is missing',
            }
            continue
        mtime = path.stat().st_mtime
        beats[feed] = {
            'status': 'observed',
            'observed_at': _iso(now),
            'mtime': _iso(datetime.fromtimestamp(mtime, UTC)),
            'age_seconds': round(now.timestamp() - mtime, 3),
        }
    return beats


def _receipt_probe(
    client: ReadOnlyClient, config: CaptureConfig, since: datetime, until: datetime
) -> dict[str, object]:
    rows = client.execute(
        f'SELECT feed, series, minute, status, rows, sha256, error_code, duration_ms, recorded_at '
        f'FROM {config.database}.worker_minute_log '
        'WHERE recorded_at > %(since)s AND recorded_at <= %(until)s ORDER BY recorded_at LIMIT 2001',
        {'since': _naive(since), 'until': _naive(until)},
    )
    if len(rows) > 2000:
        raise RuntimeError('More than 2000 receipts in one window; evidence would be truncated.')
    return {
        'since': _iso(since),
        'until': _iso(until),
        'rows': [
            {
                'feed': str(feed),
                'series': str(series),
                'minute': _iso(_utc(minute)),
                'status': str(status),
                'rows': _int(count),
                'sha256': str(digest)[:16],
                'error_code': str(code),
                'duration_ms': _int(duration),
                'recorded_at': _iso(_utc(recorded)),
            }
            for feed, series, minute, status, count, digest, code, duration, recorded in rows
        ],
    }


def _container_log_probe(
    client: ReadOnlyClient, config: CaptureConfig, since: datetime, until: datetime
) -> dict[str, object]:
    rows = client.execute(
        f"""SELECT service, countIf(level = 'ERROR'),
                   countIf(position(message, 'exiting for a restart') > 0),
                   countIf(position(message, 'Out of memory') > 0 OR position(message, 'oom') > 0)
            FROM {config.database}.container_log
            WHERE timestamp > %(since)s AND timestamp <= %(until)s GROUP BY service""",
        {'since': _naive(since), 'until': _naive(until)},
    )
    return {
        'since': _iso(since),
        'until': _iso(until),
        'services': {
            str(service): {
                'errors': _int(errors),
                'watchdog_exits': _int(exits),
                'oom_mentions': _int(oom),
            }
            for service, errors, exits, oom in rows
        },
    }


def _blocking_probe(client: ReadOnlyClient, config: CaptureConfig) -> dict[str, object]:
    rows = client.execute(
        f"""SELECT source_key, scope, code, count() FROM (
                SELECT source_key, failure_key,
                       argMax(event_type, event_time) AS state,
                       argMax(blocking_scope, event_time) AS scope,
                       argMax(error_code, event_time) AS code
                FROM {config.database}.source_failure_log GROUP BY source_key, failure_key
            ) WHERE state = 'FAILED' AND scope != 'NONE'
            GROUP BY source_key, scope, code ORDER BY source_key, scope, code""",
    )
    outstanding: dict[str, int] = {}
    for feed in ('depth', 'provisional'):
        found = client.execute(
            f'SELECT count() FROM (SELECT DISTINCT attempt_id FROM {config.database}.worker_minute_log '
            "WHERE feed=%(feed)s AND status='STARTED' AND attempt_id != %(zero)s "
            f'AND attempt_id NOT IN (SELECT attempt_id FROM {config.database}.worker_minute_log '
            "WHERE feed=%(feed)s AND status != 'STARTED'))",
            {'feed': feed, 'zero': '00000000-0000-0000-0000-000000000000'},
        )
        outstanding[feed] = _int(found[0][0])
    return {
        'open_failures': [
            {
                'source_key': str(source),
                'scope': str(scope),
                'error_code': str(code),
                'count': _int(count),
            }
            for source, scope, code, count in rows
        ],
        'outstanding_attempts': outstanding,
    }


def _metadata_value(entry: dict[str, object]) -> object:
    for name in ('intValue', 'floatValue', 'text', 'intRepr'):
        if entry.get(name) is not None:
            return entry[name]
    return None


def _dagster_probe(
    reader: DagsterReader, inventory: Inventory, since: datetime, now: datetime
) -> dict[str, object]:
    health = reader.health()
    if not health.reachable:
        raise DagsterUnreachable('The Dagster webserver did not answer the health query.')
    queued = reader.query(
        'Runs', RUNS_QUERY, {'filter': {'statuses': ['QUEUED']}, 'cursor': None, 'limit': 250}
    )
    queued_runs = _object(queued.get('runsOrError'), 'queued')
    recent = _object(
        reader.query('RecentRuns', RECENT_RUNS_QUERY, {'after': since.timestamp()}).get(
            'runsOrError'
        ),
        'recent',
    )
    success = _object(
        reader.query('JobSuccess', JOB_SUCCESS_QUERY, {'job': MAINTENANCE_JOB}).get('runsOrError'),
        'success',
    )
    if 'Runs' not in (
        queued_runs.get('__typename'),
        recent.get('__typename'),
        success.get('__typename'),
    ):
        raise DagsterUnreachable('Runs were not listed.')

    def run(item: object) -> dict[str, object]:
        entry = _object(item, 'run')
        tags = {
            str(_object(tag, 'tag')['key']): str(_object(tag, 'tag')['value'])
            for tag in cast(list[object], entry.get('tags') or [])
        }
        return {
            'run_id': str(entry.get('runId')),
            'status': str(entry.get('status')),
            'job_name': str(entry.get('jobName')),
            'created_at': entry.get('creationTime'),
            'start_time': entry.get('startTime'),
            'end_time': entry.get('endTime'),
            'partition': tags.get('dagster/partition'),
            'backfill': tags.get('dagster/backfill'),
            'operation': tags.get('origo_source_operation'),
            'reconciliation': tags.get('origo_source_reconciliation'),
        }

    checks: dict[str, object] = {}
    for name in inventory.monitor_checks:
        executions = cast(
            list[object],
            reader.query(
                'CheckExecutions',
                CHECK_EXECUTIONS_QUERY,
                {'assetKey': {'path': [MONITOR_ASSET]}, 'checkName': name},
            ).get('assetCheckExecutions')
            or [],
        )
        if not executions:
            checks[name] = None
            continue
        latest = _object(executions[0], 'execution')
        raw_evaluation: object = latest.get('evaluation')
        evaluation = _object(raw_evaluation, 'evaluation') if raw_evaluation is not None else {}
        checks[name] = {
            'status': latest.get('status'),
            'timestamp': evaluation.get('timestamp'),
            'metadata': {
                str(_object(entry, 'metadata')['label']): _metadata_value(
                    _object(entry, 'metadata')
                )
                for entry in cast(list[object], evaluation.get('metadataEntries') or [])
            },
        }
    pools: list[dict[str, object]] = []
    instance = _object(reader.query('Concurrency', CONCURRENCY_QUERY).get('instance'), 'instance')
    for item in cast(list[object], instance.get('concurrencyLimits') or []):
        entry = _object(item, 'limit')
        pools.append(
            {
                'key': entry.get('concurrencyKey'),
                'slots': entry.get('slotCount'),
                'claimed': [
                    {
                        'run_id': _object(slot, 'slot').get('runId'),
                        'step': _object(slot, 'slot').get('stepKey'),
                    }
                    for slot in cast(list[object], entry.get('claimedSlots') or [])
                ],
                'pending': len(cast(list[object], entry.get('pendingSteps') or [])),
            }
        )
    feeds: dict[str, object] = {}
    for asset in (
        *(f'{key}_provisional_feed' for key in inventory.sources),
        'binance_spot_depth_live_feed',
    ):
        feeds[asset] = _latest_materialization(reader, asset)
    return {
        'reachable': True,
        'unhealthy_daemons': list(health.unhealthy_daemons),
        'queued_runs': health.queued_runs,
        'queued': [run(item) for item in cast(list[object], queued_runs.get('results') or [])],
        'recent_since': since.timestamp(),
        'recent': [run(item) for item in cast(list[object], recent.get('results') or [])],
        'maintenance': {
            'job': MAINTENANCE_JOB,
            'latest_success': [
                run(item) for item in cast(list[object], success.get('results') or [])
            ][:1],
        },
        'monitor_checks': checks,
        'pools': pools,
        'live_feeds': feeds,
        'queried_at': now.timestamp(),
    }


def _latest_materialization(reader: DagsterReader, asset: str) -> dict[str, object] | None:
    data = _object(
        reader.query('Latest', LATEST_MATERIALIZATION_QUERY, {'assetKey': {'path': [asset]}}).get(
            'assetOrError'
        ),
        asset,
    )
    if data.get('__typename') != 'Asset':
        return None
    events = cast(list[object], data.get('assetMaterializations') or [])
    if not events:
        return None
    event = _object(events[0], 'event')
    return {
        'timestamp': event.get('timestamp'),
        'metadata': {
            str(_object(entry, 'metadata')['label']): _metadata_value(_object(entry, 'metadata'))
            for entry in cast(list[object], event.get('metadataEntries') or [])
        },
    }


def _monitor_probe(config: CaptureConfig, now: datetime) -> dict[str, object]:
    cursor = config.heartbeat_dir / 'monitor.cursor.json'
    if not cursor.is_file():
        raise FileNotFoundError(f'{cursor} is missing')
    data = _object(json.loads(cursor.read_text()), 'cursor')
    return {
        'cursor_mtime': _iso(datetime.fromtimestamp(cursor.stat().st_mtime, UTC)),
        'ticks': data.get('ticks'),
        'findings': data.get('findings'),
        'sent_keys': len(_object(data.get('sent') or {}, 'sent')),
        'receipts_after': data.get('receipts_after'),
        'logs_after': data.get('logs_after'),
    }


def _read_first_line(path: Path) -> str:
    return path.read_text().splitlines()[0].strip() if path.is_file() else ''


def _resource_probe(config: CaptureConfig, now: datetime) -> dict[str, object]:
    disks: dict[str, object] = {}
    for label, root in (
        ('parquet', config.parquet_root),
        ('arrow', config.arrow_root),
        ('publication', config.publication_root),
        ('heartbeats', config.heartbeat_dir),
    ):
        if not root.exists():
            disks[label] = {'status': 'unknown', 'reason': f'{root} does not exist'}
            continue
        usage = os.statvfs(root)
        total = usage.f_frsize * usage.f_blocks
        free = usage.f_frsize * usage.f_bavail
        disks[label] = {
            'status': 'observed',
            'path': str(root),
            'total_bytes': total,
            'free_bytes': free,
            'reserve_ratio': round(free / total, 6) if total else None,
        }
    memory: dict[str, object] = {'status': 'unknown', 'reason': '/proc/meminfo is unavailable'}
    meminfo = Path('/proc/meminfo')
    if meminfo.is_file():
        values: dict[str, int] = {}
        for line in meminfo.read_text().splitlines():
            name, _, rest = line.partition(':')
            digits = rest.split()
            if digits and digits[0].isdigit():
                values[name] = int(digits[0]) * 1024
        if 'MemTotal' in values and 'MemAvailable' in values:
            memory = {
                'status': 'observed',
                'total_bytes': values['MemTotal'],
                'available_bytes': values['MemAvailable'],
                'available_ratio': round(values['MemAvailable'] / values['MemTotal'], 6),
            }
    cgroup: dict[str, object] = {
        'status': 'unknown',
        'reason': 'cgroup v2 memory files are unavailable',
    }
    current = _read_first_line(Path('/sys/fs/cgroup/memory.current'))
    limit = _read_first_line(Path('/sys/fs/cgroup/memory.max'))
    if current.isdigit():
        cgroup = {
            'status': 'observed',
            'current_bytes': int(current),
            'limit_bytes': int(limit) if limit.isdigit() else None,
            'peak_bytes': int(peak)
            if (peak := _read_first_line(Path('/sys/fs/cgroup/memory.peak'))).isdigit()
            else None,
            'scope': 'capture process container only',
        }
    staging: list[dict[str, object]] = []
    if config.parquet_root.is_dir():
        for orphan in sorted(config.parquet_root.glob('.*staging-*')):
            staging.append(
                {
                    'path': str(orphan),
                    'age_seconds': round(now.timestamp() - orphan.stat().st_mtime, 1),
                    'directory': orphan.is_dir(),
                }
            )
    if config.publication_root.is_dir():
        for orphan in sorted(config.publication_root.glob('*/*/.*.partial-*')):
            staging.append(
                {
                    'path': str(orphan),
                    'age_seconds': round(now.timestamp() - orphan.stat().st_mtime, 1),
                    'directory': False,
                }
            )
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        'disks': disks,
        'host_memory': memory,
        'cgroup_memory': cgroup,
        'staging_orphans': staging,
        'capture_rss_bytes': usage if sys.platform == 'darwin' else usage * 1024,
    }


def _activation_snapshot(
    client: ReadOnlyClient, config: CaptureConfig, inventory: Inventory, now: datetime
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    since = _naive(now - timedelta(days=CANONICAL_EVIDENCE_DAYS))
    for key in inventory.sources:
        anchors = client.execute(
            f'SELECT anchor FROM {config.database}.source_anchor_log WHERE source_key=%(source)s',
            {'source': key},
            settings=COVERAGE_QUERY_SETTINGS,
        )
        rows.append(
            {
                'table': 'source_anchor_log',
                'source_key': key,
                'anchors': [_iso(_utc(row[0])) for row in anchors],
            }
        )
        for (
            partition,
            start,
            end,
            provisional,
            generation,
            revision,
            build,
            hashes,
            activated,
        ) in client.execute(
            f"""SELECT a.partition_key, a.partition_start, a.partition_end, a.provisional,
                       a.generation, a.revision, a.build_id, a.component_hashes,
                       l.activated_at
                FROM {config.database}.source_active_partitions AS a
                INNER JOIN (
                    SELECT partition_key, generation, activated_at
                    FROM {config.database}.source_activation_log WHERE source_key=%(source)s
                ) AS l USING (partition_key, generation)
                WHERE a.source_key=%(source)s ORDER BY a.partition_start, a.provisional""",
            {'source': key},
            settings=COVERAGE_QUERY_SETTINGS,
        ):
            rows.append(
                {
                    'table': 'source_active_partitions',
                    'source_key': key,
                    'partition_key': str(partition),
                    'partition_start': _iso(_utc(start)),
                    'partition_end': _iso(_utc(end)),
                    'provisional': bool(provisional),
                    'generation': _int(generation),
                    'revision': str(revision),
                    'build_id': str(build),
                    'component_hashes': json.loads(str(hashes)),
                    'activated_at': _iso(_utc(activated)),
                }
            )
        for partition, complete, observed in client.execute(
            f"""SELECT partition_key, complete, observed_at FROM {config.database}.source_observation_log
                WHERE source_key=%(source)s AND length(partition_key) = 10 AND observed_at > %(since)s
                ORDER BY observed_at""",
            {'source': key, 'since': since},
        ):
            rows.append(
                {
                    'table': 'source_observation_log',
                    'source_key': key,
                    'partition_key': str(partition),
                    'complete': bool(_int(complete)),
                    'observed_at': _iso(_utc(observed)),
                }
            )
        for partition, requested in client.execute(
            f'SELECT partition_key, min(requested_at) FROM {config.database}.source_discovery_log '
            'WHERE source_key=%(source)s AND requested_at > %(since)s GROUP BY partition_key',
            {'source': key, 'since': since},
        ):
            rows.append(
                {
                    'table': 'source_discovery_log',
                    'source_key': key,
                    'partition_key': str(partition),
                    'requested_at': _iso(_utc(requested)),
                }
            )
    return rows


def capture_sample(
    config: CaptureConfig,
    *,
    client: Client,
    writer: EvidenceWriter,
    now: datetime | None = None,
    policy: Policy | None = None,
    inventory: Inventory | None = None,
    dagster: DagsterReader | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Record one bounded sample for the bucket containing ``now`` and return it."""
    policy = policy or load_policy()
    products = inventory or load_inventory()
    environ = environ if environ is not None else os.environ
    started = datetime.now(UTC) if now is None else now.astimezone(UTC)
    bucket = bucket_of(started, policy.sample_period_seconds)
    observer = ReadOnlyClient(client)
    identity = runtime_identity(config, policy, products, observer)
    identity_sha = writer.write_identity(identity)
    cache = writer.file_cache()
    stamp = time.monotonic()
    lag = timedelta(seconds=policy.delivery_lag_seconds)
    period = timedelta(seconds=policy.sample_period_seconds)
    sample: dict[str, object] = {
        'schema_version': SAMPLE_SCHEMA_VERSION,
        'kind': SAMPLE_KIND,
        'bucket': _iso(bucket),
        'observed_start': _iso(started),
        'environment': config.environment,
        'identity_sha256': identity_sha,
        'host': socket.gethostname(),
        'sources': {
            key: _probe(
                started, lambda key=key: _source_probe(observer, config.database, key, started)
            )
            for key in products.sources
        },
        'consumers': {
            f'{source.key}:{consumer.key}': _probe(
                started,
                lambda source=source, consumer=consumer: _consumer_probe(
                    config, products, source.key, consumer.key, cache
                ),
            )
            for source in products.sources.values()
            for consumer in source.consumers
        },
        'depth': {
            key: _probe(
                started, lambda key=key: _depth_probe(observer, config, products, key, started, 15)
            )
            for key in products.depth
        },
        'workers': _heartbeat_probe(config, products, started),
        # Fixed, non-overlapping delivery-lagged windows: rows stamped in (b-120 s, b-60 s].
        'receipts': _probe(
            started, lambda: _receipt_probe(observer, config, bucket - period - lag, bucket - lag)
        ),
        'container_log': _probe(
            started,
            lambda: _container_log_probe(observer, config, bucket - period - lag, bucket - lag),
        ),
        'blocking': _probe(started, lambda: _blocking_probe(observer, config)),
        'monitor': _probe(started, lambda: _monitor_probe(config, started)),
        'resources': _probe(started, lambda: _resource_probe(config, started)),
    }
    reader = dagster or (DagsterReader(config.webserver_url) if config.webserver_url else None)
    sample['dagster'] = (
        _probe(
            started,
            lambda: _dagster_probe(reader, products, bucket - timedelta(minutes=15), started),
        )
        if reader is not None
        else {
            'status': 'unknown',
            'observed_at': _iso(started),
            'reason': 'DAGSTER_WEBSERVER_URL is not set',
        }
    )
    last_remote = writer.last('last_remote_probe')
    if config.remote_probes and (
        last_remote is None or started - last_remote >= REMOTE_PROBE_INTERVAL
    ):
        consumers = cast(dict[str, dict[str, object]], sample['consumers'])
        for source in products.sources.values():
            for consumer in source.consumers:
                if consumer.destination == 'remote':
                    consumers[f'{source.key}:{consumer.key}']['remote'] = _probe(
                        started, lambda source=source: _remote_probe(products, source.key, environ)
                    )
        writer.state['last_remote_probe'] = _iso(started)
    last_snapshot = writer.last('last_activation_snapshot')
    if config.activation_snapshots and (
        last_snapshot is None or started - last_snapshot >= ACTIVATION_SNAPSHOT_INTERVAL
    ):
        snapshot = _probe(
            started, lambda: {'rows': _activation_snapshot(observer, config, products, started)}
        )
        if snapshot['status'] == 'observed':
            rows = cast(list[dict[str, object]], snapshot['rows'])
            header: dict[str, object] = {
                'kind': ACTIVATION_KIND,
                'schema_version': SAMPLE_SCHEMA_VERSION,
                'bucket': _iso(bucket),
                'observed_at': _iso(started),
                'identity_sha256': identity_sha,
                'rows': len(rows),
            }
            writer.write_activations(bucket, [header, *rows])
            writer.state['last_activation_snapshot'] = _iso(started)
            sample['activation_snapshot'] = {
                'status': 'observed',
                'rows': len(rows),
                'bucket': _iso(bucket),
            }
        else:
            sample['activation_snapshot'] = snapshot
    finished = (
        datetime.now(UTC) if now is None else started + timedelta(seconds=time.monotonic() - stamp)
    )
    sample['observed_end'] = _iso(finished)
    sample['capture'] = {
        'elapsed_seconds': round(time.monotonic() - stamp, 3),
        'queries': observer.queries,
        'query_seconds': round(observer.query_seconds, 3),
        'query_settings': QUERY_SETTINGS,
    }
    sample['sample_id'] = sha256_bytes(
        f'{sample["bucket"]}|{sample["observed_start"]}|{sample["host"]}'.encode()
    )
    writer.append_sample(sample)
    writer.save_state(cache)
    writer.write_manifest()
    return sample
