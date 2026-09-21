"""Read-only semantic verification of the exact deployment and maintenance run.

This observer never creates jobs, repairs data or initializes application state.
The deployment workflow owns launch; this command must observe its terminal result.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.contracts import Client
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.workers.dagster_reader import DagsterReader
from .capture import ReadOnlyClient
from .coverage import read_coverage
from .policy import canonical_json, load_inventory, load_policy
from .publication import manifest_delivered_through

RUN_QUERY = """query DeploymentRun($id: String!) {
  runOrError(runId: $id) {
    __typename ... on Run {
      runId jobName status creationTime startTime endTime
    }
  }
}"""
MAINTENANCE_JOB = 'maintain_operational_metadata_job'
DEFAULT_DEADLINE_SECONDS = 900


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f'{name} must be an object.')
    return cast(dict[str, object], value)


def _date(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError('Deployment evidence timestamps require UTC offsets.')
    return parsed.astimezone(UTC)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('Deployment event timestamps must be numeric.')
    return float(value)


def verify_observation(
    document: Mapping[str, object], *, expected_sha: str, maintenance_run_id: str,
    ready_at: datetime, sources: Sequence[str],
) -> tuple[str, ...]:
    """Reasons this observation has not established the requested deployment outcome."""
    problems: list[str] = []
    if document.get('code_sha') != expected_sha:
        problems.append('deployed code identity does not match the candidate')
    if document.get('daemons_healthy') is not True:
        problems.append('required Dagster daemons are not healthy')
    run = _object(document.get('maintenance'), 'maintenance')
    if run.get('runId') != maintenance_run_id or run.get('jobName') != MAINTENANCE_JOB:
        problems.append('maintenance evidence belongs to another run or job')
    if run.get('status') != 'SUCCESS':
        problems.append(f'maintenance run is {run.get("status", "UNKNOWN")}, not SUCCESS')
    if _number(run.get('creationTime')) < ready_at.timestamp():
        problems.append('maintenance evidence predates this deployment')
    for key in sources:
        source = _object(_object(document.get('sources'), 'sources').get(key), key)
        if source.get('components_complete') is not True:
            problems.append(f'{key}: accepted component evidence is incomplete')
        for stamp in ('build_completed_at', 'publication_completed_at'):
            if _date(source.get(stamp)) <= ready_at:
                problems.append(f'{key}: {stamp} predates this deployment')
        if _date(source.get('published_end')) < _date(source.get('new_interval_end')):
            problems.append(f'{key}: publication has not caught up to its new accepted interval')
        if source.get('files_verified') is not True:
            problems.append(f'{key}: declared published files are missing, unreadable or mismatched')
    return tuple(problems)


def _files(manifest: Mapping[str, object], expected: set[str], deadline: float) -> bool:
    import polars as pl
    rows = manifest.get('files')
    if not isinstance(rows, list) or not rows or len(rows) > 5000:
        return False
    seen: dict[str, set[str]] = {'parquet': set(), 'arrow': set()}
    for value in cast(list[object], rows):
        entry = _object(value, 'file')
        path = Path(str(entry.get('path', '')))
        kind = 'arrow' if entry.get('kind') == 'arrow' else 'parquet'
        series = str(entry.get('series', ''))
        if series not in expected or not path.is_file():
            return False
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            while block := handle.read(1024 * 1024):
                if time.monotonic() > deadline:
                    raise TimeoutError('Deployment file verification exceeded its deadline.')
                digest.update(block)
        if digest.hexdigest() != entry.get('sha256'):
            return False
        pl.read_ipc_schema(path) if kind == 'arrow' else pl.read_parquet_schema(path)
        seen[kind].add(series)
    return all(names == expected for names in seen.values())


def observe(
    client: Client, dagster: DagsterReader, *, database: str, root: Path,
    code_sha: str, maintenance_run_id: str, ready_at: datetime, deadline: float,
) -> dict[str, object]:
    inventory = load_inventory()
    reader = ReadOnlyClient(client)
    run = _object(dagster.query('DeploymentRun', RUN_QUERY, {'id': maintenance_run_id}).get('runOrError'), 'run')
    health = dagster.health()
    sources: dict[str, object] = {}
    for spec in SOURCE_REGISTRY:
        if spec.key not in inventory.sources:
            raise ValueError('Deployment source inventory changed.')
        coverage = read_coverage(SourceStore(reader, database, spec), datetime.now(UTC))
        # Select a new completion for a minute that is within current verified coverage.
        result = reader.execute(
            f'SELECT minute, recorded_at FROM {database}.worker_minute_log '
            "WHERE feed='provisional' AND series=%(source)s AND status='OK' "
            'AND recorded_at > %(ready)s AND minute < %(frontier)s '
            'ORDER BY recorded_at DESC LIMIT 1',
            {'source': spec.key, 'ready': ready_at.replace(tzinfo=None),
             'frontier': coverage.contiguous_end.replace(tzinfo=None)},
        )
        if not result:
            raise RuntimeError(f'{spec.key}: no new verified minute has completed since readiness.')
        minute, completed = result[0]
        if not isinstance(minute, datetime) or not isinstance(completed, datetime):
            raise TypeError('Worker completion timestamps are invalid.')
        path = root / spec.key / 'mount' / 'latest.json'
        manifest = _object(json.loads(path.read_text()), 'manifest')
        committed_end = manifest_delivered_through(manifest)
        publication = reader.execute(
            f'SELECT recorded_at FROM {database}.worker_minute_log '
            "WHERE feed='provisional' AND series=%(series)s AND status='OK' "
            'AND recorded_at > %(ready)s AND sha256=%(token)s '
            'ORDER BY recorded_at DESC LIMIT 1',
            {'series': f'{spec.key}:mount', 'ready': ready_at.replace(tzinfo=None),
             'token': manifest.get('pinned_token', '')},
        )
        if not publication or not isinstance(publication[0][0], datetime):
            raise RuntimeError(f'{spec.key}: no matching new publication receipt.')
        from datetime import timedelta
        sources[spec.key] = {
            'components_complete': not coverage.incomplete_partitions,
            'new_interval_end': (minute.replace(tzinfo=UTC) + timedelta(minutes=1)).isoformat(),
            'build_completed_at': completed.replace(tzinfo=UTC).isoformat(),
            'publication_completed_at': publication[0][0].replace(tzinfo=UTC).isoformat(),
            'published_end': committed_end.isoformat(),
            'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'files_verified': _files(manifest, {series.name for series in inventory.sources[spec.key].series}, deadline),
        }
    return {'code_sha': code_sha, 'observed_at': datetime.now(UTC).isoformat(),
            'maintenance': run, 'daemons_healthy': health.reachable and not health.unhealthy_daemons,
            'sources': sources}


def wait_for_deployment(
    probe: Callable[[float], dict[str, object]], *, expected_sha: str,
    maintenance_run_id: str, ready_at: datetime, sources: Sequence[str],
    timeout_seconds: float = DEFAULT_DEADLINE_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    if not re.fullmatch(r'[0-9a-f]{40}', expected_sha):
        raise ValueError('Deployment requires the exact candidate commit SHA.')
    UUID(maintenance_run_id)
    if ready_at.tzinfo is None or not 0 < timeout_seconds <= DEFAULT_DEADLINE_SECONDS:
        raise ValueError('Deployment requires UTC readiness and a bounded timeout.')
    began = clock()
    deadline = began + timeout_seconds
    attempts: list[dict[str, object]] = []
    while clock() < deadline:
        try:
            observation = probe(deadline)
            reasons = verify_observation(observation, expected_sha=expected_sha,
                maintenance_run_id=maintenance_run_id, ready_at=ready_at, sources=sources)
            attempts.append({'elapsed_seconds': clock() - began,
                             'observation': observation, 'reasons': list(reasons)})
            if not reasons and clock() <= deadline:
                return {'verdict': 'PASS', 'elapsed_seconds': clock() - began, 'attempts': attempts}
        except (OSError, RuntimeError, ValueError, TypeError, KeyError, IndexError) as error:
            attempts.append({'elapsed_seconds': clock() - began,
                             'reason': f'{type(error).__name__}: {error}'[:1000]})
        sleep(max(0.0, min(15.0, deadline - clock())))
    return {'verdict': 'FAIL', 'elapsed_seconds': clock() - began, 'attempts': attempts,
            'reason': 'The deployment deadline elapsed without verified new delivery and maintenance success.'}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--maintenance-run-id', required=True)
    parser.add_argument('--ready-at', required=True, type=float)
    parser.add_argument('--expected-sha', required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    ready = datetime.fromtimestamp(args.ready_at, UTC)
    available = DEFAULT_DEADLINE_SECONDS - (datetime.now(UTC) - ready).total_seconds()
    if available <= 0:
        raise SystemExit('Deployment is already outside its 15-minute verification deadline.')
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    root = Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow'))
    reader = DagsterReader(os.environ.get('DAGSTER_WEBSERVER_URL', 'http://dagit:3000'), timeout_seconds=5)
    try:
        result = wait_for_deployment(
            lambda deadline: observe(client, reader, database=settings.database, root=root,
                code_sha=os.environ.get('ORIGO_CODE_SHA', ''), maintenance_run_id=args.maintenance_run_id,
                ready_at=ready, deadline=deadline), expected_sha=args.expected_sha,
            maintenance_run_id=args.maintenance_run_id, ready_at=ready,
            sources=tuple(load_inventory().sources), timeout_seconds=available,
        )
    finally:
        client.disconnect()
    result.update({'schema_version': 1, 'kind': 'steady_state_deployment',
                   'environment': 'production', 'code_sha': args.expected_sha,
                   'policy_sha256': load_policy().sha256, 'inventory_sha256': load_inventory().sha256,
                   'ready_at': ready.isoformat(), 'maintenance_run_id': args.maintenance_run_id})
    target = args.output or root / 'deployment-evidence' / f'{args.maintenance_run_id}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    from .publication import write_atomic
    write_atomic(target, canonical_json(result))
    print(json.dumps({'verdict': result['verdict'], 'evidence': str(target),
                      'elapsed_seconds': result['elapsed_seconds']}, sort_keys=True))
    return 0 if result['verdict'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
