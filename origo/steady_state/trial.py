"""Owned mixed-workload trial orchestration and independent archive oracle."""

from __future__ import annotations

import csv
import importlib.metadata
import io
import json
import math
import os
import signal
import subprocess
import sys
import traceback
import zipfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from origo.sources.contracts import Row
from origo.sources.hashing import content_hash
from origo.steady_state.trial_corpus import SOURCES, ArchiveReference, archive_url, prepare_tape
from origo.steady_state.trial_evidence import instant, list_value, object_value
from origo.steady_state.trial_resources import ROOT, OwnedClickHouse, validate_isolated_environment
from origo.steady_state.trial_transport import digest, write_json

CACHE = Path('/tmp/origo-s439-corpus').resolve()
MINUTE = timedelta(minutes=1)


def validate_request(manifest: Path, output: Path, duration: float) -> list[dict[str, object]]:
    validate_isolated_environment(os.environ, output)
    for key, value in os.environ.items():
        if value and (
            key.startswith(('ORIGO_', 'BINANCE_', 'HF_', 'HUGGINGFACE_', 'AWS_'))
            or key.upper().endswith('_PROXY')
        ):
            raise PermissionError(f'Trial refuses inherited setting {key}.')
    if output.resolve().is_relative_to(CACHE):
        raise PermissionError('Trial output cannot alter the immutable input cache.')
    if not math.isfinite(duration) or not 0 < duration <= 21600:
        raise ValueError('Duration must be positive and at most 21600 real seconds.')
    document = object_value(json.loads(manifest.read_text()), 'fixture manifest')
    if document.get('schema_version') != 1:
        raise ValueError('Version-1 official archive manifest required.')
    archives = [
        object_value(item, 'archive') for item in list_value(document.get('archives'), 'archives')
    ]
    if len(archives) != len(SOURCES) or {item.get('source_key') for item in archives} != set(
        SOURCES
    ):
        raise ValueError('Exactly one complete authentic day per required source is required.')
    pinned = object_value(
        json.loads((ROOT / 'tests/fixtures/steady_state/archive_corpus.json').read_text()),
        'pinned corpus',
    )
    identities = {
        str(row['source_key']): row
        for value in list_value(pinned['archives'], 'pinned archives')
        for row in [object_value(value, 'pinned archive')]
    }
    for item in archives:
        source = str(item['source_key'])
        if any(
            item.get(key) != identities[source].get(key)
            for key in ('date', 'url', 'sha256', 'captured_at', 'busy_hour')
        ):
            raise ValueError('Input provenance differs from the pinned authentic corpus.')
        day = date.fromisoformat(str(item['date']))
        if item.get('url') != archive_url(source, day):
            raise ValueError('Archive URL differs from the official source/day identity.')
        path = Path(str(item['cache_path']))
        if not path.resolve().is_relative_to(CACHE) or not path.is_file():
            raise PermissionError(
                'Only existing authentic files in the declared local cache are allowed.'
            )
        if digest(path) != item.get('sha256'):
            raise ValueError(f'Cached official archive digest mismatch: {source}.')
        origin = source_origin(item)
        day_start = datetime.combine(day, datetime.min.time(), UTC)
        if origin - timedelta(hours=1) < day_start or origin + timedelta(
            seconds=duration
        ) >= day_start + timedelta(days=1):
            raise ValueError(
                'Complete arrival window and boundary rows must fit the authentic day.'
            )
        instant(item.get('captured_at'), 'checksum capture time')
    volume_analysis(archives)
    return archives


def volume_analysis(archives: list[dict[str, object]]) -> dict[str, object]:
    path = ROOT / 'tests/fixtures/steady_state/volume_inventory.json'
    volume = object_value(json.loads(path.read_text()), 'volume inventory')
    start = instant(volume['window_start'], 'volume window start')
    end = instant(volume['window_end'], 'volume window end')
    if end - start != timedelta(days=30):
        raise ValueError('Thirty complete days of source-volume evidence are required.')
    expected = {start + timedelta(hours=index) for index in range(720)}
    sources = object_value(volume['sources'], 'volume sources')
    result: dict[str, object] = {}
    for item in archives:
        source = str(item['source_key'])
        recorded = object_value(sources[source], 'source volume')
        hours = [object_value(row, 'volume hour') for row in list_value(recorded['hours'], 'hours')]
        stamps = [datetime.fromisoformat(str(row['hour'])).replace(tzinfo=UTC) for row in hours]
        if (
            len(stamps) != 720
            or set(stamps) != expected
            or any(int(str(row['represented_minutes'])) != 60 for row in hours)
        ):
            raise ValueError(f'{source}: source-volume inventory has missing hour/minute evidence.')
        busiest = max(hours, key=lambda row: int(str(row['event_rows'])))
        busy = object_value(item['busy_hour'], 'archive busy hour')
        if (
            datetime.fromisoformat(str(busiest['hour'])).replace(tzinfo=UTC)
            != instant(busy['start'], 'busy start')
            or int(str(busiest['event_rows'])) != busy['rows']
        ):
            raise ValueError(f'{source}: input does not include the recorded busiest hour.')
        result[source] = {'represented_hours': len(hours), 'busiest_hour': busiest}
    return {'sha256': digest(path), 'method': volume['method'], 'sources': result}


def source_origin(item: dict[str, object]) -> datetime:
    # One withheld hour, then ordinary real-time arrivals including the busiest hour.
    busy = object_value(item.get('busy_hour'), 'busy_hour')
    return instant(busy.get('start'), 'busy_hour.start') - timedelta(hours=1)


def input_oracle(
    reference: ArchiveReference, start: datetime, end: datetime
) -> list[dict[str, object]]:
    """Parse original CSV independently of the IPC tape and provisional row mapper.

    Compare the native normalized raw component (decimal value, original ID, flags,
    and existing spot millisecond convention), never merely its activation hash.
    """
    aggregate = reference.source_key.endswith('aggtrades')
    spot = '_spot_' in reference.source_key
    groups: dict[datetime, list[Row]] = {}
    minute = start
    while minute < end:
        groups[minute] = []
        minute += MINUTE
    # Keep only one minute of rows in memory, even for a multi-million-row day.
    result: dict[datetime, dict[str, object]] = {}
    current: datetime | None = None
    rows: list[Row] = []

    def finish(bucket: datetime, values: list[Row]) -> None:
        stored = [
            tuple(float(value) if isinstance(value, Decimal) else value for value in row)
            for row in values
        ]
        result[bucket] = {
            'minute': bucket.isoformat(),
            'rows': len(values),
            'raw_sha256': content_hash(stored, schema_version=1),
            'native_revision_sha256': content_hash(values, schema_version=1),
        }

    with zipfile.ZipFile(reference.path) as archive:
        members = archive.namelist()
        if members != [reference.path.name.removesuffix('.zip') + '.csv']:
            raise ValueError('Unexpected official CSV member inventory.')
        with (
            archive.open(members[0]) as raw,
            io.TextIOWrapper(raw, encoding='utf-8', newline='') as stream,
        ):
            for index, fields in enumerate(csv.reader(stream)):
                if index == 0 and not fields[0].isdigit():
                    continue
                timestamp = int(fields[5 if aggregate else 4])
                if spot and timestamp >= 10**15:
                    timestamp //= 1000
                observed = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=timestamp)
                bucket = observed.replace(second=0, microsecond=0)
                if bucket not in groups:
                    continue
                if current is not None and bucket != current:
                    if bucket <= current:
                        raise ValueError('CSV oracle source time regressed.')
                    finish(current, rows)
                    rows = []
                current = bucket
                flags = fields[6:] if aggregate else fields[5:]
                if any(value.lower() not in ('true', 'false') for value in flags):
                    raise ValueError('Invalid authentic boolean field.')
                price, quantity = Decimal(fields[1]), Decimal(fields[2])
                if aggregate:
                    row: Row = (
                        int(fields[0]),
                        price,
                        quantity,
                        int(fields[3]),
                        int(fields[4]),
                        timestamp,
                        *(int(value.lower() == 'true') for value in flags),
                        observed,
                    )
                else:
                    quote = Decimal(fields[3]) if spot else (price * quantity).normalize()
                    row = (
                        int(fields[0]),
                        price,
                        quantity,
                        quote,
                        timestamp,
                        *(int(value.lower() == 'true') for value in flags),
                        observed,
                    )
                rows.append(row)
    if current is not None:
        finish(current, rows)
    for bucket in groups:
        if bucket not in result:
            finish(bucket, [])
    return [result[bucket] for bucket in sorted(result)]


def code_identity() -> dict[str, object]:
    paths = sorted(
        {
            *ROOT.joinpath('origo').rglob('*.py'),
            *ROOT.joinpath('origo').rglob('*.sql'),
            *ROOT.joinpath('origo/steady_state').glob('*.json'),
            ROOT / 'tools/benchmark_steady_state.py',
            ROOT / 'pyproject.toml',
            ROOT / 'uv.lock',
            ROOT / 'Dockerfile.clickhouse',
            ROOT / 'clickhouse-config.xml',
            ROOT / 'clickhouse-users.xml',
        }
    )
    files = {str(path.relative_to(ROOT)): digest(path) for path in paths if path.is_file()}
    import hashlib

    packages = {
        distribution.metadata['Name']: distribution.version
        for distribution in importlib.metadata.distributions()
    }
    commit = subprocess.run(
        ['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()
    return {
        'git_commit': commit,
        'packages': packages,
        'files': files,
        'sha256': hashlib.sha256(
            json.dumps(
                {'files': files, 'packages': packages, 'python': sys.version}, sort_keys=True
            ).encode()
        ).hexdigest(),
        'python': sys.version,
        'executable': sys.executable,
    }


def prepare_inputs(
    archives: list[dict[str, object]], output: Path, duration: float
) -> dict[str, object]:
    plan: dict[str, object] = {}
    for item in archives:
        source = str(item['source_key'])
        reference = ArchiveReference(
            source,
            date.fromisoformat(str(item['date'])),
            Path(str(item['cache_path'])),
            str(item['sha256']),
            str(item['url']),
            instant(item['captured_at'], 'captured_at'),
        )
        origin = source_origin(item)
        start = origin - timedelta(hours=1)
        end = (origin + timedelta(seconds=duration)).replace(second=0, microsecond=0) + MINUTE
        tape = prepare_tape(reference, output / 'inputs' / (source + '.arrow'))
        plan[source] = {
            'source_time_at_start': origin.isoformat(),
            'withheld': [(start + index * MINUTE).isoformat() for index in range(60)],
            'oracle': input_oracle(reference, start, end),
            'tape': tape,
            'busy_hour': item['busy_hour'],
        }
        write_json(output / 'input-checkpoint.json', plan)
    return plan


class LocalClickHouse(OwnedClickHouse):
    """Keep the existing ownership lifecycle, with an offline-only image build."""

    def _command(self, *arguments: str, timeout: float = 90) -> str:
        if arguments and arguments[0] == 'build':
            image = (ROOT / 'Dockerfile.clickhouse').read_text().splitlines()[0].split()[1]
            super()._command('image', 'inspect', image, timeout=timeout)
            arguments = ('build', '--network=none', '--pull=false', *arguments[1:])
        return super()._command(*arguments, timeout=timeout)


def run_trial(manifest: Path, output: Path, duration: float) -> int:
    archives = validate_request(manifest, output, duration)
    owned = LocalClickHouse(output)  # Validate before the first evidence write.
    output.mkdir(parents=True, exist_ok=True)
    initial = code_identity()
    write_json(
        output / 'identity.json',
        {
            'kind': 'isolated_archive_replay',
            'http_capture': False,
            'code': initial,
            'fixture_manifest_sha256': digest(manifest),
            'policy_sha256': digest(ROOT / 'origo/steady_state/policy.json'),
            'inventory_sha256': digest(ROOT / 'origo/steady_state/inventory.json'),
            'requested_duration_seconds': duration,
            'scenario': 'integrated',
            'clock': 'time.monotonic plus timezone-aware datetime.now(UTC)',
            'environment': 'isolated',
        },
    )
    try:
        plan = prepare_inputs(archives, output, duration)
        write_json(output / 'plan.json', plan)
        write_json(output / 'volume-analysis.json', volume_analysis(archives))
        write_json(output / 'fixture-manifest.json', json.loads(manifest.read_text()))
        volume = ROOT / 'tests/fixtures/steady_state/volume_inventory.json'
        write_json(output / 'volume_inventory.json', json.loads(volume.read_text()))
        with owned:
            for name, directory in (
                ('HOME', 'home'),
                ('TMPDIR', 'tmp'),
                ('XDG_CACHE_HOME', 'cache'),
            ):
                path = output / 'runtime' / directory
                path.mkdir(parents=True, exist_ok=True)
                owned.environment[name] = str(path)
            owned.environment['HF_HUB_OFFLINE'] = '1'
            owned.environment['HF_HUB_DISABLE_TELEMETRY'] = '1'
            write_json(
                output / 'resources.json',
                {
                    'image': owned.image,
                    'container_id': owned.container_id,
                    'ownership_label': owned.identity,
                    'context': owned.context,
                    'memory_gib': owned.memory_gib,
                    'cpus': 4,
                    'configuration': {
                        key: value
                        for key, value in owned.environment.items()
                        if key != 'CLICKHOUSE_PASSWORD'
                    },
                },
            )
            with (output / 'worker.log').open('w') as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        '-m',
                        'origo.steady_state.trial_worker',
                        '--output',
                        str(output),
                        '--duration-seconds',
                        str(duration),
                    ],
                    cwd=ROOT,
                    env=owned.environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    status = process.wait()
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait(timeout=5)
            if code_identity()['sha256'] != initial['sha256']:
                raise RuntimeError('Relevant code changed during trial; evidence cannot qualify.')
            return status
    except BaseException as error:
        write_json(
            output / 'failure.json',
            {
                'observed_at': datetime.now(UTC).isoformat(),
                'error': repr(error),
                'traceback': traceback.format_exc(),
                'acceptance': 'UNKNOWN',
            },
        )
        raise
    finally:
        # Seal retained artifacts on both success and failure; never erase older checkpoints.
        entries = {
            str(path.relative_to(output)): digest(path)
            for path in sorted(output.rglob('*'))
            if path.is_file()
            and not path.is_symlink()
            and 'runtime' not in path.relative_to(output).parts
            and path.name != 'evidence-manifest.json'
        }
        write_json(output / 'evidence-manifest.json', {'schema_version': 1, 'files': entries})
