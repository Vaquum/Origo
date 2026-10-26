"""Preserve qualified source workers and historical book frontiers during deployment."""

import json
import os
import subprocess
import sys
from pathlib import Path

PROTECTED = {
    'clickhouse', 'vector', 'market-state', 'provisional-worker',
    'provisional-binance-perp-trades',
    'provisional-binance-spot-aggtrades', 'provisional-binance-perp-aggtrades',
    'publication-binance-spot-trades', 'publication-binance-perp-trades',
    'perp-capture', 'perp-repair',
}
CONTROLLERS = ('dagster', 'dagit', 'monitor', 'law')
RETIRED = {
    'depth-worker', 'book-capture-spot', 'book-capture-perp',
    'provisional-binance-spot-book', 'provisional-binance-perp-book',
}
RENDER_WORKERS = {'provisional-binance-spot-top20', 'provisional-binance-perp-top20'}


def run(*arguments: str, source: str | None = None, environment: dict[str, str] | None = None) -> str:
    return subprocess.check_output(arguments, input=source, text=True, env=environment)


def containers(project: str) -> dict[str, dict[str, object]]:
    ids = run('docker', 'ps', '-aq', '--filter', f'label=com.docker.compose.project={project}').split()
    if not ids:
        raise RuntimeError('Book activation requires the existing deployment.')
    template = ('{"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
                '"id":{{json .Id}},"restarts":{{.RestartCount}},'
                '"started":{{json .State.StartedAt}},"running":{{json .State.Running}},'
                '"image":{{json .Config.Image}},"image_id":{{json .Image}},'
                '"hostname":{{json .Config.Hostname}}}')
    result: dict[str, dict[str, object]] = {}
    for line in run('docker', 'inspect', '--format', template, *ids).splitlines():
        item = json.loads(line)
        service = item['service']
        if service in RETIRED:
            raise RuntimeError('A retired book worker reappeared: '+service)
        if service in PROTECTED | set(CONTROLLERS) | RENDER_WORKERS:
            if service in result or item['running'] is not True:
                raise RuntimeError(f'Expected one running container: {service}')
            result[service] = item
    if not PROTECTED | set(CONTROLLERS) | RENDER_WORKERS <= result.keys():
        raise RuntimeError('Protected deployment service membership is incomplete.')
    return result


def source_state(controller: str) -> dict[str, object]:
    return json.loads(run('docker', 'exec', '-i', controller, 'python', '-', source='''
import json
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.binance_top20 import BINANCE_SPOT_TOP20_SPEC, BINANCE_PERP_TOP20_SPEC
settings = get_clickhouse_settings()
client = make_clickhouse_client(settings)
try:
    rows = client.execute('SELECT source_key,max(partition_end) FROM '+settings.database+
                          '.source_active_partitions GROUP BY source_key',
                          settings={'readonly':1,'max_threads':1,'max_execution_time':10})
    print(json.dumps({'frontiers': {key: str(end) for key,end in rows},
                      'stages': [s.rollout_stage.value for s in (BINANCE_SPOT_TOP20_SPEC,BINANCE_PERP_TOP20_SPEC)]}))
finally:
    client.disconnect()
'''))


def save(path: Path, value: object) -> None:
    temporary = path.with_suffix('.tmp')
    with open(temporary, 'w', opener=lambda name, flags: os.open(name, flags, 0o600)) as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def main(mode: str, root: Path, project: str) -> None:
    baseline = root / '.render-top20-before.json'
    members = containers(project)
    state = source_state(str(members['dagit']['id']))
    if mode == 'capture':
        if state['stages'] != ['LIVE', 'LIVE'] or not RENDER_WORKERS <= members.keys():
            raise RuntimeError('Qualified Render book workers must remain LIVE and running.')
        frozen = {key: end for key, end in state['frontiers'].items()
                  if key in ('binance_spot_book', 'binance_perp_book')}
        frontiers = {key: end for key, end in state['frontiers'].items() if key not in frozen}
        save(baseline, {'containers': {key: members[key] for key in sorted(
            (PROTECTED | RENDER_WORKERS) & members.keys())}, 'frontiers': frontiers,
            'frozen_frontiers': frozen})
    else:
        previous = json.loads(baseline.read_text())
        expected = previous['containers']
        if expected != {key: members.get(key) for key in expected}:
            raise RuntimeError('A protected worker identity, restart count or start time changed.')
        if any(str(state['frontiers'].get(key, '')) < end for key, end in {
            **previous['frontiers'], **previous['frozen_frontiers'],
        }.items()):
            raise RuntimeError('An existing accepted source frontier regressed.')
        if mode == 'verify' and any(str(state['frontiers'].get(key, '')) <= end
                                    for key, end in previous['frontiers'].items()):
            raise RuntimeError('An existing accepted source frontier did not advance.')
        if any(str(state['frontiers'].get(key, '')) != end
               for key, end in previous['frozen_frontiers'].items()):
            raise RuntimeError('A retired book frontier changed after shutdown.')
        save(root / ('.render-top20-' + mode + '.json'), state)
    print('Render book deployment preservation verified: '+mode)


if __name__ == '__main__':
    mode, directory, project = sys.argv[1:]
    if mode not in ('capture', 'check', 'verify'):
        raise ValueError('Expected capture, check or verify.')
    main(mode, Path(directory), project)
