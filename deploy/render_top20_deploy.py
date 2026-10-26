"""Preserve the disabled book rollback floor and the existing workers during activation."""

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

PROTECTED = {
    'clickhouse', 'vector', 'market-state', 'depth-worker', 'provisional-worker',
    'book-capture-spot', 'book-capture-perp', 'provisional-binance-spot-book',
    'provisional-binance-perp-book', 'provisional-binance-perp-trades',
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
ENVIRONMENT_ALIASES = {'ORIGO_ALERT_RESEND_API_KEY': 'RESEND_API_KEY'}


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
        if service in RETIRED and item['running'] is not True:
            continue
        if service in PROTECTED | set(CONTROLLERS) | RENDER_WORKERS:
            if service in result or item['running'] is not True:
                raise RuntimeError(f'Expected one running container: {service}')
            result[service] = item
    if not (PROTECTED - RETIRED) | set(CONTROLLERS) <= result.keys():
        raise RuntimeError('Protected deployment service membership is incomplete.')
    return result


def source_state(controller: str) -> dict[str, object]:
    return json.loads(run('docker', 'exec', '-i', controller, 'python', '-', source='''
import json
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.binance_top20 import BINANCE_SPOT_TOP20_SPEC, BINANCE_PERP_TOP20_SPEC
from origo.sources.registry import SOURCE_REGISTRY
settings = get_clickhouse_settings()
client = make_clickhouse_client(settings)
try:
    rows = client.execute('SELECT source_key,max(partition_end) FROM '+settings.database+
                          '.source_active_partitions GROUP BY source_key',
                          settings={'readonly':1,'max_threads':1,'max_execution_time':10})
    print(json.dumps({'frontiers': {key: str(end) for key,end in rows},
                      'stages': [s.rollout_stage.value for s in (BINANCE_SPOT_TOP20_SPEC,BINANCE_PERP_TOP20_SPEC)],
                      'legacy_stages': [s.rollout_stage.value for s in SOURCE_REGISTRY
                                        if s.key in ('binance_spot_book','binance_perp_book')]}))
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


def floor_environment(floor: Path) -> dict[str, str]:
    pinned = json.loads((floor / 'floor.json').read_text())
    for name in ('docker-compose.deploy.yml', 'environment.json'):
        if hashlib.sha256((floor / name).read_bytes()).hexdigest() != pinned[name]:
            raise RuntimeError('Pinned book rollback input changed: '+name)
    run('docker', 'image', 'inspect', '--format', '{{.Id}}', pinned['image_id'])
    environment = {**os.environ, **json.loads((floor / 'environment.json').read_text())}
    run('docker', 'compose', '--project-directory', str(floor.parent), '-f', str(floor / 'docker-compose.deploy.yml'), 'config', '--quiet',
        environment=environment)
    return environment


def capture_floor(root: Path, project: str, members: dict[str, dict[str, object]], state: dict[str, object]) -> None:
    floor = root / '.render-top20-floor'
    floor.mkdir(mode=0o700, exist_ok=True)
    manifest = floor / 'floor.json'
    if manifest.exists():
        floor_environment(floor)
        return
    if state['stages'] != ['DORMANT', 'DORMANT']:
        raise RuntimeError('The initial book rollback floor must declare both sources DORMANT.')
    content = (root / 'docker-compose.deploy.yml').read_bytes()
    keys = sorted(set(re.findall(r'\$\{([A-Z_][A-Z0-9_]*)', content.decode())) | ENVIRONMENT_ALIASES.keys())
    environment: dict[str, str] = {}
    for service in (*CONTROLLERS, 'depth-worker', 'book-capture-spot', 'perp-capture'):
        selected = json.loads(run('docker', 'exec', '-i', str(members[service]['id']), 'python', '-',
                                 source='import json,os\nkeys='+repr(keys)+
                                 '\nprint(json.dumps({key:os.environ[key] for key in keys if key in os.environ}))'))
        for key, value in selected.items():
            key = ENVIRONMENT_ALIASES.get(key, key)
            if service in CONTROLLERS and key in environment and environment[key] != value:
                raise RuntimeError(f'Inconsistent rollback environment: {key}')
            environment.setdefault(key, value)
    environment.update(APP_IMAGE=str(members['dagster']['image_id']),
                       CLICKHOUSE_IMAGE=str(members['clickhouse']['image_id']),
                       ORIGO_PERP_WORKER_HOSTNAME=str(members['perp-repair']['hostname']),
                       COMPOSE_PROJECT_NAME=project)
    with (floor / 'docker-compose.deploy.yml').open('wb') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    save(floor / 'environment.json', environment)
    run('docker', 'compose', '--project-directory', str(root), '-f', str(floor / 'docker-compose.deploy.yml'), 'config', '--quiet',
        environment={**os.environ, **environment})
    run('docker', 'image', 'tag', environment['APP_IMAGE'], 'origo-render-top20-rollback:floor')
    save(manifest, {'image_id': environment['APP_IMAGE'], **{
        name: hashlib.sha256((floor / name).read_bytes()).hexdigest()
        for name in ('docker-compose.deploy.yml', 'environment.json')
    }})


def main(mode: str, root: Path, project: str) -> None:
    baseline = root / '.render-top20-before.json'
    if mode == 'rollback':
        if (root / '.book-reader-cutover.json').exists():
            raise RuntimeError('Book readers have switched; the DORMANT warm-up rollback is retired.')
        floor = root / '.render-top20-floor'
        environment = {**floor_environment(floor), 'PROJECT_NAME': project}
        run('bash', str(root / 'deploy/retire_render_top20_workers.sh'), environment=environment)
        run('docker', 'compose', '--project-directory', str(root), '-p', project, '-f', str(floor / 'docker-compose.deploy.yml'),
            'up', '-d', '--no-deps', '--wait', '--wait-timeout', '600', '--force-recreate',
            *CONTROLLERS, environment=environment)
        run('docker', 'compose', '--project-directory', str(root), '-p', project, '-f', str(floor / 'docker-compose.deploy.yml'),
            'exec', '-T', 'dagster', 'python', '-m', 'origo.sources.compatibility', environment=environment)
    members = containers(project)
    state = source_state(str(members['dagit']['id']))
    if mode == 'capture':
        if state.get('legacy_stages') != ['DORMANT', 'DORMANT'] and not RETIRED <= members.keys():
            raise RuntimeError('Existing book writer membership is incomplete before its declared pause.')
        if state['stages'] == ['LIVE', 'LIVE'] and not RENDER_WORKERS <= members.keys():
            raise RuntimeError('Qualified Render book workers must remain running.')
        capture_floor(root, project, members, state)
        frontiers = {key: end for key, end in state['frontiers'].items()
                     if state.get('legacy_stages') != ['DORMANT', 'DORMANT']
                     or key not in ('binance_spot_book', 'binance_perp_book')}
        save(baseline, {'containers': {key: members[key] for key in sorted(
            (PROTECTED | RENDER_WORKERS) & members.keys())}, 'frontiers': frontiers})
    else:
        previous = json.loads(baseline.read_text())
        expected = {key: value for key, value in previous['containers'].items()
                    if mode != 'rollback' or key not in RENDER_WORKERS}
        if expected != {key: members.get(key) for key in expected}:
            raise RuntimeError('A protected worker identity, restart count or start time changed.')
        if any(str(state['frontiers'].get(key, '')) < end for key, end in previous['frontiers'].items()):
            raise RuntimeError('An existing accepted source frontier regressed.')
        if mode == 'verify' and any(str(state['frontiers'].get(key, '')) <= end
                                    for key, end in previous['frontiers'].items()):
            raise RuntimeError('An existing accepted source frontier did not advance.')
        if mode == 'rollback' and state['stages'] != ['DORMANT', 'DORMANT']:
            raise RuntimeError('Rollback did not restore both DORMANT declarations.')
        save(root / ('.render-top20-' + mode + '.json'), state)
    print('Render book deployment preservation verified: '+mode)


if __name__ == '__main__':
    mode, directory, project = sys.argv[1:]
    if mode not in ('capture', 'check', 'verify', 'rollback'):
        raise ValueError('Expected capture, check, verify or rollback.')
    main(mode, Path(directory), project)
