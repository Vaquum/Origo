from __future__ import annotations

from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_EXAMPLE = _REPO_ROOT / '.env.example'
_DEPLOY_WORKFLOW = _REPO_ROOT / '.github' / 'workflows' / 'deploy-on-merge.yml'


def _load_env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in _ENV_EXAMPLE.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key] = value
    return values


def test_env_example_uses_compose_object_store_contract() -> None:
    env_values = _load_env_example()

    assert env_values['ORIGO_OBJECT_STORE_ENDPOINT_URL'] == 'http://object-store:9000'
    assert env_values['ORIGO_OBJECT_STORE_ACCESS_KEY_ID'] == 'origo'
    assert env_values['ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY'] == 'origo-secret'
    assert env_values['ORIGO_OBJECT_STORE_BUCKET'] == 'origo-raw-artifacts'
    assert env_values['ORIGO_OBJECT_STORE_REGION'] == 'us-east-1'
    assert env_values['ORIGO_DOCKER_OBJECT_STORE_PORT'] == '19000'
    assert env_values['ORIGO_DOCKER_OBJECT_STORE_CONSOLE_PORT'] == '19001'


def test_compose_stacks_define_object_store_and_bucket_bootstrap() -> None:
    compose_paths = (
        _REPO_ROOT / 'docker-compose.yml',
        _REPO_ROOT / 'deploy' / 'docker-compose.server.yml',
        _REPO_ROOT / 'control-plane' / 'docker-compose.yml',
    )

    for compose_path in compose_paths:
        config = yaml.safe_load(compose_path.read_text(encoding='utf-8'))
        services = config['services']

        assert 'object-store' in services, compose_path
        assert 'object-store-init' in services, compose_path

        object_store = services['object-store']
        assert object_store['image'] == 'quay.io/minio/minio', compose_path

        object_store_init = services['object-store-init']
        assert object_store_init['image'] == 'quay.io/minio/mc', compose_path
        init_entrypoint = '\n'.join(object_store_init['entrypoint'])
        assert 'mc alias set local http://object-store:9000' in init_entrypoint, compose_path
        assert 'mc mb --ignore-existing "local/${ORIGO_OBJECT_STORE_BUCKET}"' in init_entrypoint, compose_path

        dagster_service_names = ('dagster-webserver', 'dagster-daemon')
        if compose_path.name == 'docker-compose.yml' and compose_path.parent.name == 'control-plane':
            dagster_service_names = ('dagit', 'dagster')

        for service_name in dagster_service_names:
            depends_on = services[service_name]['depends_on']
            assert depends_on['object-store-init']['condition'] == 'service_completed_successfully', (
                compose_path,
                service_name,
            )


def test_deploy_workflow_syncs_object_store_env_and_bootstraps_runtime() -> None:
    workflow_text = _DEPLOY_WORKFLOW.read_text(encoding='utf-8')

    for key in (
        'ORIGO_OBJECT_STORE_ENDPOINT_URL',
        'ORIGO_OBJECT_STORE_ACCESS_KEY_ID',
        'ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY',
        'ORIGO_OBJECT_STORE_BUCKET',
        'ORIGO_OBJECT_STORE_REGION',
    ):
        assert key in workflow_text

    assert 'compose_cmd up -d clickhouse bitcoin-core object-store' in workflow_text
    assert 'compose_cmd run --rm -T object-store-init' in workflow_text
    assert 'compose_cmd up -d --remove-orphans clickhouse bitcoin-core object-store' in workflow_text
