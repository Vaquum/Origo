"""Deployment dependencies of durable capture and physical output monitoring."""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('filename', ['docker-compose.yml', 'docker-compose.deploy.yml'])
def test_capture_and_consumers_share_the_durable_spool(filename: str) -> None:
    configuration = yaml.safe_load((ROOT / filename).read_text())
    services = configuration['services']
    assert 'trade-spool' in configuration['volumes']
    for name in ('trade-capture', 'provisional-worker', 'dagster', 'dagit'):
        service = services[name]
        assert 'trade-spool:/opt/origo/spool' in service['volumes']
        assert 'source-locks:/opt/origo/locks' in service['volumes']
        assert 'ORIGO_TRADE_SPOOL_DIR=/opt/origo/spool' in service['environment']
    capture = services['trade-capture']
    assert capture['command'] == 'python -m origo.workers.trade_capture'
    assert capture['healthcheck']['test'][-2:] == ['origo.workers.trade_capture', '--check']
    assert not capture.get('depends_on'), 'Capture must survive ClickHouse/Dagster downtime.'
    assert not any(entry.startswith('BINANCE_API_KEY=') for entry in capture['environment'])
    for name in ('dagster', 'dagit'):
        assert any(entry.startswith('BINANCE_API_KEY=') for entry in services[name]['environment'])


@pytest.mark.parametrize('filename', ['docker-compose.yml', 'docker-compose.deploy.yml'])
def test_monitor_can_read_the_outputs_it_certifies(filename: str) -> None:
    services = yaml.safe_load((ROOT / filename).read_text())['services']
    mounts = services['monitor']['volumes']
    assert any(entry.endswith(':/opt/parquet:ro') for entry in mounts)
    assert any(entry.endswith(':/opt/arrow:ro') for entry in mounts)
    assert 'source-publications:/opt/origo/shadow:ro' in mounts
    assert 'trade-spool:/opt/origo/spool:ro' in mounts
    assert 'source-locks:/opt/origo/locks' in mounts
    for name in ('dagster', 'dagit'):
        assert 'DAGSTER_WEBSERVER_URL=http://dagit:3000' in services[name]['environment']
        assert 'worker-heartbeats:/opt/origo/heartbeats:ro' in services[name]['volumes']


def test_merge_deployment_includes_the_independent_capture_service() -> None:
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    up = next(line for line in workflow.splitlines() if ' up -d --wait ' in line)
    assert ' trade-capture ' in up
