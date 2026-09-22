"""Owned, local-only resources for destructive fault trials. Never production."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import cast
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
LABEL = 'origo.steady-state.trial'
FORBIDDEN_ROOTS = (Path('/opt/origo'), Path('/opt/parquet'), Path('/opt/arrow'), Path('/var/lib'))


def validate_isolated_environment(environment: Mapping[str, str], output: Path) -> None:
    """Refuse inherited connections or production locations before creating anything."""
    for name, value in environment.items():
        if value and (
            name.startswith('CLICKHOUSE_')
            or name
            in {
                'DAGSTER_HOME',
                'DAGSTER_WEBSERVER_URL',
                'ORIGO_SOURCE_PUBLICATION_ROOT',
                'ORIGO_SOURCE_LOCK_DIR',
                'LOCAL_PARQUET_DIR',
                'LOCAL_ARROW_DIR',
                'ORIGO_TRADE_SPOOL_DIR',
                'BINANCE_API_KEY',
                'HF_TOKEN',
                'RESEND_API_KEY',
            }
        ):
            raise PermissionError(f'Trial refuses inherited {name}; use a clean local environment.')
    for name in ('DOCKER_HOST', 'DOCKER_CONTEXT'):
        if environment.get(name):
            raise PermissionError(f'Trial refuses {name}; the selected local context is inspected.')
    if not output.is_absolute() or output != output.resolve():
        raise PermissionError('Trial output must be an absolute, non-symlink path.')
    if output == Path('/') or any(
        output == root or output.is_relative_to(root) for root in FORBIDDEN_ROOTS
    ):
        raise PermissionError('Trial output overlaps a production or system storage location.')
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError(
            'Trial output must be new or empty; existing evidence is never overwritten.'
        )


def clean_environment() -> dict[str, str]:
    return {
        name: os.environ[name]
        for name in (
            'PATH',
            'HOME',
            'LANG',
            'LC_ALL',
            'TMPDIR',
            'SYSTEMROOT',
            'SSL_CERT_FILE',
        )
        if name in os.environ
    }


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return int(listener.getsockname()[1])


class OwnedClickHouse:
    """A label-checked container with local ports and no production volume mounts."""

    def __init__(self, output: Path, *, memory_gib: int = 3) -> None:
        validate_isolated_environment(os.environ, output)
        if isinstance(memory_gib, bool) or not 1 <= memory_gib <= 4:
            raise ValueError('The local trial admits one to four GiB of ClickHouse memory.')
        self.output = output
        self.identity = uuid4().hex
        self.name = 'origo-steady-state-trial-' + self.identity[:12]
        self.context = ''
        self.container_id = ''
        self.memory_gib = memory_gib
        self.environment: dict[str, str] = {}
        self.image = ''

    def _command(self, *arguments: str, timeout: float = 90) -> str:
        prefix = ['docker', '--context', self.context] if self.context else ['docker']
        result = subprocess.run(
            [*prefix, *arguments],
            cwd=ROOT,
            env=clean_environment(),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode:
            raise RuntimeError(f'Owned trial Docker command failed: {result.stderr[-2000:]}')
        return result.stdout.strip()

    def require_owned(self) -> None:
        if not self.container_id:
            raise PermissionError('No owned trial container was created.')
        found: object = json.loads(self._command('inspect', self.container_id))
        if not isinstance(found, list) or len(found) != 1:
            raise PermissionError('The owned container identity could not be verified.')
        item = cast(list[dict[str, object]], found)[0]
        config = cast(dict[str, object], item['Config'])
        labels = cast(dict[str, str], config['Labels'])
        if item.get('Id') != self.container_id or labels.get(LABEL) != self.identity:
            raise PermissionError(
                'Refusing to modify a container without this trial ownership label.'
            )

    def __enter__(self) -> OwnedClickHouse:
        self.context = self._command('context', 'show')
        endpoint = self._command(
            'context', 'inspect', self.context, '--format', '{{.Endpoints.docker.Host}}'
        )
        if not endpoint.startswith('unix:///'):
            raise PermissionError('Fault trials require a local Unix-socket Docker context.')
        self.output.mkdir(parents=True, exist_ok=True)
        self.image = self._command(
            'build', '--quiet', '-f', 'Dockerfile.clickhouse', '.', timeout=300
        )
        native, http = _free_port(), _free_port()
        self.environment = {
            **clean_environment(),
            'PYTHONPATH': str(ROOT),
            'CLICKHOUSE_HOST': '127.0.0.1',
            'CLICKHOUSE_PORT': str(native),
            'CLICKHOUSE_HTTP_PORT': str(http),
            'CLICKHOUSE_USER': 'default',
            'CLICKHOUSE_PASSWORD': self.identity,
            'CLICKHOUSE_DATABASE': 'origo_trial_' + self.identity,
            'ORIGO_SOURCE_LOCK_DIR': str(self.output / 'runtime' / 'locks'),
            'ORIGO_SOURCE_PUBLICATION_ROOT': str(self.output / 'runtime' / 'publications'),
            'LOCAL_PARQUET_DIR': str(self.output / 'runtime' / 'parquet'),
            'LOCAL_ARROW_DIR': str(self.output / 'runtime' / 'arrow'),
            'ORIGO_TRADE_SPOOL_DIR': str(self.output / 'runtime' / 'spool'),
            'ORIGO_HEARTBEAT_DIR': str(self.output / 'runtime' / 'heartbeats'),
            'DAGSTER_HOME': str(self.output / 'runtime' / 'dagster'),
        }
        self.container_id = self._command(
            'run',
            '-d',
            '--name',
            self.name,
            '--label',
            f'{LABEL}={self.identity}',
            '--memory',
            f'{self.memory_gib}g',
            '--cpus',
            '4',
            '-p',
            f'127.0.0.1:{native}:9000',
            '-p',
            f'127.0.0.1:{http}:8123',
            '-e',
            'CLICKHOUSE_PASSWORD=' + self.identity,
            self.image,
        )
        try:
            self.require_owned()
            # The probe runs with only this newly created local endpoint configuration.
            script = (
                'from origo.assets.create_origo_database import get_clickhouse_settings,make_clickhouse_client; '
                'c=make_clickhouse_client(get_clickhouse_settings()); '
                'assert c.execute("SELECT 1")==[(1,)]; c.disconnect()'
            )
            deadline = time.monotonic() + 60
            while True:
                result = subprocess.run(
                    [sys.executable, '-c', script],
                    cwd=ROOT,
                    env=self.environment,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('The owned local ClickHouse did not become ready.')
                time.sleep(0.5)
        except BaseException:
            self.close()
            raise
        return self

    def stop(self) -> None:
        self.require_owned()
        self._command('stop', '--time', '5', self.container_id)

    def start(self) -> None:
        self.require_owned()
        self._command('start', self.container_id)

    def close(self) -> None:
        if self.container_id:
            self.require_owned()
            self._command('rm', '--force', self.container_id)
            self.container_id = ''

    def __exit__(self, *exception: object) -> None:
        self.close()
