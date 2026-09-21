from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _retirement_command() -> str:
    source = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    lines = source.splitlines()
    matches = [
        line.strip() for line in lines if 'stop -t 30 depth-worker provisional-worker' in line
    ]
    assert len(matches) == 1
    command = matches[0]
    assert '</dev/null' in command
    assert source.index(command) < source.index('up -d --wait --wait-timeout 600')
    return command


@pytest.mark.parametrize('stop_status', [0, 42])
def test_old_receipt_writers_stop_before_the_new_image_starts(
    tmp_path: Path,
    stop_status: int,
) -> None:
    # The schema is old-writer compatible; the stop exists so only modern writers,
    # which serialize same-triple receipts under the shard lock, ever run together.
    docker = tmp_path / 'docker'
    docker.write_text(
        '#!/bin/sh\ncat >/dev/null\nprintf "%s\\n" "$*" >> "$AUDIT"\nexit "$STOP_STATUS"\n'
    )
    docker.chmod(0o700)
    audit = tmp_path / 'calls.txt'
    script = (
        'set -euo pipefail\nPROJECT_NAME=isolated-test\n'
        + _retirement_command()
        + '\necho schema_upgrade_admitted\n'
    )
    result = subprocess.run(
        ['bash', '-s'],
        input=script,
        text=True,
        capture_output=True,
        timeout=5,
        env={
            **os.environ,
            'PATH': f'{tmp_path}:{os.environ["PATH"]}',
            'AUDIT': str(audit),
            'STOP_STATUS': str(stop_status),
        },
        check=False,
    )
    assert result.returncode == stop_status
    assert ('schema_upgrade_admitted' in result.stdout) == (stop_status == 0)
    assert audit.read_text().splitlines() == [
        'compose -p isolated-test -f docker-compose.deploy.yml stop -t 30 depth-worker provisional-worker'
    ]


def test_worker_owner_markers_use_the_existing_shared_persistent_mount() -> None:
    import yaml

    for name in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        document = yaml.safe_load((ROOT / name).read_text())
        for service in ('depth-worker', 'provisional-worker'):
            mounts = document['services'][service]['volumes']
            assert 'source-locks:/opt/origo/locks' in mounts
            environment = document['services'][service]['environment']
            overrides = [
                entry for entry in environment if entry.startswith('ORIGO_SOURCE_LOCK_DIR=')
            ]
            assert overrides in ([], ['ORIGO_SOURCE_LOCK_DIR=/opt/origo/locks'])
