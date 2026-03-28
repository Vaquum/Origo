from __future__ import annotations

import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTROL_PLANE_PYPROJECT = _REPO_ROOT / 'control-plane' / 'pyproject.toml'
_CONTROL_PLANE_DOCKERFILE = _REPO_ROOT / 'docker' / 'Dockerfile.control-plane'
_DEPLOY_WORKFLOW = _REPO_ROOT / '.github' / 'workflows' / 'deploy-on-merge.yml'


def test_control_plane_declares_playwright_runtime_dependency() -> None:
    pyproject = tomllib.loads(_CONTROL_PLANE_PYPROJECT.read_text(encoding='utf-8'))
    dependencies = pyproject['project']['dependencies']

    assert 'playwright' in dependencies


def test_control_plane_image_installs_chromium_runtime() -> None:
    dockerfile_text = _CONTROL_PLANE_DOCKERFILE.read_text(encoding='utf-8')

    required_fragments = (
        'fonts-noto-color-emoji',
        'libnss3',
        'libxkbcommon0',
        'python -m playwright install chromium',
    )

    for fragment in required_fragments:
        assert fragment in dockerfile_text


def test_deploy_workflow_builds_control_plane_from_patched_dockerfile() -> None:
    workflow_text = _DEPLOY_WORKFLOW.read_text(encoding='utf-8')

    assert 'file: docker/Dockerfile.control-plane' in workflow_text
