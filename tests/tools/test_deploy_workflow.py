from __future__ import annotations

import ast
import shlex
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DEPLOY_WORKFLOW: Final[Path] = REPO_ROOT / '.github' / 'workflows' / 'deploy_on_merge.yml'


def _logical_commands(lines: list[str]) -> list[str]:
    """Join backslash continuations and multi-line quotes: one shell command each."""
    commands: list[str] = []
    current: list[str] = []
    quoted = False
    for line in lines:
        stripped = line.rstrip('\n')
        current.append(stripped.rstrip()[:-1] if stripped.rstrip().endswith('\\') else stripped)
        if stripped.count("'") % 2 == 1:
            quoted = not quoted
        if quoted or stripped.rstrip().endswith('\\'):
            continue
        commands.append(' '.join(current))
        current = []
    if current:
        commands.append(' '.join(current))
    return commands


def test_compose_execs_do_not_consume_stdin() -> None:
    # The remote deploy script arrives on stdin itself, and compose exec
    # forwards stdin into the container: a bare exec drinks the rest of the
    # script and the block silently never runs. Every exec/run detaches
    # stdin except the smoke test, which feeds python through a heredoc.
    text = DEPLOY_WORKFLOW.read_text()
    commands = _logical_commands(text.splitlines())
    execs = [command for command in commands if 'exec -T' in command or 'run --rm --no-deps -T' in command]
    assert len(execs) >= 5, execs
    for command in execs:
        if 'python - <<' in command:
            continue
        assert '</dev/null' in command, command


def test_post_up_block_is_observable() -> None:
    # Markers bracket every post-up stage and the launch verifies its run, so a
    # future skip fails loud instead of going green silently.
    text = DEPLOY_WORKFLOW.read_text()
    assert text.count("echo 'post-up:") >= 3
    assert 'maintenance launch produced no run' in text
    assert 'maintenance launched run' in text


def test_optional_dashboard_url_reaches_generated_deploy_environment() -> None:
    workflow = DEPLOY_WORKFLOW.read_text()
    assert 'ORIGO_ALERT_DASHBOARD_URL: ${{ vars.ORIGO_ALERT_DASHBOARD_URL }}' in workflow
    validation = workflow.split('      - name: Validate deploy configuration', 1)[1].split(
        '      - name:', 1
    )[0]
    assert 'ORIGO_ALERT_DASHBOARD_URL' not in validation
    script = textwrap.dedent(
        workflow.split("          python3 - <<'PY' > \"$DEPLOY_ENV_FILE\"\n", 1)[1].split(
            '\n          PY', 1
        )[0]
    )
    required = {
        node.slice.value
        for node in ast.walk(ast.parse(script))
        if isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == 'os'
        and node.value.attr == 'environ'
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    }
    assert 'ORIGO_ALERT_DASHBOARD_URL' not in required
    for dashboard_url in (None, 'https://operations.example.com/law'):
        environment = {key: 'deployment-test-value' for key in required}
        if dashboard_url is not None:
            environment['ORIGO_ALERT_DASHBOARD_URL'] = dashboard_url
        result = subprocess.run(
            [sys.executable, '-c', script],
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        exported = dict(item.split('=', 1) for item in shlex.split(result.stdout))
        assert exported['ORIGO_ALERT_DASHBOARD_URL'] == (dashboard_url or '')
        assert exported['ORIGO_ALERT_EMAIL_TO'] == 'deployment-test-value'
