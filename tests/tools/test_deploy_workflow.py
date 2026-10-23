from __future__ import annotations

import ast
import json
import os
import shlex
import shutil
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path
from typing import Final

import pytest

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DEPLOY_WORKFLOW: Final[Path] = REPO_ROOT / '.github' / 'workflows' / 'deploy_on_merge.yml'


def test_release_workflow_is_manual_serialized_and_runs_trusted_main() -> None:
    workflow = (REPO_ROOT / '.github/workflows/pr_post_release.yml').read_text()
    trigger = workflow.split('\non:\n', 1)[1].split('\npermissions:', 1)[0]
    assert [line for line in trigger.splitlines() if line.startswith('  ') and not line.startswith('    ')] == ['  workflow_dispatch:']
    assert 'group: manual-release\n  cancel-in-progress: false' in workflow
    assert "if: github.ref == 'refs/heads/main'" in workflow
    assert 'ref: main\n          fetch-depth: 0\n          persist-credentials: false' in workflow
    assert 'python tools/release.py --repo "$GITHUB_REPOSITORY" --preparation-pr "$PREPARATION_PR"' in workflow
    assert 'PREPARATION_PR: ${{ inputs.preparation_pr }}' in workflow


@pytest.mark.parametrize('fault', [
    'none', 'unmerged', 'wrong-base', 'existing-release', 'duplicate-tag', 'release-failure',
])
def test_release_pins_merged_preparation_and_rejects_duplicate_publication(
    tmp_path: Path, fault: str,
) -> None:
    repository = tmp_path / 'repo'
    repository.mkdir()

    def git(*argv: str) -> str:
        return subprocess.run(['git', *argv], cwd=repository, check=True,
                              capture_output=True, text=True).stdout.strip()

    # Real repository contents, with deliberate GitHub protocol faults below.
    for name in ('pyproject.toml', 'CHANGELOG.md'):
        shutil.copyfile(REPO_ROOT / name, repository / name)
    git('init', '-q')
    git('add', '.')
    git('-c', 'user.name=Release protocol', '-c', 'user.email=release@example.invalid',
        'commit', '-qm', 'Preparation protocol boundary')
    prepared = git('rev-parse', 'HEAD')
    shutil.copyfile(REPO_ROOT / 'README.md', repository / 'README.md')
    git('add', '.')
    git('-c', 'user.name=Release protocol', '-c', 'user.email=release@example.invalid',
        'commit', '-qm', 'Intervening ordinary merge protocol boundary')
    later = git('rev-parse', 'HEAD')
    git('update-ref', 'refs/remotes/origin/main', later)
    version = tomllib.loads((repository / 'pyproject.toml').read_text())['project']['version']
    tag = f'v{version}'
    trace = tmp_path / 'gh.jsonl'
    binaries = tmp_path / 'bin'
    binaries.mkdir()
    gh = binaries / 'gh'
    gh.write_text(f'#!{sys.executable}\n' + '''
import json, os, sys
from pathlib import Path
argv = sys.argv[1:]
fault = os.environ['RELEASE_FAULT']
tag = os.environ['RELEASE_TAG']
record = {'argv': argv}
if argv[:2] == ['release', 'create']:
    record['notes'] = Path(argv[argv.index('--notes-file') + 1]).read_text()
with Path(os.environ['RELEASE_TRACE']).open('a') as handle:
    handle.write(json.dumps(record) + '\\n')
if argv[:2] == ['pr', 'view']:
    print(json.dumps({'state': 'OPEN' if fault == 'unmerged' else 'MERGED',
        'baseRefName': 'other' if fault == 'wrong-base' else 'main',
        'mergeCommit': {'oid': os.environ['RELEASE_PREPARED']}}))
elif argv[:4] == ['api', '--paginate', '--slurp', 'repos/Vaquum/Origo/releases']:
    print(json.dumps([[{'tag_name': tag}]] if fault == 'existing-release' else [[]]))
elif argv[:2] == ['api', 'repos/Vaquum/Origo/git/refs']:
    if fault == 'duplicate-tag':
        raise SystemExit('Deliberate duplicate-tag protocol rejection')
    print('{}')
elif argv[:2] == ['release', 'create']:
    if fault == 'release-failure':
        raise SystemExit('Deliberate publication protocol failure')
    print('Release protocol completed')
else:
    raise SystemExit('Unexpected GitHub protocol command: ' + repr(argv))
''')
    gh.chmod(0o700)
    result = subprocess.run([
        sys.executable, str(REPO_ROOT / 'tools/release.py'), '--repo', 'Vaquum/Origo',
        '--preparation-pr', '608',
    ], cwd=repository, env={
        **os.environ, 'PATH': str(binaries) + os.pathsep + os.environ['PATH'],
        'RELEASE_FAULT': fault, 'RELEASE_TAG': tag, 'RELEASE_TRACE': str(trace),
        'RELEASE_PREPARED': prepared,
    }, capture_output=True, text=True)
    records = [json.loads(line) for line in trace.read_text().splitlines()]
    writes = [record for record in records if '--method' in record['argv']
              or record['argv'][:2] == ['release', 'create']]
    assert (result.returncode == 0) is (fault == 'none'), result.stdout + result.stderr
    if fault in {'unmerged', 'wrong-base', 'existing-release'}:
        assert writes == []
        return
    assert writes[0]['argv'] == [
        'api', 'repos/Vaquum/Origo/git/refs', '--method', 'POST',
        '-f', f'ref=refs/tags/{tag}', '-f', f'sha={prepared}',
    ]
    assert prepared != later
    if fault == 'duplicate-tag':
        assert len(writes) == 1
        assert 'duplicate-tag protocol rejection' in result.stderr
        return
    assert len(writes) == 2
    assert writes[1]['argv'][:7] == ['release', 'create', tag, '--repo', 'Vaquum/Origo', '--verify-tag', '--title']
    assert writes[1]['notes'].strip() == (repository / 'CHANGELOG.md').read_text().split('\n# v', 1)[0].split('\n', 1)[1].strip()
    if fault == 'release-failure':
        assert 'publication protocol failure' in result.stderr


def _deployment_session() -> tuple[str, str]:
    step = DEPLOY_WORKFLOW.read_text().split('      - name: Deploy updated images\n', 1)[1]
    script = textwrap.dedent(step.split('        run: |\n', 1)[1])
    start = script.index('\nssh ') + 1
    end = script.index('\nREMOTE\n', start) + len('\nREMOTE\n')
    session = script[start:end]
    body = session.split("<<'REMOTE'\n", 1)[1].rsplit('\nREMOTE\n', 1)[0] + '\n'
    return session, body


def _exercise_deployment_ssh(
    tmp_path: Path, code: int,
) -> tuple[list[str], str, subprocess.CompletedProcess[str]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    record = tmp_path / 'ssh.json'
    executable = tmp_path / 'ssh'
    executable.write_text(
        f'#!{sys.executable}\n'
        'import json, os, sys\n'
        'from pathlib import Path\n'
        "Path(os.environ['SSH_RECORD']).write_text(json.dumps({'argv': sys.argv[1:], 'stdin': sys.stdin.read()}))\n"
        "raise SystemExit(int(os.environ['SSH_EXIT']))\n"
    )
    executable.chmod(0o700)
    session, _ = _deployment_session()
    result = subprocess.run(
        ['bash', '-e', '-c', session + '\necho after-session\n'],
        env={**os.environ, 'PATH': str(tmp_path) + os.pathsep + os.environ['PATH'],
             'ORIGO_USER': 'deployment', 'ORIGO_HOST': 'deploy.invalid',
             'DEPLOY_DIR': '/deployment', 'SSH_RECORD': str(record), 'SSH_EXIT': str(code)},
        check=False, capture_output=True, text=True,
    )
    captured = json.loads(record.read_text())
    return captured['argv'], captured['stdin'], result


def test_deployment_ssh_configures_keepalive_for_silent_verification(tmp_path: Path) -> None:
    argv, _, result = _exercise_deployment_ssh(tmp_path, 0)
    assert result.returncode == 0
    parsed = subprocess.run(['ssh', '-G', '-F', '/dev/null', *argv],
                            check=True, capture_output=True, text=True)
    effective = dict(line.split(' ', 1) for line in parsed.stdout.splitlines())
    assert effective['serveraliveinterval'] == '30'
    assert effective['serveralivecountmax'] == '10'
    assert effective['stdinnull'] == 'no'


def test_deployment_ssh_preserves_script_and_remote_failure(tmp_path: Path) -> None:
    for code in (0, 23, 255):
        argv, received, result = _exercise_deployment_ssh(tmp_path / str(code), code)
        assert argv[-1] == "DEPLOY_DIR='/deployment' ORIGO_HOST='deploy.invalid' bash -s"
        assert received == _deployment_session()[1]
        assert received.rstrip().endswith("echo 'post-up: deployment verified'")
        assert result.returncode == code
        assert ('after-session' in result.stdout) == (code == 0)


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
        if 'python - <<' in command or '--entrypoint python dagster - <<' in command:
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
