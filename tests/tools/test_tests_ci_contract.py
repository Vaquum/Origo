from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError
from urllib.request import Request
from xml.etree import ElementTree

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_WORKFLOW = REPO_ROOT / '.github/workflows/pr_checks_tests.yml'
EXPECTED_TEST_COMMAND = 'pytest tests/origo_source_native -q --maxfail=1'


def test_pr_checks_tests_workflow_exists() -> None:
    assert TESTS_WORKFLOW.exists()


def test_pr_checks_tests_pins_python_and_runtime_suite_command() -> None:
    workflow = TESTS_WORKFLOW.read_text(encoding='utf-8')

    assert "python-version: '3.11'" in workflow
    assert "python -m pip install --upgrade pip '.[dev]'" in workflow
    assert EXPECTED_TEST_COMMAND in workflow
    assert '-n "$TEST_WORKERS" --dist loadgroup --no-loadscope-reorder --durations=30' in workflow
    assert '--merge test-results/ordinary.xml test-results/resources.xml' in workflow
    assert (
        'pytest tests/origo_source_native -q --maxfail=1 -m resource'
        in workflow
    )
    assert (
        "-m 'not resource'"
        in workflow
    )
    assert '--junitxml=test-results/ordinary.xml' in workflow
    assert '--junitxml=test-results/resources.xml' in workflow
    # Pytest deletes basetemp; the mounted filesystem root cannot be deleted.
    assert '--basetemp="$TEST_TMP/ordinary"' in workflow
    assert (
        'python tools/tests_ci.py test-results/runtime.xml test-results/collection.txt' in workflow
    )
    assert (
        'pytest tests/origo_source_native --collect-only -q > test-results/collection.txt'
        in workflow
    )
    assert 'tests/tools/test_tests_ci_contract.py' in (REPO_ROOT / '.github/workflows/pr_checks_ruleset.yml').read_text()
    assert "inputs.runner == 'github'" in workflow
    assert 'options: [server, github]' in workflow
    assert 'head.repo.full_name != github.repository' in workflow
    assert '["self-hosted", "linux", "x64", "origo-tests"]' in workflow
    assert '\'["ubuntu-latest"]\'' in workflow
    assert 'persist-credentials: false' in workflow
    assert 'cancel-in-progress: true' in workflow
    assert 'continue-on-error' not in workflow


def test_acceptance_manifest_preserves_original_entry_points() -> None:
    contract = json.loads((REPO_ROOT / '.github/tests_acceptance.json').read_text())
    manifest = contract['groups']
    assert contract['baseline_sha'] == '485521924983e3d4fd7f7b7bf3985bc44a26edb3'
    assert len(contract['baseline_nodes']) == 1052
    assert hashlib.sha256('\n'.join(contract['baseline_nodes']).encode()).hexdigest() == (
        '82c6d0c4fb6bfc58b19ce10f3190023292c87f80d3f9210e415b69dc47727203'
    )
    nodes = [node for group in manifest for node in group['nodes']]
    assert len(nodes) == len(set(nodes))
    # Original 50 selectors at SHA 4855219, independently pinned for shallow checkouts.
    assert len(nodes) == 50
    assert hashlib.sha256('\n'.join(sorted(nodes)).encode()).hexdigest() == (
        '6cd091368c8edeb21b7e3c3cc4a89349724e6cdd9b5f747de3c8b53aac252864'
    )
    assert [group['expected'] for group in manifest] == [15, 4, 18, 8, 10]
    additions = contract['baseline_additions']
    assert additions['sha'] == '4288d0a764eb526869cb96dbb6ddd23779927242'
    assert len(additions['nodes']) == 6
    assert hashlib.sha256('\n'.join(additions['nodes']).encode()).hexdigest() == (
        '8a4ce71c94d9f82c3764bda8b4b66b3a9f19108995d118bd0cfeb8ece882de98'
    )
    assert len(set(contract['baseline_nodes']) | set(additions['nodes'])) == 1058
    assert hashlib.sha256(json.dumps(
        contract['consolidations'], sort_keys=True, separators=(',', ':'),
    ).encode()).hexdigest() == '12391bf047bb1dc6ad8afba2414fb2f4e1a1e1bb949a9f4cc2e70d57ace55887'


def test_workflow_executes_verification_and_all_provenance_checks() -> None:
    workflow = TESTS_WORKFLOW.read_text()
    steps = re.split(r'(?m)^      - name: ', workflow)[1:]
    assert steps
    assert all('        if:' not in step for step in steps[:-1])
    runs = '\n'.join(
        re.search(r'(?ms)^        run: (.*)', step).group(1)
        for step in steps if re.search(r'(?m)^        run:', step)
    )
    assert EXPECTED_TEST_COMMAND in runs
    assert 'python tools/tests_ci.py test-results/runtime.xml test-results/collection.txt' in runs
    for stem in (
        'BTCUSDT-trades-2019-09-08', 'BTCUSDT-trades-2019-09-09',
        'BTCUSDT-trades-2024-04-20', 'BTCUSDT-trades-2017-08-17',
        'BTCUSDT-trades-2020-01-01',
    ):
        assert stem in runs
    assert runs.count('tools/fixture_bundle.py verify') == 2
    assert 'playwright install --with-deps chromium' in runs
    assert 'shellcheck tools/provision_tests_runner.sh deploy/tests-runner/*.sh' in (REPO_ROOT / '.github/workflows/pr_checks_lint.yml').read_text()


@pytest.fixture(scope='module')
def real_report() -> bytes:
    # Three passing records from the 1052-case run at baseline SHA 4855219.
    return (
        b'<testsuites><testsuite tests="3" errors="0" failures="0" skipped="0" time="3045.610"><testcase classname="tests.origo_source_native.test_book_vendor" name="test_real_hour_survives_original_seed_region_without_reseeding[spot]" time="12.941" /><testcase classname="tests.origo_source_native.test_book_vendor" name="test_real_hour_survives_original_seed_region_without_reseeding[perp]" time="41.670" /><testcase classname="tests.origo_source_native.test_monitor" name="test_monitor_reports_run_failures_once_and_suppresses_repeats_within_cooldown" time="1.250" /></testsuite></testsuites>'
    )


@pytest.mark.parametrize('fault', [
    'none', 'missing-file', 'malformed', 'empty', 'missing-case', 'missing-ordinary',
    'missing-original', 'duplicate', 'skipped', 'failure', 'error',
    'aggregate-skipped', 'count', 'renamed-case', 'missing-addition',
])
def test_report_cli_enforces_actual_outcomes(
    tmp_path: Path, real_report: bytes, fault: str,
) -> None:
    tree = ElementTree.fromstring(real_report)
    suite = tree.find('testsuite')
    assert suite is not None
    cases = suite.findall('testcase')
    nodes = [f"{case.attrib['classname'].replace('.', '/')}.py::{case.attrib['name']}" for case in cases]
    collection = tmp_path / 'collection.txt'
    collection.write_text('\n'.join(nodes) + '\n')
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({
        'baseline_nodes': nodes,
        'groups': [{'name': 'Actual contract execution', 'expected': 1, 'nodes': [nodes[0]]}],
    }))
    report = tmp_path / 'report.xml'
    if fault == 'empty':
        suite.clear()
        suite.attrib.update(tests='0', skipped='0', failures='0', errors='0')
    elif fault in {'missing-case', 'missing-original', 'missing-ordinary'}:
        removed = 2 if fault == 'missing-ordinary' else 1
        suite.remove(cases[removed])
        suite.attrib['tests'] = str(len(cases) - 1)
        if fault == 'missing-original':
            collection.write_text('\n'.join(node for i, node in enumerate(nodes) if i != removed) + '\n')
    elif fault == 'missing-addition':
        saved = json.loads(manifest.read_text())
        saved['baseline_nodes'] = nodes[:2]
        saved['baseline_additions'] = {'sha': '4288d0a764eb526869cb96dbb6ddd23779927242', 'nodes': nodes[2:]}
        manifest.write_text(json.dumps(saved))
        suite.remove(cases[2])
        suite.attrib['tests'] = '2'
        collection.write_text('\n'.join(nodes[:2]) + '\n')
    elif fault == 'duplicate':
        suite.append(ElementTree.fromstring(ElementTree.tostring(cases[1])))
        suite.attrib['tests'] = str(len(cases) + 1)
    elif fault in {'skipped', 'failure', 'error'}:
        ElementTree.SubElement(cases[2], fault)
    elif fault == 'aggregate-skipped':
        suite.attrib['skipped'] = '1'
    elif fault == 'count':
        suite.attrib['tests'] = str(len(cases) + 1)
    elif fault == 'renamed-case':
        cases[1].attrib['name'] += '[missing-parameter]'
    report.write_bytes(ElementTree.tostring(tree))
    if fault == 'missing-file':
        report.unlink()
    elif fault == 'malformed':
        report.write_text('<testsuites>')
    result = subprocess.run(
        [sys.executable, 'tools/tests_ci.py', str(report), str(collection), '--manifest', str(manifest)],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    assert (result.returncode == 0) is (fault == 'none'), result.stdout + result.stderr


@pytest.mark.parametrize('fault', ['none', 'dispatch', 'fork', 'repository', 'job', 'target', 'missing-event'])
def test_runner_admission_rejects_untrusted_jobs(tmp_path: Path, fault: str) -> None:
    # Head metadata from the real Origo pull request 510; fault cases mutate admission fields.
    payload = {'pull_request': {'number': 510, 'head': {
        'sha': '1d4fa1b07b96792c555eb93e54dfe3e96e83e1db',
        'repo': {'full_name': 'Vaquum/Origo'},
    }}}
    if fault == 'fork':
        payload['pull_request']['head']['repo']['full_name'] += '-fork'
    event = tmp_path / 'event.json'
    event.write_text(json.dumps(payload))
    env = dict(os.environ, GITHUB_REPOSITORY='Vaquum/Origo', GITHUB_JOB='pr_checks_tests',
               GITHUB_EVENT_NAME='pull_request', GITHUB_EVENT_PATH=str(event))
    if fault == 'dispatch':
        env['GITHUB_EVENT_NAME'] = 'workflow_dispatch'
    elif fault == 'repository':
        env['GITHUB_REPOSITORY'] += '-other'
    elif fault == 'job':
        env['GITHUB_JOB'] = 'deploy'
    elif fault == 'target':
        env['GITHUB_EVENT_NAME'] = 'pull_request_target'
    elif fault == 'missing-event':
        event.unlink()
    result = subprocess.run(['bash', 'deploy/tests-runner/admit-job.sh'], cwd=REPO_ROOT,
                            env=env, capture_output=True, text=True)
    assert (result.returncode == 0) is (fault in {'none', 'dispatch'}), result.stdout + result.stderr


@pytest.fixture
def registration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        'runner_registration', REPO_ROOT / 'deploy/tests-runner/registration.py',
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('fault', ['none', 'timeout', 'network', 'server', 'unauthorized', 'exhausted'])
def test_github_requests_recover_from_transient_errors(
    registration: ModuleType, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], fault: str,
) -> None:
    document = {'pull_request': {'number': 510, 'head': {
        'sha': '1d4fa1b07b96792c555eb93e54dfe3e96e83e1db',
        'repo': {'full_name': 'Vaquum/Origo'},
    }}}
    attempts: list[int] = []
    delays: list[int] = []

    def open_request(message: Request, *, timeout: int) -> io.BytesIO:
        assert message.full_url == 'https://api.github.com/repos/Vaquum/Origo/pulls/510'
        assert timeout == 30
        attempts.append(1)
        if fault == 'unauthorized' or fault == 'exhausted' or (fault != 'none' and len(attempts) == 1):
            if fault == 'timeout':
                raise TimeoutError('Interrupted status request')
            if fault == 'network':
                raise URLError('Interrupted status connection')
            raise HTTPError(message.full_url, 401 if fault == 'unauthorized' else 503,
                            'Controlled HTTP fault', None, None)
        return io.BytesIO(json.dumps(document).encode())

    monkeypatch.setattr(registration, 'urlopen', open_request)
    monkeypatch.setattr(registration.time, 'sleep', delays.append)
    if fault in {'unauthorized', 'exhausted'}:
        with pytest.raises(HTTPError):
            registration.request('repos/Vaquum/Origo/pulls/510', 'fixture-credential')
        assert len(attempts) == (1 if fault == 'unauthorized' else 3)
        assert delays == ([] if fault == 'unauthorized' else [2, 4])
    else:
        assert registration.request('repos/Vaquum/Origo/pulls/510', 'fixture-credential') == document
        assert len(attempts) == (1 if fault == 'none' else 2)
        assert delays == ([] if fault == 'none' else [2])
    captured = capsys.readouterr()
    assert not captured.out
    assert captured.err.count('GitHub request failed') == (
        0 if fault in {'none', 'unauthorized'} else len(attempts) - (fault != 'exhausted')
    )
    assert 'fixture-credential' not in captured.err


@pytest.mark.parametrize('interruption', ['none', 'writing', 'replacement'])
def test_token_cache_survives_process_interruption(
    registration: ModuleType, tmp_path: Path, interruption: str,
) -> None:
    document = json.loads((REPO_ROOT / '.github/tests_acceptance.json').read_text())
    cache = tmp_path / 'access.json'
    old = {'baseline_sha': document['baseline_sha']}
    cache.write_text(json.dumps(old))
    script = '''
import importlib.util, json, os, signal, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location('registration', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
if sys.argv[4] == 'writing':
    def interrupted_dump(document, handle):
        handle.write('{')
        os.kill(os.getpid(), signal.SIGTERM)
    module.json.dump = interrupted_dump
elif sys.argv[4] == 'replacement':
    def interrupted_replace(source, target):
        os.kill(os.getpid(), signal.SIGTERM)
    module.os.replace = interrupted_replace
module.save_cache(Path(sys.argv[2]), json.loads(Path(sys.argv[3]).read_text()))
'''
    result = subprocess.run([
        sys.executable, '-c', script, str(REPO_ROOT / 'deploy/tests-runner/registration.py'),
        str(cache), str(REPO_ROOT / '.github/tests_acceptance.json'), interruption,
    ], capture_output=True, text=True)
    assert result.returncode == (0 if interruption == 'none' else -15), result.stderr
    assert json.loads(cache.read_text()) == (document if interruption == 'none' else old)
    registration.save_cache(cache, document)
    assert json.loads(cache.read_text()) == document
    assert cache.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('conversion_fails', [False, True])
def test_image_refresh_preserves_the_backing_image_on_failure(
    tmp_path: Path, conversion_fails: bool,
) -> None:
    activation = (REPO_ROOT / 'deploy/tests-runner/activate.sh').read_text()
    conversion = activation.split('cd /var/lib/libvirt/images/origo-tests\n', 1)[1].split(
        '\ninstall -m 644', 1,
    )[0]
    old = (REPO_ROOT / 'deploy/tests-runner/controller.sh').read_bytes()
    prepared = (REPO_ROOT / 'deploy/tests-runner/registration.py').read_bytes()
    (tmp_path / 'clean.qcow2').write_bytes(old)
    (tmp_path / 'prepared').write_bytes(prepared)
    (tmp_path / 'runner.qcow2').write_text(json.dumps({'backing': 'clean.qcow2'}))
    binaries = tmp_path / 'bin'
    binaries.mkdir()
    qemu = binaries / 'qemu-img'
    qemu.write_text(f'#!{sys.executable}\n' + '''
import json, os, sys
from pathlib import Path
source, target = map(Path, sys.argv[-2:])
if target.name == json.loads(source.read_text())['backing']:
    raise SystemExit('Cannot overwrite the source backing image')
prepared = Path('prepared').read_bytes()
target.write_bytes(prepared[:10] if os.environ['CONVERSION_FAILS'] == '1' else prepared)
if os.environ['CONVERSION_FAILS'] == '1':
    raise SystemExit(17)
''')
    qemu.chmod(0o755)
    result = subprocess.run(['bash', '-euc', conversion], cwd=tmp_path, env={
        **os.environ, 'PATH': f'{binaries}:{os.environ["PATH"]}',
        'CONVERSION_FAILS': '1' if conversion_fails else '0',
    }, capture_output=True, text=True)
    assert result.returncode == (17 if conversion_fails else 0), result.stderr
    assert (tmp_path / 'clean.qcow2').read_bytes() == (old if conversion_fails else prepared)
    if not conversion_fails:
        assert (tmp_path / 'clean.qcow2').stat().st_mode & 0o777 == 0o444


@pytest.mark.parametrize('fault', ['none', 'missing-retained', 'unmapped', 'empty-contract', 'empty-retained', 'unknown-original', 'still-executed', 'failed-retained'])
def test_consolidated_report_requires_explicit_contract_and_passing_owner(
    tmp_path: Path, real_report: bytes, fault: str,
) -> None:
    tree = ElementTree.fromstring(real_report)
    suite = tree.find('testsuite')
    assert suite is not None
    cases = suite.findall('testcase')
    nodes = [f"{case.attrib['classname'].replace('.', '/')}.py::{case.attrib['name']}" for case in cases]
    # Exercise report substitution using original records; repository mappings name
    # the exact source/market owner and are reviewed with the preserved assertions.
    replacement = {'contract': 'Explicit preserved contract', 'retained': [nodes[0]]}
    retired = nodes[1]
    suite.remove(cases[1])
    suite.attrib['tests'] = '2'
    if fault == 'missing-retained':
        replacement['retained'] = [nodes[1]]
    elif fault == 'empty-contract':
        replacement['contract'] = ''
    elif fault == 'empty-retained':
        replacement['retained'] = []
    elif fault == 'unknown-original':
        retired += '[unknown]'
    elif fault == 'still-executed':
        retired = nodes[0]
    elif fault == 'failed-retained':
        ElementTree.SubElement(cases[0], 'failure')
    contract = {'baseline_nodes': nodes, 'consolidations': {retired: replacement}, 'groups': [
        {'name': 'Captured records', 'expected': 3, 'nodes': nodes},
    ]}
    if fault == 'unmapped':
        contract['consolidations'] = {}
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps(contract))
    collection = tmp_path / 'collection.txt'
    collection.write_text('\n'.join([nodes[0], nodes[2]]) + '\n')
    report = tmp_path / 'report.xml'
    report.write_bytes(ElementTree.tostring(tree))
    result = subprocess.run([
        sys.executable, 'tools/tests_ci.py', str(report), str(collection), '--manifest', str(manifest),
    ], cwd=REPO_ROOT, capture_output=True, text=True)
    assert (result.returncode == 0) is (fault == 'none'), result.stdout + result.stderr


@pytest.mark.parametrize('fault', [
    'none', 'empty-phase', 'missing-phase', 'truncated-phase', 'duplicate-cross-phase',
    'failed-phase',
])
def test_phase_reports_merge_without_losing_outcomes(
    tmp_path: Path, real_report: bytes, fault: str,
) -> None:
    root = ElementTree.fromstring(real_report)
    suite = root.find('testsuite')
    assert suite is not None
    cases = suite.findall('testcase')
    nodes = [f"{case.attrib['classname'].replace('.', '/')}.py::{case.attrib['name']}" for case in cases]
    reports = []
    for index, subset in enumerate((cases[:2], cases[2:])):
        phase = ElementTree.Element('testsuites')
        current = ElementTree.SubElement(phase, 'testsuite', tests=str(len(subset)), skipped='0', failures='0', errors='0', time=str(index + 1))
        current.extend(subset)
        report = tmp_path / f'phase-{index}.xml'
        report.write_bytes(ElementTree.tostring(phase))
        reports.append(str(report))
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'baseline_nodes': nodes, 'groups': [
        {'name': 'All original records', 'expected': 3, 'nodes': nodes},
    ]}))
    collection = tmp_path / 'collection.txt'
    collection.write_text('\n'.join(nodes) + '\n')
    second = Path(reports[1])
    phase = ElementTree.parse(second)
    current = phase.find('testsuite')
    assert current is not None
    if fault == 'empty-phase':
        current.remove(current.findall('testcase')[0])
        current.attrib['tests'] = '0'
    elif fault == 'duplicate-cross-phase':
        current.append(cases[0])
        current.attrib['tests'] = '2'
    elif fault == 'failed-phase':
        ElementTree.SubElement(current.findall('testcase')[0], 'failure')
    phase.write(second)
    if fault == 'missing-phase':
        second.unlink()
    elif fault == 'truncated-phase':
        second.write_text('<testsuites>')
    merged = tmp_path / 'merged.xml'
    result = subprocess.run([
        sys.executable, 'tools/tests_ci.py', str(merged), str(collection), '--manifest', str(manifest), '--merge', *reports,
    ], cwd=REPO_ROOT, capture_output=True, text=True)
    assert (result.returncode == 0) is (fault == 'none'), result.stdout + result.stderr
    if fault != 'none':
        return
    combined = ElementTree.parse(merged)
    assert len(combined.findall('.//testcase')) == 3
    assert sum(float(s.attrib['time']) for s in combined.findall('testsuite')) == 3
