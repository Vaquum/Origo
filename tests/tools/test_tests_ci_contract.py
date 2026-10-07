from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
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
    assert '-n "$TEST_WORKERS" --dist loadfile --no-loadscope-reorder --durations=30' in workflow
    assert '--junitxml=test-results/runtime.xml' in workflow
    assert 'python tools/tests_ci.py test-results/runtime.xml test-results/collection.txt' in workflow
    assert 'pytest tests/origo_source_native --collect-only -q > test-results/collection.txt' in workflow
    assert 'pytest tests/tools/test_tests_ci_contract.py -q' in workflow
    assert "inputs.runner == 'github'" in workflow
    assert 'options: [server, github]' in workflow
    assert 'head.repo.full_name != github.repository' in workflow
    assert '["self-hosted", "linux", "x64", "origo-tests"]' in workflow
    assert "'[\"ubuntu-latest\"]'" in workflow
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
    assert 'shellcheck tools/provision_tests_runner.sh deploy/tests-runner/*.sh' in runs


@pytest.fixture(scope='module')
def real_report() -> bytes:
    # Three passing records from the 1052-case run at baseline SHA 4855219.
    return (
        b'<testsuites><testsuite tests="3" errors="0" failures="0" skipped="0" time="3045.610"><testcase classname="tests.origo_source_native.test_book_vendor" name="test_real_hour_survives_original_seed_region_without_reseeding[spot]" time="12.941" /><testcase classname="tests.origo_source_native.test_book_vendor" name="test_real_hour_survives_original_seed_region_without_reseeding[perp]" time="41.670" /><testcase classname="tests.origo_source_native.test_monitor" name="test_monitor_reports_run_failures_once_and_suppresses_repeats_within_cooldown" time="1.250" /></testsuite></testsuites>'
    )


@pytest.mark.parametrize('fault', [
    'none', 'missing-file', 'malformed', 'empty', 'missing-case', 'missing-ordinary',
    'missing-original', 'duplicate', 'skipped', 'failure', 'error',
    'aggregate-skipped', 'count', 'renamed-case',
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
