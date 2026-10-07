from __future__ import annotations

import argparse
import json
from pathlib import Path
from xml.etree import ElementTree

MANIFEST = Path(__file__).resolve().parents[1] / '.github/tests_acceptance.json'


def verify_report(report: Path, collection: Path, manifest: Path = MANIFEST) -> None:
    root = ElementTree.parse(report).getroot()
    suites = list(root.iter('testsuite'))
    for suite in suites:
        for outcome in ('skipped', 'failures', 'errors'):
            if int(suite.attrib[outcome]) != 0:
                raise ValueError(f'The runtime report contains {outcome}')
    cases = root.findall('.//testcase')
    if not cases:
        raise ValueError('The runtime report contains no tests')
    if sum(int(suite.attrib['tests']) for suite in suites) != len(cases):
        raise ValueError('The runtime report test count does not match its cases')
    identities = [
        f"{case.attrib['classname'].replace('.', '/')}.py::{case.attrib['name']}"
        for case in cases
    ]
    if len(identities) != len(set(identities)):
        raise ValueError('The runtime report contains duplicate tests')
    for case, identity in zip(cases, identities, strict=True):
        if list(case.iter('skipped')) or list(case.iter('failure')) or list(case.iter('error')):
            raise ValueError(f'Test did not pass: {identity}')

    contract = json.loads(manifest.read_text(encoding='utf-8'))
    collected = [
        line for line in collection.read_text(encoding='utf-8').splitlines()
        if line.startswith('tests/') and '::' in line
    ]
    if not collected or len(collected) != len(set(collected)):
        raise ValueError('The collection must contain distinct test identities')
    if set(identities) != set(collected):
        missing = sorted(set(collected) - set(identities))
        unexpected = sorted(set(identities) - set(collected))
        raise ValueError(f'Execution differs from collection: missing={missing}, unexpected={unexpected}')
    baseline: object = contract['baseline_nodes']
    if not isinstance(baseline, list) or not baseline or not all(isinstance(node, str) for node in baseline):
        raise ValueError('The baseline must contain test identities')
    if len(baseline) != len(set(baseline)):
        raise ValueError('The baseline contains duplicate tests')
    if missing_original := sorted(set(baseline) - set(identities)):
        raise ValueError(f'Missing original runtime tests: {missing_original}')
    groups: object = contract['groups']
    if not isinstance(groups, list) or not groups:
        raise ValueError('The acceptance manifest must contain groups')
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError('An acceptance group must be an object')
        name, expected, nodes = group['name'], group['expected'], group['nodes']
        if not isinstance(name, str) or not isinstance(expected, int) or not isinstance(nodes, list):
            raise ValueError('An acceptance group requires a name, count and node list')
        count = 0
        for node in nodes:
            if not isinstance(node, str):
                raise ValueError('Acceptance nodes must be strings')
            matches = sum(identity == node or identity.startswith(f'{node}[') for identity in identities)
            if matches == 0:
                raise ValueError(f'Missing acceptance test: {node}')
            count += matches
        if count != expected:
            raise ValueError(f'{name}: expected {expected} passing cases, found {count}')
        print(f'{name}: {count} passed')
    print(f'Complete runtime report: {len(cases)} passed, no skips, failures or errors')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('report', type=Path)
    parser.add_argument('collection', type=Path)
    parser.add_argument('--manifest', type=Path, default=MANIFEST)
    parser.add_argument('--baseline', type=Path)
    args = parser.parse_args()
    verify_report(args.report, args.collection, args.manifest)
    if args.baseline is not None:
        verify_report(args.baseline, args.collection, args.manifest)
        baseline = ElementTree.parse(args.baseline).getroot()
        current = ElementTree.parse(args.report).getroot()
        before = sum(float(suite.attrib['time']) for suite in baseline.iter('testsuite'))
        after = sum(float(suite.attrib['time']) for suite in current.iter('testsuite'))
        if before <= 0 or after <= 0 or after > before / 2:
            raise ValueError(f'Required 2x runtime speedup: baseline={before:.2f}s, current={after:.2f}s')
        print(f'Runtime speedup: {before / after:.2f}x ({before:.2f}s -> {after:.2f}s)')


if __name__ == '__main__':
    main()
