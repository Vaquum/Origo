#!/usr/bin/env python3
"""Publish one deliberately prepared release from its merged PR commit."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tomllib
from pathlib import Path
from tempfile import TemporaryDirectory


def _output(*argv: str) -> str:
    return subprocess.run(argv, check=True, stdout=subprocess.PIPE, text=True).stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--preparation-pr', required=True, type=int)
    args = parser.parse_args()
    if args.preparation_pr < 1:
        parser.error('--preparation-pr must be positive')

    preparation = json.loads(_output(
        'gh', 'pr', 'view', str(args.preparation_pr), '--repo', args.repo,
        '--json', 'state,baseRefName,mergeCommit',
    ))
    if preparation['state'] != 'MERGED' or preparation['baseRefName'] != 'main':
        raise SystemExit('Release preparation must be a merged PR targeting main')
    if preparation['mergeCommit'] is None:
        raise SystemExit('Merged preparation commit is unavailable')
    commit = preparation['mergeCommit']['oid']
    if re.fullmatch(r'[0-9a-f]{40}', commit) is None:
        raise SystemExit('Preparation commit must be an immutable Git SHA')
    _output('git', 'merge-base', '--is-ancestor', commit, 'origin/main')

    project = tomllib.loads(_output('git', 'show', f'{commit}:pyproject.toml'))
    version = project['project']['version']
    if not isinstance(version, str) or re.fullmatch(r'\d+\.\d+\.\d+', version) is None:
        raise SystemExit('Prepared version must be MAJOR.MINOR.PATCH')
    tag = f'v{version}'
    changelog = _output('git', 'show', f'{commit}:CHANGELOG.md')
    sections = re.split(r'(?m)^# (v\d+\.\d+\.\d+)\s*$', changelog)
    if len(sections) < 3 or sections[1] != tag:
        raise SystemExit('First prepared changelog version must match the project version')
    notes = sections[2].strip() + '\n'
    if not any(line.strip() and not line.lstrip().startswith('#') for line in notes.splitlines()):
        raise SystemExit('Prepared changelog section must contain release notes')

    releases = json.loads(_output('gh', 'api', '--paginate', '--slurp', f'repos/{args.repo}/releases'))
    if any(release['tag_name'] == tag for page in releases for release in page):
        raise SystemExit(f'Release {tag} already exists')
    refs = json.loads(_output('gh', 'api', f'repos/{args.repo}/git/matching-refs/tags/{tag}'))
    existing = next((ref for ref in refs if ref['ref'] == f'refs/tags/{tag}'), None)
    if existing is not None and (
        existing['object']['type'] != 'commit' or existing['object']['sha'] != commit
    ):
        raise SystemExit(f'Existing tag {tag} does not match the prepared commit')
    with TemporaryDirectory(prefix='origo-release-') as directory:
        notes_file = Path(directory) / 'notes.md'
        notes_file.write_text(notes, encoding='utf-8')
        # Atomic creation rejects a racing tag instead of silently reusing its target.
        if existing is None:
            _output('gh', 'api', f'repos/{args.repo}/git/refs', '--method', 'POST',
                    '-f', f'ref=refs/tags/{tag}', '-f', f'sha={commit}')
        print(_output('gh', 'release', 'create', tag, '--repo', args.repo,
                      '--verify-tag', '--title', tag, '--notes-file', str(notes_file)).strip())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
