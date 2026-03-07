#!/usr/bin/env python3
"""
Automated CHANGELOG.md and version update script using Claude API.
This script analyzes recent commits and updates CHANGELOG.md and bumps version in pyproject.toml
following strict guidelines from https://github.com/Vaquum/dev-docs/blob/main/src/Updating-Changelog.md
"""

import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import anthropic
import tomlkit

CHANGELOG_GUIDELINES = """
# Deterministic CHANGELOG Update Rules

This document defines a deterministic pattern for updating an existing `CHANGELOG.md` that already contains the `# Changelog` header and prior releases. It describes how to append the next release entry in the same pattern.

## What an "update" means

A changelog update means adding exactly one new release block for a new version, appended to the end of the file (unless you are correcting an older entry as a special case described below).

## Deterministic update procedure

### Step 1 — Choose the next version (SemVer)

Use SemVer: `MAJOR.MINOR.PATCH`.

Deterministic bump rules:

1) If any change is breaking (requires user code changes, changes meaning of existing API, removes/renames a public symbol without aliasing) → bump MAJOR.
2) Else if any change adds new capability (new module, endpoint, feature, model, new public function/class, new CLI/tooling behavior) → bump MINOR.
3) Else → bump PATCH.

If multiple apply, choose the highest bump.

### Step 2 — Set the release date (format rules)

Release heading date rules:

- Month is full English name: `January` … `December`
- Day is integer without leading zero: `1`, `2`, `31`
- Ordinal suffix must be correct using the deterministic rules below

Ordinal suffix rules:

- If day is 11, 12, or 13 → `th`
- Otherwise:
  - ends with 1 → `st`
  - ends with 2 → `nd`
  - ends with 3 → `rd`
  - else → `th`

Examples: `1st`, `2nd`, `3rd`, `4th`, `11th`, `12th`, `13th`, `21st`, `22nd`, `23rd`, `31st`.

### Step 3 — Append the new release heading (exact template)

Append a new heading at the end of the file using exactly:

## v{MAJOR}.{MINOR}.{PATCH} on {DAY}{ORDINAL} of {Month}, {YEAR}

No alternative phrasing, no extra punctuation, no extra tags.

### Step 4 — Spacing rule after heading

There must be exactly one blank line between the heading and the first bullet.

### Step 5 — Convert changes into bullets (one change per bullet)

Rules:

- Each bullet is a single line starting with `- `
- No nested bullets
- No numbered lists
- No trailing whitespace
- Prefer one atomic change per bullet:
  - If a bullet contains "and" joining separable actions, split it
  - If the combined action is truly inseparable (e.g., a rename requires coordinated updates), keep it as one bullet

### Step 6 — Verb style (deterministic)

Bullets must start with an imperative verb. Use this canonical verb set unless there's a strong reason not to:

- Add
- Fix
- Refactor
- Move
- Rename
- Remove
- Update
- Improve
- Simplify
- Generalize
- Standardize
- Implement
- Configure
- Create
- Organize
- Disable
- Enable

Avoid past tense ("Added", "Fixed").

### Step 7 — Code-ish formatting rules

Wrap these in backticks:

- Function names: `get_klines_data`
- Class names: `UniversalExperimentLoop`
- Parameters and flags: `n_permutations`, `futures=False`
- Modules: `limen.metrics`
- File paths: `utils/get_klines_data.py`
- Literal config keys: `save_to_sqlite`

If referencing a specific file, prefer a Markdown link with backticked visible text:

- [`get_klines_data`](utils/get_klines_data.py)

Use exact spelling and casing as in the codebase.

### Step 8 — Notes and breaking changes

Notes must use exactly:

- `- **NOTE**: ...`

Breaking changes must use exactly:

- `- **BREAKING**: ...`

Placement rule:

- All `**BREAKING**` bullets must be first in the release.
- All `**NOTE**` bullets must come immediately after breaking bullets.

### Step 9 — Bullet ordering (deterministic)

Within a release, bullets must be ordered by category in this exact sequence:

1) `**BREAKING**` items
2) `**NOTE**` items
3) Add
4) Move / Rename
5) Refactor / Simplify / Generalize / Standardize
6) Fix
7) Update / Improve / Configure
8) Tests / CI / Tooling / Docs
9) Remove / Deprecate (unless breaking; then category 1)

Within each category:

- Sort lexicographically (A→Z) by the text after the initial verb/prefix.

Exception:

- If one bullet logically depends on another for readability, keep dependency order (e.g., "Rename X" before "Update docs for X").

### Step 10 — Spacing rule after the release

After the last bullet, add exactly one blank line before anything else.

## Deterministic "done" checklist

The update is valid if:

- The new release is appended at the end of the file.
- The heading matches `## vX.Y.Z on D{suffix} of Month, YYYY`.
- Ordinal suffix is correct (11/12/13 always `th`).
- Exactly one blank line after the heading and one blank line after the bullet list.
- Bullets are flat and start with `- `.
- Bullets use imperative verbs (Add/Fix/Refactor/…).
- Code identifiers/params/paths are wrapped in backticks.
- File references use Markdown links where appropriate.
- Bullets are ordered by the category ordering rules.
- No trailing whitespace.

## Append-only template (fill in and remove unused lines)

Append this block at the very end of `CHANGELOG.md` when releasing:

## vX.Y.Z on D{suffix} of Month, YYYY

- **BREAKING**: ...
- **NOTE**: ...
- Add ...
- Move ...
- Rename ...
- Refactor ...
- Fix ...
- Update ...
- Configure ...
- Add test ...
- Update docs ...

Remove any unused lines; do not keep placeholders.
"""


def get_current_version():
    """Read current version from pyproject.toml"""
    pyproject_path = Path('pyproject.toml')
    if not pyproject_path.exists():
        print(f'Error: pyproject.toml not found in {Path.cwd()}')
        sys.exit(1)

    with open(pyproject_path) as f:
        data = tomlkit.load(f)

    return data['project']['version']


def update_version_in_pyproject(new_version):
    """Update version in pyproject.toml using format-preserving tomlkit"""
    pyproject_path = Path('pyproject.toml')

    with open(pyproject_path) as f:
        data = tomlkit.load(f)

    data['project']['version'] = new_version

    with open(pyproject_path, 'w') as f:
        f.write(tomlkit.dumps(data))

    print(f'Updated version in pyproject.toml to {new_version}')


def get_recent_commits():
    """Get recent commits since the last release"""
    try:
        # Get the last tag (if any)
        result = subprocess.run(
            ['git', 'describe', '--tags', '--abbrev=0'], capture_output=True, text=True
        )

        if result.returncode == 0:
            last_tag = result.stdout.strip()
            # Get commits since last tag
            result = subprocess.run(
                ['git', 'log', f'{last_tag}..HEAD', '--oneline', '--no-merges'],
                capture_output=True,
                text=True,
            )
        else:
            # No tags exist, get recent commits (limit to last 50 to avoid unbounded growth)
            result = subprocess.run(
                ['git', 'log', '--oneline', '--no-merges', '-50'],
                capture_output=True,
                text=True,
            )

        return result.stdout.strip()
    except Exception as e:
        print(f'Error getting commits: {e}')
        return ''


def get_changed_files():
    """Get list of changed files since last release"""
    try:
        # Get the last tag (if any)
        result = subprocess.run(
            ['git', 'describe', '--tags', '--abbrev=0'], capture_output=True, text=True
        )

        if result.returncode == 0:
            last_tag = result.stdout.strip()
            # Get changed files since last tag
            result = subprocess.run(
                ['git', 'diff', '--name-status', f'{last_tag}..HEAD'],
                capture_output=True,
                text=True,
            )
        else:
            # No tags exist, get all changed files in recent commits
            result = subprocess.run(
                ['git', 'log', '--name-status', '--oneline', '--no-merges', '-20'],
                capture_output=True,
                text=True,
            )

        return result.stdout.strip()
    except Exception as e:
        print(f'Error getting changed files: {e}')
        return ''


def read_changelog_full():
    """Read current full CHANGELOG.md content"""
    changelog_path = Path('CHANGELOG.md')
    if not changelog_path.exists():
        return '# Changelog\n'

    with open(changelog_path) as f:
        return f.read()


def get_changelog_for_prompt(content):
    """Get truncated changelog (last 2 releases only) for Claude prompt to avoid context overflow"""
    if not content or content.strip() == '# Changelog':
        return content

    # Extract only the header and last 2 release blocks to keep prompt size reasonable
    lines = content.split('\n')
    if len(lines) <= 10:
        return content

    # Find release headings (lines starting with ## v)
    release_indices = [i for i, line in enumerate(lines) if line.startswith('## v')]

    if len(release_indices) <= 2:
        return content

    # Return header + last 2 releases
    header_end = release_indices[0] if release_indices else 1
    last_two_start = release_indices[-2]

    header = '\n'.join(lines[:header_end])
    last_two = '\n'.join(lines[last_two_start:])

    return f'{header}\n...\n(showing last 2 releases)\n\n{last_two}'


def main():
    # Check for required environment variable
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print('Error: ANTHROPIC_API_KEY environment variable not set')
        sys.exit(1)

    # Get current version
    current_version = get_current_version()
    print(f'Current version: {current_version}')

    # Get recent commits and changes
    recent_commits = get_recent_commits()
    if not recent_commits:
        print('No new commits since last release. Skipping update.')
        sys.exit(0)

    changed_files = get_changed_files()
    current_changelog_full = read_changelog_full()
    current_changelog_for_prompt = get_changelog_for_prompt(current_changelog_full)

    # Get current date for the release (use UTC for deterministic timestamps)
    now = datetime.now(UTC)

    # Create the prompt for Claude
    prompt = f"""You are an expert at maintaining changelogs and versioning software following strict guidelines.

**CRITICAL INSTRUCTIONS:**
You MUST follow the changelog guidelines EXACTLY as specified. There must be ZERO deviation from these rules.
Reference: https://github.com/Vaquum/dev-docs/blob/main/src/Updating-Changelog.md

Here are the STRICT guidelines you MUST follow:

{CHANGELOG_GUIDELINES}

**YOUR TASK:**

1. Analyze the commits and changed files below
2. Determine the appropriate version bump (MAJOR, MINOR, or PATCH) following the deterministic rules
3. Generate the new CHANGELOG.md entry following ALL the rules above
4. Provide the new version number

**CURRENT VERSION:** {current_version}

**CURRENT DATE:** {now.strftime('%B')} {now.day}, {now.year}

**RECENT COMMITS:**
{recent_commits}

**CHANGED FILES:**
{changed_files}

**CURRENT CHANGELOG.md:**
{current_changelog_for_prompt}

**OUTPUT FORMAT:**
Provide your response in the following exact format:

NEW_VERSION: X.Y.Z
CHANGELOG_ENTRY:
[The complete new changelog entry to append, starting with ## vX.Y.Z and including all bullets]

IMPORTANT REMINDERS:
- Follow the date formatting rules EXACTLY (e.g., "8th of February" not "08th of February")
- Use imperative verbs (Add, Fix, etc.) not past tense
- Order bullets by category as specified
- Include exactly one blank line after heading and one after bullet list
- Wrap code elements in backticks
- Ensure ordinal suffixes are correct (11th, 12th, 13th vs 1st, 2nd, 3rd, 21st, etc.)
"""

    # Call Claude API
    client = anthropic.Anthropic(api_key=api_key)

    try:
        message = client.messages.create(
            model='claude-opus-4-6',
            max_tokens=4096,
            messages=[{'role': 'user', 'content': prompt}],
        )

        response_text = message.content[0].text
        print('Claude Response:')
        print(response_text)
        print('\n' + '=' * 80 + '\n')

        # Parse the response with strict validation
        lines = response_text.split('\n')
        new_version = None
        changelog_entry_lines = []
        in_changelog = False

        for line in lines:
            if line.startswith('NEW_VERSION:'):
                new_version = line.split(':', 1)[1].strip()
            elif line.startswith('CHANGELOG_ENTRY:'):
                in_changelog = True
            elif in_changelog:
                changelog_entry_lines.append(line)

        if not new_version:
            print('Error: Could not parse new version from Claude response')
            sys.exit(1)

        # Validate version is valid SemVer (no leading zeros in numeric components)
        if not re.match(r'^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$', new_version):
            print(f'Error: Invalid SemVer version: {new_version}')
            sys.exit(1)

        # Preserve internal newlines but normalize trailing blank lines:
        # remove all trailing empty/whitespace-only lines, then add exactly one.
        changelog_entry_raw = '\n'.join(changelog_entry_lines)
        changelog_lines = changelog_entry_raw.split('\n')

        while changelog_lines and changelog_lines[-1].strip() == '':
            changelog_lines.pop()

        changelog_lines.append('')
        changelog_entry = '\n'.join(changelog_lines)

        # Validate changelog entry heading matches the required format:
        # "## v<version> on D{suffix} of Month, YYYY"
        heading_line = changelog_lines[0] if changelog_lines else ''
        heading_pattern = rf'^## v{re.escape(new_version)} on ([1-9]|[12][0-9]|3[01])(st|nd|rd|th) of (January|February|March|April|May|June|July|August|September|October|November|December), \d{{4}}$'
        if not re.match(heading_pattern, heading_line):
            print(
                f'Error: Changelog heading does not match required format for version {new_version}'
            )
            print(f'Expected format: ## v{new_version} on D{{suffix}} of Month, YYYY')
            print(f'Got: {heading_line}')
            sys.exit(1)

        # Update version in pyproject.toml
        update_version_in_pyproject(new_version)

        # Update CHANGELOG.md with full content
        changelog_path = Path('CHANGELOG.md')

        # Append the new entry (changelog_entry already has exactly one trailing blank line)
        if current_changelog_full.strip() == '# Changelog':
            # First entry
            new_changelog = f'{current_changelog_full.strip()}\n\n{changelog_entry}'
        else:
            # Append to existing changelog
            new_changelog = f'{current_changelog_full.strip()}\n\n{changelog_entry}'

        with open(changelog_path, 'w') as f:
            f.write(new_changelog)

        print(f'Successfully updated CHANGELOG.md with version {new_version}')

    except Exception as e:
        print(f'Error calling Claude API: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
