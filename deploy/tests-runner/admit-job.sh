#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import json
import os
from pathlib import Path

if os.environ['GITHUB_REPOSITORY'] != 'Vaquum/Origo':
    raise SystemExit('Runner admission rejected: unexpected repository')
if os.environ['GITHUB_JOB'] != 'pr_checks_tests':
    raise SystemExit('Runner admission rejected: unexpected job')
event = os.environ['GITHUB_EVENT_NAME']
if event == 'pull_request':
    payload = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
    if payload['pull_request']['head']['repo']['full_name'] != 'Vaquum/Origo':
        raise SystemExit('Runner admission rejected: fork pull request')
elif event != 'workflow_dispatch':
    raise SystemExit('Runner admission rejected: unexpected event')
print('Runner admission passed: Origo runtime tests')
PY
