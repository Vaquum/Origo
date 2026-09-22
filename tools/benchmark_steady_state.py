#!/usr/bin/env python3
"""Run an owned local archive-replay diagnostic or six-hour measured trial."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Direct execution must use this checkout, even without an editable installation.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture-manifest', required=True, type=Path)
    parser.add_argument('--scenario', required=True, choices=('integrated',))
    parser.add_argument('--duration-seconds', required=True, type=float)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args(argv)
    from origo.steady_state.trial import run_trial

    try:
        return run_trial(args.fixture_manifest, args.output.absolute(), args.duration_seconds)
    except Exception as error:
        print(json.dumps({'status': 'UNKNOWN', 'error': str(error)}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
