"""Validate measured migration/replay evidence; missing proof never passes."""

import argparse
import json
from pathlib import Path

from origo.maintenance.capacity import CapacityEvidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    evidence = CapacityEvidence.model_validate_json(args.evidence.read_bytes())
    violations = evidence.violations()
    print(json.dumps({'evidence': evidence.model_dump(), 'violations': violations}, indent=2))
    return int(bool(violations))


if __name__ == '__main__':
    raise SystemExit(main())
