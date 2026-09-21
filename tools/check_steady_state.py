"""Steady-state policy, read-only evidence capture and offline verification (S439).

    python tools/check_steady_state.py policy --format json
    python tools/check_steady_state.py capture --read-only --output evidence/steady-state --format json
    python tools/check_steady_state.py verify --evidence evidence/steady-state --profile production --format json

``policy`` prints the code-owned SS-01..SS-12 bounds and the fixed inventory with their
SHA256 digests and touches no runtime. ``capture`` records one bounded sample for the
current UTC minute from the existing fact stores through the standard read environment;
it never creates schemas, launches jobs or alerts, and ``--read-only`` is mandatory.
``verify`` evaluates a sealed bundle offline and exits 0 for PASS, 1 for FAIL and 2 for
UNKNOWN, invalid or incomplete evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from origo.steady_state.policy import load_inventory, load_policy, policy_document
from origo.steady_state.verification import (
    EvidenceError,
    exit_code,
    report_document,
    verify_bundle,
)


def _emit(document: dict[str, object], fmt: str) -> None:
    if fmt != 'json':
        raise SystemExit(f'Unsupported format {fmt!r}; only json is defined.')
    print(json.dumps(document, indent=2, sort_keys=True))


def _policy(args: argparse.Namespace) -> int:
    _emit(policy_document(load_policy(), load_inventory()), str(args.format))
    return 0


def capture_cost(sample: dict[str, object]) -> dict[str, object]:
    cost = sample.get('capture')
    return dict(cost) if isinstance(cost, dict) else {}


def sample_summary(sample: dict[str, object]) -> dict[str, object]:
    """Probe statuses of one sample for stdout; the evidence itself stays in the bundle."""
    statuses: dict[str, object] = {}
    for section in ('sources', 'consumers', 'depth', 'workers'):
        probes = sample.get(section)
        if isinstance(probes, dict):
            statuses[section] = {
                str(name): str(probe.get('status')) if isinstance(probe, dict) else 'unknown'
                for name, probe in dict(probes).items()
            }
    for section in ('receipts', 'container_log', 'blocking', 'dagster', 'monitor', 'resources'):
        probe = sample.get(section)
        statuses[section] = str(probe.get('status')) if isinstance(probe, dict) else 'unknown'
    return {
        'bucket': sample['bucket'],
        'sample_id': sample['sample_id'],
        'identity_sha256': sample['identity_sha256'],
        'environment': sample['environment'],
        'statuses': statuses,
        'capture': capture_cost(sample),
    }


def _capture(args: argparse.Namespace) -> int:
    if not args.read_only:
        raise SystemExit('capture requires --read-only; the observer has no other mode.')
    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.steady_state.capture import CaptureConfig, EvidenceWriter, capture_sample

    environment = str(args.environment or os.environ.get('ORIGO_ENVIRONMENT') or '')
    if environment not in ('production', 'isolated'):
        raise SystemExit(
            'capture needs --environment production|isolated (or ORIGO_ENVIRONMENT) so the '
            'evidence is labelled truthfully.'
        )
    config = CaptureConfig.from_environ(
        os.environ, environment=environment, remote_probes=not args.no_remote
    )
    policy, inventory = load_policy(), load_inventory()
    writer = EvidenceWriter(Path(args.output))
    summaries: list[dict[str, object]] = []
    remaining = max(1, int(args.loop_minutes or 1))
    while remaining:
        client = make_clickhouse_client(get_clickhouse_settings())
        try:
            sample = capture_sample(
                config, client=client, writer=writer, policy=policy, inventory=inventory
            )
        finally:
            client.disconnect()
        summaries.append(sample_summary(sample))
        remaining -= 1
        if remaining:
            elapsed = float(str(capture_cost(sample).get('elapsed_seconds', 0)))
            time.sleep(max(0.0, policy.sample_period_seconds - elapsed))
    _emit(
        {
            'output': str(Path(args.output).resolve()),
            'captured_at': datetime.now(UTC).isoformat(),
            'samples': summaries,
        },
        str(args.format),
    )
    return 0


def _verify(args: argparse.Namespace) -> int:
    profile = str(args.profile)
    if profile not in ('production', 'isolated'):
        raise SystemExit('verify --profile must be production or isolated.')
    try:
        report, denominators = verify_bundle(
            Path(args.evidence),
            profile='production' if profile == 'production' else 'isolated',
            policy=load_policy(),
            inventory=load_inventory(),
        )
    except EvidenceError as error:
        _emit({'verdict': 'UNKNOWN', 'error': str(error), 'profile': profile}, str(args.format))
        return exit_code('UNKNOWN')
    _emit(report_document(report, denominators), str(args.format))
    return exit_code(report.verdict)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='check_steady_state.py', description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    policy = commands.add_parser('policy', help='print the SS-01..SS-12 policy and inventory')
    policy.add_argument('--format', default='json')
    policy.set_defaults(handler=_policy)
    capture = commands.add_parser('capture', help='record one bounded read-only sample')
    capture.add_argument('--read-only', action='store_true')
    capture.add_argument('--output', required=True)
    capture.add_argument('--format', default='json')
    capture.add_argument('--environment', choices=('production', 'isolated'))
    capture.add_argument('--loop-minutes', type=int, default=1, help='consecutive samples')
    capture.add_argument('--no-remote', action='store_true', help='skip Hugging Face probes')
    capture.set_defaults(handler=_capture)
    verify = commands.add_parser('verify', help='evaluate a sealed evidence bundle offline')
    verify.add_argument('--evidence', required=True)
    verify.add_argument('--profile', required=True, choices=('production', 'isolated'))
    verify.add_argument('--format', default='json')
    verify.set_defaults(handler=_verify)
    args = parser.parse_args(argv)
    handler = args.handler
    return int(handler(args))


if __name__ == '__main__':
    raise SystemExit(main())
