import argparse
import sys
from pathlib import Path

from .config import MigrationSettings
from .runner import MigrationRunner


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='origo-migrations',
        description='Run versioned ClickHouse SQL migrations for Origo control-plane.',
    )
    parser.add_argument(
        'command',
        choices=['status', 'migrate'],
        help='`status` shows migration state, `migrate` applies all pending migrations.',
    )
    parser.add_argument(
        '--migrations-dir',
        type=Path,
        default=None,
        help='Override migrations directory (defaults to control-plane/migrations/sql).',
    )
    return parser


def _print_status(runner: MigrationRunner) -> int:
    statuses = runner.status()
    applied = [status for status in statuses if status.state == 'applied']
    pending = [status for status in statuses if status.state == 'pending']

    print(f'Applied: {len(applied)}')
    print(f'Pending: {len(pending)}')
    for status in statuses:
        applied_at = (
            status.applied_at.isoformat(sep=' ', timespec='seconds')
            if status.applied_at
            else '-'
        )
        print(
            f'{status.version:04d} {status.state:<7} {status.name} '
            f'checksum={status.checksum} applied_at={applied_at}'
        )
    return 0


def _run_migrate(runner: MigrationRunner) -> int:
    applied = runner.migrate()
    if not applied:
        print('No pending migrations.')
        return 0

    print(f'Applied {len(applied)} migration(s):')
    for status in applied:
        print(f'{status.version:04d} {status.name}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = MigrationSettings.from_env()
    runner = MigrationRunner(settings=settings, migrations_dir=args.migrations_dir)

    try:
        if args.command == 'status':
            return _print_status(runner)
        return _run_migrate(runner)
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        return 1
    finally:
        runner.close()


if __name__ == '__main__':
    raise SystemExit(main())
