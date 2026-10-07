"""Deployment verification: the maintenance run this deploy launched succeeded and every
enabled provisional source built a fresh minute after the deploy. It only reads."""

import argparse
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

from dagster import DagsterInstance, DagsterRunStatus

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.contracts import Client, identifier
from origo.workers.provisional import ProvisionalFeed, selected_specs
from origo.workers.receipts import WORKER_MINUTE_LOG

ENDED_UNSUCCESSFULLY = frozenset({DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED})


def _run_condition(instance: DagsterInstance, run_id: str) -> tuple[str | None, bool]:
    """The unmet run condition, if any, and whether it is final."""
    run = instance.get_run_by_id(run_id)
    if run is None:
        return f'maintenance run {run_id} does not exist', True
    if run.status == DagsterRunStatus.SUCCESS:
        return None, False
    if run.status in ENDED_UNSUCCESSFULLY:
        return f'maintenance run {run_id} ended {run.status.value}', True
    return f'maintenance run {run_id} is {run.status.value}, not SUCCESS', False


def _source_conditions(
    client: Client, database: str, keys: Sequence[str], since: datetime
) -> list[str]:
    last_closed = since.replace(second=0, microsecond=0) - timedelta(minutes=1)
    rows = client.execute(
        f"""SELECT DISTINCT series FROM {identifier(database)}.{WORKER_MINUTE_LOG}
        WHERE feed = %(feed)s AND status = 'OK' AND position(series, ':') = 0
          AND recorded_at >= %(since)s AND minute >= %(minute)s""",
        {
            'feed': ProvisionalFeed.name,
            'since': since.astimezone(UTC).replace(tzinfo=None),
            'minute': last_closed.astimezone(UTC).replace(tzinfo=None),
        },
    )
    fresh = {str(row[0]) for row in rows}
    return [
        f'source {key} has no OK minute receipt recorded since {since:%Y-%m-%dT%H:%M:%SZ} '
        f'for the minute {last_closed:%Y-%m-%dT%H:%MZ} or later'
        for key in keys
        if key not in fresh
    ]


def verify_deploy(
    instance: DagsterInstance,
    client: Client,
    database: str,
    *,
    run_id: str,
    since: datetime,
    deadline_seconds: float,
    poll_seconds: float = 10.0,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> list[str]:
    """Return the unmet conditions; an empty list means the deployment is verified."""
    keys = tuple(spec.key for spec in selected_specs({}))
    deadline = clock() + deadline_seconds
    while True:
        run_unmet, run_final = _run_condition(instance, run_id)
        unmet = [] if run_unmet is None else [run_unmet]
        unmet.extend(_source_conditions(client, database, keys, since))
        if not unmet or run_final or clock() >= deadline:
            return unmet
        sleep(min(poll_seconds, deadline - clock()))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='python -m origo.orchestration.verify_deploy', description=__doc__)
    parser.add_argument('--run-id', required=True, help='the maintenance run this deploy launched')
    parser.add_argument('--since', type=int, required=True, help='epoch seconds when verification started')
    parser.add_argument('--deadline-seconds', type=float, required=True)
    arguments = parser.parse_args(argv)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        with DagsterInstance.get() as instance:
            unmet = verify_deploy(
                instance,
                client,
                settings.database,
                run_id=arguments.run_id,
                since=datetime.fromtimestamp(arguments.since, UTC),
                deadline_seconds=arguments.deadline_seconds,
            )
    finally:
        client.disconnect()
    for condition in unmet:
        print(condition, file=sys.stderr)
    return 1 if unmet else 0


if __name__ == '__main__':
    sys.exit(main())
