"""Deployment verification: the maintenance run this deploy launched succeeded and every
enabled provisional source built a fresh minute after the deploy. It only reads."""

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
from dagster import DagsterInstance, DagsterRunStatus

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.render_book import RenderBookProvisional, object_mapping
from origo.sources.contracts import Client, identifier
from origo.sources.profiles.top20_arrow import ARROW_MAX_BYTES, current_input_token
from origo.sources.publication import file_sha256_matches
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


def _render_conditions(client: Client, database: str, keys: Sequence[str], now: datetime) -> list[str]:
    now = now.astimezone(UTC)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=2)
    parameters = {'start': start.replace(tzinfo=None), 'end': end.replace(tzinfo=None),
                  'date': start.date(), 'last_date': end.date(),
                  'prefix': start.strftime('%Y-%m-%dT%H')}
    unmet: list[str] = []
    for key in keys:
        def current(component: str, fields: str) -> str:
            return ' UNION ALL '.join(
                f"""SELECT {fields} FROM {identifier(database)}.{identifier(key + '_' + component + suffix + '_revisions')} d
                INNER JOIN (SELECT partition_key,revision,build_id
                    FROM {identifier(database)}.source_current_partitions
                    WHERE source_key=%(source)s AND partition_key >= %(prefix)s
                      AND provisional={provisional}) a
                  ON d.partition_key=a.partition_key AND d.revision=a.revision AND d.build_id=a.build_id
                WHERE source_date >= %(date)s AND source_date <= %(last_date)s
                  AND datetime >= %(start)s AND datetime < %(end)s"""
                for provisional, suffix in ((0, ''), (1, '_latest'))
            )

        parameters['source'] = key
        rows = client.execute(
            'SELECT toStartOfMinute(datetime) AS minute,count(),uniqExact(datetime),'
            'min(exchange_time_us),max(exchange_time_us) FROM ('
            + current('depth20', 'datetime,exchange_time_us')
            + ') GROUP BY minute ORDER BY minute DESC', parameters,
            settings={'readonly': 1, 'max_threads': 1, 'max_execution_time': 10},
        )
        complete = [row for row in rows if row[1:3] == (60, 60)
                    and int(str(row[3])) >= int(cast(datetime, row[0]).replace(tzinfo=UTC).timestamp()) * 1_000_000
                    and int(str(row[4])) < (int(cast(datetime, row[0]).replace(tzinfo=UTC).timestamp()) + 60) * 1_000_000]
        metrics = client.execute(current('depth20_1m', 'datetime,source_timestamp_ms'), parameters,
                                 settings={'readonly': 1, 'max_threads': 1, 'max_execution_time': 10})
        ready = [row for row in complete if (row[0], int(str(row[4])) // 1000) in metrics]
        if not ready:
            unmet.append(f'source {key} has no complete current 60-second minute with its metric')
        if key != 'binance_spot_top20_v1':
            continue
        target = Path(os.environ.get('LOCAL_ARROW_DIR', '/opt/arrow')) / key / 'latest.arrow'
        manifest = Path(os.environ.get('ORIGO_SOURCE_PUBLICATION_ROOT', '/opt/origo/shadow')) / key / 'arrow/latest.json'
        try:
            document = object_mapping(json.loads(manifest.read_bytes()))
            digest = str(document.get('sha256'))
            if (document.get('source_key') != key or document.get('depth') != 20
                    or document.get('files') != [{'path': str(target), 'sha256': digest}]
                    or not file_sha256_matches(target, digest, ARROW_MAX_BYTES)):
                raise ValueError('committed Arrow file does not match its manifest')
            if document.get('input_token') != current_input_token(
                client, database, int(str(document['window_start_us'])), int(str(document['window_end_us'])),
            ):
                raise ValueError('committed Arrow inputs differ from the selected window')
            frame = pl.read_ipc(target, columns=['exchange_time_us', 'observed_at_us'])
            minute = int(cast(datetime, ready[0][0]).replace(tzinfo=UTC).timestamp()) if ready else 0
            published = frame.filter(pl.col('exchange_time_us').is_between(
                minute * 1_000_000, (minute + 60) * 1_000_000, closed='left'))
            if (not 0 < frame.height <= 1800 or document.get('rows') != frame.height
                    or published.height != 60
                    or (published['exchange_time_us'] // 1_000_000).n_unique() != 60
                    or published['exchange_time_us'].max() != ready[0][4]):
                raise ValueError('committed Arrow does not contain the current complete minute')
        except (OSError, ValueError, KeyError, pl.exceptions.PolarsError) as error:
            unmet.append(f'source {key} Arrow is not ready: {error}')
    return unmet


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
    render_keys = tuple(spec.key for spec in selected_specs({}) if isinstance(spec.provisional, RenderBookProvisional))
    deadline = clock() + deadline_seconds
    while True:
        run_unmet, run_final = _run_condition(instance, run_id)
        unmet = [] if run_unmet is None else [run_unmet]
        unmet.extend(_source_conditions(client, database, keys, since))
        unmet.extend(_render_conditions(client, database, render_keys, datetime.now(UTC)))
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
