"""Read-only view of Dagster through the webserver's GraphQL endpoint.

The monitor depends on this, and only on this, for Dagster facts: daemon health, the
queue depth, run failures and asset check results. An unreachable webserver is reported
as such rather than raised, so the other detectors still run in the same tick.
"""

from __future__ import annotations

import http.client
import json
import logging
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

log = logging.getLogger('origo.workers.dagster')

HEALTH_QUERY = """query Health {
  instance { daemonHealth { allDaemonStatuses { daemonType healthy required } } }
  queued: runsOrError(filter: {statuses: [QUEUED]}) { __typename ... on Runs { count } }
}"""
FAILURES_QUERY = """query Failures($after: Float!) {
  runsOrError(filter: {statuses: [FAILURE], updatedAfter: $after}, limit: 200) {
    __typename ... on Runs { results { runId jobName updateTime tags { key value } } }
  }
}"""
CHECKS_QUERY = """query Checks {
  assetNodes(loadMaterializations: false) {
    assetKey { path }
    assetChecksOrError { __typename ... on AssetChecks { checks { name } } }
  }
}"""
BACKFILLS_QUERY = """query Backfills($status: BulkActionStatus!) {
  partitionBackfillsOrError(status: $status, limit: 50) {
    __typename ... on PartitionBackfills { results { id status assetSelection { path } } }
  }
}"""
CHECK_EXECUTIONS_QUERY = """query CheckExecutions($assetKey: AssetKeyInput!, $checkName: String!) {
  assetCheckExecutions(assetKey: $assetKey, checkName: $checkName, limit: 1) {
    status evaluation { timestamp }
  }
}"""


class DagsterUnreachable(RuntimeError):
    """The webserver did not answer a query within the timeout, or answered with an error."""


@dataclass(frozen=True)
class DagsterHealth:
    reachable: bool
    unhealthy_daemons: tuple[str, ...]
    queued_runs: int


@dataclass(frozen=True)
class RunFailure:
    run_id: str
    job_name: str
    partition: str
    updated_at: float


@dataclass(frozen=True)
class CheckFailure:
    asset_key: str
    check_name: str
    timestamp: float
    """When the failed evaluation was stored, not when its run started."""


def _mapping(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise DagsterUnreachable(f'Unexpected Dagster response: {what} is not an object.')
    return cast(dict[str, object], value)


def _sequence(value: object, what: str) -> list[object]:
    if not isinstance(value, list):
        raise DagsterUnreachable(f'Unexpected Dagster response: {what} is not a list.')
    return cast(list[object], value)


class DagsterReader:
    def __init__(self, base_url: str, *, timeout_seconds: float = 10.0) -> None:
        self.base_url = base_url.rstrip('/')
        self.timeout_seconds = timeout_seconds

    def query(
        self, operation: str, query: str, variables: Mapping[str, object] | None = None
    ) -> dict[str, object]:
        request = urllib.request.Request(
            self.base_url + '/graphql',
            data=json.dumps(
                {'operationName': operation, 'query': query, 'variables': dict(variables or {})}
            ).encode(),
            method='POST',
            headers={'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                document: object = json.loads(response.read())
        except urllib.error.HTTPError as error:
            raise DagsterUnreachable(f'{operation}: HTTP {error.code}') from error
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            TimeoutError,
            OSError,
            ValueError,
        ) as error:
            raise DagsterUnreachable(f'{operation}: {error}') from error
        body = _mapping(document, 'body')
        if body.get('errors'):
            raise DagsterUnreachable(f'{operation}: {json.dumps(body["errors"])[:500]}')
        return _mapping(body.get('data'), 'data')

    def health(self) -> DagsterHealth:
        try:
            data = self.query('Health', HEALTH_QUERY)
        except DagsterUnreachable as error:
            log.error('dagster unreachable: %s', error)
            return DagsterHealth(False, (), 0)
        statuses = _sequence(
            _mapping(_mapping(data.get('instance'), 'instance').get('daemonHealth'), 'health').get(
                'allDaemonStatuses'
            ),
            'daemons',
        )
        unhealthy = tuple(
            str(entry['daemonType'])
            for entry in (_mapping(item, 'daemon') for item in statuses)
            if entry.get('required') and not entry.get('healthy')
        )
        queued = _mapping(data.get('queued'), 'queued')
        count = queued.get('count')
        if queued.get('__typename') != 'Runs' or not isinstance(count, int):
            raise DagsterUnreachable('Health: queued runs were not counted.')
        return DagsterHealth(True, unhealthy, count)

    def failures_since(self, since: float) -> list[RunFailure]:
        data = self.query('Failures', FAILURES_QUERY, {'after': since})
        runs = _mapping(data.get('runsOrError'), 'runs')
        if runs.get('__typename') != 'Runs':
            raise DagsterUnreachable('Failures: runs were not listed.')
        failures: list[RunFailure] = []
        for item in _sequence(runs.get('results'), 'results'):
            run = _mapping(item, 'run')
            tags = {
                str(_mapping(tag, 'tag')['key']): str(_mapping(tag, 'tag')['value'])
                for tag in _sequence(run.get('tags'), 'tags')
            }
            updated = run.get('updateTime')
            failures.append(
                RunFailure(
                    str(run['runId']),
                    str(run['jobName']),
                    tags.get('dagster/partition', ''),
                    float(updated) if isinstance(updated, (int, float)) else 0.0,
                )
            )
        return failures

    def backfill_in_flight(self, asset_key: str) -> bool:
        """Whether a native backfill that selects ``asset_key`` is requested or being cancelled."""
        for status in ('REQUESTED', 'CANCELING'):
            data = self.query('Backfills', BACKFILLS_QUERY, {'status': status})
            backfills = _mapping(data.get('partitionBackfillsOrError'), 'backfills')
            if backfills.get('__typename') != 'PartitionBackfills':
                raise DagsterUnreachable('Backfills: backfills were not listed.')
            for item in _sequence(backfills.get('results'), 'results'):
                entry = _mapping(item, 'backfill')
                selection = entry.get('assetSelection')
                keys = {
                    '/'.join(str(part) for part in _sequence(_mapping(key, 'key').get('path'), 'path'))
                    for key in (_sequence(selection, 'selection') if selection is not None else [])
                }
                if asset_key in keys:
                    return True
        return False

    def failed_checks_since(self, since: float, *, exclude_asset: str = '') -> list[CheckFailure]:
        data = self.query('Checks', CHECKS_QUERY)
        failed: list[CheckFailure] = []
        for item in _sequence(data.get('assetNodes'), 'assetNodes'):
            node = _mapping(item, 'assetNode')
            path = [str(part) for part in _sequence(_mapping(node.get('assetKey'), 'key').get('path'), 'path')]
            asset_key = '/'.join(path)
            checks = _mapping(node.get('assetChecksOrError'), 'checks')
            if checks.get('__typename') != 'AssetChecks' or asset_key == exclude_asset:
                continue
            for entry in _sequence(checks.get('checks'), 'checks'):
                name = str(_mapping(entry, 'check')['name'])
                executions = _sequence(
                    self.query(
                        'CheckExecutions',
                        CHECK_EXECUTIONS_QUERY,
                        {'assetKey': {'path': path}, 'checkName': name},
                    ).get('assetCheckExecutions'),
                    'executions',
                )
                if not executions:
                    continue
                latest = _mapping(executions[0], 'execution')
                if latest.get('status') != 'FAILED':
                    continue
                # The execution's own timestamp is when its run started; a check evaluated
                # inside a queued or long run fails minutes after that, so the window is
                # compared with the time the evaluation was stored.
                evaluation = latest.get('evaluation')
                if not isinstance(evaluation, dict):
                    continue
                stamp = cast(dict[str, object], evaluation).get('timestamp')
                timestamp = float(stamp) if isinstance(stamp, (int, float)) else 0.0
                if timestamp > since:
                    failed.append(CheckFailure(asset_key, name, timestamp))
        return failed
