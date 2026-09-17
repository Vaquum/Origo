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
BACKFILLS_QUERY = """query Backfills {
  partitionBackfillsOrError(limit: 50) {
    __typename ... on PartitionBackfills { results { id status timestamp assetSelection { path } } }
  }
}"""
BACKFILL_RUNS_QUERY = """query BackfillRuns($job: String!, $tags: [ExecutionTag!]!) {
  byJob: runsOrError(filter: {pipelineName: $job}, limit: 1) {
    __typename ... on Runs { results { runId status creationTime jobName tags { key value } } }
  }
  byTags: runsOrError(filter: {tags: $tags}, limit: 1) {
    __typename ... on Runs { results { runId status creationTime jobName tags { key value } } }
  }
  active: runsOrError(filter: {statuses: [QUEUED, NOT_STARTED, STARTING, STARTED, CANCELING]}, limit: 200) {
    __typename ... on Runs {
      results { runId status creationTime jobName tags { key value } assetSelection { path } }
    }
  }
}"""
BACKFILL_ID_TAG = 'dagster/backfill'
_ACTIVE_BACKFILL_STATUSES = ('REQUESTED', 'CANCELING', 'FAILING')
_COMPLETED_BACKFILL_STATUSES = ('COMPLETED_SUCCESS', 'COMPLETED')
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
class Backfill:
    backfill_id: str
    status: str
    timestamp: float
    assets: tuple[str, ...]


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    status: str
    created_at: float
    job_name: str
    tags: dict[str, str]
    assets: tuple[str, ...]


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


def _asset_keys(selection: object) -> tuple[str, ...]:
    if selection is None:
        return ()
    return tuple(
        '/'.join(str(part) for part in _sequence(_mapping(key, 'key').get('path'), 'path'))
        for key in _sequence(selection, 'selection')
    )


def _runs(value: object, what: str) -> list[RunRecord]:
    listed = _mapping(value, what)
    if listed.get('__typename') != 'Runs':
        raise DagsterUnreachable(f'{what}: runs were not listed.')
    records: list[RunRecord] = []
    for item in _sequence(listed.get('results'), 'results'):
        run = _mapping(item, 'run')
        created = run.get('creationTime')
        records.append(
            RunRecord(
                str(run['runId']),
                str(run['status']),
                float(created) if isinstance(created, (int, float)) else 0.0,
                str(run.get('jobName', '')),
                {
                    str(_mapping(tag, 'tag')['key']): str(_mapping(tag, 'tag')['value'])
                    for tag in _sequence(run.get('tags'), 'tags')
                },
                _asset_keys(run.get('assetSelection')),
            )
        )
    return records


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

    def _backfills(self, asset_key: str) -> list[Backfill]:
        """Native backfills selecting ``asset_key``, newest first."""
        data = self.query('Backfills', BACKFILLS_QUERY)
        listed = _mapping(data.get('partitionBackfillsOrError'), 'backfills')
        if listed.get('__typename') != 'PartitionBackfills':
            raise DagsterUnreachable('Backfills: backfills were not listed.')
        found: list[Backfill] = []
        for item in _sequence(listed.get('results'), 'results'):
            entry = _mapping(item, 'backfill')
            assets = _asset_keys(entry.get('assetSelection'))
            if asset_key not in assets:
                continue
            stamp = entry.get('timestamp')
            found.append(
                Backfill(
                    str(entry['id']),
                    str(entry['status']),
                    float(stamp) if isinstance(stamp, (int, float)) else 0.0,
                    assets,
                )
            )
        return sorted(found, key=lambda backfill: backfill.timestamp, reverse=True)

    def backfill_owns_publication(self, source_key: str) -> bool:
        """The rule of ``origo.sources.prepare.backfill_owns_publication`` read through
        GraphQL: an active native backfill or backfill run owns publication, and so does the
        latest one until a later selection completes, so a failed or cancelled backfill never
        publishes a partial canonical state."""
        asset_key = f'build_{source_key}_canonical_revision_origo'
        canonical_job = f'refresh_{source_key}_canonical_source_job'
        backfill_job = f'backfill_{source_key}_source_job'
        backfills = self._backfills(asset_key)
        if any(backfill.status in _ACTIVE_BACKFILL_STATUSES for backfill in backfills):
            return True
        data = self.query(
            'BackfillRuns',
            BACKFILL_RUNS_QUERY,
            {
                'job': backfill_job,
                'tags': [
                    {'key': 'origo_source_key', 'value': source_key},
                    {'key': 'origo_source_operation', 'value': 'backfill'},
                ],
            },
        )
        runs = {name: _runs(data.get(name), name) for name in ('byJob', 'byTags', 'active')}

        def is_source_backfill(run: RunRecord) -> bool:
            return run.job_name == backfill_job or (
                (run.job_name == canonical_job or asset_key in run.assets)
                and run.tags.get('origo_source_reconciliation') != 'true'
                and (
                    run.job_name != canonical_job
                    or 'dagster/asset_partition_range_start' in run.tags
                    or run.tags.get('origo_source_operation') == 'backfill'
                )
            )

        if any(is_source_backfill(run) for run in runs['active']):
            return True
        latest = max(
            [*runs['byJob'], *runs['byTags']], key=lambda run: run.created_at, default=None
        )
        native = backfills[0] if backfills else None
        if native is not None and (
            latest is None
            or latest.tags.get(BACKFILL_ID_TAG) == native.backfill_id
            or native.timestamp > latest.created_at
        ):
            return native.status not in _COMPLETED_BACKFILL_STATUSES
        return latest is not None and latest.status != 'SUCCESS'

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
