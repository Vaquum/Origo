from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LAUNCH_PIPELINE_EXECUTION_MUTATION = """
mutation GraphQLClientSubmitRun($executionParams: ExecutionParams!) {
  launchPipelineExecution(executionParams: $executionParams) {
    __typename
    ... on LaunchPipelineRunSuccess {
      run {
        runId
        status
      }
    }
    ... on PipelineConfigValidationInvalid {
      errors {
        message
      }
    }
    ... on PipelineNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
    ... on UnauthorizedError {
      message
    }
  }
}
"""

GET_PIPELINE_RUN_STATUS_QUERY = """
query GraphQLClientGetRunStatus($runId: ID!) {
  pipelineRunOrError(runId: $runId) {
    __typename
    ... on PipelineRun {
      runId
      status
      tags {
        key
        value
      }
    }
    ... on Run {
      creationTime
      updateTime
    }
    ... on PipelineRunNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""


class DagsterGraphQLError(RuntimeError):
    pass


class DagsterRunNotFoundError(DagsterGraphQLError):
    pass


@dataclass(frozen=True)
class DagsterRunSnapshot:
    run_id: str
    status: str
    tags: dict[str, str]
    creation_time: datetime
    update_time: datetime


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DagsterGraphQLError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise DagsterGraphQLError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise DagsterGraphQLError(f'{label} must be a list')
    return cast(list[Any], value)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _post_graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    dagster_graphql_url = _require_env('ORIGO_DAGSTER_GRAPHQL_URL')
    payload = {'query': query, 'variables': variables}
    request = Request(
        dagster_graphql_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )

    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode('utf-8')
    except HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        raise DagsterGraphQLError(f'Dagster GraphQL HTTP {exc.code}: {detail}') from exc
    except URLError as exc:
        raise DagsterGraphQLError(
            f'Dagster GraphQL connection failed: {exc.reason}'
        ) from exc

    parsed = _expect_dict(json.loads(body), 'Dagster GraphQL response')

    errors = parsed.get('errors')
    if errors is not None:
        errors_list = _expect_list(errors, 'Dagster GraphQL errors')
        if errors_list:
            raise DagsterGraphQLError(f'Dagster GraphQL errors: {errors_list}')

    return _expect_dict(parsed.get('data'), 'Dagster GraphQL data payload')


def _parse_epoch_seconds_to_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, (float, int)):
        raise DagsterGraphQLError(f'{label} must be a number')
    return datetime.fromtimestamp(float(value), tz=UTC)


def launch_export_run(
    *,
    mode: str,
    export_format: str,
    dataset: str,
    request_payload: dict[str, Any],
    source: str,
    rights_state: str,
) -> DagsterRunSnapshot:
    repository_name = _require_env('ORIGO_DAGSTER_REPOSITORY_NAME')
    repository_location_name = _require_env('ORIGO_DAGSTER_LOCATION_NAME')
    export_job_name = _require_env('ORIGO_DAGSTER_EXPORT_JOB_NAME')

    execution_params = {
        'selector': {
            'repositoryName': repository_name,
            'repositoryLocationName': repository_location_name,
            'pipelineName': export_job_name,
        },
        'runConfigData': {},
        'mode': 'default',
        'executionMetadata': {
            'tags': [
                {'key': 'origo.export.mode', 'value': mode},
                {'key': 'origo.export.format', 'value': export_format},
                {'key': 'origo.export.dataset', 'value': dataset},
                {'key': 'origo.export.source', 'value': source},
                {'key': 'origo.export.rights_state', 'value': rights_state},
                {
                    'key': 'origo.export.request_json',
                    'value': json.dumps(request_payload, separators=(',', ':')),
                },
            ]
        },
    }

    data = _post_graphql(
        query=LAUNCH_PIPELINE_EXECUTION_MUTATION,
        variables={'executionParams': execution_params},
    )
    launch_payload = _expect_dict(
        data.get('launchPipelineExecution'),
        'launchPipelineExecution response payload',
    )

    typename = launch_payload.get('__typename')
    if typename in {'LaunchPipelineRunSuccess', 'LaunchRunSuccess'}:
        run_payload = _expect_dict(
            launch_payload.get('run'), 'Dagster launch success payload run'
        )
        run_id = run_payload.get('runId')
        status = run_payload.get('status')
        if not isinstance(run_id, str) or run_id.strip() == '':
            raise DagsterGraphQLError('Dagster launch success payload missing runId')
        if not isinstance(status, str) or status.strip() == '':
            raise DagsterGraphQLError('Dagster launch success payload missing status')
        return DagsterRunSnapshot(
            run_id=run_id,
            status=status,
            tags={},
            creation_time=datetime.now(UTC),
            update_time=datetime.now(UTC),
        )

    message = launch_payload.get('message')
    if typename == 'PipelineConfigValidationInvalid':
        errors = launch_payload.get('errors')
        raise DagsterGraphQLError(
            f'Dagster launch failed ({typename}): errors={errors}'
        )
    if isinstance(message, str) and message.strip() != '':
        raise DagsterGraphQLError(f'Dagster launch failed ({typename}): {message}')
    raise DagsterGraphQLError(f'Dagster launch failed ({typename})')


def get_run_snapshot(run_id: str) -> DagsterRunSnapshot:
    data = _post_graphql(
        query=GET_PIPELINE_RUN_STATUS_QUERY,
        variables={'runId': run_id},
    )
    run_payload = _expect_dict(
        data.get('pipelineRunOrError'), 'pipelineRunOrError payload'
    )

    typename = run_payload.get('__typename')
    if typename == 'PipelineRunNotFoundError':
        message = run_payload.get('message')
        if isinstance(message, str) and message.strip() != '':
            raise DagsterRunNotFoundError(message)
        raise DagsterRunNotFoundError(f'Run not found: {run_id}')

    if typename != 'PipelineRun':
        message = run_payload.get('message')
        if isinstance(message, str) and message.strip() != '':
            raise DagsterGraphQLError(
                f'Dagster run status failed ({typename}): {message}'
            )
        raise DagsterGraphQLError(f'Dagster run status failed ({typename})')

    status = run_payload.get('status')
    if not isinstance(status, str) or status.strip() == '':
        raise DagsterGraphQLError('PipelineRun status must be a non-empty string')

    tags_payload = _expect_list(run_payload.get('tags'), 'PipelineRun tags')

    tags: dict[str, str] = {}
    for entry in tags_payload:
        entry_dict = _expect_dict(entry, 'PipelineRun tag entry')
        key = entry_dict.get('key')
        value = entry_dict.get('value')
        if not isinstance(key, str) or key.strip() == '':
            raise DagsterGraphQLError('PipelineRun tag key must be a non-empty string')
        if not isinstance(value, str):
            raise DagsterGraphQLError('PipelineRun tag value must be a string')
        tags[key] = value

    payload_run_id = run_payload.get('runId')
    if not isinstance(payload_run_id, str) or payload_run_id.strip() == '':
        raise DagsterGraphQLError('PipelineRun runId must be a non-empty string')

    creation_time = _parse_epoch_seconds_to_utc(
        run_payload.get('creationTime'), 'PipelineRun creationTime'
    )
    raw_update_time = run_payload.get('updateTime')
    if raw_update_time is None:
        update_time = creation_time
    else:
        update_time = _parse_epoch_seconds_to_utc(
            raw_update_time, 'PipelineRun updateTime'
        )

    return DagsterRunSnapshot(
        run_id=payload_run_id,
        status=status,
        tags=tags,
        creation_time=creation_time,
        update_time=update_time,
    )
