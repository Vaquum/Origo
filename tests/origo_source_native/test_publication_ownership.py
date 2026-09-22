from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Protocol, cast

import pytest
from dagster import DagsterInstance, DagsterRunStatus, JobDefinition
from dagster._core.remote_origin import (
    ManagedGrpcPythonEnvCodeLocationOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.context import WorkspaceProcessContext, WorkspaceRequestContext
from dagster._core.workspace.load_target import EmptyWorkspaceTarget
from dagster_graphql.schema import create_schema
from graphql import ExecutionResult

from origo.sources.binance_spot_aggtrades import BINANCE_SPOT_AGGTRADES_SPEC
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.bundle import build_source_bundle
from origo.workers.dagster_reader import DagsterReader, DagsterUnreachable

SOURCE = BINANCE_SPOT_TRADES_SPEC.key


class _Schema(Protocol):
    def execute(
        self,
        query: str,
        *,
        operation_name: str,
        variable_values: dict[str, object],
        context_value: WorkspaceRequestContext,
    ) -> ExecutionResult: ...


class _InstanceReader(DagsterReader):
    def __init__(self, context: WorkspaceRequestContext) -> None:
        super().__init__('http://dagit.invalid')
        self.context = context
        self.calls = 0

    def query(
        self, operation: str, query: str, variables: Mapping[str, object] | None = None
    ) -> dict[str, object]:
        self.calls += 1
        assert variables is not None and variables['limit'] == 1
        assert not variables.get('cursor')
        result = cast(_Schema, create_schema()).execute(
            query,
            operation_name=operation,
            variable_values=dict(variables),
            context_value=self.context,
        )
        assert not result.errors, result.errors
        assert isinstance(result.data, dict)
        return cast(dict[str, object], result.data)


@pytest.fixture(scope='module')
def publication_jobs() -> dict[str, JobDefinition]:
    return {
        job.name: job
        for spec in (BINANCE_SPOT_TRADES_SPEC, BINANCE_SPOT_AGGTRADES_SPEC)
        for job in build_source_bundle(spec).jobs
    }


@pytest.fixture()
def reader(tmp_path: Path) -> Iterator[_InstanceReader]:
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        with WorkspaceProcessContext(instance, EmptyWorkspaceTarget()) as workspace:
            yield _InstanceReader(workspace.create_request_context())


@pytest.mark.parametrize('status', list(DagsterRunStatus))
def test_publication_ownership_filters_job_and_nonterminal_status(
    reader: _InstanceReader,
    publication_jobs: dict[str, JobDefinition],
    status: DagsterRunStatus,
) -> None:
    instance = reader.context.instance
    job = publication_jobs[f'publish_{SOURCE}_mount_job']
    origin = RemoteJobOrigin(
        RemoteRepositoryOrigin(
            ManagedGrpcPythonEnvCodeLocationOrigin(
                LoadableTargetOrigin(module_name='origo.definitions'), location_name='origo'
            ),
            '__repository__',
        ),
        job.name,
    )
    instance.create_run_for_job(job, status=status, remote_job_origin=origin)
    # Newer terminal runs and active runs for another source or consumer do not
    # hide an older owner or claim ownership of this publication.
    instance.create_run_for_job(job, status=DagsterRunStatus.SUCCESS)
    for name in (
        f'publish_{SOURCE}_huggingface_job',
        f'publish_{BINANCE_SPOT_AGGTRADES_SPEC.key}_mount_job',
    ):
        instance.create_run_for_job(publication_jobs[name], status=DagsterRunStatus.STARTED)

    expected = status not in {
        DagsterRunStatus.SUCCESS,
        DagsterRunStatus.FAILURE,
        DagsterRunStatus.CANCELED,
        DagsterRunStatus.MANAGED,
    }
    assert reader.publication_owns_consumer(SOURCE, 'mount') is expected
    assert reader.calls == 1


def test_publication_ownership_propagates_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = DagsterReader('http://dagit.invalid')

    def unavailable(
        operation: str, query: str, variables: Mapping[str, object] | None = None
    ) -> dict[str, object]:
        raise DagsterUnreachable('Runs: HTTP 502')

    monkeypatch.setattr(reader, 'query', unavailable)
    with pytest.raises(DagsterUnreachable, match='HTTP 502'):
        reader.publication_owns_consumer(SOURCE, 'mount')
