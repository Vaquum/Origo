"""Expose the existing gRPC worker's liveness to native Dagster monitoring."""

import json
import socket
from collections.abc import Mapping
from typing import cast

from dagster import DagsterRun
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.launcher.base import CheckRunHealthResult, LaunchRunContext, WorkerStatus
from dagster._core.launcher.default_run_launcher import DefaultRunLauncher
from dagster._core.storage.tags import GRPC_INFO_TAG
from dagster._grpc.client import DagsterGrpcClient
from dagster._grpc.types import GetCurrentRunsResult
from dagster._serdes import deserialize_value

from .policy import CLAIM_TAG, REDUNDANT_TAG, WORKER_TAG, admission_lock, advance_frontier


class OrigoRunLauncher(DefaultRunLauncher):
    def launch_run(self, context: LaunchRunContext) -> None:
        # Claim admission under the same lock as recovery, then release it before
        # the RPC so native threaded launching remains concurrent.
        with admission_lock(self._instance):
            run = self._instance.get_run_by_id(context.dagster_run.run_id)
            if run is None:
                raise ValueError('Run disappeared before launch.')
            if run.tags.get(REDUNDANT_TAG) == run.run_id:
                self._instance.report_run_canceled(
                    run, message='Redundant request canceled before worker launch.'
                )
                return
            self._instance.add_run_tags(
                run.run_id,
                {
                    WORKER_TAG: socket.gethostname(),
                    CLAIM_TAG: run.run_id,
                },
            )
            advance_frontier(self._instance, run)
        super().launch_run(context)

    @property
    def supports_check_run_worker_health(self) -> bool:
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        encoded = run.tags.get(GRPC_INFO_TAG)
        if encoded is None:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, 'Run has no gRPC worker address.')
        info: object = json.loads(encoded)
        if not isinstance(info, Mapping):
            raise ValueError('Worker address must be a mapping.')
        address = cast(Mapping[str, object], info)
        host, port, path = address.get('host'), address.get('port'), address.get('socket')
        if not isinstance(host, str):
            raise ValueError('Worker address must have a host.')
        if port is not None and not isinstance(port, int):
            raise ValueError('Worker port must be an integer.')
        if path is not None and not isinstance(path, str):
            raise ValueError('Worker socket must be a path.')
        client = DagsterGrpcClient(
            host=host, port=port, socket=path, use_ssl=bool(address.get('use_ssl'))
        )
        try:
            result = deserialize_value(client.get_current_runs(), GetCurrentRunsResult)
        except DagsterUserCodeUnreachableError as error:
            # A network failure is not evidence of worker death. Deployment recovery
            # separately receives the identities of containers confirmed retired.
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, str(error), transient=True)
        if result.serializable_error_info is not None:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN, result.serializable_error_info.message
            )
        if run.run_id in result.current_runs:
            return CheckRunHealthResult(WorkerStatus.RUNNING)
        return CheckRunHealthResult(WorkerStatus.NOT_FOUND, 'Worker server confirms run is absent.')
