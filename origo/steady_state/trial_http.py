"""The real Dagster HTTP application on an owned loopback endpoint for a trial."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable
from importlib import import_module
from typing import cast

import uvicorn
from dagster._core.remote_representation.code_location import GrpcServerCodeLocation
from dagster._core.workspace.context import WorkspaceProcessContext
from starlette.applications import Starlette

from .trial_native import NativeMaintenance


class NativeHTTP:
    def __init__(self, native: NativeMaintenance) -> None:
        if native.workspace is None:
            raise RuntimeError('Start the owned native code location before its HTTP application.')
        self.native = native
        factory = cast(
            Callable[[WorkspaceProcessContext], Starlette],
            getattr(
                import_module('dagster_webserver.app'), 'create_app_from_workspace_process_context'
            ),
        )
        app = factory(native.workspace)
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1', 0))
            self.port = int(reserved.getsockname()[1])
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host='127.0.0.1',
                port=self.port,
                log_level='warning',
                access_log=False,
                loop='asyncio',
            )
        )
        self.thread = threading.Thread(
            target=self.server.run, name='owned-dagster-http', daemon=True
        )

    def __enter__(self) -> NativeHTTP:
        self.thread.start()
        deadline = time.monotonic() + 20
        while not self.server.started:
            if not self.thread.is_alive() or time.monotonic() >= deadline:
                self.close()
                raise RuntimeError('The owned Dagster HTTP endpoint did not become ready.')
            time.sleep(0.05)
        return self

    def allowed_endpoints(self) -> tuple[set[int], set[str]]:
        workspace = self.native.workspace
        if workspace is None:
            raise RuntimeError('The native code location is no longer alive.')
        location = workspace.create_request_context().get_code_location('steady-state-trial')
        if not isinstance(location, GrpcServerCodeLocation):
            raise TypeError('The trial requires its actual managed gRPC code location.')
        ports, sockets = {self.port}, set()
        if location.host not in ('localhost', '127.0.0.1', '::1'):
            raise PermissionError('The native trial code server must remain on loopback.')
        if location.port is not None:
            ports.add(location.port)
        if location.socket is not None:
            sockets.add(location.socket)
        return ports, sockets

    def close(self) -> None:
        self.server.should_exit = True
        if self.thread.is_alive():
            self.thread.join(timeout=10)
        if self.thread.is_alive():
            self.server.force_exit = True
            self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError('The owned HTTP thread did not stop; trial shutdown failed.')

    def __exit__(self, *exception: object) -> None:
        self.close()
