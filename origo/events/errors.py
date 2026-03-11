from __future__ import annotations

from typing import Any


class OrigoEventRuntimeError(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        normalized_code = code.strip()
        if normalized_code == '':
            raise RuntimeError('code must be non-empty')
        normalized_message = message.strip()
        if normalized_message == '':
            raise RuntimeError('message must be non-empty')
        super().__init__(f'[{normalized_code}] {normalized_message}')
        self.code = normalized_code
        self.message = normalized_message
        self.context = context or {}


class EventWriterError(OrigoEventRuntimeError):
    pass


class ProjectorRuntimeError(OrigoEventRuntimeError):
    pass


class ReconciliationError(OrigoEventRuntimeError):
    pass


class StreamQuarantineError(OrigoEventRuntimeError):
    pass
