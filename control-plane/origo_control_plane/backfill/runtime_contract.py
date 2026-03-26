from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final, Literal, cast

from dagster import AssetExecutionContext

ProjectionMode = Literal['inline', 'deferred']
ExecutionMode = Literal['backfill', 'reconcile']
RuntimeAuditMode = Literal['event', 'summary']
FastInsertMode = Literal['writer', 'assume_new_partition']

BACKFILL_PROJECTION_MODE_TAG: Final[str] = 'origo.backfill.projection_mode'
BACKFILL_EXECUTION_MODE_TAG: Final[str] = 'origo.backfill.execution_mode'
BACKFILL_RUNTIME_AUDIT_MODE_TAG: Final[str] = 'origo.backfill.runtime_audit_mode'
_RUNTIME_AUDIT_MODE_ENV: Final[str] = 'ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'


@dataclass(frozen=True)
class BackfillRuntimeContract:
    projection_mode: ProjectionMode
    execution_mode: ExecutionMode
    runtime_audit_mode: RuntimeAuditMode


def _require_run_tag_or_raise(
    context: AssetExecutionContext, *, tag_name: str
) -> str:
    value = context.run.tags.get(tag_name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{tag_name} must be set and non-empty on the Dagster run')
    return value.strip().lower()


def load_backfill_runtime_contract_or_raise(
    context: AssetExecutionContext,
) -> BackfillRuntimeContract:
    projection_mode = _require_run_tag_or_raise(
        context,
        tag_name=BACKFILL_PROJECTION_MODE_TAG,
    )
    if projection_mode not in {'inline', 'deferred'}:
        raise RuntimeError(
            f'{BACKFILL_PROJECTION_MODE_TAG} must be one of [inline, deferred], '
            f'got={projection_mode!r}'
        )

    execution_mode = _require_run_tag_or_raise(
        context,
        tag_name=BACKFILL_EXECUTION_MODE_TAG,
    )
    if execution_mode not in {'backfill', 'reconcile'}:
        raise RuntimeError(
            f'{BACKFILL_EXECUTION_MODE_TAG} must be one of [backfill, reconcile], '
            f'got={execution_mode!r}'
        )

    runtime_audit_mode = _require_run_tag_or_raise(
        context,
        tag_name=BACKFILL_RUNTIME_AUDIT_MODE_TAG,
    )
    if runtime_audit_mode not in {'event', 'summary'}:
        raise RuntimeError(
            f'{BACKFILL_RUNTIME_AUDIT_MODE_TAG} must be one of [event, summary], '
            f'got={runtime_audit_mode!r}'
        )

    return BackfillRuntimeContract(
        projection_mode=cast(ProjectionMode, projection_mode),
        execution_mode=cast(ExecutionMode, execution_mode),
        runtime_audit_mode=cast(RuntimeAuditMode, runtime_audit_mode),
    )


def apply_runtime_audit_mode_or_raise(*, runtime_audit_mode: RuntimeAuditMode) -> None:
    if runtime_audit_mode not in {'event', 'summary'}:
        raise RuntimeError(
            f'runtime_audit_mode must be one of [event, summary], got={runtime_audit_mode!r}'
        )
    os.environ[_RUNTIME_AUDIT_MODE_ENV] = runtime_audit_mode


def default_exchange_runtime_tags() -> dict[str, str]:
    return {
        BACKFILL_PROJECTION_MODE_TAG: 'inline',
        BACKFILL_EXECUTION_MODE_TAG: 'backfill',
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: 'summary',
    }
