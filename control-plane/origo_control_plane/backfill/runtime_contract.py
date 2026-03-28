from __future__ import annotations

import os
from collections.abc import Mapping
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
BACKFILL_HEIGHT_START_TAG: Final[str] = 'origo.backfill.height_start'
BACKFILL_HEIGHT_END_TAG: Final[str] = 'origo.backfill.height_end'
_RUNTIME_AUDIT_MODE_ENV: Final[str] = 'ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'


@dataclass(frozen=True)
class BackfillRuntimeContract:
    projection_mode: ProjectionMode
    execution_mode: ExecutionMode
    runtime_audit_mode: RuntimeAuditMode


@dataclass(frozen=True)
class BackfillHeightWindow:
    start_height: int
    end_height: int


def _require_run_tag_or_raise(
    context: AssetExecutionContext, *, tag_name: str
) -> str:
    value = context.run.tags.get(tag_name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{tag_name} must be set and non-empty on the Dagster run')
    return value.strip().lower()


def _require_tag_value_or_raise(
    tags: Mapping[str, str], *, tag_name: str
) -> str:
    value = tags.get(tag_name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{tag_name} must be set and non-empty on the Dagster run')
    return value.strip().lower()


def load_backfill_runtime_contract_from_tags_or_raise(
    tags: Mapping[str, str],
) -> BackfillRuntimeContract:
    projection_mode = _require_tag_value_or_raise(
        tags,
        tag_name=BACKFILL_PROJECTION_MODE_TAG,
    )
    if projection_mode not in {'inline', 'deferred'}:
        raise RuntimeError(
            f'{BACKFILL_PROJECTION_MODE_TAG} must be one of [inline, deferred], '
            f'got={projection_mode!r}'
        )

    execution_mode = _require_tag_value_or_raise(
        tags,
        tag_name=BACKFILL_EXECUTION_MODE_TAG,
    )
    if execution_mode not in {'backfill', 'reconcile'}:
        raise RuntimeError(
            f'{BACKFILL_EXECUTION_MODE_TAG} must be one of [backfill, reconcile], '
            f'got={execution_mode!r}'
        )

    runtime_audit_mode = _require_tag_value_or_raise(
        tags,
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


def load_backfill_runtime_contract_or_raise(
    context: AssetExecutionContext,
) -> BackfillRuntimeContract:
    return load_backfill_runtime_contract_from_tags_or_raise(
        context.run.tags,
    )


def apply_runtime_audit_mode_or_raise(*, runtime_audit_mode: RuntimeAuditMode) -> None:
    if runtime_audit_mode not in {'event', 'summary'}:
        raise RuntimeError(
            f'runtime_audit_mode must be one of [event, summary], got={runtime_audit_mode!r}'
        )
    os.environ[_RUNTIME_AUDIT_MODE_ENV] = runtime_audit_mode


def load_backfill_height_window_or_raise(
    context: AssetExecutionContext,
) -> BackfillHeightWindow:
    start_raw = _require_run_tag_or_raise(
        context,
        tag_name=BACKFILL_HEIGHT_START_TAG,
    )
    end_raw = _require_run_tag_or_raise(
        context,
        tag_name=BACKFILL_HEIGHT_END_TAG,
    )
    try:
        start_height = int(start_raw)
    except ValueError as exc:
        raise RuntimeError(
            f'{BACKFILL_HEIGHT_START_TAG} must be integer, got={start_raw!r}'
        ) from exc
    try:
        end_height = int(end_raw)
    except ValueError as exc:
        raise RuntimeError(
            f'{BACKFILL_HEIGHT_END_TAG} must be integer, got={end_raw!r}'
        ) from exc
    if start_height < 0:
        raise RuntimeError(f'{BACKFILL_HEIGHT_START_TAG} must be >= 0')
    if end_height < 0:
        raise RuntimeError(f'{BACKFILL_HEIGHT_END_TAG} must be >= 0')
    if end_height < start_height:
        raise RuntimeError(
            f'{BACKFILL_HEIGHT_END_TAG} must be >= {BACKFILL_HEIGHT_START_TAG}'
        )
    return BackfillHeightWindow(
        start_height=start_height,
        end_height=end_height,
    )


def default_exchange_runtime_tags() -> dict[str, str]:
    return {
        BACKFILL_PROJECTION_MODE_TAG: 'inline',
        BACKFILL_EXECUTION_MODE_TAG: 'backfill',
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: 'summary',
    }
