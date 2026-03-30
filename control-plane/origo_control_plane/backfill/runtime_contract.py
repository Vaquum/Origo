from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal, NoReturn, cast

import dagster as dg
from dagster import AssetExecutionContext, OpExecutionContext

ProjectionMode = Literal['inline', 'deferred']
ExecutionMode = Literal['backfill', 'reconcile']
RuntimeAuditMode = Literal['event', 'summary']
FastInsertMode = Literal['writer', 'assume_new_partition']

BACKFILL_PROJECTION_MODE_TAG: Final[str] = 'origo.backfill.projection_mode'
BACKFILL_EXECUTION_MODE_TAG: Final[str] = 'origo.backfill.execution_mode'
BACKFILL_RUNTIME_AUDIT_MODE_TAG: Final[str] = 'origo.backfill.runtime_audit_mode'
BACKFILL_HEIGHT_START_TAG: Final[str] = 'origo.backfill.height_start'
BACKFILL_HEIGHT_END_TAG: Final[str] = 'origo.backfill.height_end'
BACKFILL_PARTITION_IDS_TAG: Final[str] = 'origo.backfill.partition_ids'
_RUNTIME_AUDIT_MODE_ENV: Final[str] = 'ORIGO_CANONICAL_RUNTIME_AUDIT_MODE'
_HELPER_WRITE_PATH_FAILURE_SUFFIX: Final[str] = (
    'Dagster/Dagit is the sole allowed creator of canonical backfill/reconcile '
    'write runs. Use Dagit before Slice 35, or a Dagster-native schedule/sensor '
    'after Slice 35.'
)
Field: Any = getattr(dg, 'Field')
DagsterExecutionContext = AssetExecutionContext | OpExecutionContext


@dataclass(frozen=True)
class BackfillRuntimeContract:
    projection_mode: ProjectionMode
    execution_mode: ExecutionMode
    runtime_audit_mode: RuntimeAuditMode


@dataclass(frozen=True)
class BackfillHeightWindow:
    start_height: int
    end_height: int


def raise_forbidden_helper_write_path_or_raise(*, helper_name: str) -> NoReturn:
    raise RuntimeError(
        f'{helper_name} is a historical helper surface only. '
        f'{_HELPER_WRITE_PATH_FAILURE_SUFFIX}'
    )


def _require_run_tag_or_raise(
    context: DagsterExecutionContext, *, tag_name: str
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


def _optional_tag_value_or_none(
    tags: Mapping[str, str], *, tag_name: str
) -> str | None:
    value = tags.get(tag_name)
    if value is None:
        return None
    normalized = value.strip()
    return None if normalized == '' else normalized.lower()


def _extract_op_config_or_none(context: object) -> Mapping[str, object] | None:
    op_config = getattr(context, 'op_config', None)
    if isinstance(op_config, Mapping):
        return cast(Mapping[str, object], op_config)
    op_execution_context = getattr(context, 'op_execution_context', None)
    nested_op_config = getattr(op_execution_context, 'op_config', None)
    if isinstance(nested_op_config, Mapping):
        return cast(Mapping[str, object], nested_op_config)
    return None


def _load_runtime_contract_from_config_or_raise(
    op_config: Mapping[str, object],
) -> BackfillRuntimeContract:
    config_values: dict[str, str] = {}
    for config_key, tag_name in (
        ('projection_mode', BACKFILL_PROJECTION_MODE_TAG),
        ('execution_mode', BACKFILL_EXECUTION_MODE_TAG),
        ('runtime_audit_mode', BACKFILL_RUNTIME_AUDIT_MODE_TAG),
    ):
        raw_value = op_config.get(config_key)
        if not isinstance(raw_value, str) or raw_value.strip() == '':
            raise RuntimeError(
                f'Launch config field {config_key!r} must be a non-empty string '
                'when Dagster run tags are not provided'
            )
        config_values[tag_name] = raw_value.strip().lower()
    return load_backfill_runtime_contract_from_tags_or_raise(config_values)


def build_backfill_runtime_config_schema(
    *,
    default_projection_mode: ProjectionMode,
) -> dict[str, Any]:
    return {
        'projection_mode': Field(
            str,
            is_required=False,
            default_value=default_projection_mode,
            description='Backfill projection mode: inline or deferred.',
        ),
        'execution_mode': Field(
            str,
            is_required=False,
            default_value='backfill',
            description='Backfill execution mode: backfill or reconcile.',
        ),
        'runtime_audit_mode': Field(
            str,
            is_required=False,
            default_value='summary',
            description='Runtime audit emission mode: event or summary.',
        ),
    }


def build_backfill_height_window_config_schema(
    *,
    default_projection_mode: ProjectionMode,
) -> dict[str, Any]:
    return {
        **build_backfill_runtime_config_schema(
            default_projection_mode=default_projection_mode,
        ),
        'height_start': Field(
            int,
            is_required=True,
            description='Inclusive start height for the backfill run.',
        ),
        'height_end': Field(
            int,
            is_required=True,
            description='Inclusive end height for the backfill run.',
        ),
    }


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
    context: DagsterExecutionContext,
) -> BackfillRuntimeContract:
    tag_values = {
        tag_name: _optional_tag_value_or_none(context.run.tags, tag_name=tag_name)
        for tag_name in (
            BACKFILL_PROJECTION_MODE_TAG,
            BACKFILL_EXECUTION_MODE_TAG,
            BACKFILL_RUNTIME_AUDIT_MODE_TAG,
        )
    }
    if any(value is not None for value in tag_values.values()):
        return load_backfill_runtime_contract_from_tags_or_raise(
            context.run.tags,
        )
    op_config = _extract_op_config_or_none(context)
    if op_config is None:
        return load_backfill_runtime_contract_from_tags_or_raise(
            context.run.tags,
        )
    return _load_runtime_contract_from_config_or_raise(op_config)


def apply_runtime_audit_mode_or_raise(*, runtime_audit_mode: RuntimeAuditMode) -> None:
    if runtime_audit_mode not in {'event', 'summary'}:
        raise RuntimeError(
            f'runtime_audit_mode must be one of [event, summary], got={runtime_audit_mode!r}'
        )
    os.environ[_RUNTIME_AUDIT_MODE_ENV] = runtime_audit_mode


def load_backfill_height_window_or_raise(
    context: DagsterExecutionContext,
) -> BackfillHeightWindow:
    start_raw = _optional_tag_value_or_none(
        context.run.tags,
        tag_name=BACKFILL_HEIGHT_START_TAG,
    )
    end_raw = _optional_tag_value_or_none(
        context.run.tags,
        tag_name=BACKFILL_HEIGHT_END_TAG,
    )
    if start_raw is None and end_raw is None:
        op_config = _extract_op_config_or_none(context)
        if op_config is not None:
            raw_start_value = op_config.get('height_start')
            raw_end_value = op_config.get('height_end')
            if not isinstance(raw_start_value, int):
                raise RuntimeError('Launch config field "height_start" must be int')
            if not isinstance(raw_end_value, int):
                raise RuntimeError('Launch config field "height_end" must be int')
            start_height = raw_start_value
            end_height = raw_end_value
            if start_height < 0:
                raise RuntimeError('height_start must be >= 0')
            if end_height < 0:
                raise RuntimeError('height_end must be >= 0')
            if end_height < start_height:
                raise RuntimeError('height_end must be >= height_start')
            return BackfillHeightWindow(
                start_height=start_height,
                end_height=end_height,
            )
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


def default_exchange_backfill_runtime_tags() -> dict[str, str]:
    return {
        BACKFILL_PROJECTION_MODE_TAG: 'deferred',
        BACKFILL_EXECUTION_MODE_TAG: 'backfill',
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: 'summary',
    }


def default_exchange_live_runtime_tags() -> dict[str, str]:
    return {
        BACKFILL_PROJECTION_MODE_TAG: 'inline',
        BACKFILL_EXECUTION_MODE_TAG: 'backfill',
        BACKFILL_RUNTIME_AUDIT_MODE_TAG: 'summary',
    }
