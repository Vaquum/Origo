from __future__ import annotations

from types import SimpleNamespace

import pytest
from origo_control_plane.backfill.runtime_contract import (
    BACKFILL_HEIGHT_END_TAG,
    BACKFILL_HEIGHT_START_TAG,
    apply_runtime_audit_mode_or_raise,
    default_exchange_backfill_runtime_tags,
    default_exchange_live_runtime_tags,
    load_backfill_height_window_or_raise,
    load_backfill_runtime_contract_or_raise,
)


def _build_context(
    *,
    tags: dict[str, str],
    op_config: dict[str, object] | None = None,
) -> object:
    payload: dict[str, object] = {'run': SimpleNamespace(tags=tags)}
    if op_config is not None:
        payload['op_config'] = op_config
    return SimpleNamespace(**payload)


def test_load_backfill_runtime_contract_reads_required_tags() -> None:
    context = _build_context(
        tags={
            'origo.backfill.projection_mode': 'deferred',
            'origo.backfill.execution_mode': 'reconcile',
            'origo.backfill.runtime_audit_mode': 'summary',
        }
    )

    runtime_contract = load_backfill_runtime_contract_or_raise(context)  # type: ignore[arg-type]

    assert runtime_contract.projection_mode == 'deferred'
    assert runtime_contract.execution_mode == 'reconcile'
    assert runtime_contract.runtime_audit_mode == 'summary'


def test_load_backfill_runtime_contract_fails_loud_on_missing_tag() -> None:
    context = _build_context(
        tags={
            'origo.backfill.projection_mode': 'deferred',
            'origo.backfill.execution_mode': 'backfill',
        }
    )

    with pytest.raises(
        RuntimeError,
        match=r'origo\.backfill\.runtime_audit_mode must be set and non-empty',
    ):
        load_backfill_runtime_contract_or_raise(context)  # type: ignore[arg-type]


def test_load_backfill_runtime_contract_reads_op_config_when_tags_absent() -> None:
    context = _build_context(
        tags={},
        op_config={
            'projection_mode': 'deferred',
            'execution_mode': 'backfill',
            'runtime_audit_mode': 'summary',
        },
    )

    runtime_contract = load_backfill_runtime_contract_or_raise(context)  # type: ignore[arg-type]

    assert runtime_contract.projection_mode == 'deferred'
    assert runtime_contract.execution_mode == 'backfill'
    assert runtime_contract.runtime_audit_mode == 'summary'


def test_load_backfill_runtime_contract_prefers_tags_over_op_config() -> None:
    context = _build_context(
        tags={
            'origo.backfill.projection_mode': 'inline',
            'origo.backfill.execution_mode': 'reconcile',
            'origo.backfill.runtime_audit_mode': 'event',
        },
        op_config={
            'projection_mode': 'deferred',
            'execution_mode': 'backfill',
            'runtime_audit_mode': 'summary',
        },
    )

    runtime_contract = load_backfill_runtime_contract_or_raise(context)  # type: ignore[arg-type]

    assert runtime_contract.projection_mode == 'inline'
    assert runtime_contract.execution_mode == 'reconcile'
    assert runtime_contract.runtime_audit_mode == 'event'


def test_default_exchange_backfill_runtime_tags_are_explicit() -> None:
    assert default_exchange_backfill_runtime_tags() == {
        'origo.backfill.projection_mode': 'deferred',
        'origo.backfill.execution_mode': 'backfill',
        'origo.backfill.runtime_audit_mode': 'summary',
    }


def test_default_exchange_live_runtime_tags_are_explicit() -> None:
    assert default_exchange_live_runtime_tags() == {
        'origo.backfill.projection_mode': 'inline',
        'origo.backfill.execution_mode': 'backfill',
        'origo.backfill.runtime_audit_mode': 'summary',
    }


def test_apply_runtime_audit_mode_rejects_invalid_value() -> None:
    with pytest.raises(RuntimeError, match='runtime_audit_mode must be one of'):
        apply_runtime_audit_mode_or_raise(runtime_audit_mode='broken')  # type: ignore[arg-type]


def test_load_backfill_height_window_reads_required_tags() -> None:
    context = _build_context(
        tags={
            BACKFILL_HEIGHT_START_TAG: '100',
            BACKFILL_HEIGHT_END_TAG: '200',
        }
    )

    window = load_backfill_height_window_or_raise(context)  # type: ignore[arg-type]

    assert window.start_height == 100
    assert window.end_height == 200


def test_load_backfill_height_window_fails_loud_on_invalid_range() -> None:
    context = _build_context(
        tags={
            BACKFILL_HEIGHT_START_TAG: '200',
            BACKFILL_HEIGHT_END_TAG: '100',
        }
    )

    with pytest.raises(
        RuntimeError,
        match=r'origo\.backfill\.height_end must be >= origo\.backfill\.height_start',
    ):
        load_backfill_height_window_or_raise(context)  # type: ignore[arg-type]


def test_load_backfill_height_window_reads_op_config_when_tags_absent() -> None:
    context = _build_context(
        tags={},
        op_config={
            'height_start': 100,
            'height_end': 200,
        },
    )

    window = load_backfill_height_window_or_raise(context)  # type: ignore[arg-type]

    assert window.start_height == 100
    assert window.end_height == 200
