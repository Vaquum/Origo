from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from origo.pathing import resolve_repo_relative_path

from .errors import as_scraper_error

RightsState = Literal['Hosted Allowed', 'BYOK Required', 'Ingest Only']
_ALLOWED_RIGHTS_STATES: frozenset[RightsState] = frozenset(
    {'Hosted Allowed', 'BYOK Required', 'Ingest Only'}
)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for key, item_value in raw_map.items():
        if not isinstance(key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[key] = item_value
    return normalized


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return value


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


@dataclass(frozen=True)
class ScraperRightsDecision:
    source_key: str
    source_id: str
    rights_state: RightsState


def _resolve_matrix_path() -> Path:
    path_value = _require_env('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')
    path = resolve_repo_relative_path(path_value)
    if not path.exists():
        raise RuntimeError(f'Rights matrix file missing: {path}')
    return path


def _load_matrix() -> dict[str, Any]:
    matrix_path = _resolve_matrix_path()
    try:
        parsed = json.loads(matrix_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f'Invalid JSON in rights matrix {matrix_path}: {exc.msg}'
        ) from exc
    return _expect_dict(parsed, 'Rights matrix')


def _validate_rights_state(*, source_key: str, rights_state: str) -> RightsState:
    if rights_state not in _ALLOWED_RIGHTS_STATES:
        raise RuntimeError(
            f'Invalid rights state for source={source_key}: {rights_state}. '
            f'Allowed={sorted(_ALLOWED_RIGHTS_STATES)}'
        )
    return rights_state


def resolve_scraper_rights(*, source_key: str, source_id: str) -> ScraperRightsDecision:
    matrix = _load_matrix()
    sources_payload = _expect_dict(matrix.get('sources'), 'Rights matrix sources')

    source_payload = sources_payload.get(source_key)
    if source_payload is None:
        raise as_scraper_error(
            code='SCRAPER_RIGHTS_MISSING_STATE',
            message=f'No rights state found for source_key={source_key}',
            details={'source_key': source_key},
        )

    source_payload_dict = _expect_dict(
        source_payload, f'Rights matrix source[{source_key}]'
    )
    rights_state_raw = _expect_non_empty_str(
        source_payload_dict.get('rights_state'),
        f'Rights matrix source[{source_key}].rights_state',
    )
    rights_state = _validate_rights_state(
        source_key=source_key,
        rights_state=rights_state_raw,
    )

    source_ids = _expect_list(
        source_payload_dict.get('source_ids'),
        f'Rights matrix source[{source_key}].source_ids',
    )
    normalized_ids: set[str] = set()
    for entry in source_ids:
        normalized_ids.add(
            _expect_non_empty_str(
                entry,
                f'Rights matrix source[{source_key}].source_ids[]',
            )
        )
    if source_id not in normalized_ids:
        raise as_scraper_error(
            code='SCRAPER_RIGHTS_SOURCE_ID_UNCLASSIFIED',
            message=(
                f'source_id={source_id} is not classified for source_key={source_key}'
            ),
            details={
                'source_key': source_key,
                'source_id': source_id,
            },
        )

    return ScraperRightsDecision(
        source_key=source_key,
        source_id=source_id,
        rights_state=rights_state,
    )
