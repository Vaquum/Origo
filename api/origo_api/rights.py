from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from .schemas import RawQueryDataset

RightsState = Literal['Hosted Allowed', 'BYOK Required', 'Ingest Only']
ShadowPromotionState = Literal['shadow', 'promoted']

_ALLOWED_RIGHTS_STATES: frozenset[RightsState] = frozenset(
    {'Hosted Allowed', 'BYOK Required', 'Ingest Only'}
)
_ALLOWED_SHADOW_PROMOTION_STATES: frozenset[ShadowPromotionState] = frozenset(
    {'shadow', 'promoted'}
)
_ALLOWED_EXPORT_DATASETS: frozenset[RawQueryDataset] = frozenset(
    {
        'spot_trades',
        'spot_agg_trades',
        'futures_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
    }
)
_ALLOWED_QUERY_DATASETS: frozenset[RawQueryDataset] = frozenset(
    {
        'spot_trades',
        'spot_agg_trades',
        'futures_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
    }
)
_ETF_QUERY_SERVING_STATE_ENV = 'ORIGO_ETF_QUERY_SERVING_STATE'
_FRED_QUERY_SERVING_STATE_ENV = 'ORIGO_FRED_QUERY_SERVING_STATE'


class RightsGateError(RuntimeError):
    def __init__(self, *, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SourceRightsDecision:
    source: str
    rights_state: RightsState
    legal_signoff_artifact: str | None


@dataclass(frozen=True)
class ExportRightsDecision:
    source: str
    dataset: RawQueryDataset
    rights_state: RightsState
    legal_signoff_artifact: str | None


@dataclass(frozen=True)
class QueryRightsDecision:
    dataset: RawQueryDataset
    sources: tuple[SourceRightsDecision, ...]
    serving_state: ShadowPromotionState | None


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
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return value


def _resolve_matrix_path() -> Path:
    path_value = _require_env('ORIGO_SOURCE_RIGHTS_MATRIX_PATH')
    matrix_path = Path(path_value)
    if not matrix_path.is_absolute():
        matrix_path = Path.cwd() / matrix_path
    if not matrix_path.exists():
        raise RuntimeError(f'Rights matrix file missing: {matrix_path}')
    return matrix_path


def _load_rights_matrix() -> dict[str, Any]:
    matrix_path = _resolve_matrix_path()
    try:
        raw_data = json.loads(matrix_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f'Invalid JSON in rights matrix: {matrix_path}: {exc.msg}'
        ) from exc
    return _expect_dict(raw_data, 'Rights matrix')


def _validate_export_dataset(raw_dataset: str, label: str) -> RawQueryDataset:
    if raw_dataset not in _ALLOWED_EXPORT_DATASETS:
        raise RuntimeError(
            f'{label} contains unsupported dataset: {raw_dataset}. '
            f'Allowed={sorted(_ALLOWED_EXPORT_DATASETS)}'
        )
    return raw_dataset


def _validate_rights_state(value: str, label: str) -> RightsState:
    if value not in _ALLOWED_RIGHTS_STATES:
        raise RuntimeError(
            f'{label} has invalid rights_state: {value}. '
            f'Allowed={sorted(_ALLOWED_RIGHTS_STATES)}'
        )
    return value


def _validate_query_dataset(raw_dataset: str, label: str) -> RawQueryDataset:
    if raw_dataset not in _ALLOWED_QUERY_DATASETS:
        raise RuntimeError(
            f'{label} contains unsupported query dataset: {raw_dataset}. '
            f'Allowed={sorted(_ALLOWED_QUERY_DATASETS)}'
        )
    return raw_dataset


def _extract_legal_signoff_artifact(
    *, source_payload: dict[str, Any], source_name: str
) -> str | None:
    legal_signoff_raw = source_payload.get('legal_signoff_artifact')
    if legal_signoff_raw is None:
        return None
    return _expect_non_empty_str(
        legal_signoff_raw,
        f'Rights matrix source[{source_name}].legal_signoff_artifact',
    )


def _extract_query_datasets(
    *, source_payload: dict[str, Any], source_name: str
) -> list[RawQueryDataset]:
    datasets_payload = _expect_list(
        source_payload.get('datasets'),
        f'Rights matrix source[{source_name}].datasets',
    )
    datasets: list[RawQueryDataset] = []
    for entry in datasets_payload:
        dataset_name = _expect_non_empty_str(
            entry, f'Rights matrix source[{source_name}].datasets[]'
        )
        datasets.append(
            _validate_query_dataset(
                dataset_name,
                label=f'Rights matrix source[{source_name}]',
            )
        )
    return datasets


def _resolve_query_serving_state(*, dataset: RawQueryDataset) -> ShadowPromotionState | None:
    serving_state_env: str
    if dataset == 'etf_daily_metrics':
        serving_state_env = _ETF_QUERY_SERVING_STATE_ENV
    elif dataset == 'fred_series_metrics':
        serving_state_env = _FRED_QUERY_SERVING_STATE_ENV
    else:
        return None

    serving_state = _require_env(serving_state_env)
    if serving_state not in _ALLOWED_SHADOW_PROMOTION_STATES:
        raise RuntimeError(
            f'{serving_state_env} must be one of '
            f'{sorted(_ALLOWED_SHADOW_PROMOTION_STATES)}'
        )

    if serving_state == 'shadow':
        raise RightsGateError(
            code='QUERY_SERVING_SHADOW_MODE',
            message=(
                f'Query serving is blocked for dataset={dataset} '
                'while serving state is shadow (promotion required)'
            ),
        )
    return serving_state


def _collect_source_decisions_for_dataset(
    *, dataset: RawQueryDataset
) -> list[SourceRightsDecision]:
    matrix = _load_rights_matrix()
    sources_payload = _expect_dict(matrix.get('sources'), 'Rights matrix sources')

    matched_sources: list[SourceRightsDecision] = []
    for source_name, raw_source_payload in sources_payload.items():
        source_payload = _expect_dict(
            raw_source_payload, f'Rights matrix source[{source_name}]'
        )
        rights_state = _validate_rights_state(
            _expect_non_empty_str(
                source_payload.get('rights_state'),
                f'Rights matrix source[{source_name}].rights_state',
            ),
            label=f'Rights matrix source[{source_name}]',
        )
        source_datasets = _extract_query_datasets(
            source_payload=source_payload,
            source_name=source_name,
        )
        if dataset not in source_datasets:
            continue
        matched_sources.append(
            SourceRightsDecision(
                source=source_name,
                rights_state=rights_state,
                legal_signoff_artifact=_extract_legal_signoff_artifact(
                    source_payload=source_payload,
                    source_name=source_name,
                ),
            )
        )
    return matched_sources


def _enforce_legal_signoff_for_hosted_allowed(
    *,
    source: str,
    rights_state: RightsState,
    legal_signoff_artifact: str | None,
    error_code: str,
) -> None:
    if rights_state != 'Hosted Allowed':
        return
    if legal_signoff_artifact is None:
        raise RightsGateError(
            code=error_code,
            message=(
                f'Hosted Allowed source={source} is missing '
                'legal_signoff_artifact'
            ),
        )

    legal_signoff_path = _resolve_artifact_path(legal_signoff_artifact)
    if not legal_signoff_path.exists():
        raise RightsGateError(
            code=error_code,
            message=(
                f'Legal signoff artifact missing for source={source}: '
                f'{legal_signoff_path}'
            ),
        )


def resolve_query_rights(
    *, dataset: RawQueryDataset, auth_token: str | None
) -> QueryRightsDecision:
    matched_sources = _collect_source_decisions_for_dataset(dataset=dataset)
    if len(matched_sources) == 0:
        raise RightsGateError(
            code='QUERY_RIGHTS_MISSING_STATE',
            message=f'No rights classification found for query dataset: {dataset}',
        )

    ingest_only_sources = sorted(
        source.source
        for source in matched_sources
        if source.rights_state == 'Ingest Only'
    )
    if len(ingest_only_sources) != 0:
        raise RightsGateError(
            code='QUERY_RIGHTS_INGEST_ONLY',
            message=(
                f'Query is blocked for dataset={dataset}: rights state is Ingest Only '
                f'for sources={ingest_only_sources}'
            ),
        )

    byok_sources = sorted(
        source.source
        for source in matched_sources
        if source.rights_state == 'BYOK Required'
    )
    token = auth_token.strip() if auth_token is not None else ''
    if len(byok_sources) != 0 and token == '':
        raise RightsGateError(
            code='QUERY_RIGHTS_BYOK_REQUIRED',
            message=(
                f'Query requires BYOK auth token for dataset={dataset} '
                f'sources={byok_sources}'
            ),
        )

    for source_decision in matched_sources:
        _enforce_legal_signoff_for_hosted_allowed(
            source=source_decision.source,
            rights_state=source_decision.rights_state,
            legal_signoff_artifact=source_decision.legal_signoff_artifact,
            error_code='QUERY_RIGHTS_LEGAL_SIGNOFF_MISSING',
        )

    serving_state = _resolve_query_serving_state(dataset=dataset)

    return QueryRightsDecision(
        dataset=dataset,
        sources=tuple(matched_sources),
        serving_state=serving_state,
    )


def resolve_export_rights(
    *, dataset: RawQueryDataset, auth_token: str | None
) -> ExportRightsDecision:
    _validate_export_dataset(dataset, 'Export dataset')
    matched_sources = _collect_source_decisions_for_dataset(dataset=dataset)

    if len(matched_sources) == 0:
        raise RightsGateError(
            code='EXPORT_RIGHTS_MISSING_STATE',
            message=f'No rights classification found for dataset: {dataset}',
        )

    ingest_only_sources = sorted(
        source.source
        for source in matched_sources
        if source.rights_state == 'Ingest Only'
    )
    if len(ingest_only_sources) != 0:
        raise RightsGateError(
            code='EXPORT_RIGHTS_INGEST_ONLY',
            message=(
                f'Export is blocked for dataset={dataset}: rights state is Ingest Only '
                f'for sources={ingest_only_sources}'
            ),
        )

    byok_sources = sorted(
        source.source
        for source in matched_sources
        if source.rights_state == 'BYOK Required'
    )
    token = auth_token.strip() if auth_token is not None else ''
    if len(byok_sources) != 0 and token == '':
        raise RightsGateError(
            code='EXPORT_RIGHTS_BYOK_REQUIRED',
            message=(
                f'Export requires BYOK auth token for dataset={dataset} '
                f'sources={byok_sources}'
            ),
        )

    if len(matched_sources) > 1:
        raise RightsGateError(
            code='EXPORT_RIGHTS_AMBIGUOUS_SOURCE',
            message=(
                f'Export dataset={dataset} is mapped to multiple rights sources: '
                f'{sorted(source.source for source in matched_sources)}'
            ),
        )

    source_decision = matched_sources[0]
    decision = ExportRightsDecision(
        source=source_decision.source,
        dataset=dataset,
        rights_state=source_decision.rights_state,
        legal_signoff_artifact=source_decision.legal_signoff_artifact,
    )
    _enforce_export_rights(decision=decision, auth_token=auth_token)
    return decision


def _resolve_artifact_path(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _enforce_export_rights(
    *, decision: ExportRightsDecision, auth_token: str | None
) -> None:
    if decision.rights_state == 'Ingest Only':
        raise RightsGateError(
            code='EXPORT_RIGHTS_INGEST_ONLY',
            message=(
                f'Export is blocked for source={decision.source} '
                f'dataset={decision.dataset}: rights state is Ingest Only'
            ),
        )

    if decision.rights_state == 'BYOK Required':
        if auth_token is None or auth_token.strip() == '':
            raise RightsGateError(
                code='EXPORT_RIGHTS_BYOK_REQUIRED',
                message=(
                    f'Export requires BYOK auth token for source={decision.source} '
                    f'dataset={decision.dataset}'
                ),
            )
        return

    _enforce_legal_signoff_for_hosted_allowed(
        source=decision.source,
        rights_state=decision.rights_state,
        legal_signoff_artifact=decision.legal_signoff_artifact,
        error_code='EXPORT_RIGHTS_LEGAL_SIGNOFF_MISSING',
    )
