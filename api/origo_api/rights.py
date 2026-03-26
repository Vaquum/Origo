from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.backfill_state import CanonicalBackfillStateStore

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
        'binance_spot_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }
)
_ALLOWED_QUERY_DATASETS: frozenset[RawQueryDataset] = frozenset(
    {
        'binance_spot_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }
)
_ETF_QUERY_SERVING_STATE_ENV = 'ORIGO_ETF_QUERY_SERVING_STATE'
_FRED_QUERY_SERVING_STATE_ENV = 'ORIGO_FRED_QUERY_SERVING_STATE'


@dataclass(frozen=True)
class QueryServingPromotionContract:
    source_id: str
    stream_id: str
    native_projector_id: str
    aligned_projector_id: str


_QUERY_SERVING_PROMOTION_CONTRACTS: dict[RawQueryDataset, QueryServingPromotionContract] = {
    'etf_daily_metrics': QueryServingPromotionContract(
        source_id='etf',
        stream_id='etf_daily_metrics',
        native_projector_id='etf_daily_metrics_native_v1',
        aligned_projector_id='etf_daily_metrics_aligned_1s_v1',
    ),
    'fred_series_metrics': QueryServingPromotionContract(
        source_id='fred',
        stream_id='fred_series_metrics',
        native_projector_id='fred_series_metrics_native_v1',
        aligned_projector_id='fred_series_metrics_aligned_1s_v1',
    ),
}


class RightsGateError(RuntimeError):
    def __init__(self, *, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SourceRightsDecision:
    source: str
    rights_state: RightsState
    rights_provisional: bool
    legal_signoff_artifact: str | None


@dataclass(frozen=True)
class ExportRightsDecision:
    source: str
    dataset: RawQueryDataset
    rights_state: RightsState
    rights_provisional: bool
    legal_signoff_artifact: str | None


@dataclass(frozen=True)
class QueryRightsDecision:
    dataset: RawQueryDataset
    sources: tuple[SourceRightsDecision, ...]
    serving_state: ShadowPromotionState | None

    def response_metadata(self) -> tuple[RightsState, bool]:
        if len(self.sources) == 0:
            raise RuntimeError(
                f'No source rights decisions found for dataset={self.dataset}'
            )
        first_source = self.sources[0]
        for source in self.sources[1:]:
            if source.rights_state != first_source.rights_state:
                raise RuntimeError(
                    f'Query rights_state is ambiguous for dataset={self.dataset}: '
                    f'{sorted({entry.rights_state for entry in self.sources})}'
                )
            if source.rights_provisional != first_source.rights_provisional:
                raise RuntimeError(
                    f'Query rights_provisional is ambiguous for dataset={self.dataset}: '
                    f'{sorted({entry.rights_provisional for entry in self.sources})}'
                )
        return first_source.rights_state, first_source.rights_provisional


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _require_int_env(name: str) -> int:
    raw = _require_env(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f'{name} must be an integer, got {raw!r}') from exc


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


def _expect_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise RuntimeError(f'{label} must be a boolean')
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
    _assert_promoted_serving_projection_coverage_or_raise(dataset=dataset)
    return serving_state


def _build_clickhouse_native_client() -> ClickHouseClient:
    return ClickHouseClient(
        host=_require_env('CLICKHOUSE_HOST'),
        port=_require_int_env('CLICKHOUSE_PORT'),
        user=_require_env('CLICKHOUSE_USER'),
        password=_require_env('CLICKHOUSE_PASSWORD'),
        database=_require_env('CLICKHOUSE_DATABASE'),
        secure=False,
    )


def _assert_promoted_serving_projection_coverage_or_raise(
    *,
    dataset: RawQueryDataset,
) -> None:
    promotion_contract = _QUERY_SERVING_PROMOTION_CONTRACTS.get(dataset)
    if promotion_contract is None:
        return

    client = _build_clickhouse_native_client()
    try:
        state_store = CanonicalBackfillStateStore(
            client=client,
            database=_require_env('CLICKHOUSE_DATABASE'),
        )
        for projector_id in (
            promotion_contract.native_projector_id,
            promotion_contract.aligned_projector_id,
        ):
            try:
                state_store.assert_projector_partition_coverage_matches_terminal_proof_or_raise(
                    projector_id=projector_id,
                    source_id=promotion_contract.source_id,
                    stream_id=promotion_contract.stream_id,
                )
            except Exception as exc:
                raise RightsGateError(
                    code='QUERY_SERVING_PROMOTION_INCOMPLETE',
                    message=(
                        'Query serving cannot be promoted because projector coverage '
                        'does not exactly match terminal proof coverage '
                        f'for dataset={dataset}, projector_id={projector_id}: {exc}'
                    ),
                ) from exc
    finally:
        try:
            client.disconnect()
        except Exception as exc:
            raise RuntimeError(
                f'Failed to close ClickHouse native client cleanly: {exc}'
            ) from exc


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
        rights_provisional = _expect_bool(
            source_payload.get('rights_provisional'),
            f'Rights matrix source[{source_name}].rights_provisional',
        )
        if rights_state != 'Hosted Allowed' and rights_provisional:
            raise RuntimeError(
                f'Rights matrix source[{source_name}] has rights_provisional=true '
                'but rights_state is not Hosted Allowed'
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
                rights_provisional=rights_provisional,
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
        rights_provisional=source_decision.rights_provisional,
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
