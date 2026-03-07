from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from .contracts import FREDSeriesRegistryEntry

_DEFAULT_REGISTRY_RELATIVE_PATH = Path('contracts/fred-series-registry.json')


def default_fred_series_registry_path() -> Path:
    return Path(__file__).resolve().parents[2] / _DEFAULT_REGISTRY_RELATIVE_PATH


def _require_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be a JSON object')
    raw_mapping = cast(dict[object, object], value)
    for raw_key in raw_mapping:
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
    return cast(dict[str, object], raw_mapping)


def _require_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[object], value)


def _require_string(
    payload: dict[str, object], key: str, context: str, allow_empty: bool = False
) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f'{context}.{key} must be a string')
    if not allow_empty and value.strip() == '':
        raise RuntimeError(f'{context}.{key} must be non-empty')
    return value


def _optional_string(
    payload: dict[str, object], key: str, context: str
) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f'{context}.{key} must be a string when set')
    if value.strip() == '':
        raise RuntimeError(f'{context}.{key} cannot be empty when set')
    return value


def load_fred_series_registry(
    path: Path | None = None,
) -> tuple[str, list[FREDSeriesRegistryEntry]]:
    registry_path = default_fred_series_registry_path() if path is None else path
    if not registry_path.exists():
        raise RuntimeError(
            f'FRED series registry file does not exist: {registry_path.as_posix()}'
        )

    raw_payload = registry_path.read_text(encoding='utf-8')
    decoded = json.loads(raw_payload)
    payload = _require_object(decoded, 'registry')

    version = _require_string(payload, 'version', 'registry')
    raw_series = _require_list(payload.get('series'), 'registry.series')
    if len(raw_series) == 0:
        raise RuntimeError('registry.series must contain at least one entry')

    entries: list[FREDSeriesRegistryEntry] = []
    seen_series_ids: set[str] = set()
    seen_source_ids: set[str] = set()
    seen_metric_names: set[str] = set()

    for index, raw_entry in enumerate(raw_series):
        context = f'registry.series[{index}]'
        entry_payload = _require_object(raw_entry, context)
        entry = FREDSeriesRegistryEntry(
            series_id=_require_string(entry_payload, 'series_id', context),
            source_id=_require_string(entry_payload, 'source_id', context),
            metric_name=_require_string(entry_payload, 'metric_name', context),
            metric_unit=_optional_string(entry_payload, 'metric_unit', context),
            frequency_hint=_optional_string(entry_payload, 'frequency_hint', context),
            notes=_optional_string(entry_payload, 'notes', context),
        )

        if entry.series_id in seen_series_ids:
            raise RuntimeError(
                f'Duplicate series_id in registry: {entry.series_id} ({context})'
            )
        if entry.source_id in seen_source_ids:
            raise RuntimeError(
                f'Duplicate source_id in registry: {entry.source_id} ({context})'
            )
        if entry.metric_name in seen_metric_names:
            raise RuntimeError(
                f'Duplicate metric_name in registry: {entry.metric_name} ({context})'
            )

        seen_series_ids.add(entry.series_id)
        seen_source_ids.add(entry.source_id)
        seen_metric_names.add(entry.metric_name)
        entries.append(entry)

    return version, entries
