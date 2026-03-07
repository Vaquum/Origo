from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

from clickhouse_connect import get_client as _raw_get_client

from origo.query.native_core import resolve_clickhouse_http_settings
from origo.scraper.contracts import PersistedRawArtifact, RawArtifact, ScrapeRunContext
from origo.scraper.object_store import persist_raw_artifact

from .client import FREDClient
from .contracts import FREDSeriesRegistryEntry
from .normalize import FREDLongMetricRow

get_client = cast(Any, _raw_get_client)

_FRED_METRICS_TABLE = 'fred_series_metrics_long'
_INSERT_COLUMN_NAMES = [
    'metric_id',
    'source_id',
    'metric_name',
    'metric_unit',
    'metric_value_string',
    'metric_value_int',
    'metric_value_float',
    'metric_value_bool',
    'observed_at_utc',
    'dimensions_json',
    'provenance_json',
    'ingested_at_utc',
]


@dataclass(frozen=True)
class FREDRawSeriesBundle:
    source_id: str
    series_id: str
    source_uri: str
    fetched_at_utc: datetime
    registry_version: str
    metadata_payload: dict[str, object]
    observations_payload: dict[str, object]

    def __post_init__(self) -> None:
        if self.source_id.strip() == '':
            raise ValueError('source_id must be non-empty')
        if self.series_id.strip() == '':
            raise ValueError('series_id must be non-empty')
        if self.source_uri.strip() == '':
            raise ValueError('source_uri must be non-empty')
        if self.fetched_at_utc.tzinfo is None:
            raise ValueError('fetched_at_utc must include timezone information')
        if self.registry_version.strip() == '':
            raise ValueError('registry_version must be non-empty')
        if len(self.metadata_payload) == 0:
            raise ValueError('metadata_payload must be non-empty')
        if len(self.observations_payload) == 0:
            raise ValueError('observations_payload must be non-empty')


def _artifact_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _series_bundle_payload(bundle: FREDRawSeriesBundle) -> dict[str, object]:
    return {
        'series_id': bundle.series_id,
        'source_id': bundle.source_id,
        'source_uri': bundle.source_uri,
        'registry_version': bundle.registry_version,
        'fetched_at_utc': bundle.fetched_at_utc.isoformat(),
        'metadata_payload': bundle.metadata_payload,
        'observations_payload': bundle.observations_payload,
    }


def _artifact_id_from_bundle(bundle: FREDRawSeriesBundle, content_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(bundle.source_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(bundle.series_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(bundle.fetched_at_utc.isoformat().encode('utf-8'))
    digest.update(b'|')
    digest.update(content_sha256.encode('utf-8'))
    return digest.hexdigest()


def persist_fred_long_metrics_to_clickhouse(
    *,
    rows: list[FREDLongMetricRow],
    auth_token: str | None = None,
) -> int:
    if len(rows) == 0:
        raise ValueError('rows must be non-empty')

    settings = resolve_clickhouse_http_settings(auth_token=auth_token)
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )
    ingested_at_utc = datetime.now(UTC)
    insert_rows: list[list[Any]] = []
    for row in rows:
        insert_rows.append(
            [
                row.metric_id,
                row.source_id,
                row.metric_name,
                row.metric_unit,
                row.metric_value_string,
                row.metric_value_int,
                row.metric_value_float,
                row.metric_value_bool,
                row.observed_at_utc,
                row.dimensions_json,
                row.provenance_json,
                ingested_at_utc,
            ]
        )

    try:
        try:
            client.command(f'DESCRIBE TABLE {settings.database}.{_FRED_METRICS_TABLE}')
        except Exception as exc:
            raise RuntimeError(
                f'ClickHouse table is missing: {settings.database}.{_FRED_METRICS_TABLE}. '
                'Apply SQL migrations before running FRED persistence.'
            ) from exc

        client.insert(
            table=f'{settings.database}.{_FRED_METRICS_TABLE}',
            data=insert_rows,
            column_names=_INSERT_COLUMN_NAMES,
        )
        return len(insert_rows)
    finally:
        client.close()


def build_fred_raw_bundles(
    *,
    client: FREDClient,
    registry_entries: list[FREDSeriesRegistryEntry],
    registry_version: str,
    observation_start: date | None = None,
    observation_end: date | None = None,
) -> list[FREDRawSeriesBundle]:
    if len(registry_entries) == 0:
        raise ValueError('registry_entries must be non-empty')
    if registry_version.strip() == '':
        raise ValueError('registry_version must be non-empty')
    if (
        observation_start is not None
        and observation_end is not None
        and observation_start > observation_end
    ):
        raise ValueError(
            'observation_start must be <= observation_end, got '
            f'{observation_start.isoformat()} > {observation_end.isoformat()}'
        )

    bundles: list[FREDRawSeriesBundle] = []
    for entry in registry_entries:
        fetched_at_utc = datetime.now(UTC)
        metadata_payload = client.fetch_series_metadata_payload(series_id=entry.series_id)
        observations_payload = client.fetch_series_observations_payload(
            series_id=entry.series_id,
            observation_start=observation_start,
            observation_end=observation_end,
            sort_order='asc',
            limit=None,
        )
        bundles.append(
            FREDRawSeriesBundle(
                source_id=entry.source_id,
                series_id=entry.series_id,
                source_uri=f'fred://series/{entry.series_id}',
                fetched_at_utc=fetched_at_utc,
                registry_version=registry_version,
                metadata_payload=metadata_payload,
                observations_payload=observations_payload,
            )
        )
    return bundles


def persist_fred_raw_bundles_to_object_store(
    *,
    bundles: list[FREDRawSeriesBundle],
    run_id: str,
    run_started_at_utc: datetime,
) -> list[PersistedRawArtifact]:
    if len(bundles) == 0:
        raise ValueError('bundles must be non-empty')
    if run_id.strip() == '':
        raise ValueError('run_id must be non-empty')
    if run_started_at_utc.tzinfo is None:
        raise ValueError('run_started_at_utc must include timezone information')

    run_context = ScrapeRunContext(
        run_id=run_id,
        started_at_utc=run_started_at_utc.astimezone(UTC),
    )

    persisted_artifacts: list[PersistedRawArtifact] = []
    for bundle in bundles:
        payload = _series_bundle_payload(bundle)
        content = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode(
            'utf-8'
        )
        content_sha256 = _artifact_sha256(content)
        artifact_id = _artifact_id_from_bundle(bundle, content_sha256)
        artifact = RawArtifact(
            artifact_id=artifact_id,
            source_id=bundle.source_id,
            source_uri=bundle.source_uri,
            fetched_at_utc=bundle.fetched_at_utc.astimezone(UTC),
            fetch_method='http',
            artifact_format='json',
            content_sha256=content_sha256,
            content=content,
            metadata={
                'artifact_type': 'fred_series_bundle',
                'registry_version': bundle.registry_version,
                'series_id': bundle.series_id,
            },
        )
        persisted_artifacts.append(
            persist_raw_artifact(
                artifact=artifact,
                run_context=run_context,
            )
        )

    return persisted_artifacts
