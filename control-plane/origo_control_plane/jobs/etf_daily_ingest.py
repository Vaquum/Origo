from __future__ import annotations

# pyright: reportUnknownParameterType=false, reportMissingParameterType=false
import hashlib
import importlib
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, cast
from urllib.parse import parse_qsl, urlparse

import dagster as dg
from clickhouse_driver import Client as ClickhouseClient
from dagster import OpExecutionContext

from origo.events import (
    CanonicalBackfillStateStore,
    CanonicalStreamKey,
    SourceIdentityMaterial,
    build_partition_source_proof,
    canonical_event_id_from_key,
    canonical_event_idempotency_key,
)
from origo.events.backfill_state import canonical_proof_matches_source_proof
from origo.events.errors import ReconciliationError
from origo.scraper.contracts import (
    NormalizedMetricRecord,
    PersistedRawArtifact,
    RawArtifact,
    ScrapeRunContext,
    SourceDescriptor,
)
from origo.scraper.etf_adapters import build_s4_03_issuer_adapters
from origo.scraper.etf_canonical_event_ingest import (
    build_etf_canonical_payload,
    write_etf_normalized_records_to_canonical,
)
from origo.scraper.pipeline import PipelineSourceResult, run_scraper_pipeline
from origo_control_plane.backfill import apply_runtime_audit_mode_or_raise
from origo_control_plane.backfill.runtime_contract import (
    BackfillRuntimeContract,
    load_backfill_runtime_contract_from_tags_or_raise,
)
from origo_control_plane.config import resolve_clickhouse_native_settings
from origo_control_plane.utils.etf_aligned_projector import (
    project_etf_daily_metrics_aligned,
)
from origo_control_plane.utils.etf_native_projector import (
    project_etf_daily_metrics_native,
)

job: Any = getattr(dg, 'job')
op: Any = getattr(dg, 'op')

_CANONICAL_SOURCE_ID = 'etf'
_CANONICAL_STREAM_ID = 'etf_daily_metrics'
_PAYLOAD_ENCODING = 'utf-8'
_RAW_ARTIFACTS_PREFIX = 'raw-artifacts/'
_ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER = 'official_date_parameter'
_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD = 'archive_capture_forward'
_ISHARES_NO_DATA_MARKER = 'Fund Holdings as of,"-"'
_ISHARES_SOURCE_ID = 'etf_ishares_ibit_daily'


@dataclass(frozen=True)
class _PartitionSourceBatch:
    source_result: PipelineSourceResult
    records: list[NormalizedMetricRecord]


@dataclass(frozen=True)
class _PartitionBackfillResult:
    partition_id: str
    rows_processed: int
    rows_inserted: int
    rows_duplicate: int
    write_path: str
    partition_proof_state: str
    partition_proof_digest_sha256: str
    native_projection_summary: dict[str, int]
    aligned_projection_summary: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            'partition_id': self.partition_id,
            'rows_processed': self.rows_processed,
            'rows_inserted': self.rows_inserted,
            'rows_duplicate': self.rows_duplicate,
            'write_path': self.write_path,
            'partition_proof_state': self.partition_proof_state,
            'partition_proof_digest_sha256': self.partition_proof_digest_sha256,
            'native_projection_summary': self.native_projection_summary,
            'aligned_projection_summary': self.aligned_projection_summary,
        }


@dataclass(frozen=True)
class _BackfillRunSummary:
    total_sources: int
    total_parsed_records: int
    total_normalized_records: int
    total_inserted_rows: int
    total_native_projected_rows: int
    total_aligned_projected_rows: int
    partition_results: list[_PartitionBackfillResult]


@dataclass(frozen=True)
class _ArchivedArtifactLoad:
    manifest_payload: dict[str, Any]
    raw_artifact: RawArtifact
    persisted_artifact: PersistedRawArtifact


@dataclass(frozen=True)
class _ETFHistoricalAvailabilityContract:
    source_id: str
    history_mode: str
    first_partition_id: str | None = None


_ETF_HISTORICAL_AVAILABILITY_CONTRACTS: Final[dict[str, _ETFHistoricalAvailabilityContract]] = {
    'etf_ark_arkb_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_ark_arkb_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_bitwise_bitb_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_bitwise_bitb_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_coinshares_brrr_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_coinshares_brrr_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_fidelity_fbtc_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_fidelity_fbtc_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_franklin_ezbc_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_franklin_ezbc_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_grayscale_gbtc_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_grayscale_gbtc_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_hashdex_defi_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_hashdex_defi_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_invesco_btco_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_invesco_btco_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
    'etf_ishares_ibit_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_ishares_ibit_daily',
        history_mode=_ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER,
        first_partition_id='2024-01-11',
    ),
    'etf_vaneck_hodl_daily': _ETFHistoricalAvailabilityContract(
        source_id='etf_vaneck_hodl_daily',
        history_mode=_ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD,
    ),
}


def _build_clickhouse_client_or_raise() -> tuple[ClickhouseClient, str]:
    native_settings = resolve_clickhouse_native_settings()
    return (
        ClickhouseClient(
            host=native_settings.host,
            port=native_settings.port,
            user=native_settings.user,
            password=native_settings.password,
            database=native_settings.database,
            compression=True,
            send_receive_timeout=900,
        ),
        native_settings.database,
    )


def _require_object_store_env_or_raise(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _build_object_store_client_and_bucket_or_raise() -> tuple[Any, str]:
    try:
        boto3_module = importlib.import_module('boto3')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'ETF historical archive replay requires boto3. Install with `uv add boto3`.'
        ) from exc
    client_factory = getattr(boto3_module, 'client', None)
    if client_factory is None:
        raise RuntimeError('boto3.client was not found')
    endpoint_url = _require_object_store_env_or_raise('ORIGO_OBJECT_STORE_ENDPOINT_URL')
    access_key_id = _require_object_store_env_or_raise(
        'ORIGO_OBJECT_STORE_ACCESS_KEY_ID'
    )
    secret_access_key = _require_object_store_env_or_raise(
        'ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY'
    )
    region = _require_object_store_env_or_raise('ORIGO_OBJECT_STORE_REGION')
    bucket = _require_object_store_env_or_raise('ORIGO_OBJECT_STORE_BUCKET')
    client = client_factory(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )
    return client, bucket


def _partition_id_for_record_or_raise(record: NormalizedMetricRecord) -> str:
    return record.observed_at_utc.astimezone(UTC).date().isoformat()


def _partition_id_to_date_or_raise(*, partition_id: str) -> date:
    try:
        return date.fromisoformat(partition_id)
    except ValueError as exc:
        raise RuntimeError(f'ETF partition_id must be ISO date, got {partition_id}') from exc


def _iter_business_partition_ids_inclusive(
    *,
    start_partition_id: str,
    end_partition_id: str,
) -> tuple[str, ...]:
    start_day = _partition_id_to_date_or_raise(partition_id=start_partition_id)
    end_day = _partition_id_to_date_or_raise(partition_id=end_partition_id)
    if start_day > end_day:
        raise RuntimeError(
            'ETF historical availability window is invalid: '
            f'start_partition_id={start_partition_id} end_partition_id={end_partition_id}'
        )
    partition_ids: list[str] = []
    current = start_day
    while current <= end_day:
        if current.weekday() < 5:
            partition_ids.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(partition_ids)


def _partition_id_from_ishares_source_uri_or_raise(*, source_uri: str) -> str:
    parsed = urlparse(source_uri)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    compact_value = params.get('asOfDate')
    if compact_value is None or compact_value.strip() == '':
        raise RuntimeError(
            'iShares historical source_uri is missing asOfDate query parameter: '
            f'{source_uri}'
        )
    if len(compact_value) != 8 or not compact_value.isdigit():
        raise RuntimeError(
            'iShares historical source_uri asOfDate must be YYYYMMDD digits, '
            f'got {compact_value!r}'
        )
    try:
        return datetime.strptime(compact_value, '%Y%m%d').date().isoformat()
    except ValueError as exc:
        raise RuntimeError(
            'iShares historical source_uri asOfDate must be a valid calendar date, '
            f'got {compact_value!r}'
        ) from exc


def _archived_no_data_partition_id_or_none(
    *,
    source_id: str,
    artifact: RawArtifact,
) -> str | None:
    if source_id != _ISHARES_SOURCE_ID:
        return None
    csv_text = artifact.content.decode('utf-8-sig', errors='replace')
    if _ISHARES_NO_DATA_MARKER not in csv_text:
        return None
    return _partition_id_from_ishares_source_uri_or_raise(source_uri=artifact.source_uri)


def _build_expected_etf_archive_partitions_by_source_or_raise(
    *,
    valid_partition_ids_by_source: dict[str, set[str]],
    no_data_partition_ids_by_source: dict[str, set[str]],
    required_partition_filter: set[str],
) -> tuple[dict[str, tuple[str, ...]], list[dict[str, Any]], list[dict[str, Any]]]:
    expected_partitions_by_source: dict[str, tuple[str, ...]] = {}
    unavailable_sources: list[dict[str, Any]] = []
    zero_history_sources: list[dict[str, Any]] = []

    for source_id in sorted(_ETF_HISTORICAL_AVAILABILITY_CONTRACTS):
        contract = _ETF_HISTORICAL_AVAILABILITY_CONTRACTS[source_id]
        valid_partitions = set(valid_partition_ids_by_source.get(source_id, set()))
        no_data_partitions = set(no_data_partition_ids_by_source.get(source_id, set()))
        evidence_partitions = tuple(sorted(valid_partitions | no_data_partitions))
        if evidence_partitions == ():
            if contract.history_mode == _ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD:
                zero_history_sources.append(
                    {
                        'source_id': source_id,
                        'history_mode': contract.history_mode,
                        'reason': 'zero_history_before_first_valid_archive',
                    }
                )
            else:
                unavailable_sources.append(
                    {
                        'source_id': source_id,
                        'history_mode': contract.history_mode,
                        'reason': 'no_valid_archived_artifacts',
                    }
                )
            continue

        if contract.history_mode == _ETF_HISTORY_MODE_OFFICIAL_DATE_PARAMETER:
            if contract.first_partition_id is None:
                raise RuntimeError(
                    'ETF official historical contract requires first_partition_id: '
                    f'source_id={source_id}'
                )
            expected_partitions = _iter_business_partition_ids_inclusive(
                start_partition_id=contract.first_partition_id,
                end_partition_id=evidence_partitions[-1],
            )
            expected_partitions = tuple(
                partition_id
                for partition_id in expected_partitions
                if partition_id not in no_data_partitions
            )
        elif contract.history_mode == _ETF_HISTORY_MODE_ARCHIVE_CAPTURE_FORWARD:
            expected_partitions = _iter_business_partition_ids_inclusive(
                start_partition_id=evidence_partitions[0],
                end_partition_id=evidence_partitions[-1],
            )
        else:
            raise RuntimeError(
                'Unknown ETF historical availability mode: '
                f'source_id={source_id} history_mode={contract.history_mode}'
            )

        if required_partition_filter:
            expected_partitions = tuple(
                partition_id
                for partition_id in expected_partitions
                if partition_id in required_partition_filter
            )
            if expected_partitions == ():
                continue

        expected_partitions_by_source[source_id] = expected_partitions

    return expected_partitions_by_source, unavailable_sources, zero_history_sources


def _build_legacy_etf_daily_ingest_summary(
    *,
    context: OpExecutionContext,
) -> dict[str, Any]:
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-daily-{context.run_id}',
        started_at_utc=started_at_utc,
    )
    native_client, database = _build_clickhouse_client_or_raise()

    total_sources = 0
    total_parsed_records = 0
    total_normalized_records = 0
    total_inserted_rows = 0
    total_native_projected_rows = 0
    total_aligned_projected_rows = 0
    per_adapter_summary: list[dict[str, Any]] = []

    try:
        for adapter in build_s4_03_issuer_adapters():
            pipeline_result = run_scraper_pipeline(
                adapter=adapter,
                run_context=run_context,
            )
            total_sources += pipeline_result.total_sources
            total_parsed_records += pipeline_result.total_parsed_records
            total_normalized_records += pipeline_result.total_normalized_records
            total_inserted_rows += pipeline_result.total_inserted_rows

            partition_ids = sorted(
                {
                    _partition_id_for_record_or_raise(normalized_record)
                    for source_result in pipeline_result.source_results
                    for normalized_record in source_result.normalized_records
                }
            )
            if partition_ids == []:
                raise RuntimeError(
                    f'ETF pipeline adapter={adapter.adapter_name} produced zero partition ids'
                )

            native_projection_summary = project_etf_daily_metrics_native(
                client=native_client,
                database=database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            )
            aligned_projection_summary = project_etf_daily_metrics_aligned(
                client=native_client,
                database=database,
                partition_ids=partition_ids,
                run_id=context.run_id,
                projected_at_utc=datetime.now(UTC),
            )
            total_native_projected_rows += native_projection_summary.rows_written
            total_aligned_projected_rows += aligned_projection_summary.rows_written
            per_adapter_summary.append(
                {
                    'adapter_name': adapter.adapter_name,
                    'total_sources': pipeline_result.total_sources,
                    'total_parsed_records': pipeline_result.total_parsed_records,
                    'total_normalized_records': pipeline_result.total_normalized_records,
                    'total_inserted_rows': pipeline_result.total_inserted_rows,
                    'native_projection_summary': native_projection_summary.to_dict(),
                    'aligned_projection_summary': aligned_projection_summary.to_dict(),
                    'source_ids': sorted(
                        {
                            source_result.source.source_id
                            for source_result in pipeline_result.source_results
                        }
                    ),
                    'partition_ids': partition_ids,
                }
            )
    finally:
        _disconnect_clickhouse_client_or_raise(
            client=native_client,
            context=context,
        )

    return {
        'run_id': run_context.run_id,
        'started_at_utc': started_at_utc.isoformat(),
        'total_sources': total_sources,
        'total_parsed_records': total_parsed_records,
        'total_normalized_records': total_normalized_records,
        'total_inserted_rows': total_inserted_rows,
        'total_native_projected_rows': total_native_projected_rows,
        'total_aligned_projected_rows': total_aligned_projected_rows,
        'per_adapter_summary_json': json.dumps(
            per_adapter_summary,
            ensure_ascii=True,
            sort_keys=True,
        ),
    }


def _disconnect_clickhouse_client_or_raise(
    *,
    client: ClickhouseClient,
    context: OpExecutionContext,
) -> None:
    try:
        client.disconnect()
    except Exception as exc:
        active_exception = sys.exc_info()[1]
        if active_exception is not None:
            active_exception.add_note(
                f'ClickHouse disconnect failed during cleanup: {exc}'
            )
            context.log.warning(
                f'Failed to disconnect ClickHouse client cleanly: {exc}'
            )
        else:
            raise RuntimeError(
                f'Failed to disconnect ClickHouse client cleanly: {exc}'
            ) from exc


def _build_etf_adapter_source_index_or_raise(
    *,
    run_context: ScrapeRunContext,
) -> dict[str, tuple[Any, SourceDescriptor]]:
    source_index: dict[str, tuple[Any, SourceDescriptor]] = {}
    for adapter in build_s4_03_issuer_adapters():
        discovered_sources = list(adapter.discover_sources(run_context=run_context))
        if len(discovered_sources) != 1:
            raise RuntimeError(
                'ETF archived replay requires exactly one source descriptor per adapter, '
                f'got {len(discovered_sources)} for adapter={adapter.adapter_name}'
            )
        source = discovered_sources[0]
        if source.source_id in source_index:
            raise RuntimeError(
                f'Duplicate ETF source_id discovered for archived replay: {source.source_id}'
            )
        source_index[source.source_id] = (adapter, source)
    return source_index


def _load_all_raw_artifact_manifest_payloads_or_raise(
    *,
    client: Any,
    bucket: str,
) -> list[dict[str, Any]]:
    manifest_payloads: list[dict[str, Any]] = []
    request_kwargs: dict[str, Any] = {
        'Bucket': bucket,
        'Prefix': _RAW_ARTIFACTS_PREFIX,
    }
    while True:
        response = client.list_objects_v2(**request_kwargs)
        for item in response.get('Contents', []):
            key = item.get('Key')
            if not isinstance(key, str) or not key.endswith('manifest.json'):
                continue
            body = client.get_object(Bucket=bucket, Key=key)['Body'].read()
            try:
                payload = json.loads(body.decode('utf-8'))
            except Exception as exc:
                raise RuntimeError(
                    f'Failed to decode raw artifact manifest json for key={key}: {exc}'
                ) from exc
            if not isinstance(payload, dict):
                raise RuntimeError(
                    f'Raw artifact manifest must decode to an object for key={key}'
                )
            payload_with_key: dict[str, Any] = {}
            for payload_key, payload_value in cast(dict[object, object], payload).items():
                if not isinstance(payload_key, str) or payload_key.strip() == '':
                    raise RuntimeError(
                        f'Raw artifact manifest keys must be non-empty strings for key={key}'
                    )
                payload_with_key[payload_key] = payload_value
            payload_with_key['manifest_key'] = key
            manifest_payloads.append(payload_with_key)
        next_token = response.get('NextContinuationToken')
        if not isinstance(next_token, str) or next_token.strip() == '':
            break
        request_kwargs['ContinuationToken'] = next_token
    return manifest_payloads


def _parse_iso_datetime_or_raise(*, value: Any, label: str) -> datetime:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty ISO datetime string')
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'{label} must be valid ISO datetime, got {value!r}') from exc
    if parsed.tzinfo is None:
        raise RuntimeError(f'{label} must include timezone information')
    return parsed.astimezone(UTC)


def _parse_s3_uri_or_raise(uri: str, *, label: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != 's3' or parsed.netloc.strip() == '' or parsed.path.strip() == '':
        raise RuntimeError(f'{label} must be a valid s3:// URI, got {uri!r}')
    return parsed.netloc, parsed.path.lstrip('/')


def _load_archived_raw_artifact_from_manifest_or_raise(
    *,
    client: Any,
    bucket: str,
    manifest_payload: dict[str, Any],
) -> _ArchivedArtifactLoad:
    source_id = manifest_payload.get('source_id')
    source_uri = manifest_payload.get('source_uri')
    artifact_id = manifest_payload.get('artifact_id')
    fetch_method = manifest_payload.get('fetch_method')
    artifact_format = manifest_payload.get('artifact_format')
    content_sha256 = manifest_payload.get('content_sha256')
    fetched_at_utc = _parse_iso_datetime_or_raise(
        value=manifest_payload.get('fetched_at_utc'),
        label='manifest.fetched_at_utc',
    )
    persisted_at_utc = _parse_iso_datetime_or_raise(
        value=manifest_payload.get('persisted_at_utc'),
        label='manifest.persisted_at_utc',
    )
    metadata_raw = manifest_payload.get('metadata')
    manifest_uri = manifest_payload.get('manifest_uri')
    if not isinstance(source_id, str) or source_id.strip() == '':
        raise RuntimeError('manifest.source_id must be non-empty')
    if not isinstance(source_uri, str) or source_uri.strip() == '':
        raise RuntimeError('manifest.source_uri must be non-empty')
    if not isinstance(artifact_id, str) or artifact_id.strip() == '':
        raise RuntimeError('manifest.artifact_id must be non-empty')
    if not isinstance(fetch_method, str) or fetch_method.strip() == '':
        raise RuntimeError('manifest.fetch_method must be non-empty')
    if not isinstance(artifact_format, str) or artifact_format.strip() == '':
        raise RuntimeError('manifest.artifact_format must be non-empty')
    if not isinstance(content_sha256, str) or content_sha256.strip() == '':
        raise RuntimeError('manifest.content_sha256 must be non-empty')
    if not isinstance(metadata_raw, dict):
        raise RuntimeError('manifest.metadata must be an object')
    metadata: dict[str, str] = {}
    for raw_key, raw_value in cast(dict[object, object], metadata_raw).items():
        if not isinstance(raw_key, str) or raw_key.strip() == '':
            raise RuntimeError('manifest.metadata keys must be non-empty strings')
        if not isinstance(raw_value, str) or raw_value.strip() == '':
            raise RuntimeError(
                f'manifest.metadata[{raw_key!r}] must be a non-empty string'
            )
        metadata[raw_key] = raw_value
    artifact_key_raw = manifest_payload.get('artifact_key')
    if isinstance(artifact_key_raw, str) and artifact_key_raw.strip() != '':
        artifact_key = artifact_key_raw
    else:
        storage_uri = manifest_payload.get('storage_uri')
        if not isinstance(storage_uri, str) or storage_uri.strip() == '':
            raise RuntimeError(
                'manifest must provide artifact_key or storage_uri for archive replay'
            )
        storage_bucket, artifact_key = _parse_s3_uri_or_raise(
            storage_uri,
            label='manifest.storage_uri',
        )
        if storage_bucket != bucket:
            raise RuntimeError(
                'manifest.storage_uri bucket mismatch for archive replay: '
                f'{storage_bucket} != {bucket}'
            )
    content = client.get_object(Bucket=bucket, Key=artifact_key)['Body'].read()
    content_hash = hashlib.sha256(content).hexdigest()
    if content_hash != content_sha256:
        raise RuntimeError(
            'Archived ETF artifact content hash mismatch: '
            f'expected={content_sha256} actual={content_hash} '
            f'source_id={source_id} artifact_id={artifact_id}'
        )
    manifest_key_raw = manifest_payload.get('manifest_key')
    if not isinstance(manifest_key_raw, str) or manifest_key_raw.strip() == '':
        raise RuntimeError('manifest_key must be present on archive manifest payload')
    manifest_bucket, manifest_key = (
        _parse_s3_uri_or_raise(manifest_uri, label='manifest.manifest_uri')
        if isinstance(manifest_uri, str) and manifest_uri.strip() != ''
        else (bucket, manifest_key_raw)
    )
    if manifest_bucket != bucket:
        raise RuntimeError(
            'manifest.manifest_uri bucket mismatch for archive replay: '
            f'{manifest_bucket} != {bucket}'
        )
    raw_artifact = RawArtifact(
        artifact_id=artifact_id,
        source_id=source_id,
        source_uri=source_uri,
        fetched_at_utc=fetched_at_utc,
        fetch_method=cast(Any, fetch_method),
        artifact_format=cast(Any, artifact_format),
        content_sha256=content_sha256,
        content=content,
        metadata=metadata,
    )
    persisted_artifact = PersistedRawArtifact(
        artifact_id=artifact_id,
        storage_uri=f's3://{bucket}/{artifact_key}',
        manifest_uri=f's3://{bucket}/{manifest_key}',
        persisted_at_utc=persisted_at_utc,
    )
    return _ArchivedArtifactLoad(
        manifest_payload=manifest_payload,
        raw_artifact=raw_artifact,
        persisted_artifact=persisted_artifact,
    )


def _normalized_records_fingerprint(records: list[NormalizedMetricRecord]) -> str:
    digest = hashlib.sha256()
    for item in sorted(records, key=lambda record: record.metric_id):
        digest.update(item.metric_id.encode(_PAYLOAD_ENCODING))
        digest.update(b'|')
        digest.update(_canonical_payload_sha256(item).encode(_PAYLOAD_ENCODING))
        digest.update(b'\n')
    return digest.hexdigest()


def _archived_result_revision_order_key(
    result: PipelineSourceResult,
) -> tuple[datetime, datetime, str]:
    return (
        result.artifact.fetched_at_utc,
        result.persisted_artifact.persisted_at_utc,
        result.artifact.artifact_id,
    )


def _load_etf_archived_source_results_or_raise(
    *,
    run_context: ScrapeRunContext,
    required_partition_ids: tuple[str, ...] = (),
    log: Any | None = None,
) -> list[PipelineSourceResult]:
    adapter_source_index = _build_etf_adapter_source_index_or_raise(
        run_context=run_context
    )
    client, bucket = _build_object_store_client_and_bucket_or_raise()
    manifest_payloads = _load_all_raw_artifact_manifest_payloads_or_raise(
        client=client,
        bucket=bucket,
    )
    expected_source_ids = tuple(sorted(adapter_source_index))
    required_partition_set = set(required_partition_ids)
    if manifest_payloads == []:
        raise RuntimeError(
            'ETF historical archive replay requires archived issuer artifacts, '
            'but the object-store manifest inventory is empty'
        )

    deduped_results: dict[tuple[str, str], tuple[PipelineSourceResult, str]] = {}
    invalid_artifacts: list[dict[str, Any]] = []
    no_data_artifacts: list[dict[str, Any]] = []
    superseded_artifacts: list[dict[str, Any]] = []
    no_data_partition_ids_by_source: dict[str, set[str]] = defaultdict(set)

    for manifest_payload in sorted(
        manifest_payloads,
        key=lambda item: (
            str(item.get('source_id', '')),
            str(item.get('fetched_at_utc', '')),
            str(item.get('artifact_id', '')),
        ),
    ):
        source_id = manifest_payload.get('source_id')
        if not isinstance(source_id, str) or source_id not in adapter_source_index:
            continue
        adapter, source = adapter_source_index[source_id]
        try:
            archived = _load_archived_raw_artifact_from_manifest_or_raise(
                client=client,
                bucket=bucket,
                manifest_payload=manifest_payload,
            )
            no_data_partition_id = _archived_no_data_partition_id_or_none(
                source_id=source_id,
                artifact=archived.raw_artifact,
            )
            if no_data_partition_id is not None:
                if required_partition_set and no_data_partition_id not in required_partition_set:
                    continue
                no_data_partition_ids_by_source[source_id].add(no_data_partition_id)
                no_data_artifacts.append(
                    {
                        'source_id': source_id,
                        'partition_id': no_data_partition_id,
                        'artifact_id': archived.raw_artifact.artifact_id,
                        'manifest_uri': manifest_payload.get('manifest_uri'),
                    }
                )
                continue
            parsed_records = list(
                adapter.parse(
                    artifact=archived.raw_artifact,
                    source=source,
                    run_context=run_context,
                )
            )
            normalized_records = list(
                adapter.normalize(
                    parsed_records=parsed_records,
                    source=source,
                    run_context=run_context,
                )
            )
            if normalized_records == []:
                raise RuntimeError(
                    f'Archived ETF artifact produced zero normalized records for source_id={source_id}'
                )
            partition_ids = sorted(
                {_partition_id_for_record_or_raise(record) for record in normalized_records}
            )
            if len(partition_ids) != 1:
                raise RuntimeError(
                    'Archived ETF artifact must map to exactly one daily partition, '
                    f'got {partition_ids} for source_id={source_id}'
                )
        except Exception as exc:
            invalid_artifacts.append(
                {
                    'source_id': source_id,
                    'artifact_id': manifest_payload.get('artifact_id'),
                    'manifest_uri': manifest_payload.get('manifest_uri'),
                    'error': str(exc),
                }
            )
            continue

        partition_id = partition_ids[0]
        if required_partition_set and partition_id not in required_partition_set:
            continue
        pipeline_source_result = PipelineSourceResult(
            source=source,
            artifact=archived.raw_artifact,
            persisted_artifact=archived.persisted_artifact,
            parsed_records=parsed_records,
            normalized_records=normalized_records,
            inserted_row_count=0,
        )
        record_key = (source_id, partition_id)
        records_fingerprint = _normalized_records_fingerprint(normalized_records)
        existing_entry = deduped_results.get(record_key)
        if existing_entry is None:
            deduped_results[record_key] = (pipeline_source_result, records_fingerprint)
            continue
        existing_result, _existing_fingerprint = existing_entry
        existing_order_key = _archived_result_revision_order_key(existing_result)
        incoming_order_key = _archived_result_revision_order_key(pipeline_source_result)
        if incoming_order_key <= existing_order_key:
            superseded_artifacts.append(
                {
                    'source_id': source_id,
                    'partition_id': partition_id,
                    'selected_artifact_id': existing_result.artifact.artifact_id,
                    'ignored_artifact_id': pipeline_source_result.artifact.artifact_id,
                }
            )
            continue
        superseded_artifacts.append(
            {
                'source_id': source_id,
                'partition_id': partition_id,
                'selected_artifact_id': pipeline_source_result.artifact.artifact_id,
                'ignored_artifact_id': existing_result.artifact.artifact_id,
            }
        )
        deduped_results[record_key] = (pipeline_source_result, records_fingerprint)

    valid_partition_ids_by_source: dict[str, set[str]] = defaultdict(set)
    for (source_id, partition_id), _entry in deduped_results.items():
        valid_partition_ids_by_source[source_id].add(partition_id)
    expected_partitions_by_source, unavailable_sources, zero_history_sources = (
        _build_expected_etf_archive_partitions_by_source_or_raise(
            valid_partition_ids_by_source=valid_partition_ids_by_source,
            no_data_partition_ids_by_source=no_data_partition_ids_by_source,
            required_partition_filter=required_partition_set,
        )
    )
    missing_source_partitions = [
        {
            'source_id': source_id,
            'history_mode': _ETF_HISTORICAL_AVAILABILITY_CONTRACTS[source_id].history_mode,
            'missing_partition_ids': [
                partition_id
                for partition_id in expected_partitions
                if partition_id not in valid_partition_ids_by_source[source_id]
            ][:20],
        }
        for source_id, expected_partitions in sorted(expected_partitions_by_source.items())
        if any(
            partition_id not in valid_partition_ids_by_source[source_id]
            for partition_id in expected_partitions
        )
    ]
    if invalid_artifacts != [] and log is not None:
        log.warning(
            'ETF archive replay ignored invalid archived artifacts: '
            + json.dumps(
                {
                    'invalid_artifact_count': len(invalid_artifacts),
                    'invalid_artifacts': invalid_artifacts[:10],
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    if no_data_artifacts != [] and log is not None:
        log.warning(
            'ETF archive replay honored archived no-data evidence: '
            + json.dumps(
                {
                    'no_data_artifact_count': len(no_data_artifacts),
                    'no_data_artifacts': no_data_artifacts[:10],
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    if superseded_artifacts != [] and log is not None:
        log.warning(
            'ETF archive replay selected later archived revisions: '
            + json.dumps(
                {
                    'superseded_artifact_count': len(superseded_artifacts),
                    'superseded_artifacts': superseded_artifacts[:10],
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    if zero_history_sources != [] and log is not None:
        log.warning(
            'ETF archive replay surfaced zero-history snapshot-only issuers: '
            + json.dumps(
                {
                    'zero_history_source_count': len(zero_history_sources),
                    'zero_history_sources': zero_history_sources[:20],
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    if unavailable_sources != [] or missing_source_partitions != []:
        raise RuntimeError(
            'ETF historical archive coverage is incomplete: '
            + json.dumps(
                {
                    'required_source_count': len(expected_partitions_by_source),
                    'expected_source_ids': list(expected_source_ids),
                    'available_source_count': len(valid_partition_ids_by_source),
                    'zero_history_sources': zero_history_sources[:20],
                    'unavailable_sources': unavailable_sources[:20],
                    'missing_source_partitions': missing_source_partitions[:20],
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    if deduped_results == {}:
        raise RuntimeError(
            'ETF historical archive replay found no valid archived issuer artifacts'
        )
    return [
        item[0]
        for _key, item in sorted(
            deduped_results.items(),
            key=lambda pair: (pair[0][1], pair[0][0]),
        )
    ]


def _build_partition_batches_or_raise(
    source_results: list[PipelineSourceResult],
) -> dict[str, list[_PartitionSourceBatch]]:
    grouped: dict[str, list[_PartitionSourceBatch]] = {}
    for source_result in source_results:
        records_by_partition: dict[str, list[NormalizedMetricRecord]] = {}
        for record in source_result.normalized_records:
            partition_id = _partition_id_for_record_or_raise(record)
            records_by_partition.setdefault(partition_id, []).append(record)
        for partition_id, partition_records in records_by_partition.items():
            grouped.setdefault(partition_id, []).append(
                _PartitionSourceBatch(
                    source_result=source_result,
                    records=sorted(
                        partition_records,
                        key=lambda item: item.metric_id,
                    ),
                )
            )
    if grouped == {}:
        raise RuntimeError('ETF backfill prepared zero partition batches')
    return grouped


def _canonical_payload_sha256(record: NormalizedMetricRecord) -> str:
    payload_raw = json.dumps(
        build_etf_canonical_payload(record=record),
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    ).encode(_PAYLOAD_ENCODING)
    return hashlib.sha256(payload_raw).hexdigest()


def _build_etf_partition_source_proof_or_raise(
    *,
    partition_id: str,
    batches: list[_PartitionSourceBatch],
) -> Any:
    if batches == []:
        raise RuntimeError(
            f'ETF source proof requires non-empty batches for partition_id={partition_id}'
        )
    artifacts: list[dict[str, Any]] = []
    materials: list[SourceIdentityMaterial] = []
    for batch in sorted(
        batches,
        key=lambda item: (
            item.source_result.source.source_id,
            item.source_result.artifact.artifact_id,
        ),
    ):
        source_result = batch.source_result
        artifacts.append(
            {
                'source_id': source_result.source.source_id,
                'source_uri': source_result.source.source_uri,
                'source_name': source_result.source.source_name,
                'artifact_id': source_result.artifact.artifact_id,
                'artifact_sha256': source_result.artifact.content_sha256,
                'artifact_format': source_result.artifact.artifact_format,
                'fetch_method': source_result.artifact.fetch_method,
                'storage_uri': source_result.persisted_artifact.storage_uri,
                'manifest_uri': source_result.persisted_artifact.manifest_uri,
                'fetched_at_utc': source_result.artifact.fetched_at_utc.isoformat(),
                'persisted_at_utc': source_result.persisted_artifact.persisted_at_utc.isoformat(),
                'record_count': len(batch.records),
            }
        )
        for record in batch.records:
            idempotency_key = canonical_event_idempotency_key(
                source_id=_CANONICAL_SOURCE_ID,
                stream_id=_CANONICAL_STREAM_ID,
                partition_id=partition_id,
                source_offset_or_equivalent=record.metric_id,
            )
            materials.append(
                SourceIdentityMaterial(
                    source_offset_or_equivalent=record.metric_id,
                    event_id=str(canonical_event_id_from_key(idempotency_key)),
                    payload_sha256_raw=_canonical_payload_sha256(record),
                )
            )
    return build_partition_source_proof(
        stream_key=CanonicalStreamKey(
            source_id=_CANONICAL_SOURCE_ID,
            stream_id=_CANONICAL_STREAM_ID,
            partition_id=partition_id,
        ),
        offset_ordering='lexicographic',
        source_artifact_identity={
            'partition_id': partition_id,
            'artifact_count': len(artifacts),
            'artifacts': artifacts,
        },
        materials=materials,
        allow_empty_partition=False,
        allow_duplicate_offsets=False,
    )


def _execute_etf_partition_backfill_or_raise(
    *,
    context: OpExecutionContext,
    client: ClickhouseClient,
    database: str,
    partition_id: str,
    batches: list[_PartitionSourceBatch],
    runtime_contract: BackfillRuntimeContract,
) -> _PartitionBackfillResult:
    records = [record for batch in batches for record in batch.records]
    if records == []:
        raise RuntimeError(
            f'ETF partition batch cannot be empty for partition_id={partition_id}'
        )

    source_proof = _build_etf_partition_source_proof_or_raise(
        partition_id=partition_id,
        batches=batches,
    )
    state_store = CanonicalBackfillStateStore(
        client=client,
        database=database,
    )
    try:
        state_store.assert_partition_can_execute_or_raise(
            stream_key=source_proof.stream_key,
            execution_mode=runtime_contract.execution_mode,
        )
    except ReconciliationError as exc:
        if runtime_contract.execution_mode == 'backfill' and exc.code == 'RECONCILE_REQUIRED':
            state_store.record_partition_state(
                source_proof=source_proof,
                state='reconcile_required',
                reason='backfill_execution_requires_reconcile',
                run_id=context.run_id,
                recorded_at_utc=datetime.now(UTC),
                proof_details={'trigger_message': exc.message},
            )
        raise

    execution_assessment = state_store.assess_partition_execution(
        stream_key=source_proof.stream_key
    )
    reconcile_existing_canonical_rows = (
        runtime_contract.execution_mode == 'reconcile'
        and execution_assessment.canonical_row_count > 0
    )

    source_manifested_at_utc = datetime.now(UTC)
    state_store.record_source_manifest(
        source_proof=source_proof,
        run_id=context.run_id,
        manifested_at_utc=source_manifested_at_utc,
    )
    state_store.record_partition_state(
        source_proof=source_proof,
        state='source_manifested',
        reason='source_manifest_recorded',
        run_id=context.run_id,
        recorded_at_utc=source_manifested_at_utc,
    )

    current_canonical_matches_source = False
    current_canonical_proof = None
    if reconcile_existing_canonical_rows:
        current_canonical_proof = state_store.compute_canonical_partition_proof_or_raise(
            source_proof=source_proof
        )
        current_canonical_matches_source = canonical_proof_matches_source_proof(
            source_proof=source_proof,
            canonical_proof=current_canonical_proof,
        )

    if reconcile_existing_canonical_rows and current_canonical_matches_source:
        write_summary = {
            'rows_processed': source_proof.source_row_count,
            'rows_inserted': 0,
            'rows_duplicate': source_proof.source_row_count,
        }
        write_path = 'reconcile_proof_only'
        proof_reason = 'reconcile_existing_canonical_rows_detected'
    else:
        write_summary = write_etf_normalized_records_to_canonical(
            client=client,
            database=database,
            records=records,
            run_id=context.run_id,
            ingested_at_utc=datetime.now(UTC),
        ).to_dict()
        if reconcile_existing_canonical_rows:
            write_path = 'reconcile_writer_repair'
            proof_reason = 'reconcile_writer_repair_completed'
        else:
            write_path = 'writer'
            proof_reason = 'canonical_write_completed'

    rows_processed = int(write_summary['rows_processed'])
    rows_inserted = int(write_summary['rows_inserted'])
    rows_duplicate = int(write_summary['rows_duplicate'])
    if rows_processed != len(records):
        raise RuntimeError(
            'ETF canonical writer summary mismatch: '
            f'rows_processed={rows_processed} expected={len(records)} '
            f'partition_id={partition_id}'
        )
    if rows_inserted + rows_duplicate != rows_processed:
        raise RuntimeError(
            'ETF canonical writer summary mismatch: '
            f'rows_inserted+rows_duplicate={rows_inserted + rows_duplicate} '
            f'rows_processed={rows_processed} partition_id={partition_id}'
        )

    proof_recorded_at_utc = datetime.now(UTC)
    state_store.record_partition_state(
        source_proof=source_proof,
        state='canonical_written_unproved',
        reason=proof_reason,
        run_id=context.run_id,
        recorded_at_utc=proof_recorded_at_utc,
    )
    proof_input = current_canonical_proof if write_path == 'reconcile_proof_only' else None
    partition_proof = state_store.prove_partition_or_quarantine(
        source_proof=source_proof,
        run_id=context.run_id,
        recorded_at_utc=proof_recorded_at_utc,
        canonical_proof=proof_input,
    )

    projected_at_utc = datetime.now(UTC)
    if runtime_contract.projection_mode == 'inline':
        native_projection_summary = project_etf_daily_metrics_native(
            client=client,
            database=database,
            partition_ids=[partition_id],
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        ).to_dict()
        aligned_projection_summary = project_etf_daily_metrics_aligned(
            client=client,
            database=database,
            partition_ids=[partition_id],
            run_id=context.run_id,
            projected_at_utc=projected_at_utc,
        ).to_dict()
    else:
        native_projection_summary = {
            'partitions_processed': 0,
            'batches_processed': 0,
            'events_processed': 0,
            'rows_written': 0,
        }
        aligned_projection_summary = {
            'partitions_processed': 0,
            'policies_recorded': 0,
            'policies_duplicate': 0,
            'batches_processed': 0,
            'events_processed': 0,
            'rows_written': 0,
        }

    return _PartitionBackfillResult(
        partition_id=partition_id,
        rows_processed=rows_processed,
        rows_inserted=rows_inserted,
        rows_duplicate=rows_duplicate,
        write_path=write_path,
        partition_proof_state=partition_proof.state,
        partition_proof_digest_sha256=partition_proof.proof_digest_sha256,
        native_projection_summary=native_projection_summary,
        aligned_projection_summary=aligned_projection_summary,
    )


def _run_etf_backfill_or_raise(*, context: OpExecutionContext) -> _BackfillRunSummary:
    runtime_contract = load_backfill_runtime_contract_from_tags_or_raise(
        context.run.tags,
    )
    apply_runtime_audit_mode_or_raise(
        runtime_audit_mode=runtime_contract.runtime_audit_mode
    )
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-daily-{context.run_id}',
        started_at_utc=started_at_utc,
    )
    native_client, database = _build_clickhouse_client_or_raise()

    try:
        collected_source_results = _load_etf_archived_source_results_or_raise(
            run_context=run_context,
            log=context.log,
        )
        partition_batches = _build_partition_batches_or_raise(collected_source_results)
        partition_results = [
            _execute_etf_partition_backfill_or_raise(
                context=context,
                client=native_client,
                database=database,
                partition_id=partition_id,
                batches=partition_batches[partition_id],
                runtime_contract=runtime_contract,
            )
            for partition_id in sorted(partition_batches)
        ]
    finally:
        _disconnect_clickhouse_client_or_raise(
            client=native_client,
            context=context,
        )

    return _BackfillRunSummary(
        total_sources=len(collected_source_results),
        total_parsed_records=sum(
            len(item.parsed_records) for item in collected_source_results
        ),
        total_normalized_records=sum(
            len(item.normalized_records) for item in collected_source_results
        ),
        total_inserted_rows=sum(result.rows_inserted for result in partition_results),
        total_native_projected_rows=sum(
            result.native_projection_summary['rows_written']
            for result in partition_results
        ),
        total_aligned_projected_rows=sum(
            result.aligned_projection_summary['rows_written']
            for result in partition_results
        ),
        partition_results=partition_results,
    )


@op(name='origo_etf_daily_ingest_step')
def origo_etf_daily_ingest_step(context) -> None:
    _run_etf_daily_ingest_step_or_raise(context=cast(OpExecutionContext, context))


def _run_etf_daily_ingest_step_or_raise(*, context: OpExecutionContext) -> None:
    context.add_output_metadata(_build_legacy_etf_daily_ingest_summary(context=context))


@op(name='origo_etf_daily_backfill_step')
def origo_etf_daily_backfill_step(context) -> None:
    _run_etf_daily_backfill_step_or_raise(context=cast(OpExecutionContext, context))


def _run_etf_daily_backfill_step_or_raise(*, context: OpExecutionContext) -> None:
    started_at_utc = datetime.now(UTC)
    summary = _run_etf_backfill_or_raise(context=context)
    context.add_output_metadata(
        {
            'run_id': f'etf-daily-{context.run_id}',
            'started_at_utc': started_at_utc.isoformat(),
            'total_sources': summary.total_sources,
            'total_parsed_records': summary.total_parsed_records,
            'total_normalized_records': summary.total_normalized_records,
            'total_inserted_rows': summary.total_inserted_rows,
            'total_native_projected_rows': summary.total_native_projected_rows,
            'total_aligned_projected_rows': summary.total_aligned_projected_rows,
            'partition_results_json': json.dumps(
                [result.to_dict() for result in summary.partition_results],
                ensure_ascii=True,
                sort_keys=True,
            ),
        }
    )


@job(name='origo_etf_daily_ingest_job')
def origo_etf_daily_ingest_job() -> None:
    origo_etf_daily_ingest_step()


@job(name='origo_etf_daily_backfill_job')
def origo_etf_daily_backfill_job() -> None:
    origo_etf_daily_backfill_step()
