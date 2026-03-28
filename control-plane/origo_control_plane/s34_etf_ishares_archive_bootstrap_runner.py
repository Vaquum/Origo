from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from origo.scraper.contracts import (
    NormalizedMetricRecord,
    PersistedRawArtifact,
    RawArtifact,
    ScrapeRunContext,
    SourceDescriptor,
)
from origo.scraper.etf_adapters import ISharesIBITAdapter
from origo.scraper.object_store import persist_raw_artifact
from origo.scraper.retry import load_fetch_retry_policy_from_env, retry_with_backoff

_DATASET = 'etf_daily_metrics'
_ISHARES_SOURCE_ID = 'etf_ishares_ibit_daily'
_ISHARES_SOURCE_NAME = 'iShares Bitcoin Trust holdings CSV'
_ISHARES_SOURCE_URI = (
    'https://www.ishares.com/us/products/333011/fund/1467271812596.ajax'
    '?dataType=fund&fileName=IBIT_holdings&fileType=csv'
)
_ISHARES_HISTORY_START_DATE = date(2024, 1, 11)
_ISHARES_NO_DATA_MARKER = 'Fund Holdings as of,"-"'
_RAW_ARTIFACTS_PREFIX = 'raw-artifacts/'


@dataclass(frozen=True)
class _BootstrapSummary:
    control_run_id: str
    requested_start_date: str
    requested_end_date: str
    requested_weekday_count: int
    skipped_existing_days: tuple[str, ...]
    skipped_no_data_days: tuple[str, ...]
    persisted_days: tuple[str, ...]
    persisted_artifact_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            'dataset': _DATASET,
            'source_id': _ISHARES_SOURCE_ID,
            'control_run_id': self.control_run_id,
            'requested_start_date': self.requested_start_date,
            'requested_end_date': self.requested_end_date,
            'requested_weekday_count': self.requested_weekday_count,
            'skipped_existing_days': list(self.skipped_existing_days),
            'skipped_no_data_days': list(self.skipped_no_data_days),
            'persisted_days': list(self.persisted_days),
            'persisted_artifact_count': self.persisted_artifact_count,
        }


@dataclass(frozen=True)
class _ArchivedArtifactLoad:
    raw_artifact: RawArtifact
    persisted_artifact: PersistedRawArtifact


def _default_run_id() -> str:
    return f's34-etf-ishares-archive-bootstrap-{datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")}'


def _parse_iso_date_or_raise(*, value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f'Invalid {label}: {value}. Expected YYYY-MM-DD') from exc


def _iter_weekdays_inclusive(*, start_day: date, end_day: date) -> tuple[date, ...]:
    if start_day > end_day:
        raise RuntimeError(
            f'iShares archive bootstrap start_day must be <= end_day, got {start_day} > {end_day}'
        )
    days: list[date] = []
    current = start_day
    while current <= end_day:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return tuple(days)


def _set_query_param(*, uri: str, key: str, value: str) -> str:
    parsed = urlparse(uri)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params[key] = value
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(params),
            parsed.fragment,
        )
    )


def _build_ishares_source_for_day(*, run_context: ScrapeRunContext, request_day: date) -> SourceDescriptor:
    request_day_compact = request_day.strftime('%Y%m%d')
    return SourceDescriptor(
        source_id=_ISHARES_SOURCE_ID,
        source_name=_ISHARES_SOURCE_NAME,
        source_uri=_set_query_param(
            uri=_ISHARES_SOURCE_URI,
            key='asOfDate',
            value=request_day_compact,
        ),
        discovered_at_utc=run_context.started_at_utc,
        metadata={
            'issuer': 'ishares',
            'ticker': 'IBIT',
            'rights_source': 'ishares',
            'request_as_of_date': request_day.isoformat(),
            'request_as_of_date_compact': request_day_compact,
        },
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
            'ETF historical archive bootstrap requires boto3. Install with `uv add boto3`.'
        ) from exc
    client_factory = getattr(boto3_module, 'client', None)
    if client_factory is None:
        raise RuntimeError('boto3.client was not found')
    client = client_factory(
        's3',
        endpoint_url=_require_object_store_env_or_raise('ORIGO_OBJECT_STORE_ENDPOINT_URL'),
        aws_access_key_id=_require_object_store_env_or_raise(
            'ORIGO_OBJECT_STORE_ACCESS_KEY_ID'
        ),
        aws_secret_access_key=_require_object_store_env_or_raise(
            'ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY'
        ),
        region_name=_require_object_store_env_or_raise('ORIGO_OBJECT_STORE_REGION'),
    )
    return client, _require_object_store_env_or_raise('ORIGO_OBJECT_STORE_BUCKET')


def _load_all_raw_artifact_manifest_payloads_or_raise(
    *,
    client: Any,
    bucket: str,
) -> list[dict[str, Any]]:
    manifest_payloads: list[dict[str, Any]] = []
    request_kwargs: dict[str, Any] = {'Bucket': bucket, 'Prefix': _RAW_ARTIFACTS_PREFIX}
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
        raw_artifact=raw_artifact,
        persisted_artifact=persisted_artifact,
    )


def _partition_id_for_record_or_raise(record: NormalizedMetricRecord) -> str:
    return record.observed_at_utc.astimezone(UTC).date().isoformat()


def _load_existing_valid_ishares_archive_days_or_raise(
    *,
    run_context: ScrapeRunContext,
) -> tuple[str, ...]:
    adapter = ISharesIBITAdapter()
    client, bucket = _build_object_store_client_and_bucket_or_raise()
    manifest_payloads = _load_all_raw_artifact_manifest_payloads_or_raise(
        client=client,
        bucket=bucket,
    )
    valid_days: set[str] = set()
    for manifest_payload in manifest_payloads:
        if manifest_payload.get('source_id') != _ISHARES_SOURCE_ID:
            continue
        try:
            archived = _load_archived_raw_artifact_from_manifest_or_raise(
                client=client,
                bucket=bucket,
                manifest_payload=manifest_payload,
            )
            source = SourceDescriptor(
                source_id=_ISHARES_SOURCE_ID,
                source_name=_ISHARES_SOURCE_NAME,
                source_uri=archived.raw_artifact.source_uri,
                discovered_at_utc=run_context.started_at_utc,
                metadata={'issuer': 'ishares', 'ticker': 'IBIT', 'rights_source': 'ishares'},
            )
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
        except Exception:
            continue
        partition_ids = sorted(
            {_partition_id_for_record_or_raise(record) for record in normalized_records}
        )
        if len(partition_ids) != 1:
            continue
        valid_days.add(partition_ids[0])
    return tuple(sorted(valid_days))


def _validate_ishares_artifact_or_raise(
    *,
    adapter: ISharesIBITAdapter,
    artifact: RawArtifact,
    source: SourceDescriptor,
    run_context: ScrapeRunContext,
    request_day: date,
) -> str:
    csv_text = artifact.content.decode('utf-8-sig', errors='replace')
    if _ISHARES_NO_DATA_MARKER in csv_text:
        raise RuntimeError(f'no_data:{request_day.isoformat()}')
    parsed_records = list(
        adapter.parse(
            artifact=artifact,
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
    partition_ids = sorted(
        {_partition_id_for_record_or_raise(record) for record in normalized_records}
    )
    if partition_ids != [request_day.isoformat()]:
        raise RuntimeError(
            'iShares historical artifact partition mismatch: '
            f'request_day={request_day.isoformat()} partition_ids={partition_ids}'
        )
    return partition_ids[0]


def run_s34_etf_ishares_archive_bootstrap_or_raise(
    *,
    run_id: str,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    if start_date < _ISHARES_HISTORY_START_DATE:
        raise RuntimeError(
            'iShares historical archive bootstrap start_date is before supported history start: '
            f'start_date={start_date.isoformat()} supported_start_date={_ISHARES_HISTORY_START_DATE.isoformat()}'
        )
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f'etf-ishares-archive-bootstrap-{run_id}',
        started_at_utc=started_at_utc,
    )
    request_days = _iter_weekdays_inclusive(start_day=start_date, end_day=end_date)
    adapter = ISharesIBITAdapter()
    fetch_policy = load_fetch_retry_policy_from_env()
    existing_days = set(
        _load_existing_valid_ishares_archive_days_or_raise(run_context=run_context)
    )
    skipped_existing_days: list[str] = []
    skipped_no_data_days: list[str] = []
    persisted_days: list[str] = []

    for request_day in request_days:
        partition_id = request_day.isoformat()
        if partition_id in existing_days:
            skipped_existing_days.append(partition_id)
            continue
        source = _build_ishares_source_for_day(
            run_context=run_context,
            request_day=request_day,
        )
        artifact = retry_with_backoff(
            operation_name=f'{adapter.adapter_name}.fetch',
            operation=lambda: adapter.fetch(
                source=source,
                run_context=run_context,
            ),
            policy=fetch_policy,
        )
        try:
            validated_partition_id = _validate_ishares_artifact_or_raise(
                adapter=adapter,
                artifact=artifact,
                source=source,
                run_context=run_context,
                request_day=request_day,
            )
        except RuntimeError as exc:
            if str(exc) == f'no_data:{partition_id}':
                skipped_no_data_days.append(partition_id)
                continue
            raise
        persist_raw_artifact(
            artifact=artifact,
            run_context=run_context,
        )
        persisted_days.append(validated_partition_id)
        existing_days.add(validated_partition_id)

    summary = _BootstrapSummary(
        control_run_id=run_id,
        requested_start_date=start_date.isoformat(),
        requested_end_date=end_date.isoformat(),
        requested_weekday_count=len(request_days),
        skipped_existing_days=tuple(sorted(skipped_existing_days)),
        skipped_no_data_days=tuple(sorted(skipped_no_data_days)),
        persisted_days=tuple(sorted(persisted_days)),
        persisted_artifact_count=len(persisted_days),
    )
    return summary.to_dict()


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m origo_control_plane.s34_etf_ishares_archive_bootstrap_runner',
        description='Bootstrap iShares ETF historical raw artifacts into the Slice-34 object-store archive.',
    )
    parser.add_argument(
        '--run-id',
        default=_default_run_id(),
        help='Operator-facing control run id.',
    )
    parser.add_argument(
        '--start-date',
        default=_ISHARES_HISTORY_START_DATE.isoformat(),
        help='Inclusive iShares historical start date (YYYY-MM-DD).',
    )
    parser.add_argument(
        '--end-date',
        default=datetime.now(UTC).date().isoformat(),
        help='Inclusive iShares historical end date (YYYY-MM-DD).',
    )
    return parser


def main() -> None:
    args = _build_argument_parser().parse_args()
    print(
        json.dumps(
            run_s34_etf_ishares_archive_bootstrap_or_raise(
                run_id=args.run_id,
                start_date=_parse_iso_date_or_raise(
                    value=args.start_date,
                    label='--start-date',
                ),
                end_date=_parse_iso_date_or_raise(
                    value=args.end_date,
                    label='--end-date',
                ),
            ),
            ensure_ascii=True,
            sort_keys=True,
        )
    )


if __name__ == '__main__':
    main()
