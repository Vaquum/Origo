from __future__ import annotations

import importlib
import json
import os
from datetime import UTC, datetime
from typing import Any

from .contracts import PersistedRawArtifact, RawArtifact, ScrapeRunContext


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _load_boto3_client() -> Any:
    try:
        boto3_module = importlib.import_module('boto3')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'Object-store persistence requires boto3. Install with `uv add boto3`.'
        ) from exc
    client_factory = getattr(boto3_module, 'client', None)
    if client_factory is None:
        raise RuntimeError('boto3.client was not found')
    return client_factory


def _build_object_keys(
    *,
    run_id: str,
    source_id: str,
    artifact_id: str,
    extension: str,
) -> tuple[str, str]:
    base = f'raw-artifacts/{run_id}/{source_id}/{artifact_id}'
    artifact_key = f'{base}/artifact.{extension}'
    manifest_key = f'{base}/manifest.json'
    return artifact_key, manifest_key


def persist_raw_artifact(
    *,
    artifact: RawArtifact,
    run_context: ScrapeRunContext,
) -> PersistedRawArtifact:
    endpoint_url = _require_env('ORIGO_OBJECT_STORE_ENDPOINT_URL')
    access_key_id = _require_env('ORIGO_OBJECT_STORE_ACCESS_KEY_ID')
    secret_access_key = _require_env('ORIGO_OBJECT_STORE_SECRET_ACCESS_KEY')
    bucket = _require_env('ORIGO_OBJECT_STORE_BUCKET')
    region = _require_env('ORIGO_OBJECT_STORE_REGION')

    extension_map = {
        'html': 'html',
        'json': 'json',
        'csv': 'csv',
        'pdf': 'pdf',
        'binary': 'bin',
    }
    extension = extension_map[artifact.artifact_format]
    artifact_key, manifest_key = _build_object_keys(
        run_id=run_context.run_id,
        source_id=artifact.source_id,
        artifact_id=artifact.artifact_id,
        extension=extension,
    )

    client_factory = _load_boto3_client()
    client = client_factory(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )

    client.put_object(
        Bucket=bucket,
        Key=artifact_key,
        Body=artifact.content,
    )

    persisted_at = datetime.now(UTC)
    manifest = {
        'artifact_id': artifact.artifact_id,
        'source_id': artifact.source_id,
        'source_uri': artifact.source_uri,
        'fetch_method': artifact.fetch_method,
        'artifact_format': artifact.artifact_format,
        'content_sha256': artifact.content_sha256,
        'fetched_at_utc': artifact.fetched_at_utc.isoformat(),
        'persisted_at_utc': persisted_at.isoformat(),
        'metadata': artifact.metadata,
        'run_id': run_context.run_id,
        'artifact_key': artifact_key,
    }
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode(
            'utf-8'
        ),
        ContentType='application/json',
    )

    return PersistedRawArtifact(
        artifact_id=artifact.artifact_id,
        storage_uri=f's3://{bucket}/{artifact_key}',
        manifest_uri=f's3://{bucket}/{manifest_key}',
        persisted_at_utc=persisted_at,
    )
