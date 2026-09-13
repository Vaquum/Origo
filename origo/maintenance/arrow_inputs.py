"""Use the Parquet publisher's atomic file generations before starting Arrow runs."""

import hashlib
import json
import os
from pathlib import Path

from dagster import AssetKey, DagsterRunStatus, RunRequest, RunsFilter, RunStatusSensorContext
from dagster._core.event_api import AssetRecordsFilter

from origo.assets.publish_binance_spot_klines_to_mount import (
    DEFAULT_MOUNT_DIR,
    SPECS,
    MountKlineSpec,
)


def source_identity(spec: MountKlineSpec, root: Path) -> str | None:
    files = sorted((root / spec.sub_path).glob('*/*.parquet'))
    if not files:
        return None
    generations: list[tuple[str, int, int, int]] = []
    for path in files:
        stat = path.stat()
        generations.append(
            (str(path.relative_to(root)), stat.st_ino, stat.st_size, stat.st_mtime_ns)
        )
    return hashlib.sha256(json.dumps(generations, separators=(',', ':')).encode()).hexdigest()


def changed_arrow_requests(context: RunStatusSensorContext) -> list[RunRequest]:
    root = Path(os.environ.get('LOCAL_PARQUET_DIR', DEFAULT_MOUNT_DIR))
    requests: list[RunRequest] = []
    for spec in SPECS:
        identity = source_identity(spec, root)
        if identity is None:
            continue
        latest = context.instance.fetch_materializations(
            AssetRecordsFilter(
                asset_key=AssetKey('build_bar_store_arrow'), asset_partitions=[spec.name]
            ),
            limit=1,
        ).records
        if latest:
            materialization = latest[0].asset_materialization
            if materialization is not None:
                recorded = materialization.metadata.get('source_identity')
                if recorded is not None and recorded.value == identity:
                    continue
        runs = context.instance.get_runs(
            RunsFilter(
                job_name='build_bar_store_arrow_job',
                tags={'dagster/partition': spec.name, 'origo_arrow_input': identity},
            ),
            limit=1,
        )
        attempt = 0
        if runs:
            run = runs[0]
            if run.status not in (DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED):
                continue
            attempt = int(run.tags['origo_arrow_attempt']) + 1
        requests.append(
            RunRequest(
                partition_key=spec.name,
                run_key=f'{spec.name}:{identity}:{attempt}',
                tags={'origo_arrow_input': identity, 'origo_arrow_attempt': str(attempt)},
            )
        )
    context.log.info('Arrow source check: series=%s changed_or_retry=%s', len(SPECS), len(requests))
    return requests
