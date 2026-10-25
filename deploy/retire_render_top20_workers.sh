#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_NAME:?PROJECT_NAME is required}"

controller="$(docker ps -q --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=dagster)"
test -n "$controller"
docker exec -i "$controller" python - <<'PY'
from dataclasses import replace

from dagster import DagsterInstance, DagsterRunStatus, RunsFilter

from origo.sources.binance_top20 import top20_source
from origo.sources.contracts import RolloutStage
from origo.sources.prepare import backfill_active, prepare_source


def pause_top20_admission(instance: DagsterInstance) -> None:
    specs = tuple(top20_source(market) for market in ('spot', 'perp'))
    for spec in specs:
        prepare_source(replace(
            spec, rollout_stage=RolloutStage.DORMANT,
            consumers=tuple(replace(consumer, public=False) for consumer in spec.consumers),
        ), instance)
    active = instance.get_runs(RunsFilter(statuses=[
        DagsterRunStatus.NOT_STARTED, DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING,
    ]))
    for spec in specs:
        if backfill_active(instance, spec) or any(
            spec.key in run.job_name
            or spec.key in (run.tags.get('origo_source_key'), run.tags.get('origo_projection_source_key'))
            or any(spec.key in key.to_user_string() for key in (run.asset_selection or ()))
            for run in active
        ):
            raise RuntimeError(f'{spec.key} work remains active; drain its native runs before rollback.')


if __name__ == '__main__':
    with DagsterInstance.get() as instance:
        pause_top20_admission(instance)
PY

for service in provisional-binance-spot-top20 provisional-binance-perp-top20; do
  ids="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
  if [ -n "$ids" ]; then
    docker stop $ids </dev/null
    docker rm $ids </dev/null
  fi
  remaining="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
  if [ -n "$remaining" ]; then
    echo "Render book worker remains: $service; rollback refused" >&2
    exit 1
  fi
done

monitor="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=monitor)"
heartbeats="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.volume=worker-heartbeats)"
test -n "$monitor"
test -n "$heartbeats"
image="$(docker inspect --format '{{.Image}}' "$monitor")"
docker run --rm --pull=never --network none \
  --mount "type=volume,source=$heartbeats,target=/heartbeats" --entrypoint rm "$image" -f \
  /heartbeats/provisional_binance_spot_top20_v1.heartbeat \
  /heartbeats/provisional_binance_perp_top20_v1.heartbeat
