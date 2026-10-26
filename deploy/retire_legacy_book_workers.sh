#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_NAME:?PROJECT_NAME is required}"
controller="$(docker ps -q --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=dagster)"
test -n "$controller"
docker exec -i "$controller" python - <<'PY'
from dagster import DagsterInstance
from origo.sources.contracts import RolloutStage
from origo.sources.prepare import retire_book_admission
from origo.sources.registry import SOURCE_REGISTRY
sources = [s for s in SOURCE_REGISTRY if s.key in ('binance_spot_book', 'binance_perp_book')]
assert len(sources) == 2 and all(s.rollout_stage == RolloutStage.DORMANT for s in sources)
with DagsterInstance.get() as instance:
    retire_book_admission(instance)
PY
for service in depth-worker provisional-binance-spot-book provisional-binance-perp-book book-capture-spot book-capture-perp; do
  ids="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
  if [ -n "$ids" ]; then
    docker stop $ids </dev/null
    docker rm $ids </dev/null
  fi
  test -z "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
done
monitor="$(docker ps -q --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=monitor)"
heartbeats="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.volume=worker-heartbeats)"
test -n "$monitor" && test -n "$heartbeats"
image="$(docker inspect --format '{{.Image}}' "$monitor")"
docker run --rm --pull=never --network none \
  --mount "type=volume,source=$heartbeats,target=/heartbeats" --entrypoint rm "$image" -f \
  /heartbeats/depth.heartbeat /heartbeats/book_capture_spot.heartbeat /heartbeats/book_capture_perp.heartbeat \
  /heartbeats/provisional_binance_spot_book.heartbeat /heartbeats/provisional_binance_perp_book.heartbeat
