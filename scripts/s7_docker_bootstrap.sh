#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SLICE_DIR="${ROOT_DIR}/spec/slices/slice-7-docker-local-platform"
SEED_JSON="${SLICE_DIR}/seed-ingest-results.json"
MIGRATIONS_LOG="${SLICE_DIR}/migrations.log"
BOOTSTRAP_LOG="${SLICE_DIR}/bootstrap.log"

mkdir -p "${SLICE_DIR}"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  echo "ERROR: ${ROOT_DIR}/.env is required." >&2
  exit 1
fi

set -a
source "${ROOT_DIR}/.env"
set +a

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "ERROR: ${name} must be set and non-empty." >&2
    exit 1
  fi
}

required_vars=(
  ORIGO_DOCKER_CLICKHOUSE_HOST
  ORIGO_DOCKER_CLICKHOUSE_PORT
  ORIGO_DOCKER_CLICKHOUSE_HTTP_PORT
  ORIGO_DOCKER_CLICKHOUSE_USER
  ORIGO_DOCKER_CLICKHOUSE_PASSWORD
  ORIGO_DOCKER_CLICKHOUSE_DATABASE
  ORIGO_DOCKER_DAGSTER_HOST
  ORIGO_DOCKER_DAGSTER_PORT
  ORIGO_DOCKER_API_PORT
  ORIGO_DOCKER_DAGSTER_GRAPHQL_URL
  ORIGO_DOCKER_API_BASE_URL
  ORIGO_DOCKER_EXPORT_ROOT_DIR
  ORIGO_DOCKER_ETF_ANOMALY_LOG_PATH
  ORIGO_INTERNAL_API_KEY
  ORIGO_QUERY_MAX_CONCURRENCY
  ORIGO_QUERY_MAX_QUEUE
  ORIGO_ALIGNED_QUERY_MAX_CONCURRENCY
  ORIGO_ALIGNED_QUERY_MAX_QUEUE
  ORIGO_ALIGNED_FRESHNESS_MAX_AGE_SECONDS
  ORIGO_ETF_QUERY_SERVING_STATE
  ORIGO_FRED_QUERY_SERVING_STATE
  ORIGO_ETF_DAILY_STALE_MAX_AGE_DAYS
  ORIGO_FRED_SOURCE_PUBLISH_STALE_MAX_AGE_DAYS
  ORIGO_FRED_ALERT_AUDIT_LOG_PATH
  ORIGO_FRED_DISCORD_WEBHOOK_URL
  ORIGO_FRED_DISCORD_TIMEOUT_SECONDS
  ORIGO_DAGSTER_REPOSITORY_NAME
  ORIGO_DAGSTER_LOCATION_NAME
  ORIGO_DAGSTER_EXPORT_JOB_NAME
  ORIGO_ETF_DAILY_SCHEDULE_CRON
  ORIGO_ETF_DAILY_SCHEDULE_TIMEZONE
  ORIGO_ETF_DAILY_RETRY_MAX_ATTEMPTS
  ORIGO_ETF_DAILY_RETRY_SENSOR_MIN_INTERVAL_SECONDS
  ORIGO_ETF_DISCORD_WEBHOOK_URL
  ORIGO_ETF_ALERT_SENSOR_MIN_INTERVAL_SECONDS
  ORIGO_ETF_DISCORD_TIMEOUT_SECONDS
  ORIGO_EXPORT_MAX_CONCURRENCY
  ORIGO_EXPORT_MAX_QUEUE
  ORIGO_SOURCE_RIGHTS_MATRIX_PATH
  ORIGO_EXPORT_AUDIT_LOG_PATH
)

for env_name in "${required_vars[@]}"; do
  require_env "${env_name}"
done

cd "${ROOT_DIR}"

{
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] docker compose down -v --remove-orphans"
  docker compose down -v --remove-orphans
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] docker compose up -d --build"
  docker compose up -d --build
} | tee "${BOOTSTRAP_LOG}"

wait_healthy() {
  local service="$1"
  local max_attempts=120
  local attempt=1
  while (( attempt <= max_attempts )); do
    local container_id
    container_id="$(docker compose ps -q "${service}")"
    if [[ -n "${container_id}" ]]; then
      local health
      health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
      if [[ "${health}" == "healthy" || "${health}" == "running" ]]; then
        echo "Service ${service} is ${health}."
        return 0
      fi
    fi
    sleep 2
    ((attempt++))
  done
  echo "ERROR: Service ${service} failed to become healthy." >&2
  docker compose logs "${service}" >&2
  return 1
}

wait_healthy clickhouse
wait_healthy dagster-webserver
wait_healthy api

docker compose exec -T dagster-webserver \
  python -m origo_control_plane.migrations.cli migrate > "${MIGRATIONS_LOG}"

docker compose exec -T dagster-webserver python - <<'PY' > "${SEED_JSON}"
import json
from dagster import build_asset_context
from origo_control_plane.assets.daily_trades_to_origo import insert_daily_binance_trades_to_origo

results = []
for day in ('2017-08-17', '2017-08-18'):
    context = build_asset_context(partition_key=day)
    result = insert_daily_binance_trades_to_origo(context)
    result['partition_day'] = day
    results.append(result)

print(json.dumps({'ingest_results': results}, sort_keys=True))
PY

docker compose ps > "${SLICE_DIR}/docker-compose-ps.txt"
