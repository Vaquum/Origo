#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SLICE_DIR="${ROOT_DIR}/spec/slices/slice-7-docker-local-platform"
PROOF_JSON="${SLICE_DIR}/proof-s7-local-docker.json"
SEED_JSON="${SLICE_DIR}/seed-ingest-results.json"
BASELINE_JSON="${SLICE_DIR}/baseline-fixture-2017-08-17_2017-08-18.json"
SMOKE_RESULT_JSON="${SLICE_DIR}/smoke-result.json"

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
  ORIGO_DOCKER_DAGSTER_GRAPHQL_URL
  ORIGO_DOCKER_API_BASE_URL
  ORIGO_INTERNAL_API_KEY
)

for env_name in "${required_vars[@]}"; do
  require_env "${env_name}"
done

CURRENT_STEP='bootstrap'

collect_runtime_evidence() {
  if ! docker compose ps > "${SLICE_DIR}/docker-compose-ps.txt"; then
    echo 'ERROR: failed to collect docker compose ps output.' >&2
  fi
  if ! docker compose logs --no-color --tail 300 > "${SLICE_DIR}/docker-compose-tail.log"; then
    echo 'ERROR: failed to collect docker compose logs output.' >&2
  fi
}

on_exit() {
  local exit_code=$?
  collect_runtime_evidence
  if (( exit_code != 0 )); then
    cat > "${SMOKE_RESULT_JSON}" <<EOF
{"status":"failed","failed_step":"${CURRENT_STEP}","exit_code":${exit_code}}
EOF
  fi
  exit "${exit_code}"
}

trap on_exit EXIT

cd "${ROOT_DIR}"

"${ROOT_DIR}/scripts/s7_docker_bootstrap.sh"

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

CURRENT_STEP='api-query-export-proof'
export ROOT_DIR SLICE_DIR PROOF_JSON SEED_JSON BASELINE_JSON ORIGO_DOCKER_DAGSTER_GRAPHQL_URL ORIGO_DOCKER_API_BASE_URL ORIGO_INTERNAL_API_KEY
python3 - <<'PY'
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

root = Path(os.environ['ROOT_DIR'])
slice_dir = Path(os.environ['SLICE_DIR'])
seed_path = Path(os.environ['SEED_JSON'])
proof_path = Path(os.environ['PROOF_JSON'])
baseline_path = Path(os.environ['BASELINE_JSON'])

api_base_url = os.environ['ORIGO_DOCKER_API_BASE_URL'].rstrip('/')
api_key = os.environ['ORIGO_INTERNAL_API_KEY']


class ApiError(RuntimeError):
    pass


def _request_json(method: str, path: str, payload: dict | None = None) -> dict:
    url = f'{api_base_url}{path}'
    body_bytes = None
    headers = {
        'X-API-Key': api_key,
        'Content-Type': 'application/json',
    }
    if payload is not None:
        body_bytes = json.dumps(payload, separators=(',', ':')).encode('utf-8')
    request = urllib.request.Request(url=url, data=body_bytes, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode('utf-8')
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        raise ApiError(f'HTTP {exc.code} {path}: {detail}') from exc


def _canonical_timestamp_ms(raw_value: int) -> int:
    if raw_value >= 10**15:
        return raw_value // 1000
    return raw_value


def _fingerprint(*, rows: list[dict], day_start_iso: str) -> dict[str, object]:
    if not rows:
        raise RuntimeError('Expected non-empty rows for fingerprint generation')

    day_start = datetime.fromisoformat(day_start_iso.replace('Z', '+00:00'))
    day_end = day_start + timedelta(days=1)
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int(day_end.timestamp() * 1000)

    canonical_rows = []
    quantity_sum = 0.0
    quote_quantity_sum = 0.0
    timestamps_ms: list[int] = []
    trade_ids: list[int] = []

    for row in rows:
        trade_id = int(row['trade_id'])
        timestamp_ms = _canonical_timestamp_ms(int(row['timestamp']))
        quantity = float(row['quantity'])
        quote_quantity = float(row['quote_quantity'])
        timestamps_ms.append(timestamp_ms)
        trade_ids.append(trade_id)
        quantity_sum += quantity
        quote_quantity_sum += quote_quantity
        canonical_rows.append(
            {
                'trade_id': trade_id,
                'timestamp_ms': timestamp_ms,
                'quantity': quantity,
                'quote_quantity': quote_quantity,
            }
        )

    canonical_rows.sort(key=lambda item: (item['timestamp_ms'], item['trade_id']))
    canonical_blob = json.dumps(canonical_rows, sort_keys=True, separators=(',', ':'))

    return {
        'row_count': len(canonical_rows),
        'first_trade_id': min(trade_ids),
        'last_trade_id': max(trade_ids),
        'first_event_offset_ms': min(timestamps_ms) - day_start_ms,
        'last_event_offset_ms': day_end_ms - max(timestamps_ms),
        'quantity_sum': round(quantity_sum, 12),
        'quote_quantity_sum': round(quote_quantity_sum, 12),
        'rows_hash_sha256': hashlib.sha256(canonical_blob.encode('utf-8')).hexdigest(),
    }


def _run_native_query(day: str) -> dict:
    day_start = f'{day}T00:00:00Z'
    day_end_dt = datetime.fromisoformat(f'{day}T00:00:00+00:00') + timedelta(days=1)
    day_end = day_end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return _request_json(
        'POST',
        '/v1/raw/query',
        {
            'mode': 'native',
            'sources': ['binance_spot_trades'],
            'fields': ['trade_id', 'timestamp', 'quantity', 'quote_quantity'],
            'time_range': [day_start, day_end],
            'strict': False,
        },
    )


def _run_aligned_query(day: str) -> dict:
    day_start = f'{day}T00:00:00Z'
    day_end_dt = datetime.fromisoformat(f'{day}T00:00:00+00:00') + timedelta(days=1)
    day_end = day_end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return _request_json(
        'POST',
        '/v1/raw/query',
        {
            'mode': 'aligned_1s',
            'sources': ['binance_spot_trades'],
            'fields': ['aligned_at_utc', 'open_price', 'close_price', 'trade_count'],
            'time_range': [day_start, day_end],
            'strict': False,
        },
    )


def _submit_export(mode: str, export_format: str, start: str, end: str) -> dict:
    payload = {
        'mode': mode,
        'format': export_format,
        'dataset': 'binance_spot_trades',
        'time_range': [start, end],
        'strict': False,
    }
    return _request_json('POST', '/v1/raw/export', payload)


def _poll_export(export_id: str) -> dict:
    deadline = time.time() + 180
    while time.time() < deadline:
        status_payload = _request_json('GET', f'/v1/raw/export/{export_id}')
        status = status_payload['status']
        if status == 'succeeded':
            return status_payload
        if status == 'failed':
            raise RuntimeError(f'Export failed: {json.dumps(status_payload, sort_keys=True)}')
        time.sleep(2)
    raise RuntimeError(f'Export polling timed out for export_id={export_id}')


seed_payload = json.loads(seed_path.read_text(encoding='utf-8'))
seed_results: list[dict] = seed_payload['ingest_results']
seed_by_day = {entry['partition_day']: entry for entry in seed_results}

run1_by_day: dict[str, dict] = {}
run2_by_day: dict[str, dict] = {}
for day in ('2017-08-17', '2017-08-18'):
    run1 = _run_native_query(day)
    run2 = _run_native_query(day)
    run1_by_day[day] = _fingerprint(rows=run1['rows'], day_start_iso=f'{day}T00:00:00Z')
    run2_by_day[day] = _fingerprint(rows=run2['rows'], day_start_iso=f'{day}T00:00:00Z')

aligned_query_payload = _run_aligned_query('2017-08-17')

native_export_submit = _submit_export(
    'native',
    'parquet',
    '2017-08-17T00:00:00Z',
    '2017-08-19T00:00:00Z',
)
native_export_status = _poll_export(native_export_submit['export_id'])

aligned_export_submit = _submit_export(
    'aligned_1s',
    'csv',
    '2017-08-17T00:00:00Z',
    '2017-08-18T00:00:00Z',
)
aligned_export_status = _poll_export(aligned_export_submit['export_id'])

for status_payload in (native_export_status, aligned_export_status):
    artifact = status_payload.get('artifact')
    if artifact is None:
        raise RuntimeError(f'Missing artifact metadata in export status: {status_payload}')
    artifact_uri = artifact.get('uri')
    if not isinstance(artifact_uri, str) or artifact_uri.strip() == '':
        raise RuntimeError(f'Invalid export artifact URI: {artifact_uri}')
    subprocess.run(
        ['docker', 'compose', 'exec', '-T', 'api', 'test', '-f', artifact_uri],
        check=True,
        cwd=root,
    )

proof_payload = {
    'proof_timestamp_utc': datetime.now(timezone.utc).isoformat(),
    'stack': {
        'api_base_url': api_base_url,
        'dagster_graphql_url': os.environ['ORIGO_DOCKER_DAGSTER_GRAPHQL_URL'],
    },
    'seed_days': ['2017-08-17', '2017-08-18'],
    'native_query_replay_deterministic': run1_by_day == run2_by_day,
    'aligned_query_row_count': int(aligned_query_payload['row_count']),
    'native_export': {
        'export_id': native_export_submit['export_id'],
        'status': native_export_status['status'],
        'artifact': native_export_status['artifact'],
    },
    'aligned_export': {
        'export_id': aligned_export_submit['export_id'],
        'status': aligned_export_status['status'],
        'artifact': aligned_export_status['artifact'],
    },
}

proof_path.write_text(
    json.dumps(proof_payload, indent=2, sort_keys=True),
    encoding='utf-8',
)

source_checksums = []
for day in ('2017-08-17', '2017-08-18'):
    day_seed = seed_by_day[day]
    source_checksums.append(
        {
            'day': day,
            'zip_sha256': day_seed['zip_checksum'],
            'csv_sha256': day_seed['csv_checksum'],
            'rows_inserted': day_seed['rows_inserted'],
        }
    )

baseline_payload = {
    'fixture_window': {
        'start_utc': '2017-08-17T00:00:00Z',
        'end_utc': '2017-08-19T00:00:00Z',
        'days': ['2017-08-17', '2017-08-18'],
    },
    'source_checksums': source_checksums,
    'run_1_fingerprints': run1_by_day,
    'run_2_fingerprints': run2_by_day,
    'deterministic_match': run1_by_day == run2_by_day,
    'column_key': {
        'row_count': 'Number of rows returned for the exact day window.',
        'first_trade_id': 'Minimum trade_id in the day window.',
        'last_trade_id': 'Maximum trade_id in the day window.',
        'first_event_offset_ms': 'First event timestamp offset in milliseconds from day start.',
        'last_event_offset_ms': 'Milliseconds from last event timestamp to day end.',
        'quantity_sum': 'Sum of quantity across rows in the day window.',
        'quote_quantity_sum': 'Sum of quote_quantity across rows in the day window.',
        'rows_hash_sha256': 'SHA256 of canonicalized (timestamp_ms, trade_id, quantity, quote_quantity) row set.',
    },
}

baseline_path.write_text(
    json.dumps(baseline_payload, indent=2, sort_keys=True),
    encoding='utf-8',
)
PY

CURRENT_STEP='restart-persistence-proof'
docker compose restart clickhouse dagster-webserver dagster-daemon api
wait_healthy clickhouse
wait_healthy dagster-webserver
wait_healthy api

CURRENT_STEP='restart-fingerprint-compare'
export ROOT_DIR SLICE_DIR PROOF_JSON BASELINE_JSON ORIGO_DOCKER_API_BASE_URL ORIGO_INTERNAL_API_KEY
python3 - <<'PY'
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

proof_path = Path(os.environ['PROOF_JSON'])
baseline_path = Path(os.environ['BASELINE_JSON'])
api_base_url = os.environ['ORIGO_DOCKER_API_BASE_URL'].rstrip('/')
api_key = os.environ['ORIGO_INTERNAL_API_KEY']


def _request_json(payload: dict) -> dict:
    request = urllib.request.Request(
        url=f'{api_base_url}/v1/raw/query',
        data=json.dumps(payload, separators=(',', ':')).encode('utf-8'),
        method='POST',
        headers={
            'X-API-Key': api_key,
            'Content-Type': 'application/json',
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        raise RuntimeError(f'HTTP {exc.code}: {detail}') from exc


def _canonical_timestamp_ms(raw_value: int) -> int:
    if raw_value >= 10**15:
        return raw_value // 1000
    return raw_value


def _fingerprint(rows: list[dict], day: str) -> dict[str, object]:
    day_start = datetime.fromisoformat(f'{day}T00:00:00+00:00')
    day_end = day_start + timedelta(days=1)
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int(day_end.timestamp() * 1000)

    quantity_sum = 0.0
    quote_quantity_sum = 0.0
    trade_ids: list[int] = []
    timestamps: list[int] = []
    canonical_rows: list[dict[str, object]] = []

    for row in rows:
        trade_id = int(row['trade_id'])
        timestamp = _canonical_timestamp_ms(int(row['timestamp']))
        quantity = float(row['quantity'])
        quote_quantity = float(row['quote_quantity'])
        trade_ids.append(trade_id)
        timestamps.append(timestamp)
        quantity_sum += quantity
        quote_quantity_sum += quote_quantity
        canonical_rows.append(
            {
                'trade_id': trade_id,
                'timestamp_ms': timestamp,
                'quantity': quantity,
                'quote_quantity': quote_quantity,
            }
        )

    canonical_rows.sort(key=lambda item: (int(item['timestamp_ms']), int(item['trade_id'])))
    canonical_blob = json.dumps(canonical_rows, sort_keys=True, separators=(',', ':'))

    import hashlib

    return {
        'row_count': len(canonical_rows),
        'first_trade_id': min(trade_ids),
        'last_trade_id': max(trade_ids),
        'first_event_offset_ms': min(timestamps) - day_start_ms,
        'last_event_offset_ms': day_end_ms - max(timestamps),
        'quantity_sum': round(quantity_sum, 12),
        'quote_quantity_sum': round(quote_quantity_sum, 12),
        'rows_hash_sha256': hashlib.sha256(canonical_blob.encode('utf-8')).hexdigest(),
    }


run_3: dict[str, dict[str, object]] = {}
for day in ('2017-08-17', '2017-08-18'):
    start_iso = f'{day}T00:00:00Z'
    end_iso = (
        datetime.fromisoformat(f'{day}T00:00:00+00:00') + timedelta(days=1)
    ).strftime('%Y-%m-%dT%H:%M:%SZ')
    payload = _request_json(
        {
            'mode': 'native',
            'sources': ['binance_spot_trades'],
            'fields': ['trade_id', 'timestamp', 'quantity', 'quote_quantity'],
            'time_range': [start_iso, end_iso],
            'strict': False,
        }
    )
    run_3[day] = _fingerprint(payload['rows'], day)

proof_payload = json.loads(proof_path.read_text(encoding='utf-8'))
baseline_payload = json.loads(baseline_path.read_text(encoding='utf-8'))
run_1 = baseline_payload['run_1_fingerprints']

post_restart_match = run_3 == run_1
proof_payload['post_restart_fingerprints'] = run_3
proof_payload['post_restart_matches_run_1'] = post_restart_match
proof_payload['deterministic_match_full'] = bool(
    baseline_payload.get('deterministic_match', False) and post_restart_match
)
proof_path.write_text(
    json.dumps(proof_payload, indent=2, sort_keys=True),
    encoding='utf-8',
)

baseline_payload['run_3_post_restart_fingerprints'] = run_3
baseline_payload['deterministic_match'] = bool(
    baseline_payload.get('deterministic_match', False) and post_restart_match
)
baseline_path.write_text(
    json.dumps(baseline_payload, indent=2, sort_keys=True),
    encoding='utf-8',
)
PY

CURRENT_STEP='complete'
cat > "${SMOKE_RESULT_JSON}" <<EOF
{"status":"passed","failed_step":null,"exit_code":0}
EOF
