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
for service in book-capture-spot book-capture-perp; do
  ids="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
  if [ -n "$ids" ]; then
    docker stop $ids </dev/null
    docker rm $ids </dev/null
  fi
  test -z "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
done
docker exec -i "$controller" python - <<'PYTHON'
import json,time
from datetime import datetime,timezone,timedelta
from origo.assets.create_origo_database import get_clickhouse_settings,make_clickhouse_client
from origo.sources.adapters.book_local import book_root,_revision_key
from origo.sources.adapters.book_spool import sealed_minutes
from origo.sources.registry import SOURCE_REGISTRY
settings=get_clickhouse_settings();client=make_clickhouse_client(settings)
boundary=datetime.now(timezone.utc)
try:
    for spec in SOURCE_REGISTRY:
        if spec.key not in ('binance_spot_book','binance_perp_book'):
            continue
        market='spot' if '_spot_' in spec.key else 'perp'
        seals=sealed_minutes(book_root(),market,spec.partitions.start,boundary)
        pending={m['minute_start'].replace('+00:00','Z'):_revision_key((m,)) for m in seals}
        deadline=time.monotonic()+180
        first=True
        while pending:
            selection='' if first else ' AND partition_key IN %(keys)s'
            rows=client.execute('SELECT partition_key,argMax(revision,generation) FROM '+settings.database+
                '.source_activation_log WHERE source_key=%(source)s AND provisional'+selection+' GROUP BY partition_key',
                {'source':spec.key,'keys':tuple(pending)},
                settings={'readonly':1,'max_threads':1,'max_execution_time':15,'max_memory_usage':268435456,
                          'log_comment':'legacy-book-writer-drain'})
            accepted=dict(rows)
            pending={key:revision for key,revision in pending.items() if accepted.get(key)!=revision}
            first=False
            if pending:
                if time.monotonic()>=deadline:
                    raise RuntimeError('Sealed native book minutes remain unaccepted: '+str(sorted(pending)))
                time.sleep(5)
        print(json.dumps({'source':spec.key,'sealed_minutes_drained':len(seals),'boundary':boundary.isoformat()}),flush=True)
    minute=boundary.replace(second=0,microsecond=0)-timedelta(minutes=1)
    deadline=time.monotonic()+180
    while True:
        tail={table:client.execute('SELECT count() FROM '+settings.database+'.'+table+
            ' WHERE datetime>=%(start)s AND datetime<%(end)s',
            {'start':minute,'end':minute+timedelta(minutes=1)},
            settings={'readonly':1,'max_threads':1,'max_execution_time':15,
                      'log_comment':'legacy-book-writer-drain'})[0][0]
            for table in ('binance_spot_depth20_snapshots','binance_spot_depth20_1m')}
        if all(tail.values()):
            break
        if time.monotonic()>=deadline:
            raise RuntimeError('Legacy Spot20 boundary minute has not reached raw and projection: '+str(tail))
        time.sleep(5)
    print(json.dumps({'legacy_spot20_minute':minute.isoformat(),'stored_rows':tail}),flush=True)
finally:
    client.disconnect()
PYTHON
for service in provisional-binance-spot-book provisional-binance-perp-book depth-worker; do
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
