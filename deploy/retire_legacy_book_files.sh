#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_NAME:?PROJECT_NAME is required}"
: "${DEPLOY_DIR:?DEPLOY_DIR is required}"
for service in depth-worker book-capture-spot book-capture-perp provisional-binance-spot-book provisional-binance-perp-book; do
  test -z "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter "label=com.docker.compose.service=$service")"
done
controller="$(docker ps -q --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=dagster)"
test -n "$controller"
docker exec -i "$controller" python - <<'PYTHON'
import shutil
from pathlib import Path

from dagster import DagsterInstance

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.binance_top20 import BINANCE_SPOT_TOP20_SPEC, BINANCE_PERP_TOP20_SPEC
from origo.sources.prepare import retire_book_admission
from origo.sources.registry import SOURCE_REGISTRY

assert not {'binance_spot_book', 'binance_perp_book'} & {s.key for s in SOURCE_REGISTRY}
with DagsterInstance.get() as instance:
    retire_book_admission(instance)
settings = get_clickhouse_settings()
client = make_clickhouse_client(settings)
try:
    retired = tuple(name for spec in (BINANCE_SPOT_TOP20_SPEC, BINANCE_PERP_TOP20_SPEC)
                    for name in spec.retired_tables)
    assert len(retired) == 36
    assert not client.execute('SELECT name FROM system.tables WHERE database=%(database)s '
                              'AND name IN %(names)s',
                              {'database': settings.database, 'names': retired},
                              settings={'readonly': 1, 'max_threads': 1, 'max_execution_time': 10})
finally:
    client.disconnect()
for path in (
    '/var/lib/origo-book-spool/spot', '/var/lib/origo-book-spool/perp',
    '/opt/arrow/depth20_snapshots', '/opt/arrow/depth200_snapshots',
):
    target = Path(path)
    if target.exists():
        shutil.rmtree(target)
for name in (
    'depth.heartbeat', 'book_capture_spot.heartbeat', 'book_capture_perp.heartbeat',
    'provisional_binance_spot_book.heartbeat', 'provisional_binance_perp_book.heartbeat',
    'book_capture_spot.status.json', 'book_capture_perp.status.json',
):
    (Path('/opt/origo/heartbeats') / name).unlink(missing_ok=True)
print('Retired book files removed; replacement cache, Arrow and historical metadata preserved.')
PYTHON
rm -rf -- "$DEPLOY_DIR/.render-top20-floor"
rm -f -- "$DEPLOY_DIR/deploy/retire_render_top20_workers.sh" "$DEPLOY_DIR/deploy/retire_legacy_book_workers.sh"
if [ -n "$(docker image ls -q origo-render-top20-rollback:floor)" ]; then
  docker image rm origo-render-top20-rollback:floor
fi
