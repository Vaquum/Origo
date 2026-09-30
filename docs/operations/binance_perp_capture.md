# Raw-perp capture deployment and evidence

[S461](https://github.com/Vaquum/Origo/issues/461) captures individual trades into a
bounded durable spool. Capture owns `.140`; the deployed historical repair worker
owns `.144`. Other feeds, native repairs and future books retain `.167`.

## Deployment contract

`perp-capture` uses the existing application image and Compose project. Deployment
stops previous repair containers, recreates repair with only `.144`, verifies every
running repair container's role, then starts capture. A failed role check never
starts capture. The handover checks local process/configuration facts, not provider
or reader health. Database and Dagit availability are not capture dependencies.
Application-image changes restart capture; overlap or a verified historical bridge
must account for that interruption before the affected minutes can activate.

Capture has one CPU, 512 MiB RAM, a read-only root filesystem, no capabilities and
`no-new-privileges`. UID 0 is deliberate: the existing shared lock/heartbeat volumes
are root-owned. Only the spool, locks and heartbeat mounts are writable. The spool
owns the 16 GiB total byte ceiling, including partial/bridge/control artifacts but not
the SQLite `-wal`/`-shm` sidecars, which appear and vanish with connections (capacity
reserves 8 MiB of headroom for them); Compose named volumes do not impose a filesystem quota. Capture has no listener,
API key, database credential, Docker socket or inherited worker environment. Host
networking is required to bind the existing host address; it is not network isolation.

## Rollback before reverting S461

A pre-capture workflow has no capture-retirement logic. Before deploying a revert,
stop and remove only containers carrying the current project's capture service
label. Run this block from the deployment directory with its existing environment
and `PROJECT_NAME`; retain the spool and shared limiter volumes. This is the explicitly
reviewed rollback step, not a claim that a blind revert or `up -d` removes an orphan.

```bash
set -euo pipefail
capture_ids="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=perp-capture)"
if [ -n "$capture_ids" ]; then
  docker stop $capture_ids </dev/null
  docker rm $capture_ids </dev/null
fi
remaining="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT_NAME" --filter label=com.docker.compose.service=perp-capture)"
if [ -n "$remaining" ]; then
  echo 'Capture retirement incomplete; rollback refused' >&2
  exit 1
fi
docker compose -p "$PROJECT_NAME" -f docker-compose.deploy.yml run --rm --no-deps --entrypoint rm monitor -f /opt/origo/heartbeats/perp_capture.heartbeat /opt/origo/heartbeats/perp_capture.status.json /opt/origo/heartbeats/perp_capture.status.tmp
```

The one-off command removes only retired capture liveness/status files from the
shared heartbeat volume. This prevents the pre-capture monitor's heartbeat inventory
from reporting a permanently stale collector. It starts no monitor process or dependency;
a removal failure blocks rollback. Other worker heartbeats and cooldowns remain intact.

Only after this succeeds may the revert restore dual-IP repair. Removing the stopped
container disables its `unless-stopped` restart policy without deleting any volume.
No global orphan removal, `down -v`, limiter reset or spool deletion is permitted.

## Regression evidence and live validation

CI replays committed genuine recent responses from September 21 against their
checksum-verified archive extract and the retained partial historical crosscheck.
A separate processing regression uses the existing high-volume historical corpus,
explicitly as offline delivery input. It verifies actual trade preservation and
spool/process bounds; it does not claim that those pages came from the recent
endpoint during a busy minute.

The normal source-native test suite runs these regressions without network access,
new capture data, a future daily archive, acceptance manifests or saved code-hash
attestations. Runtime edits are checked by executing the tests again.

Provider throughput, observed gap-repair cost, actual rollout recovery and the
six-hour production freshness/cost window remain live validation on
[PRD460](https://github.com/Vaquum/Origo/issues/460). CI and a merge do not certify
those production results. Experimental captures and one-off replay measurements
are retained locally and in this PR's earlier commits, outside the routine CI path.
