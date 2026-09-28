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
owns the 16 GiB total byte ceiling, including partial/bridge/control artifacts;
Compose named volumes do not impose a filesystem quota. Capture has no listener,
API key, database credential, Docker socket or inherited worker environment. Host
networking is required to bind the existing host address; it is not network isolation.

## Rollback before reverting S461

A pre-capture workflow has no capture-retirement logic. Before deploying a revert,
stop and remove only containers carrying the current project's capture service
label. Run this block with the deployment's existing `PROJECT_NAME`; retain the
spool and shared limiter volumes. This is the explicitly reviewed rollback step,
not a claim that a blind revert or `up -d` removes an orphan.

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
```

Only after this succeeds may the revert restore dual-IP repair. Removing the stopped
container disables its `unless-stopped` restart policy without deleting any volume.
No global orphan removal, `down -v`, limiter reset or spool deletion is permitted.

## Evidence status

Executed deployment-shell tests prove role ordering, failed-handover refusal and
exact project/service retirement. An isolated process test SIGTERMs and restarts
the real collector over authentic September 21 responses, preserving overlap and
the exact 9,948 rows of a complete minute. Its transport and cadence are replayed;
it does not prove real Docker rollout downtime,
provider overlap, busy-minute parity or production freshness. Those require the
retained authentic corpus and actual process/deployment interruption measurements;
production closeout remains on [PRD460](https://github.com/Vaquum/Origo/issues/460).
