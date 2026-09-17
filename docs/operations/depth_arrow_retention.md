# Depth Arrow retention

Depth20 and depth200 minute IPC chunks are a live cache. The publisher keeps chunks at or after `floor(now_UTC, minute) - 30 minutes`, plus the exact current `latest.json` target even when stale. The 15-minute source repair window fits inside that window. Source rows remain in ClickHouse; historical exports are separate from this cache.

`publish_depth_snapshot_chunk` serializes chunk publication, manifest replacement and expiry through the existing `.depth*_snapshots.lock`. Expired requests return `skipped_expired`; late eligible requests can write their minute without moving the manifest backwards. An expired asset invocation skips the source query. Expiry runs on publication, so a stopped publisher can leave old files behind without adding new ones.

Only canonical `chunks/YYYY/MM/DD/HH/YYYYMMDDTHHMM00Z.arrow` files under the two depth series can expire. The current manifest's series, partition, target path, checksum and IPC payload must validate first. Symlinks are never followed for publication or deletion; unrecognized chunk paths remain and generate warnings. Filesystem errors fail the Dagster run. Empty canonical chunk directories are removed. No retention index, new service or schedule is introduced.

Republishing the current latest minute first hard-links its committed bytes to `.latest.previous.arrow` under the series lock. If the process exits before committing the new manifest, the next publisher validates that recovery copy against the existing manifest and restores it before expiry. If the manifest was committed, its matching new target is retained and the recovery copy is removed. An invalid recovery copy cannot authorize expiry. This private file exists only during replacement or interrupted recovery; it is not a retention index.

The existing uncompressed, single-record-batch IPC schema and manifest fields remain unchanged. Bar/prediction versions and their `latest.arrow` links, legacy flat depth exports, and ClickHouse business tables are outside this policy. Open memory maps remain readable after an expired path is unlinked. A reader resolving an old pathname and delaying its open must reread `latest.json` if that path expires.

## Mandatory preflight before the first deployment

Deployment enables destructive expiry. **Do not deploy while source coverage or a historical consumer dependency remains unresolved.** Rolling back code cannot restore deleted files. Keep the PR in draft until both series' preflight is complete; do not use its merge to initiate an unverified cleanup.

1. Inventory every Arrow mount, including stopped containers and host processes. Save container IDs/images, read/write mode and actual application read paths. Inspect deployed source, not just mount names. Resolve any reader that expects arbitrary historical depth filenames before proceeding.
2. Capture each current manifest and target. Enumerate canonical minute paths with inode, modification time, logical bytes and allocated bytes. At a fixed UTC cutoff, identify all older paths except the protected target. Retain malformed paths and outside-root symlinks. This inventory is the proposed deletion set, not authorization inferred from file age alone.
3. Compare **every candidate's content** with the retained raw ClickHouse snapshot table. Counts and minimum/maximum timestamps alone are insufficient. Validate the IPC schema, book depth, row ordering and null absence. Serialize each row as little-endian 64-bit `ts`, `source_timestamp_ms`, `last_update_id`, followed by bid price/quantity pairs and ask price/quantity pairs. Compare SHA-256 of the ordered complete minute with the same encoding from `binance_spot_depth20_snapshots FINAL` or `binance_spot_depth200_snapshots FINAL`. Check count and unique timestamp count as well; duplicate or mismatching source rows require investigation. Record the whole IPC SHA-256 alongside the content proof.
4. Bound source queries to one hour, two threads, 20 seconds and 512 MiB. Measure throughput before the full scan. A candidate rewritten during verification must be checked as its new version; preserve the original observation. Recheck file identity after reading. Revalidate changed files and minutes that became eligible since inventory immediately before deployment. Missing coverage blocks expiry for the affected series.
5. Capture bar/prediction/legacy inputs separately: original version filenames, hashes and latest link targets. Verify Praxis replay inputs still load through its actual reader. Record manifest lag and normal source repair state. Preserve all preflight artifacts before activation.

The September 15 operation is recorded under `/var/lib/origo-depth-arrow-retention-20260915` on the existing server. Its `candidate-inventory.jsonl`, `arrow-mounts.json`, captured `consumer-code/`, verifier source, progress, errors and final content proofs are operator evidence; they are not runtime retention state. The verifier uses `/tmp/origo-depth-retention` in the deployed Dagster container. Copy its completed proofs to the host operation directory before any container replacement. Check actual terminal status and `source-coverage-complete.json`; an ETA or progress count is not completion.

The initial fixed-cutoff inventory at `2026-09-15T08:46:00Z` contains 130,647 depth20 candidates (52,433,879,040 allocated bytes) and 130,796 depth200 candidates (50,891,436,032 allocated bytes). These figures describe the inventory, not reclaimed space. The later deployment needs the incremental eligibility/identity check above.

## Verified deployed consumer paths

The inventory captured Origo image `41fbd596e222729cb6d0b1abff17e504bb208f81`, Praxis `0.90.0`, Crucible `932455408729b55f38e6f63d34d2707188afd925`, Mill `5dff14b6e4e9929ca1c3139190734540ed0433e4`, and Furnace `58efd3b51f822345970429e5829a97b4f9ef2965`.

- Praxis `replay/load_replay_bars.py::load_replay_bars` reads `<arrow>/<series>/latest.arrow` and `<conduit>/<series>/latest.arrow`; its live price reader also uses the bar `latest.arrow`. The separate binsim depth feed is HTTP, not this chunk tree.
- Crucible `datasets.py::_klines_path` constructs `<arrow>/<series>/latest.arrow` and passes it to Limen's historical reader.
- Mill `bars.py` maps supported time/dollar bar series; `settings.py` constructs their `latest.arrow` paths.
- Furnace `prediction_cache.py::_series_klines` and `maker_eval.py` read bar `latest.arrow`; the scheduler uses the same prediction-cache path. Prediction output is in the separate conduit mount.
- `tdw-arrow-tester` mounts Arrow read-only and runs `sleep infinity`; it has no active file-reading workload.
- Origo's depth worker (`origo.workers.depth`) publishes each minute's chunk; the Dagster/Dagit containers keep the source and repair jobs for operator backfills. No consumer above requires historical depth chunks for replay. Recheck this inventory if any image or reader changes before rollout.

## First expiry and acceptance

After preflight and reviewed merge, verify the actual deployed image/version before observing the first normal `build_depth_snapshot_store_arrow_job` for **each** series (an operator run; the depth worker publishes the current chunks and shares the same retention path). Do not add another scheduler or launch duplicate backfill work. Capture each native Dagster run ID, terminal status, expiry log, cutoff, protected path, expired file count, reclaimed logical bytes and sweep duration. Failures must appear in the same run's logs and status.

Measure filesystem allocated bytes before and after the first sweep; do not substitute logged logical file sizes for physical reclamation. Validate both current manifests and open their targets. Enumerate remaining chunks: no recognized file may precede the recorded cutoff except its protected target. Account for ordinary publication while inspecting the tree.

Observe subsequent normal publications and the existing repair window. A missing recent chunk must be rebuilt without moving `latest.json` backwards or becoming repeatedly expired. An old queued request must report `skipped_expired` and leave its expired pathname absent. Confirm bounded counts/bytes and publication lag after catch-up.

Finally, verify preserved bar/prediction/legacy versions and links, allowing documented normal publisher updates to latest links, and rerun the same Praxis input read. Record actual results and errors in Dagster. Publish the before/after metrics and proof hashes in the slice closeout. CI results do not establish production reclamation.

## Regression evidence

`pytest tests/origo_source_native/test_build_depth_snapshot_store_arrow.py -q` exercises the cutoff, stale latest, late/expired publication, real OS lock contention, readable IPC/memory maps, the actual 15-minute repair setting, error propagation and unrelated-store preservation. Fixtures are two unchanged rows from each recorded production minute, plus genuine bar/prediction/legacy samples. `fixtures/depth_arrow_retention/provenance.json` binds source paths, timestamps, original hashes, selected rows and fixture hashes; every captured depth minute matched ClickHouse before extraction.
