"""Initialize physical page reclamation during a backed-up, quiesced maintenance outage."""

import argparse
import json
import time
from pathlib import Path

from dagster import DagsterInstance

from origo.maintenance.archive import archive_path
from origo.maintenance.backup import BackupReceipt
from origo.maintenance.compaction import initialize_compaction
from origo.maintenance.sqlite import Layout, maintenance_lock


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--instance-home', type=Path, required=True)
    parser.add_argument('--backup-receipt', type=Path, required=True)
    parser.add_argument('--writers-quiesced', action='store_true', required=True)
    parser.add_argument('--max-runtime-seconds', type=int, default=900)
    args = parser.parse_args()
    if args.max_runtime_seconds <= 0:
        raise ValueError('The maintenance outage must have a positive runtime bound.')
    receipt = BackupReceipt.model_validate_json(args.backup_receipt.read_bytes())
    with DagsterInstance.from_config(str(args.instance_home)) as instance:
        layout = Layout.from_instance(instance)
        if not (
            receipt.instance_id == instance.run_storage.get_run_storage_id()
            and receipt.verified_at <= time.time() < receipt.expires_at
            and receipt.verified_runs > 0
            and receipt.snapshot_id
            and Path(receipt.restored_home).is_dir()
        ):
            raise RuntimeError('Physical compaction requires a current verified restore receipt.')
        deadline = time.monotonic() + args.max_runtime_seconds
        paths = [layout.runs, layout.events, layout.schedules]
        packed = archive_path(layout.events.parent)
        if packed.exists():
            paths.append(packed)
        with maintenance_lock(layout.runs.parent / 'operational-maintenance/maintenance.lock', 1):
            for path in paths:
                released = initialize_compaction(path, deadline, 1)
                print(json.dumps({'database': path.name, 'released_bytes': released}), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
