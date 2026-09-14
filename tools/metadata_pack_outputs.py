"""Pack existing asset outputs after every writer uses Origo's packed IO manager."""

import argparse
import json
import time
from pathlib import Path

from dagster import DagsterInstance

from origo.maintenance.backup import BackupReceipt
from origo.maintenance.outputs import OutputStore, pack_existing_assets
from origo.maintenance.sqlite import Layout, maintenance_lock
from origo.maintenance.worker import directory_bytes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--instance-home', type=Path, required=True)
    parser.add_argument('--backup-receipt', type=Path, required=True)
    parser.add_argument('--all-writers-use-packed-io', action='store_true', required=True)
    parser.add_argument('--max-runtime-seconds', type=int, default=600)
    args = parser.parse_args()
    if args.max_runtime_seconds <= 0:
        raise ValueError('Output migration must have a positive runtime bound.')
    receipt = BackupReceipt.model_validate_json(args.backup_receipt.read_bytes())
    with DagsterInstance.from_config(str(args.instance_home)) as instance:
        if not (
            receipt.instance_id == instance.run_storage.get_run_storage_id()
            and receipt.verified_at <= time.time() < receipt.expires_at
            and receipt.verified_runs > 0
            and receipt.snapshot_id
            and Path(receipt.restored_home).is_dir()
        ):
            raise RuntimeError('Output packing requires a current verified restore receipt.')
        layout = Layout.from_instance(instance)
        deadline = time.monotonic() + args.max_runtime_seconds
        store = OutputStore(Path(instance.storage_directory()))
        with maintenance_lock(layout.runs.parent / 'operational-maintenance/maintenance.lock', 1):
            before = directory_bytes(layout, deadline)
            packed = pack_existing_assets(
                store, (key.path for key in instance.get_asset_keys()), deadline
            )
            after = directory_bytes(layout, deadline)
            print(
                json.dumps({'packed_outputs': packed, 'net_released_bytes': before - after}),
                flush=True,
            )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
