"""Enable registered component additions after deployment retires previous writers."""

import os
from pathlib import Path

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client

from .contracts import RolloutStage
from .lifecycle import SourceRuntime
from .registry import SOURCE_REGISTRY
from .storage import SourceStore


def activate_components() -> None:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        for spec in SOURCE_REGISTRY:
            groups = sorted({item.activation_group for item in spec.components if item.activation_group})
            if spec.rollout_stage == RolloutStage.DORMANT or not groups:
                continue
            runtime = SourceRuntime(
                spec,
                SourceStore(client, settings.database, spec),
                Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR', '/opt/origo/locks')),
                'deployment:component-rollout',
            )
            for group in groups:
                runtime.enable_components(group)
                print(f'source={spec.key} component_group={group} enabled=true', flush=True)
    finally:
        client.disconnect()


def main() -> None:
    activate_components()


if __name__ == '__main__':
    main()
