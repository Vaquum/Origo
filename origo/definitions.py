# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

from collections.abc import Iterator

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetsDefinition,
    AssetSpec,
    Failure,
    FreshnessPolicy,
    Definitions,
    define_asset_job,
    multi_asset_check,
)

from .orchestration.recovery import recover_orchestration_job

from .assets.create_origo_database import (
    create_origo_database,
)
from .workers.market_state_api import ASSET as MARKET_STATE_QUERY_ASSET
from .workers.runtime import LIVE_FEED_FRESHNESS_WINDOW


create_origo_database_job = define_asset_job(
    name='create_origo_database_job', selection=['create_origo_database'],
)


# The monitor worker (origo.workers.monitor) evaluates these checks every minute from
# outside Dagster and reports them through the webserver; Dagit shows them on this asset.
MONITOR_CHECK_NAMES = (
    'collectors_serving',
    'dagster_reachable',
    'data_current',
    'no_error_logs',
    'publication_current',
    'queue_bounded',
    'workers_alive',
)
origo_monitor = AssetsDefinition(
    specs=[
        AssetSpec(
            'origo_monitor',
            group_name='monitoring',
            description='The monitor worker: seven checks evaluated every minute outside Dagster.',
        )
    ]
)


@multi_asset_check(
    specs=[AssetCheckSpec(name=name, asset='origo_monitor') for name in MONITOR_CHECK_NAMES]
)
def origo_monitor_checks() -> Iterator[AssetCheckResult]:
    raise Failure(
        'origo_monitor checks are evaluated by the monitor worker, not inside Dagster.'
    )
    yield AssetCheckResult(passed=False)  # pragma: no cover - unreachable, typing only


# The live feeds: external assets the workers materialize every tick. Their freshness
# policy is the feed's liveness in Dagit, evaluated by the daemon without a run; the
# monitor's heartbeat check is what alerts on a stalled worker.
market_state_query_service = AssetsDefinition(
    specs=[
        AssetSpec(
            MARKET_STATE_QUERY_ASSET,
            group_name='live_feeds',
            description='The market state query service materializes this every cleanup tick.',
            freshness_policy=FreshnessPolicy.time_window(fail_window=LIVE_FEED_FRESHNESS_WINDOW),
        )
    ]
)


defs = Definitions(
    assets=[origo_monitor, market_state_query_service, create_origo_database],
    asset_checks=[origo_monitor_checks],
    jobs=[create_origo_database_job],
)

# TODO: Put everything in to same order in all segments of the code

from .maintenance.dagster_metadata import (
    maintain_operational_metadata,
    maintain_operational_metadata_job,
    operational_metadata_maintenance_schedule,
)

from .sources.bootstrap import prepare_revisioned_sources_job
from .sources.bundle import build_source_bundle
from .sources.registry import SOURCE_REGISTRY

_registered_source_bundles = tuple(
    build_source_bundle(spec) for spec in SOURCE_REGISTRY
)

defs = Definitions.merge(
    defs,
    Definitions(
        assets=[maintain_operational_metadata],
        jobs=[maintain_operational_metadata_job, prepare_revisioned_sources_job, recover_orchestration_job],
        schedules=[operational_metadata_maintenance_schedule],
    ),
    *(
        Definitions(
            assets=bundle.assets,
            jobs=bundle.jobs,
            schedules=bundle.schedules,
            sensors=bundle.sensors,
        )
        for bundle in _registered_source_bundles
    ),
)
