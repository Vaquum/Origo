from datetime import date
from typing import Final

from .adapters.binance_spot_agg_daily import BinanceSpotAggDaily
from .adapters.binance_spot_agg_rest import BinanceSpotAggProvisional
from .contracts import (
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceNames,
)
from .profiles.spot_agg import SPOT_AGG_COMPONENTS
from .profiles.spot_agg_consumers import SPOT_AGG_CONSUMERS

BINANCE_SPOT_AGGTRADES_SPEC: Final[RevisionedSourceSpec] = RevisionedSourceSpec(
    key='binance_spot_aggtrades',
    rollout_stage=RolloutStage.LIVE,
    schema_version=1,
    names=SourceNames('binance_spot_aggtrades'),
    partitions=PartitionPolicy(date(2017, 8, 17)),
    canonical=BinanceSpotAggDaily(),
    provisional=BinanceSpotAggProvisional(),
    components=SPOT_AGG_COMPONENTS,
    consumers=SPOT_AGG_CONSUMERS,
    orchestration=OrchestrationSpec('0 4 * * *', '* * * * *', '30 * * * *'),
    retired_tables=(),
    aliases=(),
)
