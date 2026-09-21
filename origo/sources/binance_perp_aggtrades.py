from datetime import date
from typing import Final

from .adapters.binance_perp_agg_daily import BinancePerpAggDaily
from .adapters.binance_perp_agg_rest import BinancePerpAggProvisional
from .contracts import (
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceNames,
)
from .profiles.perp_agg import PERP_AGG_COMPONENTS
from .profiles.perp_agg_consumers import PERP_AGG_CONSUMERS

BINANCE_PERP_AGGTRADES_SPEC: Final[RevisionedSourceSpec] = RevisionedSourceSpec(
    key='binance_perp_aggtrades',
    rollout_stage=RolloutStage.CANARY,
    schema_version=1,
    names=SourceNames('binance_perp_aggtrades'),
    partitions=PartitionPolicy(date(2019, 12, 31)),
    canonical=BinancePerpAggDaily(),
    provisional=BinancePerpAggProvisional(),
    components=PERP_AGG_COMPONENTS,
    consumers=PERP_AGG_CONSUMERS,
    # UM Vision zips publish ~07:35 UTC; asking at 04:00 buys 4h of red 404s.
    orchestration=OrchestrationSpec('5 8 * * *', '* * * * *', '30 * * * *'),
    retired_tables=(),
    aliases=(),
)
