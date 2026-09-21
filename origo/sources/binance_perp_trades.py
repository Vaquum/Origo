from datetime import date
from typing import Final

from .adapters.binance_perp_daily import BinancePerpDaily
from .adapters.binance_perp_rest import BinancePerpProvisional
from .contracts import (
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceNames,
)
from .profiles.perp import PERP_COMPONENTS, PERP_RETIRED_TABLES
from .profiles.perp_consumers import PERP_CONSUMERS

BINANCE_PERP_TRADES_SPEC: Final[RevisionedSourceSpec] = RevisionedSourceSpec(
    key='binance_perp_trades',
    rollout_stage=RolloutStage.LIVE,
    schema_version=1,
    names=SourceNames('binance_perp_trades'),
    partitions=PartitionPolicy(date(2019, 9, 8)),
    canonical=BinancePerpDaily(),
    provisional=BinancePerpProvisional(),
    components=PERP_COMPONENTS,
    consumers=PERP_CONSUMERS,
    # UM Vision zips publish ~07:35 UTC; asking at 04:00 buys 4h of red 404s.
    orchestration=OrchestrationSpec('5 8 * * *', '* * * * *', '30 * * * *'),
    retired_tables=PERP_RETIRED_TABLES,
)
