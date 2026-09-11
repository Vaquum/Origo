from datetime import date
from typing import Final

from .adapters.binance_daily import BinanceSpotDaily
from .adapters.binance_spot_rest import BinanceSpotProvisional
from .contracts import (
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceNames,
)
from .profiles.spot import SPOT_COMPONENTS
from .profiles.spot_consumers import SPOT_CONSUMERS
from .profiles.spot_parity import verify_spot_legacy

BINANCE_SPOT_TRADES_SPEC: Final[RevisionedSourceSpec] = RevisionedSourceSpec(
    key='binance_spot_trades',
    rollout_stage=RolloutStage.CANARY,
    schema_version=1,
    verify=verify_spot_legacy,
    names=SourceNames('binance_spot_trades'),
    partitions=PartitionPolicy(date(2017, 8, 17)),
    canonical=BinanceSpotDaily(),
    provisional=BinanceSpotProvisional(),
    components=SPOT_COMPONENTS,
    consumers=SPOT_CONSUMERS,
    orchestration=OrchestrationSpec('0 4 * * *', '* * * * *', '30 * * * *'),
)
