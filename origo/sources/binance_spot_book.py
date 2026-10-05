from typing import Final

from .adapters.book_local import LocalBookProvisional
from .adapters.book_vendor import CryptoHFTBookHourly
from .contracts import (
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceNames,
    SourceReadPolicy,
)
from .profiles.book import BOOK_FIRST_DAY, book_components

BINANCE_SPOT_BOOK_SPEC: Final[RevisionedSourceSpec] = RevisionedSourceSpec(
    key='binance_spot_book',
    rollout_stage=RolloutStage.CANARY,
    schema_version=1,
    names=SourceNames('binance_spot_book'),
    partitions=PartitionPolicy(BOOK_FIRST_DAY, interval='hour'),
    canonical=CryptoHFTBookHourly('spot'),
    provisional=LocalBookProvisional('spot'),
    components=book_components(),
    consumers=(),
    orchestration=OrchestrationSpec('15 * * * *', '* * * * *', '30 * * * *'),
    read_policy=SourceReadPolicy.AVAILABLE,
)
