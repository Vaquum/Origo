from datetime import UTC, datetime
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
from .profiles.book import BOOK_FIRST_DAY, BOOK_FIRST_HOUR, book_components

BINANCE_SPOT_BOOK_SPEC: Final[RevisionedSourceSpec] = RevisionedSourceSpec(
    key='binance_spot_book',
    rollout_stage=RolloutStage.CANARY,
    schema_version=1,
    names=SourceNames('binance_spot_book'),
    partitions=PartitionPolicy(BOOK_FIRST_DAY, interval='hour', first_hour=BOOK_FIRST_HOUR['spot'], canonical_lag_hours=1,
                               previous_starts=(datetime(2026, 10, 4, tzinfo=UTC),)),
    canonical=CryptoHFTBookHourly('spot'),
    provisional=LocalBookProvisional('spot'),
    components=book_components(),
    consumers=(),
    orchestration=OrchestrationSpec('15 * * * *', '* * * * *', '5,20,35,50 * * * *', canonical_concurrency=2),
    read_policy=SourceReadPolicy.AVAILABLE,
)
