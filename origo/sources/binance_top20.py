import os
from datetime import date
from pathlib import Path
from typing import Final

from .adapters.render_book import (
    Market,
    RenderBookCache,
    RenderBookCanonical,
    RenderBookProvisional,
)
from .contracts import (
    ConsumerSpec,
    OrchestrationSpec,
    PartitionPolicy,
    RevisionedSourceSpec,
    RolloutStage,
    SourceNames,
    SourceReadPolicy,
)
from .profiles.top20 import top20_components
from .profiles.top20_arrow import ARROW_MAX_BYTES, publish_top20_arrow


def top20_source(market: Market) -> RevisionedSourceSpec:
    key = f'binance_{market}_top20_v1'
    cache = RenderBookCache(
        Path(os.environ.get('ORIGO_RENDER_BOOK_ROOT', '/var/lib/origo-book-spool/render-top20-v1')),
        market,
    )
    return RevisionedSourceSpec(
        key=key,
        rollout_stage=RolloutStage.DORMANT,
        schema_version=1,
        names=SourceNames(key),
        partitions=PartitionPolicy(date(2026, 10, 24), interval='hour', first_hour=13),
        canonical=RenderBookCanonical(cache),
        provisional=RenderBookProvisional(cache),
        components=top20_components(),
        consumers=(ConsumerSpec('arrow', publish_top20_arrow, verify_file_bytes=ARROW_MAX_BYTES),)
        if market == 'spot' else (),
        orchestration=OrchestrationSpec('5 * * * *', '* * * * *', '30 * * * *', canonical_concurrency=2),
        read_policy=SourceReadPolicy.AVAILABLE,
    )


BINANCE_SPOT_TOP20_SPEC: Final[RevisionedSourceSpec] = top20_source('spot')
BINANCE_PERP_TOP20_SPEC: Final[RevisionedSourceSpec] = top20_source('perp')
