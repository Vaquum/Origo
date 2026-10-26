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

RETIRED_BOOK_COMPONENTS: Final[dict[str, frozenset[str]]] = {
    f'binance_{market}_book': frozenset(
        f'{component}{suffix}'
        for component in ('depth20', 'depth200', 'depth20_1m', 'depth200_1m')
        for suffix in ('', '_latest')
    )
    for market in ('spot', 'perp')
}


def top20_source(market: Market) -> RevisionedSourceSpec:
    key = f'binance_{market}_top20_v1'
    retired = f'binance_{market}_book'
    retired_tables = tuple(
        f'{retired}_{component}_{suffix}'
        for suffix in ('current', 'revisions')
        for component in sorted(RETIRED_BOOK_COMPONENTS[retired])
    )
    if market == 'spot':
        retired_tables += (
            'binance_spot_depth20_snapshots', 'binance_spot_depth20_1m',
            'binance_spot_depth200_snapshots', 'binance_spot_depth200_1m',
        )
    cache = RenderBookCache(
        Path(os.environ.get('ORIGO_RENDER_BOOK_ROOT', '/var/lib/origo-book-spool/render-top20-v1')),
        market,
    )
    return RevisionedSourceSpec(
        key=key,
        rollout_stage=RolloutStage.LIVE,
        schema_version=1,
        names=SourceNames(key),
        partitions=PartitionPolicy(date(2026, 10, 24), interval='hour', first_hour=13),
        canonical=RenderBookCanonical(cache),
        provisional=RenderBookProvisional(cache),
        components=top20_components(),
        consumers=(ConsumerSpec('arrow', publish_top20_arrow, public=True, verify_file_bytes=ARROW_MAX_BYTES),)
        if market == 'spot' else (),
        orchestration=OrchestrationSpec('5 * * * *', '* * * * *', '30 * * * *', canonical_concurrency=2),
        read_policy=SourceReadPolicy.AVAILABLE,
        retired_tables=retired_tables,
    )


BINANCE_SPOT_TOP20_SPEC: Final[RevisionedSourceSpec] = top20_source('spot')
BINANCE_PERP_TOP20_SPEC: Final[RevisionedSourceSpec] = top20_source('perp')
