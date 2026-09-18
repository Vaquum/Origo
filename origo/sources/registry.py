from typing import Final

from .binance_perp_trades import BINANCE_PERP_TRADES_SPEC
from .binance_spot_aggtrades import BINANCE_SPOT_AGGTRADES_SPEC
from .binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from .contracts import RevisionedSourceSpec

SOURCE_REGISTRY: Final[tuple[RevisionedSourceSpec, ...]] = (
    BINANCE_SPOT_TRADES_SPEC,
    BINANCE_PERP_TRADES_SPEC,
    BINANCE_SPOT_AGGTRADES_SPEC,
)
