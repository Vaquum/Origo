from typing import Any, NoReturn


def _raise_unavailable() -> NoReturn:
    raise NotImplementedError(
        'standard_bars.py is intentionally excluded from Origo for now. '
        'Bar builders are unavailable in this build.'
    )


def volume_bars(*args: Any, **kwargs: Any) -> NoReturn:
    _raise_unavailable()


def trade_bars(*args: Any, **kwargs: Any) -> NoReturn:
    _raise_unavailable()


def liquidity_bars(*args: Any, **kwargs: Any) -> NoReturn:
    _raise_unavailable()


__all__ = ['liquidity_bars', 'trade_bars', 'volume_bars']
