"""Keep native job backfills resumable while the daemon workspace shuts down."""
from collections.abc import Callable

from dagster._core.errors import DagsterCodeLocationNotFoundError
from dagster._daemon import backfill as native_backfill
from dagster._daemon.cli import main as native_main

_SEAM = '_is_retryable_backfill_error'
_native_retryable: Callable[[Exception], bool] = getattr(native_backfill, _SEAM)


def _retryable_backfill_error(error: Exception) -> bool:
    return isinstance(error, DagsterCodeLocationNotFoundError) or _native_retryable(error)


def main() -> None:
    setattr(native_backfill, _SEAM, _retryable_backfill_error)
    native_main()


if __name__ == '__main__':
    main()
