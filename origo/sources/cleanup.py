from collections.abc import Callable, Iterator
from contextlib import contextmanager

from dagster import get_dagster_logger


@contextmanager
def preserve_primary_failure(operation: str, cleanup: Callable[[], object]) -> Iterator[None]:
    """Run cleanup on exit; retain and log both errors when the operation also fails."""
    logger = get_dagster_logger('origo.sources')
    try:
        yield
    except BaseException as primary:
        try:
            cleanup()
        except Exception as error:
            logger.exception('Cleanup failed: %s', operation)
            primary.add_note(f'{operation} also failed: {type(error).__name__}')
        raise
    else:
        cleanup()
