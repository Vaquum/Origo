from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass

from .errors import as_scraper_error


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _require_int_env(name: str) -> int:
    raw = _require_env(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f'{name} must be an integer, got {raw}') from exc


def _require_float_env(name: str) -> float:
    raw = _require_env(name)
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f'{name} must be a float, got {raw}') from exc


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    initial_backoff_seconds: float
    backoff_multiplier: float

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError('max_attempts must be > 0')
        if self.initial_backoff_seconds <= 0:
            raise ValueError('initial_backoff_seconds must be > 0')
        if self.backoff_multiplier < 1.0:
            raise ValueError('backoff_multiplier must be >= 1.0')


@dataclass(frozen=True)
class RetryHookEvent:
    operation_name: str
    attempt: int
    max_attempts: int
    next_backoff_seconds: float
    error_type: str
    error_message: str


def load_fetch_retry_policy_from_env() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=_require_int_env('ORIGO_SCRAPER_FETCH_MAX_ATTEMPTS'),
        initial_backoff_seconds=_require_float_env(
            'ORIGO_SCRAPER_FETCH_BACKOFF_INITIAL_SECONDS'
        ),
        backoff_multiplier=_require_float_env('ORIGO_SCRAPER_FETCH_BACKOFF_MULTIPLIER'),
    )


def retry_with_backoff[T](
    *,
    operation_name: str,
    operation: Callable[[], T],
    policy: RetryPolicy,
    on_retry: Callable[[RetryHookEvent], None] | None = None,
) -> T:
    delay_seconds = policy.initial_backoff_seconds

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt >= policy.max_attempts:
                raise as_scraper_error(
                    code='SCRAPER_RETRY_EXHAUSTED',
                    message=(
                        f'{operation_name} failed after {policy.max_attempts} attempts'
                    ),
                    details={'operation_name': operation_name},
                    cause=exc,
                ) from exc

            if on_retry is not None:
                on_retry(
                    RetryHookEvent(
                        operation_name=operation_name,
                        attempt=attempt,
                        max_attempts=policy.max_attempts,
                        next_backoff_seconds=delay_seconds,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
            time.sleep(delay_seconds)
            delay_seconds *= policy.backoff_multiplier

    raise RuntimeError('retry_with_backoff loop exhausted unexpectedly')
