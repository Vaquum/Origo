from __future__ import annotations

import argparse
import json
import statistics
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from typing import Literal, cast

import requests

from origo_control_plane.utils.exchange_source_contracts import (
    EXCHANGE_SOURCE_REQUEST_TIMEOUT_SECONDS,
    ExchangeSourceHttpError,
    resolve_bybit_daily_file_url,
    resolve_okx_daily_file_url_or_raise,
    verify_md5_base64_or_raise,
)

Dataset = Literal['okx_spot_trades', 'bybit_spot_trades']
ProbeMode = Literal['concurrency', 'okx_rate_interval']
_BYBIT_GZIP_MAGIC = b'\x1f\x8b'
_BYBIT_ADMISSION_READ_BYTES = 65536


@dataclass(frozen=True)
class ProbeAttemptResult:
    dataset: Dataset
    date_str: str
    success: bool
    duration_seconds: float
    bytes_downloaded: int
    error_kind: str | None
    status_code: int | None
    detail: str | None


@dataclass(frozen=True)
class ProbeLevelResult:
    concurrency: int
    passed: bool
    rounds: int
    attempts: int
    success_count: int
    failure_count: int
    failure_kinds: dict[str, int]
    status_code_counts: dict[str, int]
    median_duration_seconds: float
    p95_duration_seconds: float
    max_duration_seconds: float
    bytes_downloaded: int


@dataclass(frozen=True)
class ProbeSearchResult:
    dataset: Dataset
    tested_at_utc: str
    sample_start_date: str
    sample_day_count: int
    rounds_per_level: int
    initial_concurrency: int
    max_concurrency_cap: int
    ceiling_found: bool
    max_passing_concurrency: int
    first_failing_concurrency: int | None
    level_results: list[ProbeLevelResult]


@dataclass(frozen=True)
class ProbeRateLevelResult:
    interval_seconds: float
    passed: bool
    attempts: int
    success_count: int
    failure_count: int
    failure_kinds: dict[str, int]
    status_code_counts: dict[str, int]
    median_duration_seconds: float
    p95_duration_seconds: float
    max_duration_seconds: float
    bytes_downloaded: int


@dataclass(frozen=True)
class OkxRateSearchResult:
    dataset: Literal['okx_spot_trades']
    tested_at_utc: str
    sample_start_date: str
    sample_day_count: int
    attempts_per_level: int
    cooldown_seconds: float
    initial_interval_seconds: float
    max_interval_seconds: float
    interval_step_seconds: float
    minimal_passing_interval_seconds: float
    maximal_safe_requests_per_second: float
    level_results: list[ProbeRateLevelResult]


def _classify_request_exception(exc: Exception) -> tuple[str, int | None, str]:
    if isinstance(exc, ExchangeSourceHttpError):
        return exc.error_kind, exc.status_code, str(exc)
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        status_code = response.status_code if response is not None else None
        if status_code == 429:
            return 'http_429', status_code, str(exc)
        return 'http_error', status_code, str(exc)
    if isinstance(exc, requests.Timeout):
        return 'timeout', None, str(exc)
    if isinstance(exc, requests.RequestException):
        return 'request_exception', None, str(exc)
    return 'runtime_error', None, str(exc)


def _probe_okx_once(*, date_str: str) -> ProbeAttemptResult:
    started = time.monotonic()
    bytes_downloaded = 0
    try:
        _, file_url = resolve_okx_daily_file_url_or_raise(date_str=date_str)
        response = requests.get(
            file_url,
            timeout=EXCHANGE_SOURCE_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        zip_payload = response.content
        bytes_downloaded = len(zip_payload)
        content_md5 = response.headers.get('Content-MD5')
        if content_md5 is None or content_md5.strip() == '':
            raise RuntimeError('OKX source response is missing Content-MD5 header')
        verify_md5_base64_or_raise(payload=zip_payload, content_md5_b64=content_md5)
        with zipfile.ZipFile(BytesIO(zip_payload)) as zip_ref:
            csv_names = [name for name in zip_ref.namelist() if name.lower().endswith('.csv')]
            if len(csv_names) != 1:
                raise RuntimeError(
                    'OKX daily zip must contain exactly one CSV file, '
                    f'got={csv_names}'
                )
            with zip_ref.open(csv_names[0]) as csv_file:
                csv_payload = csv_file.read(1)
        if csv_payload == b'':
            raise RuntimeError('OKX daily CSV payload is empty')
        return ProbeAttemptResult(
            dataset='okx_spot_trades',
            date_str=date_str,
            success=True,
            duration_seconds=time.monotonic() - started,
            bytes_downloaded=bytes_downloaded,
            error_kind=None,
            status_code=None,
            detail=None,
        )
    except Exception as exc:
        error_kind, status_code, detail = _classify_request_exception(exc)
        return ProbeAttemptResult(
            dataset='okx_spot_trades',
            date_str=date_str,
            success=False,
            duration_seconds=time.monotonic() - started,
            bytes_downloaded=bytes_downloaded,
            error_kind=error_kind,
            status_code=status_code,
            detail=detail,
        )


def _probe_bybit_once(*, date_str: str) -> ProbeAttemptResult:
    started = time.monotonic()
    bytes_downloaded = 0
    try:
        _, file_url = resolve_bybit_daily_file_url(date_str=date_str)
        with requests.get(
            file_url,
            timeout=EXCHANGE_SOURCE_REQUEST_TIMEOUT_SECONDS,
            stream=True,
        ) as response:
            response.raise_for_status()
            source_etag = response.headers.get('ETag')
            if source_etag is None or source_etag.strip() == '':
                raise RuntimeError('Bybit source response is missing ETag header')
            chunk_iterator = response.iter_content(chunk_size=_BYBIT_ADMISSION_READ_BYTES)
            first_chunk = next(chunk_iterator, b'')
            bytes_downloaded = len(first_chunk)
            if first_chunk == b'':
                raise RuntimeError('Bybit source file payload is empty')
            if not first_chunk.startswith(_BYBIT_GZIP_MAGIC):
                raise RuntimeError(
                    'Bybit source payload does not begin with gzip magic bytes'
                )
        return ProbeAttemptResult(
            dataset='bybit_spot_trades',
            date_str=date_str,
            success=True,
            duration_seconds=time.monotonic() - started,
            bytes_downloaded=bytes_downloaded,
            error_kind=None,
            status_code=None,
            detail=None,
        )
    except Exception as exc:
        error_kind, status_code, detail = _classify_request_exception(exc)
        return ProbeAttemptResult(
            dataset='bybit_spot_trades',
            date_str=date_str,
            success=False,
            duration_seconds=time.monotonic() - started,
            bytes_downloaded=bytes_downloaded,
            error_kind=error_kind,
            status_code=status_code,
            detail=detail,
        )


def _build_interval_levels_or_raise(
    *,
    initial_interval_seconds: float,
    max_interval_seconds: float,
    interval_step_seconds: float,
) -> list[float]:
    if initial_interval_seconds < 0:
        raise RuntimeError(
            '--initial-interval-seconds must be >= 0, '
            f'got {initial_interval_seconds}'
        )
    if max_interval_seconds < initial_interval_seconds:
        raise RuntimeError(
            '--max-interval-seconds must be >= --initial-interval-seconds, '
            f'got max={max_interval_seconds} initial={initial_interval_seconds}'
        )
    if interval_step_seconds <= 0:
        raise RuntimeError(
            '--interval-step-seconds must be > 0, '
            f'got {interval_step_seconds}'
        )
    levels: list[float] = []
    current = initial_interval_seconds
    while current <= max_interval_seconds + 1e-9:
        levels.append(round(current, 6))
        current += interval_step_seconds
    return levels


def _build_sample_dates_or_raise(*, start_date: str, day_count: int) -> list[str]:
    if day_count <= 0:
        raise RuntimeError(f'--sample-day-count must be > 0, got {day_count}')
    try:
        parsed_start = date.fromisoformat(start_date)
    except ValueError as exc:
        raise RuntimeError(
            f'Invalid --sample-start-date value={start_date!r}; expected YYYY-MM-DD'
        ) from exc
    return [
        (parsed_start + timedelta(days=offset)).isoformat()
        for offset in range(day_count)
    ]


def _summarize_level_or_raise(
    *,
    concurrency: int,
    rounds: int,
    attempts: list[ProbeAttemptResult],
) -> ProbeLevelResult:
    if concurrency <= 0:
        raise RuntimeError(f'concurrency must be > 0, got {concurrency}')
    if rounds <= 0:
        raise RuntimeError(f'rounds must be > 0, got {rounds}')
    if attempts == []:
        raise RuntimeError('probe attempts must be non-empty')
    durations = [attempt.duration_seconds for attempt in attempts]
    success_count = sum(1 for attempt in attempts if attempt.success)
    failure_count = len(attempts) - success_count
    failure_kinds: dict[str, int] = {}
    status_code_counts: dict[str, int] = {}
    bytes_downloaded = 0
    for attempt in attempts:
        bytes_downloaded += attempt.bytes_downloaded
        if attempt.error_kind is not None:
            failure_kinds[attempt.error_kind] = failure_kinds.get(attempt.error_kind, 0) + 1
        if attempt.status_code is not None:
            key = str(attempt.status_code)
            status_code_counts[key] = status_code_counts.get(key, 0) + 1
    p95_index = max(0, min(len(durations) - 1, round((len(durations) - 1) * 0.95)))
    sorted_durations = sorted(durations)
    return ProbeLevelResult(
        concurrency=concurrency,
        passed=failure_count == 0,
        rounds=rounds,
        attempts=len(attempts),
        success_count=success_count,
        failure_count=failure_count,
        failure_kinds=failure_kinds,
        status_code_counts=status_code_counts,
        median_duration_seconds=statistics.median(sorted_durations),
        p95_duration_seconds=sorted_durations[p95_index],
        max_duration_seconds=max(sorted_durations),
        bytes_downloaded=bytes_downloaded,
    )


def _summarize_rate_level_or_raise(
    *,
    interval_seconds: float,
    attempts: list[ProbeAttemptResult],
) -> ProbeRateLevelResult:
    if interval_seconds < 0:
        raise RuntimeError(f'interval_seconds must be >= 0, got {interval_seconds}')
    if attempts == []:
        raise RuntimeError('probe attempts must be non-empty')
    durations = [attempt.duration_seconds for attempt in attempts]
    success_count = sum(1 for attempt in attempts if attempt.success)
    failure_count = len(attempts) - success_count
    failure_kinds: dict[str, int] = {}
    status_code_counts: dict[str, int] = {}
    bytes_downloaded = 0
    for attempt in attempts:
        bytes_downloaded += attempt.bytes_downloaded
        if attempt.error_kind is not None:
            failure_kinds[attempt.error_kind] = failure_kinds.get(attempt.error_kind, 0) + 1
        if attempt.status_code is not None:
            key = str(attempt.status_code)
            status_code_counts[key] = status_code_counts.get(key, 0) + 1
    p95_index = max(0, min(len(durations) - 1, round((len(durations) - 1) * 0.95)))
    sorted_durations = sorted(durations)
    return ProbeRateLevelResult(
        interval_seconds=interval_seconds,
        passed=failure_count == 0,
        attempts=len(attempts),
        success_count=success_count,
        failure_count=failure_count,
        failure_kinds=failure_kinds,
        status_code_counts=status_code_counts,
        median_duration_seconds=statistics.median(sorted_durations),
        p95_duration_seconds=sorted_durations[p95_index],
        max_duration_seconds=max(sorted_durations),
        bytes_downloaded=bytes_downloaded,
    )


def _run_probe_level_or_raise(
    *,
    dataset: Dataset,
    concurrency: int,
    rounds: int,
    sample_dates: list[str],
) -> ProbeLevelResult:
    if sample_dates == []:
        raise RuntimeError('sample_dates must be non-empty')
    if dataset == 'okx_spot_trades':
        def probe_once(date_str: str) -> ProbeAttemptResult:
            return _probe_okx_once(date_str=date_str)
    else:
        def probe_once(date_str: str) -> ProbeAttemptResult:
            return _probe_bybit_once(date_str=date_str)
    attempts: list[ProbeAttemptResult] = []
    for round_index in range(rounds):
        round_dates = [
            sample_dates[(round_index * concurrency + index) % len(sample_dates)]
            for index in range(concurrency)
        ]
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(probe_once, date_str)
                for date_str in round_dates
            ]
            for future in as_completed(futures):
                attempts.append(future.result())
    return _summarize_level_or_raise(
        concurrency=concurrency,
        rounds=rounds,
        attempts=attempts,
    )


def _run_okx_rate_level_or_raise(
    *,
    interval_seconds: float,
    attempts_per_level: int,
    sample_dates: list[str],
    cooldown_seconds: float,
) -> ProbeRateLevelResult:
    if attempts_per_level <= 0:
        raise RuntimeError(
            f'attempts_per_level must be > 0, got {attempts_per_level}'
        )
    if sample_dates == []:
        raise RuntimeError('sample_dates must be non-empty')
    if cooldown_seconds < 0:
        raise RuntimeError(f'cooldown_seconds must be >= 0, got {cooldown_seconds}')
    if cooldown_seconds > 0:
        time.sleep(cooldown_seconds)
    attempts: list[ProbeAttemptResult] = []
    for attempt_index in range(attempts_per_level):
        date_str = sample_dates[attempt_index % len(sample_dates)]
        attempts.append(_probe_okx_once(date_str=date_str))
        if attempt_index != attempts_per_level - 1 and interval_seconds > 0:
            time.sleep(interval_seconds)
    return _summarize_rate_level_or_raise(
        interval_seconds=interval_seconds,
        attempts=attempts,
    )


def _search_concurrency_ceiling_or_raise(
    *,
    dataset: Dataset,
    sample_start_date: str,
    sample_day_count: int,
    rounds_per_level: int,
    initial_concurrency: int,
    max_concurrency_cap: int,
) -> ProbeSearchResult:
    if initial_concurrency <= 0:
        raise RuntimeError(
            f'--initial-concurrency must be > 0, got {initial_concurrency}'
        )
    if max_concurrency_cap < initial_concurrency:
        raise RuntimeError(
            '--max-concurrency-cap must be >= --initial-concurrency, '
            f'got cap={max_concurrency_cap} initial={initial_concurrency}'
        )
    sample_dates = _build_sample_dates_or_raise(
        start_date=sample_start_date,
        day_count=sample_day_count,
    )
    cache: dict[int, ProbeLevelResult] = {}

    def evaluate(level: int) -> ProbeLevelResult:
        cached = cache.get(level)
        if cached is not None:
            return cached
        result = _run_probe_level_or_raise(
            dataset=dataset,
            concurrency=level,
            rounds=rounds_per_level,
            sample_dates=sample_dates,
        )
        cache[level] = result
        return result

    last_pass = 0
    first_fail: int | None = None
    level = initial_concurrency
    while True:
        result = evaluate(level)
        if result.passed:
            last_pass = level
            if level == max_concurrency_cap:
                break
            next_level = min(level * 2, max_concurrency_cap)
            if next_level == level:
                break
            level = next_level
            continue
        first_fail = level
        break

    if last_pass == 0:
        raise RuntimeError(
            'No passing concurrency level found; source fetch path failed even at '
            f'concurrency={initial_concurrency} for dataset={dataset}'
        )

    if first_fail is not None:
        low = last_pass + 1
        high = first_fail - 1
        while low <= high:
            mid = (low + high) // 2
            result = evaluate(mid)
            if result.passed:
                last_pass = mid
                low = mid + 1
            else:
                first_fail = mid
                high = mid - 1

    ordered_results = [cache[level_key] for level_key in sorted(cache)]
    return ProbeSearchResult(
        dataset=dataset,
        tested_at_utc=datetime.now(UTC).isoformat(),
        sample_start_date=sample_start_date,
        sample_day_count=sample_day_count,
        rounds_per_level=rounds_per_level,
        initial_concurrency=initial_concurrency,
        max_concurrency_cap=max_concurrency_cap,
        ceiling_found=first_fail is not None,
        max_passing_concurrency=last_pass,
        first_failing_concurrency=first_fail,
        level_results=ordered_results,
    )


def _search_okx_min_safe_interval_or_raise(
    *,
    sample_start_date: str,
    sample_day_count: int,
    attempts_per_level: int,
    cooldown_seconds: float,
    initial_interval_seconds: float,
    max_interval_seconds: float,
    interval_step_seconds: float,
) -> OkxRateSearchResult:
    sample_dates = _build_sample_dates_or_raise(
        start_date=sample_start_date,
        day_count=sample_day_count,
    )
    interval_levels = _build_interval_levels_or_raise(
        initial_interval_seconds=initial_interval_seconds,
        max_interval_seconds=max_interval_seconds,
        interval_step_seconds=interval_step_seconds,
    )
    level_results: list[ProbeRateLevelResult] = []
    for interval_seconds in interval_levels:
        level_result = _run_okx_rate_level_or_raise(
            interval_seconds=interval_seconds,
            attempts_per_level=attempts_per_level,
            sample_dates=sample_dates,
            cooldown_seconds=cooldown_seconds,
        )
        level_results.append(level_result)
        if level_result.passed:
            requests_per_second = (
                float('inf')
                if interval_seconds == 0
                else round(1.0 / interval_seconds, 6)
            )
            return OkxRateSearchResult(
                dataset='okx_spot_trades',
                tested_at_utc=datetime.now(UTC).isoformat(),
                sample_start_date=sample_start_date,
                sample_day_count=sample_day_count,
                attempts_per_level=attempts_per_level,
                cooldown_seconds=cooldown_seconds,
                initial_interval_seconds=initial_interval_seconds,
                max_interval_seconds=max_interval_seconds,
                interval_step_seconds=interval_step_seconds,
                minimal_passing_interval_seconds=interval_seconds,
                maximal_safe_requests_per_second=requests_per_second,
                level_results=level_results,
            )
    raise RuntimeError(
        'No passing OKX rate interval found; source link-resolution remained unsafe '
        f'from interval={initial_interval_seconds} through interval={max_interval_seconds}'
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Empirically probe real exchange source fetch paths to determine the '
            'highest passing concurrency ceiling or minimal safe OKX link-resolution '
            'interval for Slice 34 exchange backfill.'
        ),
    )
    parser.add_argument(
        '--probe-mode',
        default='concurrency',
        choices=['concurrency', 'okx_rate_interval'],
        help='Probe exchange source by concurrent fetch ceiling or OKX paced link-resolution interval.',
    )
    parser.add_argument(
        '--dataset',
        required=True,
        choices=['okx_spot_trades', 'bybit_spot_trades'],
        help='Exchange dataset whose source fetch path should be probed.',
    )
    parser.add_argument(
        '--sample-start-date',
        required=True,
        help='First UTC day used to build the rolling source probe sample set.',
    )
    parser.add_argument(
        '--sample-day-count',
        type=int,
        default=32,
        help='Number of consecutive UTC days used as the probe sample pool.',
    )
    parser.add_argument(
        '--rounds-per-level',
        type=int,
        default=2,
        help='How many repeated concurrent rounds each tested level must survive.',
    )
    parser.add_argument(
        '--attempts-per-level',
        type=int,
        default=5,
        help='How many sequential attempts each OKX interval level must survive.',
    )
    parser.add_argument(
        '--initial-concurrency',
        type=int,
        default=1,
        help='Lowest concurrency level to test first.',
    )
    parser.add_argument(
        '--max-concurrency-cap',
        type=int,
        default=None,
        help='Upper bound for the ceiling search.',
    )
    parser.add_argument(
        '--initial-interval-seconds',
        type=float,
        default=0.0,
        help='Lowest OKX sequential link-resolution interval to test first.',
    )
    parser.add_argument(
        '--max-interval-seconds',
        type=float,
        default=2.0,
        help='Upper bound for OKX sequential link-resolution interval search.',
    )
    parser.add_argument(
        '--interval-step-seconds',
        type=float,
        default=0.25,
        help='Step size for OKX sequential link-resolution interval search.',
    )
    parser.add_argument(
        '--cooldown-seconds',
        type=float,
        default=6.0,
        help='Cooldown applied before each OKX interval cohort to avoid cross-level contamination.',
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    probe_mode = cast(ProbeMode, args.probe_mode)
    if probe_mode == 'concurrency':
        if args.max_concurrency_cap is None:
            raise RuntimeError(
                'probe_mode=concurrency requires explicit --max-concurrency-cap'
            )
        result: ProbeSearchResult | OkxRateSearchResult = _search_concurrency_ceiling_or_raise(
            dataset=args.dataset,
            sample_start_date=args.sample_start_date,
            sample_day_count=args.sample_day_count,
            rounds_per_level=args.rounds_per_level,
            initial_concurrency=args.initial_concurrency,
            max_concurrency_cap=int(args.max_concurrency_cap),
        )
    else:
        if args.dataset != 'okx_spot_trades':
            raise RuntimeError(
                'probe_mode=okx_rate_interval only supports dataset=okx_spot_trades'
            )
        result = _search_okx_min_safe_interval_or_raise(
            sample_start_date=args.sample_start_date,
            sample_day_count=args.sample_day_count,
            attempts_per_level=args.attempts_per_level,
            cooldown_seconds=args.cooldown_seconds,
            initial_interval_seconds=args.initial_interval_seconds,
            max_interval_seconds=args.max_interval_seconds,
            interval_step_seconds=args.interval_step_seconds,
        )
    print(
        json.dumps(
            {
                **asdict(result),
                'level_results': [asdict(level) for level in result.level_results],
            },
            sort_keys=True,
            indent=2,
        )
    )


if __name__ == '__main__':
    main()
