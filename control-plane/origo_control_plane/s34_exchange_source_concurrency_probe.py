from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import statistics
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from typing import Any, Literal, cast

import requests

Dataset = Literal['okx_spot_trades', 'bybit_spot_trades']

_OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT = (
    'https://www.okx.com/priapi/v5/broker/public/trade-data/download-link'
)
_OKX_SPOT_INSTRUMENT_ID = 'BTC-USDT'
_OKX_TRADE_HISTORY_MODULE = '1'
_OKX_SOURCE_DAY_UTC_OFFSET_HOURS = 8
_BYBIT_BASE_URL = 'https://public.bybit.com/trading/BTCUSDT/'
_BYBIT_SYMBOL = 'BTCUSDT'
_REQUEST_TIMEOUT_SECONDS = 120


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


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        normalized[str(raw_key)] = raw_value
    return normalized


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return value


def _verify_md5_base64_or_raise(*, payload: bytes, content_md5_b64: str) -> None:
    digest_md5 = hashlib.md5(payload).digest()
    actual_b64 = base64.b64encode(digest_md5).decode('ascii')
    if actual_b64 != content_md5_b64:
        raise RuntimeError(
            'Content-MD5 mismatch: '
            f'expected={content_md5_b64} actual={actual_b64}'
        )


def _okx_source_day_window_utc_ms(date_str: str) -> tuple[int, int]:
    parsed_day = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=UTC)
    start_utc = parsed_day - timedelta(hours=_OKX_SOURCE_DAY_UTC_OFFSET_HOURS)
    end_utc_exclusive = start_utc + timedelta(days=1)
    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc_exclusive.timestamp() * 1000),
    )


def _resolve_okx_daily_file_url_or_raise(*, date_str: str) -> tuple[str, str]:
    begin_ms, _ = _okx_source_day_window_utc_ms(date_str)
    payload = {
        'module': _OKX_TRADE_HISTORY_MODULE,
        'instType': 'SPOT',
        'instQueryParam': {'instIdList': [_OKX_SPOT_INSTRUMENT_ID]},
        'dateQuery': {
            'dateAggrType': 'daily',
            'begin': str(begin_ms),
            'end': str(begin_ms),
        },
    }

    response = requests.post(
        _OKX_TRADE_DATA_DOWNLOAD_LINK_ENDPOINT,
        json=payload,
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 429:
        retry_after = response.headers.get('Retry-After')
        raise RuntimeError(
            'OKX download-link endpoint rate-limited the request: '
            f'date={date_str} retry_after={retry_after!r}'
        )
    response.raise_for_status()

    body = _expect_dict(response.json(), 'OKX download-link response')
    code = body.get('code')
    if code != '0':
        raise RuntimeError(f'OKX download-link API returned non-zero code: {code}')

    raw_data = _expect_dict(body.get('data'), 'OKX download-link response.data')
    raw_details = _expect_list(raw_data.get('details'), 'OKX download-link response.details')
    if len(raw_details) != 1:
        raise RuntimeError(
            'OKX download-link response data.details must contain exactly one item '
            f'for date={date_str}, got={raw_details}'
        )
    detail = _expect_dict(raw_details[0], 'OKX download-link response.details[0]')
    raw_group_details = _expect_list(
        detail.get('groupDetails'),
        'OKX download-link response.details[0].groupDetails',
    )
    if len(raw_group_details) != 1:
        raise RuntimeError(
            'OKX download-link response detail.groupDetails must contain exactly one '
            f'item for date={date_str}, got={raw_group_details}'
        )
    group_detail = _expect_dict(
        raw_group_details[0],
        'OKX download-link response.details[0].groupDetails[0]',
    )

    filename = _expect_non_empty_str(
        group_detail.get('filename'),
        'OKX download-link response filename',
    )
    url = _expect_non_empty_str(
        group_detail.get('url'),
        'OKX download-link response url',
    )

    expected_filename = f'{_OKX_SPOT_INSTRUMENT_ID}-trades-{date_str}.zip'
    if filename != expected_filename:
        raise RuntimeError(
            'OKX download-link filename mismatch for requested day: '
            f'expected={expected_filename} got={filename}'
        )
    return filename, url


def _resolve_bybit_daily_file_url(*, date_str: str) -> tuple[str, str]:
    filename = f'{_BYBIT_SYMBOL}{date_str}.csv.gz'
    return filename, _BYBIT_BASE_URL + filename


def _classify_request_exception(exc: Exception) -> tuple[str, int | None, str]:
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
        _, file_url = _resolve_okx_daily_file_url_or_raise(date_str=date_str)
        response = requests.get(file_url, timeout=_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        zip_payload = response.content
        bytes_downloaded = len(zip_payload)
        content_md5 = response.headers.get('Content-MD5')
        if content_md5 is None or content_md5.strip() == '':
            raise RuntimeError('OKX source response is missing Content-MD5 header')
        _verify_md5_base64_or_raise(payload=zip_payload, content_md5_b64=content_md5)
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
        _, file_url = _resolve_bybit_daily_file_url(date_str=date_str)
        response = requests.get(file_url, timeout=_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        gzip_payload = response.content
        bytes_downloaded = len(gzip_payload)
        if len(gzip_payload) == 0:
            raise RuntimeError('Bybit source file payload is empty')
        source_etag = response.headers.get('ETag')
        if source_etag is None or source_etag.strip() == '':
            raise RuntimeError('Bybit source response is missing ETag header')
        csv_payload = gzip.decompress(gzip_payload)
        if len(csv_payload) == 0:
            raise RuntimeError('Bybit CSV payload is empty after gzip decompression')
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Empirically probe real exchange source fetch paths to determine the '
            'highest passing concurrency ceiling for Slice 34 exchange backfill.'
        ),
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
        '--initial-concurrency',
        type=int,
        default=1,
        help='Lowest concurrency level to test first.',
    )
    parser.add_argument(
        '--max-concurrency-cap',
        type=int,
        required=True,
        help='Upper bound for the ceiling search.',
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    result = _search_concurrency_ceiling_or_raise(
        dataset=args.dataset,
        sample_start_date=args.sample_start_date,
        sample_day_count=args.sample_day_count,
        rounds_per_level=args.rounds_per_level,
        initial_concurrency=args.initial_concurrency,
        max_concurrency_cap=args.max_concurrency_cap,
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
