"""Build the rolling BTC briefing history from the origo ClickHouse tables.

The history is one object carrying the two sections in ``HISTORY_SECTIONS``:
``HISTORY_15M_DAYS`` days of 15-minute bars and ``HISTORY_1D_DAYS`` days of
daily bars, both rolled up from the 1m ``binance_spot_trades_time_current`` projection.
Both spans end where ``through_day`` begins, so the history is the multi-day
run-up to that day and the day's own ``btc_briefing/2`` feed file continues
it without overlapping it. The two spans are not round numbers: they are the
lookbacks the consuming briefing is computed over, and they are here so that
briefing reads its history from this dataset instead of re-fetching it from
an exchange REST API on every run.

Completeness policy: the one the daily feed applies, stretched over the span.
Every bar must be built from exactly its full complement of distinct 1m
source minutes (15 for a 15m bar, 1440 for a daily bar) with no duplicated 1m
row, and the bar count must be exactly the span's, otherwise the build raises
instead of publishing a gapped history a consumer would read as real quiet.

``bar_start`` is declared as UTC epoch seconds in the SQL itself via
``toUnixTimestamp``, and the bar grid is anchored to the span's opening
midnight, so the whole span sits on one midnight-aligned grid and no bar
straddles a day boundary.

The asset is daily-partitioned alongside the feed: each partition rebuilds
the whole window and republishes it to the ``vaquum/btc_briefing_feed``
HuggingFace dataset as the single rolling file ``btc_briefing_history.json``.
Unlike the per-day feed files there is nothing to accumulate and no latest
pointer to advance -- an older window has no value once a newer one exists --
so a partition older than the published one is refused, before its rollup
runs, rather than uploaded.
"""

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import date, timedelta
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol, cast

from dagster import AssetExecutionContext, asset
from huggingface_hub import HfApi

from .briefing_partitions import daily_partitions
from .publish_btc_briefing_feed import (
    BARS_1D_SECONDS,
    BARS_15M_PER_DAY,
    BARS_15M_SECONDS,
    BRIEFING_DATASET_REPO_ID,
    DEFAULT_CLICKHOUSE_HTTP_PORT,
    MINUTES_PER_DAY,
)

HISTORY_VERSION: Final[str] = 'btc_briefing_history/1'
HISTORY_SECTIONS: Final[tuple[str, ...]] = ('bars_15m', 'bars_1d')
HISTORY_15M_DAYS: Final[int] = 70
HISTORY_1D_DAYS: Final[int] = 61

HISTORY_FILE_NAME: Final[str] = 'btc_briefing_history.json'

_SQL_DIR = Path(__file__).parent / 'sql'


class _ArrowTableProtocol(Protocol):
    def to_pylist(self) -> list[dict[str, object]]:
        raise NotImplementedError


class _ClickHouseArrowClientProtocol(Protocol):
    def query_arrow(
        self,
        query: str,
        parameters: Mapping[str, object] | None = None,
    ) -> _ArrowTableProtocol:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def _get_clickhouse_http_port() -> int:
    value = os.environ.get('CLICKHOUSE_HTTP_PORT', str(DEFAULT_CLICKHOUSE_HTTP_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_HTTP_PORT environment variable must be an integer.') from exc


def _make_clickhouse_arrow_client() -> _ClickHouseArrowClientProtocol:
    client_factory = getattr(import_module('clickhouse_connect'), 'get_client')
    return cast(
        _ClickHouseArrowClientProtocol,
        client_factory(
            host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
            port=_get_clickhouse_http_port(),
            username=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ['CLICKHOUSE_PASSWORD'],
        ),
    )


def _span_bars(
    client: _ClickHouseArrowClientProtocol,
    *,
    through_day: date,
    days: int,
    bucket_seconds: int,
) -> list[dict[str, object]]:
    arrow_table = client.query_arrow(
        (_SQL_DIR / 'briefing_history_bars.sql').read_text(encoding='utf-8'),
        parameters={
            'through_day': through_day,
            'days': days,
            'bucket_seconds': bucket_seconds,
        },
    )
    return arrow_table.to_pylist()


def _require_complete_bars(
    bars: list[dict[str, object]],
    *,
    through_day: date,
    days: int,
    section: str,
    expected_bars: int,
    minutes_per_bar: int,
) -> None:
    span = f'{(through_day - timedelta(days=days)).isoformat()}..{through_day.isoformat()}'
    if len(bars) != expected_bars:
        raise RuntimeError(
            f'{span} has {len(bars)} {section} bars where {expected_bars} are '
            'required; refusing to build a short briefing history.'
        )
    short_bars = [bar for bar in bars if bar['source_minutes'] != minutes_per_bar]
    if short_bars:
        raise RuntimeError(
            f'{span} has {len(short_bars)} {section} bars not built from exactly '
            f'{minutes_per_bar} distinct source minutes; refusing to build a short '
            'briefing history.'
        )
    duplicated_bars = [bar for bar in bars if bar['source_rows'] != bar['source_minutes']]
    if duplicated_bars:
        raise RuntimeError(
            f'{span} has {len(duplicated_bars)} {section} bars with duplicated 1m '
            'source rows; refusing to build a briefing history from a corrupt '
            'projection.'
        )


def _content_sha256(history: Mapping[str, object]) -> str:
    """The history's digest over every field but the digest itself.

    The digest travels inside the file it describes, so it is taken over the
    canonical JSON of the other fields: a reader recomputes it by dropping
    ``sha256`` and re-serializing with sorted keys.
    """
    payload = {key: value for key, value in history.items() if key != 'sha256'}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()


def build_briefing_history(
    client: _ClickHouseArrowClientProtocol,
    through_day: date,
) -> dict[str, object]:
    """Build the ``btc_briefing_history/1`` dict for the span before ``through_day``.

    The dict carries ``version``, ``through_day``, every section in
    ``HISTORY_SECTIONS`` as a list of row dicts straight from
    ``briefing_history_bars.sql``, and ``sha256`` over the rest of itself.
    ``bars_15m`` covers the ``HISTORY_15M_DAYS`` days and ``bars_1d`` the
    ``HISTORY_1D_DAYS`` days ending at midnight opening ``through_day``, which
    is itself excluded: that day is published as its own feed file. Raises if
    either span is short by a bar, carries a bar built from fewer than its
    full complement of distinct 1m minutes, or carries a duplicated 1m source
    row, so a gap can never masquerade as a published history.
    """
    bars_15m = _span_bars(
        client,
        through_day=through_day,
        days=HISTORY_15M_DAYS,
        bucket_seconds=BARS_15M_SECONDS,
    )
    _require_complete_bars(
        bars_15m,
        through_day=through_day,
        days=HISTORY_15M_DAYS,
        section='bars_15m',
        expected_bars=HISTORY_15M_DAYS * BARS_15M_PER_DAY,
        minutes_per_bar=BARS_15M_SECONDS // 60,
    )

    bars_1d = _span_bars(
        client,
        through_day=through_day,
        days=HISTORY_1D_DAYS,
        bucket_seconds=BARS_1D_SECONDS,
    )
    _require_complete_bars(
        bars_1d,
        through_day=through_day,
        days=HISTORY_1D_DAYS,
        section='bars_1d',
        expected_bars=HISTORY_1D_DAYS,
        minutes_per_bar=MINUTES_PER_DAY,
    )

    history: dict[str, object] = {
        'version': HISTORY_VERSION,
        'through_day': through_day.isoformat(),
        'bars_15m': bars_15m,
        'bars_1d': bars_1d,
    }
    history['sha256'] = _content_sha256(history)
    return history


def _get_huggingface_token() -> str:
    token = os.environ.get('HF_TOKEN') or os.environ.get('HUGGINGFACE_HUB_TOKEN')
    if not token:
        raise RuntimeError(
            'HF_TOKEN or HUGGINGFACE_HUB_TOKEN must be set before publishing to Hugging Face.'
        )
    return token


class _HfApiProtocol(Protocol):
    def repo_exists(self, *, repo_id: str, repo_type: str) -> bool:
        raise NotImplementedError

    def file_exists(self, *, repo_id: str, filename: str, repo_type: str) -> bool:
        raise NotImplementedError

    def hf_hub_download(self, *, repo_id: str, filename: str, repo_type: str) -> str:
        raise NotImplementedError

    def create_repo(self, *, repo_id: str, repo_type: str, exist_ok: bool) -> None:
        raise NotImplementedError

    def upload_folder(
        self,
        *,
        folder_path: str,
        repo_id: str,
        repo_type: str,
        commit_message: str,
    ) -> None:
        raise NotImplementedError


def _make_hf_api(token: str) -> _HfApiProtocol:
    return cast(_HfApiProtocol, HfApi(token=token))


def _published_through_day(api: _HfApiProtocol, repo_id: str) -> date | None:
    """The day named by the dataset's current history file, or None before the first publish.

    Read from the history file itself rather than from the dataset's small
    ``latest.json``: that pointer names the day of the *feed* file, a separate
    contract that says nothing about which window the history carries.
    """
    if not api.repo_exists(repo_id=repo_id, repo_type='dataset'):
        return None
    if not api.file_exists(repo_id=repo_id, filename=HISTORY_FILE_NAME, repo_type='dataset'):
        return None
    published_path = api.hf_hub_download(
        repo_id=repo_id, filename=HISTORY_FILE_NAME, repo_type='dataset'
    )
    published_raw: object = json.loads(Path(published_path).read_text(encoding='utf-8'))
    if not isinstance(published_raw, dict):
        raise RuntimeError(f'{repo_id} {HISTORY_FILE_NAME} is not a JSON object.')
    published = cast(dict[str, object], published_raw)
    # The version is checked before the day is trusted. This guard decides whether
    # a freshly built window may replace the published one, so reading a
    # through_day out of a payload written under some other contract -- an older
    # schema, a hand edit, a different producer -- would answer that question
    # from a field that need not mean what this code assumes.
    published_version = published.get('version')
    if published_version != HISTORY_VERSION:
        raise RuntimeError(
            f'{repo_id} {HISTORY_FILE_NAME} declares version {published_version!r}, '
            f'not {HISTORY_VERSION!r}.'
        )
    published_day = published.get('through_day')
    if not isinstance(published_day, str):
        raise RuntimeError(f'{repo_id} {HISTORY_FILE_NAME} does not carry a through_day string.')
    return date.fromisoformat(published_day)


def publish_briefing_history_to_huggingface(
    history: Mapping[str, object], *, repo_id: str, token: str
) -> dict[str, object]:
    """Upload the rolling history to the HuggingFace dataset ``repo_id``.

    Writes the single file ``btc_briefing_history.json``, rewritten in place
    on every publish: the history is a moving window, so there is no per-day
    accumulation and no latest pointer. Whether this window may replace the
    published one is settled by ``publish_btc_briefing_history`` below before
    the window is built, so every call here writes.
    """
    raw_through_day = history['through_day']
    if not isinstance(raw_through_day, str):
        raise RuntimeError(
            f'History through_day must be an ISO date string, got {raw_through_day!r}.'
        )

    history_bytes = (json.dumps(history, sort_keys=True) + '\n').encode('utf-8')
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / HISTORY_FILE_NAME).write_bytes(history_bytes)
        api = _make_hf_api(token)
        api.create_repo(repo_id=repo_id, repo_type='dataset', exist_ok=True)
        api.upload_folder(
            folder_path=str(tmp_path),
            repo_id=repo_id,
            repo_type='dataset',
            commit_message=f'Update {HISTORY_VERSION} through {raw_through_day}',
        )

    return {
        'repo_id': repo_id,
        'file_name': HISTORY_FILE_NAME,
        'through_day': raw_through_day,
        'sha256': history['sha256'],
    }


@asset(
    name='publish_btc_briefing_history',
    partitions_def=daily_partitions,
    group_name='binance_data',
    description=(
        'Builds, validates and publishes the rolling BTC briefing history '
        '(btc_briefing_history/1) for the 70 15m days and 61 daily days before '
        'the partition day from the origo ClickHouse tables to the '
        'vaquum/btc_briefing_feed HuggingFace dataset.'
    ),
)
def publish_btc_briefing_history(context: AssetExecutionContext) -> dict[str, object]:
    through_day = date.fromisoformat(context.partition_key)

    # Both refusals below are decided before the rollup: build_briefing_history
    # scans the whole span of the 1m projection twice, and neither a missing
    # credential nor an already-newer published window has any use for what it
    # would produce.
    token = _get_huggingface_token()
    published_through_day = _published_through_day(_make_hf_api(token), BRIEFING_DATASET_REPO_ID)
    if published_through_day is not None and through_day < published_through_day:
        context.log.info(
            f'Refused {HISTORY_VERSION} through {through_day.isoformat()}: '
            f'{BRIEFING_DATASET_REPO_ID} already carries the newer '
            f'{published_through_day.isoformat()}, and the rolling file never rolls '
            'backwards. Nothing was built and nothing was uploaded.'
        )
        return {
            'repo_id': BRIEFING_DATASET_REPO_ID,
            'file_name': HISTORY_FILE_NAME,
            'through_day': through_day.isoformat(),
            'uploaded': False,
        }

    client = _make_clickhouse_arrow_client()
    try:
        history = build_briefing_history(client, through_day)
    finally:
        client.close()

    result = publish_briefing_history_to_huggingface(
        history,
        repo_id=BRIEFING_DATASET_REPO_ID,
        token=token,
    )

    section_counts = {
        section_name: len(cast(list[dict[str, object]], history[section_name]))
        for section_name in HISTORY_SECTIONS
    }
    context.log.info(
        f'Published {HISTORY_VERSION} through {through_day.isoformat()} to '
        f'{BRIEFING_DATASET_REPO_ID}: {section_counts}.'
    )
    return {**result, 'uploaded': True}
