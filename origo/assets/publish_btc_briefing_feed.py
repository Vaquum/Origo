"""Build the daily BTC briefing feed from the origo ClickHouse tables.

The feed is one dict per UTC day carrying the six sections in
``FEED_SECTIONS``: 15-minute and daily OHLCV bars from the 1m
``binance_spot_klines`` projection, measured volume-at-price from
``binance_daily_spot_trades``, and per-minute series, exact daily
percentiles and 8-hour session aggregates of the ``binance_spot_depth200_1m``
book projection.

Completeness policy: bars must cover the day exactly (96 x 15 distinct
source minutes, 1 x 1440, with no duplicated 1m row) and every section must
be non-empty, otherwise the build raises instead of returning a short or
double-counted feed. Book minute coverage below
1440 is carried visibly in the section rows rather than rejected, because
the per-minute book capture is best-effort and a single missed minute must
not suppress the whole feed.

Every time field the sections carry (``bar_start``, ``minute_start``,
``session_start``) is declared as UTC epoch seconds in the SQL itself via
``toUnixTimestamp``, so the feed's time representation is part of the
``btc_briefing/2`` contract rather than inherited from the server's Arrow
serialization of ``DateTime``.

The asset is daily-partitioned: each partition builds, validates and
publishes that day's feed to the ``vaquum/btc_briefing_feed`` HuggingFace
dataset as one JSON file per day plus a ``latest.json`` pointer, following
the snapshot pattern of the kline publishers.
"""

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol, cast

from dagster import AssetExecutionContext, asset
from huggingface_hub import HfApi

from .daily_trades_to_origo import daily_partitions

FEED_VERSION: Final[str] = 'btc_briefing/2'
FEED_SECTIONS: Final[tuple[str, ...]] = (
    'bars_15m',
    'bars_1d',
    'volume_at_price',
    'book_percentiles',
    'book_series',
    'book_sessions',
)

BARS_15M_SECONDS = 900
BARS_1D_SECONDS = 86400
MINUTES_PER_DAY = 1440
BARS_15M_PER_DAY = 96
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
BRIEFING_DATASET_REPO_ID = 'vaquum/btc_briefing_feed'

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


def _section_rows(
    client: _ClickHouseArrowClientProtocol,
    sql_file_name: str,
    parameters: Mapping[str, object],
) -> list[dict[str, object]]:
    arrow_table = client.query_arrow(
        (_SQL_DIR / sql_file_name).read_text(encoding='utf-8'),
        parameters=parameters,
    )
    return arrow_table.to_pylist()


def _require_complete_bars(
    bars: list[dict[str, object]],
    *,
    day: date,
    section: str,
    expected_bars: int,
    minutes_per_bar: int,
) -> None:
    if len(bars) != expected_bars:
        raise RuntimeError(
            f'{day.isoformat()} has {len(bars)} {section} bars where {expected_bars} '
            'are required; refusing to build a short briefing feed.'
        )
    short_bars = [bar for bar in bars if bar['source_minutes'] != minutes_per_bar]
    if short_bars:
        raise RuntimeError(
            f'{day.isoformat()} has {len(short_bars)} {section} bars not built from '
            f'exactly {minutes_per_bar} distinct source minutes; refusing to build '
            'a short briefing feed.'
        )
    duplicated_bars = [bar for bar in bars if bar['source_rows'] != bar['source_minutes']]
    if duplicated_bars:
        raise RuntimeError(
            f'{day.isoformat()} has {len(duplicated_bars)} {section} bars with '
            'duplicated 1m source rows; refusing to build a briefing feed from a '
            'corrupt projection.'
        )


def _require_rows(rows: list[dict[str, object]], *, day: date, section: str) -> None:
    if not rows:
        raise RuntimeError(
            f'{day.isoformat()} returned no {section} rows; refusing to build an '
            'empty briefing section.'
        )


def build_briefing_feed(
    client: _ClickHouseArrowClientProtocol, day: date
) -> dict[str, object]:
    """Build the ``btc_briefing/2`` feed dict for one complete UTC day.

    The dict carries ``feed_version``, ``day`` and every section in
    ``FEED_SECTIONS``, each section a list of row dicts straight from its
    SQL file in ``origo/assets/sql/``. Raises if the day's bars are not
    complete (96 x 15 source minutes, 1 x 1440) or any section comes back
    empty, so a short day can never masquerade as a published feed.
    """
    bars_15m = _section_rows(
        client, 'briefing_bars.sql', {'day': day, 'bucket_seconds': BARS_15M_SECONDS}
    )
    _require_complete_bars(
        bars_15m,
        day=day,
        section='bars_15m',
        expected_bars=BARS_15M_PER_DAY,
        minutes_per_bar=BARS_15M_SECONDS // 60,
    )

    bars_1d = _section_rows(
        client, 'briefing_bars.sql', {'day': day, 'bucket_seconds': BARS_1D_SECONDS}
    )
    _require_complete_bars(
        bars_1d,
        day=day,
        section='bars_1d',
        expected_bars=1,
        minutes_per_bar=MINUTES_PER_DAY,
    )

    sections: dict[str, list[dict[str, object]]] = {
        'bars_15m': bars_15m,
        'bars_1d': bars_1d,
        'volume_at_price': _section_rows(
            client, 'briefing_volume_at_price.sql', {'day': day}
        ),
        'book_percentiles': _section_rows(
            client, 'briefing_book_percentiles.sql', {'day': day}
        ),
        'book_series': _section_rows(client, 'briefing_book_series.sql', {'day': day}),
        'book_sessions': _section_rows(
            client, 'briefing_book_sessions.sql', {'day': day}
        ),
    }
    for section_name, section_rows in sections.items():
        _require_rows(section_rows, day=day, section=section_name)

    return {
        'feed_version': FEED_VERSION,
        'day': day.isoformat(),
        **sections,
    }


def _get_huggingface_token() -> str:
    token = os.environ.get('HF_TOKEN') or os.environ.get('HUGGINGFACE_HUB_TOKEN')
    if not token:
        raise RuntimeError(
            'HF_TOKEN or HUGGINGFACE_HUB_TOKEN must be set before publishing to Hugging Face.'
        )
    return token


def _build_dataset_card(*, day: str, file_name: str, sha256: str) -> str:
    return f"""# BTC daily briefing feed

This dataset retains one JSON file per published UTC day. Read each file's
`feed_version`; historical files keep the contract under which they were
published.

The latest snapshot uses `{FEED_VERSION}` and is built from the origo ClickHouse
tables: 15m/1d OHLCV bars, measured volume-at-price in integer satoshis split
by taker side, and per-minute series, exact daily percentiles and 8h session
aggregates of the depth200 book. Its time fields are UTC epoch seconds.

Latest snapshot:

- file: `{file_name}`
- day: `{day}`
- sha256: `{sha256}`
"""


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


def _published_latest_day(api: _HfApiProtocol, repo_id: str) -> date | None:
    """The day named by the dataset's current ``latest.json``, or None before the first publish."""
    if not api.repo_exists(repo_id=repo_id, repo_type='dataset'):
        return None
    if not api.file_exists(repo_id=repo_id, filename='latest.json', repo_type='dataset'):
        return None
    latest_path = api.hf_hub_download(
        repo_id=repo_id, filename='latest.json', repo_type='dataset'
    )
    latest_raw: object = json.loads(Path(latest_path).read_text(encoding='utf-8'))
    if not isinstance(latest_raw, dict):
        raise RuntimeError(f'{repo_id} latest.json is not a JSON object.')
    latest_day = cast(dict[str, object], latest_raw).get('day')
    if not isinstance(latest_day, str):
        raise RuntimeError(f'{repo_id} latest.json does not carry a day string.')
    return date.fromisoformat(latest_day)


def publish_briefing_feed_to_huggingface(
    feed: Mapping[str, object], *, repo_id: str, token: str
) -> dict[str, object]:
    """Upload one day's feed dict to the HuggingFace dataset ``repo_id``.

    Writes ``btc_briefing_<yyyymmdd>.json`` and uploads it in one commit; past
    day files are left in place, so the dataset accumulates one file per
    published day. The ``latest.json`` pointer and the dataset card are
    rewritten only when this day is at least the day the dataset currently
    points at: backfilling an older partition never repoints ``latest`` at a
    stale day, while re-publishing the pointed-at day refreshes the pointer so
    its checksum always matches the current bytes of the file it names.
    """
    raw_day = feed['day']
    if not isinstance(raw_day, str):
        raise RuntimeError(f'Feed day must be an ISO date string, got {raw_day!r}.')
    published_day = date.fromisoformat(raw_day)
    day = published_day.isoformat()

    file_name = f'btc_briefing_{day.replace("-", "")}.json'
    feed_bytes = (json.dumps(feed, sort_keys=True) + '\n').encode('utf-8')
    sha256 = hashlib.sha256(feed_bytes).hexdigest()

    api = _make_hf_api(token)
    latest_day = _published_latest_day(api, repo_id)
    advance_latest = latest_day is None or published_day >= latest_day

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / file_name).write_bytes(feed_bytes)
        if advance_latest:
            (tmp_path / 'latest.json').write_text(
                json.dumps(
                    {
                        'feed_version': FEED_VERSION,
                        'day': day,
                        'file_name': file_name,
                        'sha256': sha256,
                        'generated_at_utc': datetime.now(UTC).isoformat(),
                    },
                    indent=2,
                )
                + '\n',
                encoding='utf-8',
            )
            (tmp_path / 'README.md').write_text(
                _build_dataset_card(day=day, file_name=file_name, sha256=sha256),
                encoding='utf-8',
            )

        api.create_repo(repo_id=repo_id, repo_type='dataset', exist_ok=True)
        api.upload_folder(
            folder_path=str(tmp_path),
            repo_id=repo_id,
            repo_type='dataset',
            commit_message=f'Add {FEED_VERSION} feed for {day}',
        )

    return {
        'repo_id': repo_id,
        'file_name': file_name,
        'day': day,
        'sha256': sha256,
        'latest_updated': advance_latest,
    }


@asset(
    name='publish_btc_briefing_feed',
    partitions_def=daily_partitions,
    group_name='binance_data',
    description=(
        'Builds, validates and publishes the daily BTC briefing feed '
        '(btc_briefing/2) for the partition day from the origo ClickHouse tables '
        'to the vaquum/btc_briefing_feed HuggingFace dataset.'
    ),
)
def publish_btc_briefing_feed(context: AssetExecutionContext) -> dict[str, object]:
    day = date.fromisoformat(context.partition_key)
    client = _make_clickhouse_arrow_client()
    try:
        feed = build_briefing_feed(client, day)
    finally:
        client.close()

    result = publish_briefing_feed_to_huggingface(
        feed,
        repo_id=BRIEFING_DATASET_REPO_ID,
        token=_get_huggingface_token(),
    )

    section_counts = {
        section_name: len(cast(list[dict[str, object]], feed[section_name]))
        for section_name in FEED_SECTIONS
    }
    context.log.info(
        f'Published {FEED_VERSION} feed for {day.isoformat()} to '
        f'{BRIEFING_DATASET_REPO_ID}: {section_counts}.'
    )
    return result
