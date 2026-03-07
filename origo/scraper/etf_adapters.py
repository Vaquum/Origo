from __future__ import annotations

import csv
import hashlib
import html as html_lib
import importlib
import io
import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from types import ModuleType
from typing import Any, Final, cast
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from .browser_fetch import BrowserEngine, fetch_browser_source
from .contracts import (
    MetricValue,
    NormalizedMetricRecord,
    ParsedRecord,
    RawArtifact,
    ScraperAdapter,
    ScrapeRunContext,
    SourceDescriptor,
)
from .http_fetch import fetch_http_source
from .normalize import normalize_parsed_records

_HTTP_TIMEOUT_SECONDS: Final[float] = 30.0
_USER_AGENT: Final[str] = 'origo-scraper/0.1 (+https://github.com/Vaquum/Origo)'
_RE_BUILD_ID: Final[re.Pattern[str]] = re.compile(r'"buildId":"([A-Za-z0-9_-]+)"')
_RE_NON_ALNUM: Final[re.Pattern[str]] = re.compile(r'[^a-z0-9]+')
_RE_ISHARES_AS_OF: Final[re.Pattern[str]] = re.compile(
    r'^Fund Holdings as of,\s*"([^"]+)"',
    re.MULTILINE,
)
_RE_ISHARES_SHARES_OUTSTANDING: Final[re.Pattern[str]] = re.compile(
    r'^Shares Outstanding,\s*"([^"]+)"',
    re.MULTILINE,
)
_RE_HTML_SCRIPT_BLOCK: Final[re.Pattern[str]] = re.compile(
    r'(?is)<script.*?>.*?</script>'
)
_RE_HTML_STYLE_BLOCK: Final[re.Pattern[str]] = re.compile(r'(?is)<style.*?>.*?</style>')
_RE_HTML_TAG: Final[re.Pattern[str]] = re.compile(r'(?is)<[^>]+>')
_RE_HASHDEX_DEFI_PRODUCT_START: Final[re.Pattern[str]] = re.compile(
    r'"Hashdex Bitcoin ETF","DEFI",'
)
_RE_HASHDEX_DEFI_HOLDING_REFS: Final[re.Pattern[str]] = re.compile(
    r'\{"values":\d+,"downloadLink":\d+\},\[(?P<holding_refs>[^\]]+)\]'
)
_RE_HASHDEX_DEFI_DOWNLOAD_LINK: Final[re.Pattern[str]] = re.compile(
    r'"Hashdex Bitcoin ETF","DEFI".*?"(?P<download_link>https://hdx-website-cms-prod-upload-bucket\.s3\.amazonaws\.com/DEFI_Holdings\.xlsx)"',
    re.S,
)
_RE_HASHDEX_DEFI_REFERENCE_ROW_FULL: Final[re.Pattern[str]] = re.compile(
    r'\{"name":\d+,"price":\d+,"shares":\d+,"ticker":\d+,"weight":\d+,"referenceDate":\d+,"totalNetAssets":\d+,"sharesOutstanding":\d+,"weight_percentage":\d+\},"[^"]+","[^"]+","[^"]+","[^"]+","[^"]+","(?P<reference_date>[^"]+)","(?P<total_net_assets>[^"]+)","(?P<shares_outstanding>[^"]+)","[^"]+"',
    re.S,
)
_RE_HASHDEX_DEFI_BITCOIN_ROW_FULL: Final[re.Pattern[str]] = re.compile(
    r'\{"name":\d+,"price":\d+,"shares":\d+,"ticker":\d+,"weight":\d+,"referenceDate":\d+,"totalNetAssets":\d+,"sharesOutstanding":\d+,"weight_percentage":\d+\},"Bitcoin","(?P<price>[^"]+)","(?P<shares>[^"]+)","BTC","(?P<weight>[^"]+)","(?P<reference_date>[^"]+)","(?P<total_net_assets>[^"]+)","(?P<shares_outstanding>[^"]+)","(?P<weight_percentage>[^"]+)"',
    re.S,
)
_RE_HASHDEX_DEFI_BITCOIN_ROW_SHORT: Final[re.Pattern[str]] = re.compile(
    r'\{"name":\d+,"price":\d+,"shares":\d+,"ticker":\d+,"weight":\d+,"referenceDate":\d+,"totalNetAssets":\d+,"sharesOutstanding":\d+,"weight_percentage":\d+\},"Bitcoin","(?P<price>[^"]+)","(?P<shares>[^"]+)","BTC","(?P<weight>[^"]+)","(?P<weight_percentage>[^"]+)"',
    re.S,
)

_ISHARES_IBIT_HOLDINGS_URI: Final[str] = (
    'https://www.ishares.com/us/products/333011/fund/1467271812596.ajax'
    '?dataType=fund&fileName=IBIT_holdings&fileType=csv'
)

_INVESCO_BTCO_HOLDINGS_URI: Final[str] = (
    'https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/'
    '46091J101/holdings/fund?idType=cusip&productType=ETF'
)

_BITWISE_BITB_HOME_URI: Final[str] = 'https://bitbetf.com/'
_BITWISE_NEXT_DATA_URI_TEMPLATE: Final[str] = (
    'https://bitbetf.com/_next/data/{build_id}/index.json'
)

_ARK_ARKB_HOLDINGS_URI: Final[str] = (
    'https://assets.ark-funds.com/fund-documents/funds-etf-csv/'
    'ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv'
)

_VANECK_HODL_HOLDINGS_URI: Final[str] = (
    'https://www.vaneck.com/Main/HoldingsBlock/GetDataset/'
    '?blockId=348327&pageId=243755&ticker=HODL'
)

_FRANKLIN_EZBC_PRODUCT_URI: Final[str] = (
    'https://www.franklintempleton.com/investments/options/'
    'exchange-traded-funds/products/39639/SINGLCLASS'
)

_GRAYSCALE_GBTC_PAGE_URI: Final[str] = 'https://etfs.grayscale.com/gbtc'
_FIDELITY_FBTC_PAGE_URI: Final[str] = (
    'https://digital.fidelity.com/prgw/digital/research/quote/dashboard/summary'
    '?symbol=FBTC'
)
_COINSHARES_BRRR_PAGE_URI: Final[str] = 'https://coinshares.com/us/etf/brrr/'
_HASHDEX_DEFI_PAYLOAD_URI: Final[str] = 'https://hashdex.com/en-US/_payload.json'
_BROWSER_ENGINE_FIREFOX: Final[BrowserEngine] = 'firefox'

_FRANKLIN_REQUIRED_PDS_OPS: Final[tuple[str, ...]] = (
    'ProductCommon',
    'Identifiers',
    'PricingDetails',
    'Aum',
    'Holdings',
)

_DATE_FORMATS: Final[tuple[str, ...]] = (
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%m/%d/%Y',
    '%m-%d-%Y',
    '%b-%d-%Y',
    '%b %d, %Y',
    '%B %d, %Y',
    '%d-%b-%Y',
)
_ISSUER_ALLOWED_HOST_SUFFIXES: Final[dict[str, tuple[str, ...]]] = {
    'ishares': ('ishares.com',),
    'invesco': ('invesco.com',),
    'bitwise': ('bitbetf.com',),
    'ark': ('ark-funds.com',),
    'vaneck': ('vaneck.com',),
    'franklin': ('franklintempleton.com',),
    'grayscale': ('grayscale.com',),
    'fidelity': ('fidelity.com',),
    'coinshares': ('coinshares.com',),
    'hashdex': ('hashdex.com',),
}
_ETF_DAILY_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    'row_index',
    'issuer',
    'ticker',
    'as_of_date',
    'btc_units',
    'btc_market_value_usd',
    'total_net_assets_usd',
    'holdings_row_count',
)
_ETF_DAILY_OPTIONAL_FIELDS: Final[tuple[str, ...]] = (
    'btc_weight_pct',
    'cash_market_value_usd',
    'shares_outstanding',
    'nav_per_share_usd',
    'market_price_per_share_usd',
    'premium_discount_pct',
    'net_assets_as_of_date',
    'total_net_assets_reported_usd',
    'total_holdings_market_value_usd',
    'wallet_count',
    'wallet_balance_total_btc',
    'proof_of_reserves_total_btc',
    'proof_of_reserves_timestamp_utc',
    'fund_data_updated_at_utc',
    'btc_price_usd',
    'holdings_download_url',
    'source_cusip',
    'source_isin',
    'source_fund_id',
)
_ETF_DAILY_ALLOWED_FIELDS: Final[frozenset[str]] = frozenset(
    _ETF_DAILY_REQUIRED_FIELDS + _ETF_DAILY_OPTIONAL_FIELDS
)
_ETF_DAILY_ALLOWED_NORMALIZED_METRICS: Final[frozenset[str]] = frozenset(
    {*_ETF_DAILY_ALLOWED_FIELDS, 'observed_at_utc'}
)
_ETF_DAILY_METRIC_UNITS: Final[dict[str, str]] = {
    'btc_units': 'BTC',
    'wallet_balance_total_btc': 'BTC',
    'proof_of_reserves_total_btc': 'BTC',
    'btc_market_value_usd': 'USD',
    'total_net_assets_usd': 'USD',
    'total_net_assets_reported_usd': 'USD',
    'total_holdings_market_value_usd': 'USD',
    'cash_market_value_usd': 'USD',
    'nav_per_share_usd': 'USD',
    'market_price_per_share_usd': 'USD',
    'btc_price_usd': 'USD',
    'btc_weight_pct': 'PCT',
    'premium_discount_pct': 'PCT',
    'holdings_row_count': 'COUNT',
    'shares_outstanding': 'COUNT',
    'wallet_count': 'COUNT',
}


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    result: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        result[raw_key] = raw_value
    return result


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _expect_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return value


def _parse_optional_float(value: Any, *, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError(f'{label} must not be boolean')
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw == '':
            return None
        if raw in {'-', '--', '\u2013', '\u2014', '\u2212'}:
            return None
        normalized = raw.replace(',', '').replace('$', '').replace('%', '')
        if normalized.startswith('(') and normalized.endswith(')'):
            normalized = f'-{normalized[1:-1]}'
        try:
            return float(normalized)
        except ValueError as exc:
            raise RuntimeError(
                f'{label} must be numeric-compatible, got: {value}'
            ) from exc
    raise RuntimeError(f'{label} has unsupported numeric type: {type(value)}')


def _parse_required_float(value: Any, *, label: str) -> float:
    parsed = _parse_optional_float(value, label=label)
    if parsed is None:
        raise RuntimeError(f'{label} must be present and numeric')
    return parsed


def _parse_date_to_iso(value: str, *, label: str) -> str:
    raw = value.strip()
    if raw == '':
        raise RuntimeError(f'{label} must be non-empty')

    for date_format in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(raw, date_format)
            return parsed.date().isoformat()
        except ValueError:
            continue

    normalized = raw.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RuntimeError(f'{label} is not a supported date format: {value}') from exc

    if parsed.tzinfo is None:
        return parsed.date().isoformat()
    return parsed.astimezone(UTC).date().isoformat()


def _normalize_hostname(hostname: str) -> str:
    normalized = hostname.strip().lower().rstrip('.')
    if normalized == '':
        raise RuntimeError('Hostname must be non-empty')
    return normalized


def _allowed_host_suffixes_for_issuer(*, issuer: str) -> tuple[str, ...]:
    suffixes = _ISSUER_ALLOWED_HOST_SUFFIXES.get(issuer)
    if suffixes is None or len(suffixes) == 0:
        raise RuntimeError(f'No allowed host suffixes configured for issuer={issuer}')
    return tuple(_normalize_hostname(value) for value in suffixes)


def _host_matches_suffixes(*, host: str, suffixes: tuple[str, ...]) -> bool:
    normalized_host = _normalize_hostname(host)
    for suffix in suffixes:
        if normalized_host == suffix:
            return True
        if normalized_host.endswith(f'.{suffix}'):
            return True
    return False


def _uri_is_allowed_for_issuer(*, source_uri: str, issuer: str) -> bool:
    parsed = urlparse(source_uri)
    if parsed.scheme.lower() != 'https':
        return False
    if parsed.hostname is None:
        return False
    return _host_matches_suffixes(
        host=parsed.hostname,
        suffixes=_allowed_host_suffixes_for_issuer(issuer=issuer),
    )


def _assert_uri_origin_allowed_for_issuer(
    *,
    source_uri: str,
    issuer: str,
    context_label: str,
) -> None:
    parsed = urlparse(source_uri)
    if parsed.scheme.lower() != 'https':
        raise RuntimeError(
            f'{context_label} must use https for issuer={issuer}, got uri={source_uri}'
        )
    if parsed.hostname is None:
        raise RuntimeError(
            f'{context_label} is missing hostname for issuer={issuer}, uri={source_uri}'
        )
    allowed_suffixes = _allowed_host_suffixes_for_issuer(issuer=issuer)
    if not _host_matches_suffixes(host=parsed.hostname, suffixes=allowed_suffixes):
        raise RuntimeError(
            f'{context_label} host is not issuer-origin for issuer={issuer}: '
            f'host={parsed.hostname}, allowed_suffixes={allowed_suffixes}'
        )


def _assert_artifact_origin_allowed_for_issuer(
    *,
    artifact: RawArtifact,
    source: SourceDescriptor,
    issuer: str,
    adapter_name: str,
) -> None:
    final_url = artifact.metadata.get('final_url')
    candidate_url = final_url if final_url is not None else source.source_uri
    _assert_uri_origin_allowed_for_issuer(
        source_uri=candidate_url,
        issuer=issuer,
        context_label=f'{adapter_name} fetch result',
    )


def _html_lines_from_content(content: bytes) -> list[str]:
    html_text = content.decode('utf-8', errors='replace')
    no_script = _RE_HTML_SCRIPT_BLOCK.sub(' ', html_text)
    no_style = _RE_HTML_STYLE_BLOCK.sub(' ', no_script)
    tags_to_newlines = _RE_HTML_TAG.sub('\n', no_style)
    unescaped = html_lib.unescape(tags_to_newlines).replace('\xa0', ' ')
    no_carriage = unescaped.replace('\r', '')
    normalized_newlines = re.sub(r'\n+', '\n', no_carriage)
    normalized_spaces = re.sub(r'[ \t]+', ' ', normalized_newlines)
    return [line.strip() for line in normalized_spaces.split('\n') if line.strip() != '']


def _find_line_index_containing(
    *,
    lines: Sequence[str],
    needle: str,
    label: str,
    start_index: int = 0,
) -> int:
    lowered_needle = needle.lower()
    for index in range(start_index, len(lines)):
        if lowered_needle in lines[index].lower():
            return index
    raise RuntimeError(f'{label} not found in rendered source lines')


def _find_line_index_exact(
    *,
    lines: Sequence[str],
    candidates: tuple[str, ...],
    label: str,
    start_index: int = 0,
) -> int:
    normalized = {candidate.strip().lower() for candidate in candidates}
    for index in range(start_index, len(lines)):
        if lines[index].strip().lower() in normalized:
            return index
    raise RuntimeError(f'{label} not found in rendered source lines')


def _extract_as_of_after_index(
    *,
    lines: Sequence[str],
    anchor_index: int,
    lookahead: int,
    label: str,
) -> str:
    end_index = min(len(lines), anchor_index + lookahead + 1)
    as_of_re = re.compile(r'As of\s+(.+)$', re.IGNORECASE)
    for idx in range(anchor_index, end_index):
        match = as_of_re.search(lines[idx])
        if match is None:
            continue
        value = match.group(1).strip()
        if value != '':
            return value
    raise RuntimeError(f'{label} missing "As of ..." value')


def _extract_numeric_value_after_index(
    *,
    lines: Sequence[str],
    anchor_index: int,
    lookahead: int,
    label: str,
) -> str:
    numeric_re = re.compile(r'^[\+\-]?\$?[A-Za-z0-9,\.\-%]+$')
    end_index = min(len(lines), anchor_index + lookahead + 1)
    for idx in range(anchor_index + 1, end_index):
        candidate = lines[idx].strip()
        if candidate == '':
            continue
        if candidate.lower().startswith('as of '):
            continue
        if candidate.lower() == 'close':
            continue
        if numeric_re.match(candidate) is None:
            continue
        return candidate
    raise RuntimeError(f'{label} numeric value not found')


def _observed_at_utc_for_day(day_iso: str) -> str:
    return f'{day_iso}T00:00:00+00:00'


def _canonicalize_key(value: str) -> str:
    normalized = _RE_NON_ALNUM.sub('', value.lower())
    return normalized


def _row_value(
    row: dict[str | None, str | None],
    *,
    candidates: tuple[str, ...],
) -> str | None:
    by_key: dict[str, str | None] = {}
    for key, cell in row.items():
        if key is None:
            continue
        by_key[_canonicalize_key(key)] = cell
    for candidate in candidates:
        cell = by_key.get(_canonicalize_key(candidate))
        if cell is None:
            continue
        stripped = cell.strip()
        if stripped == '':
            continue
        return stripped
    return None


def _build_record_id(*, artifact_id: str, payload: dict[str, MetricValue]) -> str:
    digest = hashlib.sha256()
    digest.update(artifact_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        ).encode('utf-8')
    )
    return digest.hexdigest()


def _validate_payload(payload: dict[str, Any]) -> dict[str, MetricValue]:
    normalized: dict[str, MetricValue] = {}
    for key, value in payload.items():
        if key.strip() == '':
            raise RuntimeError('Parsed payload keys must be non-empty')
        if value is None or isinstance(value, (str, int, float, bool)):
            normalized[key] = value
            continue
        raise RuntimeError(
            f'Parsed payload value for key={key} must be scalar, got {type(value)}'
        )
    return normalized


def _normalize_optional_count_field(
    *,
    payload: dict[str, Any],
    field_name: str,
    context_label: str,
) -> None:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return
    parsed_value = _parse_required_float(
        raw_value,
        label=f'{context_label}.{field_name}',
    )
    if not parsed_value.is_integer():
        raise RuntimeError(
            f'{context_label}.{field_name} must be a whole number, got {parsed_value}'
        )
    payload[field_name] = int(parsed_value)


def _canonicalize_etf_daily_payload(
    *,
    payload: dict[str, Any],
    context_label: str,
) -> dict[str, Any]:
    missing_fields = sorted(
        field_name
        for field_name in _ETF_DAILY_REQUIRED_FIELDS
        if field_name not in payload or payload[field_name] is None
    )
    if len(missing_fields) != 0:
        raise RuntimeError(
            f'{context_label} missing required ETF daily fields: {missing_fields}'
        )

    unexpected_fields = sorted(
        field_name
        for field_name in payload
        if field_name not in _ETF_DAILY_ALLOWED_FIELDS
    )
    if len(unexpected_fields) != 0:
        raise RuntimeError(
            f'{context_label} includes unexpected ETF daily fields: {unexpected_fields}'
        )

    canonical_payload = dict(payload)
    canonical_payload['issuer'] = _expect_non_empty_str(
        canonical_payload.get('issuer'),
        f'{context_label}.issuer',
    ).lower()
    canonical_payload['ticker'] = _expect_non_empty_str(
        canonical_payload.get('ticker'),
        f'{context_label}.ticker',
    ).upper()

    _normalize_optional_count_field(
        payload=canonical_payload,
        field_name='row_index',
        context_label=context_label,
    )
    _normalize_optional_count_field(
        payload=canonical_payload,
        field_name='holdings_row_count',
        context_label=context_label,
    )
    _normalize_optional_count_field(
        payload=canonical_payload,
        field_name='wallet_count',
        context_label=context_label,
    )

    return canonical_payload


def _metric_unit_for_etf_metric(metric_name: str) -> str | None:
    return _ETF_DAILY_METRIC_UNITS.get(metric_name)


def _assert_utc_daily_semantics(
    *,
    records: Sequence[NormalizedMetricRecord],
    source_id: str,
) -> None:
    if len(records) == 0:
        raise RuntimeError(
            f'UTC daily semantics check requires non-empty records for source={source_id}'
        )

    as_of_values = [
        record.metric_value
        for record in records
        if record.metric_name == 'as_of_date'
    ]
    if len(as_of_values) != 1:
        raise RuntimeError(
            'ETF UTC daily semantics requires exactly one as_of_date metric '
            f'for source={source_id}, got {len(as_of_values)}'
        )

    as_of_value = as_of_values[0]
    if not isinstance(as_of_value, str) or as_of_value.strip() == '':
        raise RuntimeError(
            f'ETF UTC daily semantics requires non-empty string as_of_date '
            f'for source={source_id}'
        )
    as_of_date = _parse_date_to_iso(as_of_value, label='as_of_date')

    for record in records:
        observed = record.observed_at_utc.astimezone(UTC)
        if (
            observed.hour != 0
            or observed.minute != 0
            or observed.second != 0
            or observed.microsecond != 0
        ):
            raise RuntimeError(
                'ETF UTC daily semantics requires observed_at_utc at midnight '
                f'for source={source_id}, metric={record.metric_name}, '
                f'observed_at_utc={observed.isoformat()}'
            )
        if observed.date().isoformat() != as_of_date:
            raise RuntimeError(
                'ETF UTC daily semantics requires observed day == as_of_date '
                f'for source={source_id}, metric={record.metric_name}, '
                f'observed_day={observed.date().isoformat()}, as_of_date={as_of_date}'
            )


def _build_single_record(
    *,
    artifact: RawArtifact,
    payload: dict[str, Any],
    parser_name: str,
    parser_version: str,
) -> ParsedRecord:
    canonical_payload = _canonicalize_etf_daily_payload(
        payload=payload,
        context_label=parser_name,
    )
    normalized_payload = _validate_payload(canonical_payload)
    return ParsedRecord(
        record_id=_build_record_id(
            artifact_id=artifact.artifact_id,
            payload=normalized_payload,
        ),
        artifact_id=artifact.artifact_id,
        payload=normalized_payload,
        parser_name=parser_name,
        parser_version=parser_version,
        parsed_at_utc=_now_utc(),
    )


def _normalize_daily_records(
    *,
    parsed_records: Sequence[ParsedRecord],
    source: SourceDescriptor,
    artifact: RawArtifact,
) -> list[NormalizedMetricRecord]:
    enriched_records: list[ParsedRecord] = []
    for parsed_record in parsed_records:
        payload = dict(parsed_record.payload)
        as_of_raw = payload.get('as_of_date')
        if not isinstance(as_of_raw, str) or as_of_raw.strip() == '':
            raise RuntimeError('Parsed payload must include non-empty as_of_date')

        as_of_iso = _parse_date_to_iso(as_of_raw, label='as_of_date')
        payload['as_of_date'] = as_of_iso
        payload['observed_at_utc'] = _observed_at_utc_for_day(as_of_iso)

        enriched_records.append(
            ParsedRecord(
                record_id=parsed_record.record_id,
                artifact_id=parsed_record.artifact_id,
                payload=payload,
                parser_name=parsed_record.parser_name,
                parser_version=parsed_record.parser_version,
                parsed_at_utc=parsed_record.parsed_at_utc,
            )
        )

    raw_records = normalize_parsed_records(
        parsed_records=enriched_records,
        source=source,
        artifact=artifact,
    )
    normalized_records: list[NormalizedMetricRecord] = []
    for record in raw_records:
        if record.metric_name not in _ETF_DAILY_ALLOWED_NORMALIZED_METRICS:
            raise RuntimeError(
                'Unexpected ETF normalized metric name from parsed payload: '
                f'{record.metric_name}'
            )
        normalized_records.append(
            NormalizedMetricRecord(
                metric_id=record.metric_id,
                source_id=record.source_id,
                metric_name=record.metric_name,
                metric_value=record.metric_value,
                metric_unit=_metric_unit_for_etf_metric(record.metric_name),
                observed_at_utc=record.observed_at_utc,
                dimensions=dict(record.dimensions),
                provenance=record.provenance,
            )
        )
    _assert_utc_daily_semantics(
        records=normalized_records,
        source_id=source.source_id,
    )
    return normalized_records


def _build_source_descriptor(
    *,
    run_context: ScrapeRunContext,
    source_id: str,
    source_name: str,
    source_uri: str,
    issuer: str,
    ticker: str,
    extra_metadata: dict[str, str] | None = None,
) -> SourceDescriptor:
    _assert_uri_origin_allowed_for_issuer(
        source_uri=source_uri,
        issuer=issuer,
        context_label='source descriptor',
    )

    metadata: dict[str, str] = {
        'issuer': issuer,
        'ticker': ticker,
        'rights_source': issuer,
    }
    if extra_metadata is not None:
        metadata.update(extra_metadata)

    return SourceDescriptor(
        source_id=source_id,
        source_name=source_name,
        source_uri=source_uri,
        discovered_at_utc=run_context.started_at_utc,
        metadata=metadata,
    )


def _load_json_payload(*, artifact: RawArtifact, label: str) -> dict[str, Any]:
    try:
        decoded = json.loads(artifact.content.decode('utf-8'))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'{label} returned invalid JSON: {exc.msg}') from exc
    return _expect_dict(decoded, label)


def _force_json_artifact(artifact: RawArtifact) -> RawArtifact:
    if artifact.artifact_format == 'json':
        return artifact

    metadata = dict(artifact.metadata)
    metadata['forced_artifact_format'] = 'json'
    return RawArtifact(
        artifact_id=artifact.artifact_id,
        source_id=artifact.source_id,
        source_uri=artifact.source_uri,
        fetched_at_utc=artifact.fetched_at_utc,
        fetch_method=artifact.fetch_method,
        artifact_format='json',
        content_sha256=artifact.content_sha256,
        content=artifact.content,
        metadata=metadata,
    )


def _load_playwright_sync_api() -> ModuleType:
    try:
        return importlib.import_module('playwright.sync_api')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'Playwright is required for browser-backed ETF adapters. '
            'Install with `uv add playwright` and run `playwright install chromium`.'
        ) from exc


def _hash_content_bytes(content: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()


def _build_source_artifact_id(
    *,
    source_id: str,
    source_uri: str,
    content_sha256: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(source_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(source_uri.encode('utf-8'))
    digest.update(b'|')
    digest.update(content_sha256.encode('utf-8'))
    return digest.hexdigest()


def _decode_escaped_json_field(raw_value: str) -> str:
    try:
        return cast(str, json.loads(f'"{raw_value}"'))
    except json.JSONDecodeError:
        return raw_value


def _extract_escaped_json_field(*, html: str, field: str, label: str) -> str:
    pattern = re.compile(rf'\\"{re.escape(field)}\\":\\"([^\\"]+)\\"')
    match = pattern.search(html)
    if match is None:
        raise RuntimeError(f'{label} missing escaped field "{field}"')
    return _decode_escaped_json_field(match.group(1))


def _parse_scaled_currency(value: Any, *, label: str) -> float:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')

    raw = value.strip().replace('$', '').replace(',', '')
    scale = 1.0
    suffixes: tuple[tuple[str, float], ...] = (
        ('trillion', 1_000_000_000_000.0),
        ('billion', 1_000_000_000.0),
        ('million', 1_000_000.0),
        ('thousand', 1_000.0),
    )
    lowered = raw.lower()
    for suffix, multiplier in suffixes:
        if lowered.endswith(suffix):
            raw = raw[: -len(suffix)].strip()
            scale = multiplier
            break
    parsed = _parse_required_float(raw, label=label)
    return parsed * scale


def _parse_scaled_compact_number(value: Any, *, label: str) -> float:
    if not isinstance(value, str) or value.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')

    raw = value.strip().replace('\xa0', ' ')
    raw = raw.replace('$', '').replace(',', '').replace('%', '')
    raw = re.sub(r'(?i)\busd\b', '', raw).strip()
    if raw == '':
        raise RuntimeError(f'{label} must contain a numeric value')

    multiplier = 1.0
    suffix_match = re.search(r'([KMBT])$', raw, re.IGNORECASE)
    if suffix_match is not None:
        suffix = suffix_match.group(1).upper()
        raw = raw[:-1].strip()
        if suffix == 'K':
            multiplier = 1_000.0
        elif suffix == 'M':
            multiplier = 1_000_000.0
        elif suffix == 'B':
            multiplier = 1_000_000_000.0
        elif suffix == 'T':
            multiplier = 1_000_000_000_000.0

    parsed = _parse_required_float(raw, label=label)
    return parsed * multiplier


def _fetch_franklin_pds_artifact(
    *,
    source: SourceDescriptor,
    run_context: ScrapeRunContext,
    timeout_ms: int = 90_000,
) -> RawArtifact:
    if timeout_ms <= 0:
        raise ValueError(f'timeout_ms must be > 0, got {timeout_ms}')
    _assert_uri_origin_allowed_for_issuer(
        source_uri=source.source_uri,
        issuer='franklin',
        context_label='Franklin EZBC source URI',
    )

    playwright_module = _load_playwright_sync_api()
    sync_playwright = getattr(playwright_module, 'sync_playwright', None)
    if sync_playwright is None:
        raise RuntimeError('playwright.sync_api.sync_playwright was not found')

    captured_ops: dict[str, dict[str, Any]] = {}
    final_url = source.source_uri
    status_code: int | None = None
    disallowed_response_url: str | None = None

    with sync_playwright() as playwright:
        chromium: Any = getattr(playwright, 'chromium')
        browser = chromium.launch(headless=True)
        try:
            page = browser.new_page()

            def _on_response(response: Any) -> None:
                nonlocal disallowed_response_url
                if not isinstance(response.url, str):
                    return
                if 'api/pds/price-and-performance' not in response.url:
                    return
                if not _uri_is_allowed_for_issuer(
                    source_uri=response.url,
                    issuer='franklin',
                ):
                    disallowed_response_url = response.url
                    return
                status = getattr(response, 'status', None)
                if not isinstance(status, int) or status < 200 or status >= 300:
                    return

                query = parse_qs(urlparse(response.url).query)
                op_values = query.get('op')
                if op_values is None or len(op_values) == 0:
                    return
                op_name = op_values[0]
                if op_name.strip() == '':
                    return

                try:
                    response_text = response.text()
                except Exception:
                    return

                try:
                    decoded = json.loads(response_text)
                except json.JSONDecodeError:
                    return

                decoded_obj = _expect_dict(decoded, f'Franklin PDS op={op_name}')
                captured_ops[op_name] = decoded_obj

            page.on('response', _on_response)
            response = page.goto(
                source.source_uri,
                timeout=timeout_ms,
                wait_until='networkidle',
            )
            final_url_value = page.url
            if isinstance(final_url_value, str) and final_url_value.strip() != '':
                final_url = final_url_value
            if response is not None and isinstance(response.status, int):
                status_code = response.status
        finally:
            browser.close()

    if disallowed_response_url is not None:
        raise RuntimeError(
            'Franklin EZBC browser capture attempted non-issuer origin URL: '
            f'{disallowed_response_url}'
        )

    _assert_uri_origin_allowed_for_issuer(
        source_uri=final_url,
        issuer='franklin',
        context_label='Franklin EZBC final URL',
    )

    missing_ops = sorted(
        op_name for op_name in _FRANKLIN_REQUIRED_PDS_OPS if op_name not in captured_ops
    )
    if len(missing_ops) != 0:
        joined = ', '.join(missing_ops)
        raise RuntimeError(f'Franklin EZBC browser capture missing required ops: {joined}')

    operations_payload = {
        op_name: captured_ops[op_name] for op_name in _FRANKLIN_REQUIRED_PDS_OPS
    }
    payload = {
        'final_url': final_url,
        'operations': operations_payload,
    }
    content = json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    ).encode('utf-8')
    content_sha256 = _hash_content_bytes(content)
    artifact_id = _build_source_artifact_id(
        source_id=source.source_id,
        source_uri=source.source_uri,
        content_sha256=content_sha256,
    )
    return RawArtifact(
        artifact_id=artifact_id,
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=_now_utc(),
        fetch_method='browser',
        artifact_format='json',
        content_sha256=content_sha256,
        content=content,
        metadata={
            'captured_ops': ','.join(_FRANKLIN_REQUIRED_PDS_OPS),
            'final_url': final_url,
            'run_id': run_context.run_id,
            'source_name': source.source_name,
            'status_code': str(status_code) if status_code is not None else 'unknown',
        },
    )


def _resolve_bitwise_build_id() -> str:
    _assert_uri_origin_allowed_for_issuer(
        source_uri=_BITWISE_BITB_HOME_URI,
        issuer='bitwise',
        context_label='Bitwise home URI',
    )
    request = Request(
        _BITWISE_BITB_HOME_URI,
        headers={'User-Agent': _USER_AGENT, 'Accept': 'text/html'},
        method='GET',
    )
    final_url = _BITWISE_BITB_HOME_URI
    try:
        with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            status_code = response.status
            final_url = response.geturl()
            body = response.read()
    except HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        raise RuntimeError(
            f'Bitwise home request failed with status {exc.code}: {detail}'
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f'Bitwise home request connection error: {exc.reason}'
        ) from exc

    if status_code < 200 or status_code >= 300:
        raise RuntimeError(
            f'Bitwise home request returned non-success status {status_code}'
        )
    _assert_uri_origin_allowed_for_issuer(
        source_uri=final_url,
        issuer='bitwise',
        context_label='Bitwise home final URL',
    )

    html = body.decode('utf-8', errors='replace')
    match = _RE_BUILD_ID.search(html)
    if match is None:
        raise RuntimeError('Unable to resolve Bitwise Next.js buildId from homepage')
    return match.group(1)


class ISharesIBITAdapter:
    adapter_name = 'etf_ishares_ibit'
    _source_id = 'etf_ishares_ibit_daily'
    _source_name = 'iShares Bitcoin Trust holdings CSV'
    _parser_name = 'ishares_ibit_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_ISHARES_IBIT_HOLDINGS_URI,
                issuer='ishares',
                ticker='IBIT',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = fetch_http_source(
            source=source,
            run_context=run_context,
            request_headers={
                'User-Agent': _USER_AGENT,
                'Accept': 'text/csv',
            },
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='ishares',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context

        if artifact.artifact_format != 'csv':
            raise RuntimeError(
                f'{self.adapter_name} expected csv artifact, got {artifact.artifact_format}'
            )

        csv_text = artifact.content.decode('utf-8-sig')
        as_of_match = _RE_ISHARES_AS_OF.search(csv_text)
        if as_of_match is None:
            raise RuntimeError('iShares IBIT holdings CSV missing "Fund Holdings as of"')
        as_of_raw = as_of_match.group(1).strip()
        if as_of_raw == '':
            raise RuntimeError('iShares IBIT holdings CSV has empty as-of value')

        shares_outstanding_match = _RE_ISHARES_SHARES_OUTSTANDING.search(csv_text)
        shares_outstanding_raw = (
            shares_outstanding_match.group(1).strip()
            if shares_outstanding_match is not None
            else None
        )

        lines = csv_text.splitlines()
        header_line_index = -1
        for idx, line in enumerate(lines):
            if line.strip().startswith('Ticker,'):
                header_line_index = idx
                break
        if header_line_index < 0:
            raise RuntimeError('iShares IBIT holdings CSV missing holdings table header')

        table_lines: list[str] = []
        for offset, line in enumerate(lines[header_line_index:]):
            stripped = line.strip()
            if offset > 0 and stripped in {'', '\xa0'}:
                break
            table_lines.append(line)

        reader = csv.DictReader(io.StringIO('\n'.join(table_lines)))
        rows: list[dict[str | None, str | None]] = []
        for raw_row in reader:
            row = cast(dict[str | None, str | None], raw_row)
            if not any(
                cell is not None and cell.strip() != '' for cell in row.values()
            ):
                continue
            rows.append(row)

        if len(rows) == 0:
            raise RuntimeError('iShares IBIT holdings CSV returned zero data rows')

        bitcoin_row: dict[str | None, str | None] | None = None
        for row in rows:
            name_raw = _row_value(row, candidates=('Name',))
            ticker_raw = _row_value(row, candidates=('Ticker',))
            name = name_raw.lower() if name_raw is not None else ''
            ticker = ticker_raw.upper() if ticker_raw is not None else ''
            if 'bitcoin' in name or ticker in {'BTC', 'XBT'}:
                bitcoin_row = row
                break

        if bitcoin_row is None:
            raise RuntimeError('iShares IBIT holdings CSV missing bitcoin row')

        btc_units = _parse_required_float(
            _row_value(bitcoin_row, candidates=('Shares', 'Quantity', 'Units')),
            label='iShares bitcoin units',
        )
        btc_market_value = _parse_required_float(
            _row_value(
                bitcoin_row,
                candidates=('Market Value', 'Notional Value'),
            ),
            label='iShares bitcoin market value',
        )
        btc_weight_pct = _parse_optional_float(
            _row_value(bitcoin_row, candidates=('Weight (%)', 'Weight')),
            label='iShares bitcoin weight',
        )

        total_market_value_usd = 0.0
        for row in rows:
            market_value = _parse_optional_float(
                _row_value(row, candidates=('Market Value',)),
                label='iShares row market value',
            )
            if market_value is not None:
                total_market_value_usd += market_value

        cash_market_value_usd: float | None = None
        for row in rows:
            name_raw = _row_value(row, candidates=('Name',))
            ticker_raw = _row_value(row, candidates=('Ticker',))
            name = name_raw.lower() if name_raw is not None else ''
            ticker = ticker_raw.upper() if ticker_raw is not None else ''
            if 'cash' not in name and ticker != 'USD':
                continue
            cash_market_value_usd = _parse_optional_float(
                _row_value(row, candidates=('Market Value',)),
                label='iShares cash market value',
            )
            break

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'ishares',
            'ticker': 'IBIT',
            'as_of_date': as_of_raw,
            'btc_units': btc_units,
            'btc_market_value_usd': btc_market_value,
            'btc_weight_pct': btc_weight_pct,
            'cash_market_value_usd': cash_market_value_usd,
            'total_net_assets_usd': total_market_value_usd,
            'shares_outstanding': _parse_optional_float(
                shares_outstanding_raw,
                label='iShares shares outstanding',
            ),
            'holdings_row_count': len(rows),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class InvescoBTCOAdapter:
    adapter_name = 'etf_invesco_btco'
    _source_id = 'etf_invesco_btco_daily'
    _source_name = 'Invesco Galaxy Bitcoin ETF holdings JSON'
    _parser_name = 'invesco_btco_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_INVESCO_BTCO_HOLDINGS_URI,
                issuer='invesco',
                ticker='BTCO',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        fetched = fetch_http_source(
            source=source,
            run_context=run_context,
            request_headers={
                'User-Agent': _USER_AGENT,
                'Accept': 'application/json, text/plain, */*',
            },
        )
        artifact = _force_json_artifact(fetched)
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='invesco',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context

        payload_obj = _load_json_payload(artifact=artifact, label='Invesco BTCO')
        holdings_raw = _expect_list(payload_obj.get('holdings'), 'Invesco holdings')
        if len(holdings_raw) == 0:
            raise RuntimeError('Invesco BTCO holdings payload is empty')

        bitcoin_row: dict[str, Any] | None = None
        total_market_value_usd = 0.0
        for idx, holding_raw in enumerate(holdings_raw, start=1):
            holding = _expect_dict(holding_raw, f'Invesco holdings[{idx}]')
            issuer_name = _expect_non_empty_str(
                holding.get('issuerName'),
                f'Invesco holdings[{idx}].issuerName',
            )
            market_value = _parse_required_float(
                holding.get('marketValueBase'),
                label=f'Invesco holdings[{idx}].marketValueBase',
            )
            total_market_value_usd += market_value
            if 'bitcoin' in issuer_name.lower():
                bitcoin_row = holding

        if bitcoin_row is None:
            raise RuntimeError('Invesco BTCO holdings payload missing bitcoin row')

        effective_business_date = payload_obj.get('effectiveBusinessDate')
        effective_date = payload_obj.get('effectiveDate')

        as_of_raw: str | None = None
        if isinstance(effective_business_date, str) and effective_business_date.strip() != '':
            as_of_raw = effective_business_date
        elif isinstance(effective_date, str) and effective_date.strip() != '':
            as_of_raw = effective_date
        if as_of_raw is None:
            raise RuntimeError('Invesco BTCO payload missing effective date')

        btc_units = _parse_required_float(
            bitcoin_row.get('units'),
            label='Invesco bitcoin units',
        )
        btc_market_value = _parse_required_float(
            bitcoin_row.get('marketValueBase'),
            label='Invesco bitcoin market value',
        )
        btc_weight_pct = _parse_optional_float(
            bitcoin_row.get('percentageOfTotalNetAssets'),
            label='Invesco bitcoin weight',
        )

        total_number_of_holdings = _parse_required_float(
            payload_obj.get('totalNumberOfHoldings'),
            label='Invesco totalNumberOfHoldings',
        )

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'invesco',
            'ticker': 'BTCO',
            'as_of_date': as_of_raw,
            'btc_units': btc_units,
            'btc_market_value_usd': btc_market_value,
            'btc_weight_pct': btc_weight_pct,
            'total_net_assets_usd': total_market_value_usd,
            'holdings_row_count': int(total_number_of_holdings),
            'source_cusip': _expect_non_empty_str(
                payload_obj.get('cusip'),
                'Invesco cusip',
            ),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class BitwiseBITBAdapter:
    adapter_name = 'etf_bitwise_bitb'
    _source_id = 'etf_bitwise_bitb_daily'
    _source_name = 'Bitwise BITB next-data JSON'
    _parser_name = 'bitwise_bitb_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        build_id = _resolve_bitwise_build_id()
        source_uri = _BITWISE_NEXT_DATA_URI_TEMPLATE.format(build_id=build_id)
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=source_uri,
                issuer='bitwise',
                ticker='BITB',
                extra_metadata={'build_id': build_id},
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        fetched = fetch_http_source(
            source=source,
            run_context=run_context,
            request_headers={
                'User-Agent': _USER_AGENT,
                'Accept': 'application/json, text/plain, */*',
            },
        )
        artifact = _force_json_artifact(fetched)
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='bitwise',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context

        payload_obj = _load_json_payload(artifact=artifact, label='Bitwise BITB')
        page_props = _expect_dict(payload_obj.get('pageProps'), 'Bitwise pageProps')
        fund_data_outer = _expect_dict(
            page_props.get('fundData'),
            'Bitwise pageProps.fundData',
        )
        fund_data = _expect_dict(
            fund_data_outer.get('data'),
            'Bitwise pageProps.fundData.data',
        )
        fund_details = _expect_dict(
            fund_data.get('fundDetails'),
            'Bitwise fundDetails',
        )
        holdings = _expect_dict(fund_data.get('holdings'), 'Bitwise holdings')
        holdings_basket = _expect_list(holdings.get('basket'), 'Bitwise holdings.basket')
        if len(holdings_basket) == 0:
            raise RuntimeError('Bitwise holdings.basket is empty')

        bitcoin_row: dict[str, Any] | None = None
        total_holdings_market_value_usd = 0.0
        for idx, basket_item_raw in enumerate(holdings_basket, start=1):
            basket_item = _expect_dict(
                basket_item_raw,
                f'Bitwise holdings.basket[{idx}]',
            )
            company_name = _expect_non_empty_str(
                basket_item.get('companyName'),
                f'Bitwise holdings.basket[{idx}].companyName',
            )
            market_value = _parse_required_float(
                basket_item.get('marketValue'),
                label=f'Bitwise holdings.basket[{idx}].marketValue',
            )
            total_holdings_market_value_usd += market_value
            if 'bitcoin' in company_name.lower():
                bitcoin_row = basket_item

        if bitcoin_row is None:
            raise RuntimeError('Bitwise holdings.basket missing bitcoin row')

        wallets = _expect_dict(fund_data.get('wallets'), 'Bitwise wallets')
        wallet_balances = _expect_list(
            wallets.get('walletBalances'),
            'Bitwise wallets.walletBalances',
        )
        wallet_count = 0
        wallet_balance_total_btc = 0.0
        for idx, wallet_raw in enumerate(wallet_balances, start=1):
            wallet = _expect_dict(
                wallet_raw,
                f'Bitwise wallets.walletBalances[{idx}]',
            )
            active = wallet.get('active')
            if isinstance(active, bool) and active:
                wallet_count += 1
            balance = _parse_optional_float(
                wallet.get('balance'),
                label=f'Bitwise wallets.walletBalances[{idx}].balance',
            )
            if balance is not None:
                wallet_balance_total_btc += balance

        snapshot = _expect_dict(
            page_props.get('proofOfReservesSnapshotData'),
            'Bitwise proofOfReservesSnapshotData',
        )

        fund_details_as_of_raw = fund_details.get('asOfDate')
        holdings_as_of_raw = holdings.get('asOfDate')
        as_of_raw: str | None = None
        if isinstance(fund_details_as_of_raw, str) and fund_details_as_of_raw.strip() != '':
            as_of_raw = fund_details_as_of_raw
        elif isinstance(holdings_as_of_raw, str) and holdings_as_of_raw.strip() != '':
            as_of_raw = holdings_as_of_raw
        if as_of_raw is None:
            raise RuntimeError('Bitwise payload missing asOfDate')

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'bitwise',
            'ticker': 'BITB',
            'as_of_date': as_of_raw,
            'btc_units': _parse_required_float(
                bitcoin_row.get('shares'),
                label='Bitwise bitcoin shares',
            ),
            'btc_market_value_usd': _parse_required_float(
                bitcoin_row.get('marketValue'),
                label='Bitwise bitcoin market value',
            ),
            'total_net_assets_usd': _parse_required_float(
                fund_details.get('netAssets'),
                label='Bitwise fundDetails.netAssets',
            ),
            'shares_outstanding': _parse_required_float(
                fund_details.get('sharesOutstanding'),
                label='Bitwise fundDetails.sharesOutstanding',
            ),
            'holdings_row_count': _parse_required_float(
                fund_details.get('numHoldings'),
                label='Bitwise fundDetails.numHoldings',
            ),
            'total_holdings_market_value_usd': total_holdings_market_value_usd,
            'wallet_count': wallet_count,
            'wallet_balance_total_btc': wallet_balance_total_btc,
            'proof_of_reserves_total_btc': _parse_required_float(
                snapshot.get('totalReserve'),
                label='Bitwise proofOfReservesSnapshotData.totalReserve',
            ),
            'proof_of_reserves_timestamp_utc': _expect_non_empty_str(
                snapshot.get('timestamp'),
                'Bitwise proofOfReservesSnapshotData.timestamp',
            ),
            'fund_data_updated_at_utc': _expect_non_empty_str(
                fund_data.get('updatedAt'),
                'Bitwise fundData.updatedAt',
            ),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class Ark21SharesARKBAdapter:
    adapter_name = 'etf_ark_arkb'
    _source_id = 'etf_ark_arkb_daily'
    _source_name = 'ARK 21Shares ARKB holdings CSV'
    _parser_name = 'ark_arkb_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_ARK_ARKB_HOLDINGS_URI,
                issuer='ark',
                ticker='ARKB',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = fetch_http_source(
            source=source,
            run_context=run_context,
            request_headers={
                'User-Agent': _USER_AGENT,
                'Accept': 'text/csv',
            },
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='ark',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context

        if artifact.artifact_format != 'csv':
            raise RuntimeError(
                f'{self.adapter_name} expected csv artifact, got {artifact.artifact_format}'
            )

        rows: list[dict[str | None, str | None]] = []
        reader = csv.DictReader(io.StringIO(artifact.content.decode('utf-8-sig')))
        for raw_row in reader:
            row = cast(dict[str | None, str | None], raw_row)
            if not any(
                cell is not None and cell.strip() != '' for cell in row.values()
            ):
                continue
            rows.append(row)

        if len(rows) == 0:
            raise RuntimeError('ARK ARKB holdings CSV returned zero rows')

        bitcoin_row: dict[str | None, str | None] | None = None
        for row in rows:
            company = _row_value(row, candidates=('company', 'name'))
            ticker = _row_value(row, candidates=('ticker',))
            company_name = company.lower() if company is not None else ''
            ticker_name = ticker.upper() if ticker is not None else ''
            if 'bitcoin' in company_name or ticker_name in {'BTC', 'XBT'}:
                bitcoin_row = row
                break

        if bitcoin_row is None:
            raise RuntimeError('ARK ARKB holdings CSV missing bitcoin row')

        as_of_raw = _expect_non_empty_str(
            _row_value(bitcoin_row, candidates=('date',)),
            'ARK ARKB date',
        )
        btc_units = _parse_required_float(
            _row_value(bitcoin_row, candidates=('shares',)),
            label='ARK ARKB bitcoin shares',
        )
        btc_market_value = _parse_required_float(
            _row_value(bitcoin_row, candidates=('market value ($)', 'market value')),
            label='ARK ARKB bitcoin market value',
        )
        btc_weight_pct = _parse_optional_float(
            _row_value(bitcoin_row, candidates=('weight (%)', 'weight')),
            label='ARK ARKB bitcoin weight',
        )

        total_market_value_usd = 0.0
        for idx, row in enumerate(rows, start=1):
            market_value = _parse_optional_float(
                _row_value(row, candidates=('market value ($)', 'market value')),
                label=f'ARK ARKB row[{idx}] market value',
            )
            if market_value is not None:
                total_market_value_usd += market_value

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'ark',
            'ticker': 'ARKB',
            'as_of_date': as_of_raw,
            'btc_units': btc_units,
            'btc_market_value_usd': btc_market_value,
            'btc_weight_pct': btc_weight_pct,
            'total_net_assets_usd': total_market_value_usd,
            'holdings_row_count': len(rows),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class VanEckHODLAdapter:
    adapter_name = 'etf_vaneck_hodl'
    _source_id = 'etf_vaneck_hodl_daily'
    _source_name = 'VanEck HODL holdings dataset JSON'
    _parser_name = 'vaneck_hodl_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_VANECK_HODL_HOLDINGS_URI,
                issuer='vaneck',
                ticker='HODL',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        fetched = fetch_http_source(
            source=source,
            run_context=run_context,
            request_headers={
                'User-Agent': _USER_AGENT,
                'Accept': 'application/json, text/plain, */*',
            },
        )
        artifact = _force_json_artifact(fetched)
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='vaneck',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context

        payload_obj = _load_json_payload(artifact=artifact, label='VanEck HODL')
        holdings_raw = _expect_list(payload_obj.get('Holdings'), 'VanEck Holdings')
        if len(holdings_raw) == 0:
            raise RuntimeError('VanEck HODL holdings payload is empty')

        bitcoin_row: dict[str, Any] | None = None
        total_market_value_usd = 0.0
        for idx, holding_raw in enumerate(holdings_raw, start=1):
            holding = _expect_dict(holding_raw, f'VanEck Holdings[{idx}]')
            market_value = _parse_required_float(
                holding.get('MV'),
                label=f'VanEck Holdings[{idx}].MV',
            )
            total_market_value_usd += market_value
            holding_name = _expect_non_empty_str(
                holding.get('HoldingName'),
                f'VanEck Holdings[{idx}].HoldingName',
            )
            if 'bitcoin' in holding_name.lower():
                bitcoin_row = holding

        if bitcoin_row is None:
            raise RuntimeError('VanEck HODL holdings payload missing bitcoin row')

        as_of_raw = _expect_non_empty_str(payload_obj.get('AsOfDate'), 'VanEck AsOfDate')
        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'vaneck',
            'ticker': 'HODL',
            'as_of_date': as_of_raw,
            'btc_units': _parse_required_float(
                bitcoin_row.get('Shares'),
                label='VanEck bitcoin shares',
            ),
            'btc_market_value_usd': _parse_required_float(
                bitcoin_row.get('MV'),
                label='VanEck bitcoin market value',
            ),
            'btc_weight_pct': _parse_optional_float(
                bitcoin_row.get('Weight'),
                label='VanEck bitcoin weight',
            ),
            'total_net_assets_usd': total_market_value_usd,
            'holdings_row_count': len(holdings_raw),
            'source_cusip': (
                str(cusip_raw).strip()
                if (cusip_raw := bitcoin_row.get('CUSIP')) is not None
                and str(cusip_raw).strip() != ''
                else None
            ),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class FranklinEZBCAdapter:
    adapter_name = 'etf_franklin_ezbc'
    _source_id = 'etf_franklin_ezbc_daily'
    _source_name = 'Franklin EZBC PDS portfolio payload'
    _parser_name = 'franklin_ezbc_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_FRANKLIN_EZBC_PRODUCT_URI,
                issuer='franklin',
                ticker='EZBC',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = _fetch_franklin_pds_artifact(
            source=source,
            run_context=run_context,
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='franklin',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context

        payload_obj = _load_json_payload(artifact=artifact, label='Franklin EZBC capture')
        operations_raw = _expect_dict(
            payload_obj.get('operations'),
            'Franklin EZBC operations',
        )

        def _operation_payload(op_name: str) -> dict[str, Any]:
            return _expect_dict(
                operations_raw.get(op_name),
                f'Franklin EZBC operations.{op_name}',
            )

        product_common = _expect_dict(
            _expect_dict(
                _operation_payload('ProductCommon').get('data'),
                'Franklin ProductCommon.data',
            ).get('ProductDetails'),
            'Franklin ProductCommon.data.ProductDetails',
        )
        identifiers_overview = _expect_dict(
            _expect_dict(
                _operation_payload('Identifiers').get('data'),
                'Franklin Identifiers.data',
            ).get('Overview'),
            'Franklin Identifiers.data.Overview',
        )
        identifiers_shareclass = _expect_list(
            identifiers_overview.get('shareclass'),
            'Franklin Identifiers shareclass',
        )
        if len(identifiers_shareclass) == 0:
            raise RuntimeError('Franklin EZBC identifiers shareclass is empty')
        identifiers = _expect_dict(
            _expect_dict(
                identifiers_shareclass[0],
                'Franklin Identifiers shareclass[0]',
            ).get('identifiers'),
            'Franklin Identifiers shareclass[0].identifiers',
        )

        pricing_overview = _expect_dict(
            _expect_dict(
                _operation_payload('PricingDetails').get('data'),
                'Franklin PricingDetails.data',
            ).get('Overview'),
            'Franklin PricingDetails.data.Overview',
        )
        pricing_shareclass = _expect_list(
            pricing_overview.get('shareclass'),
            'Franklin PricingDetails shareclass',
        )
        if len(pricing_shareclass) == 0:
            raise RuntimeError('Franklin EZBC pricing shareclass is empty')
        pricing_nav = _expect_dict(
            _expect_dict(
                pricing_shareclass[0],
                'Franklin PricingDetails shareclass[0]',
            ).get('nav'),
            'Franklin PricingDetails shareclass[0].nav',
        )

        aum = _expect_dict(
            _expect_dict(
                _expect_dict(
                    _operation_payload('Aum').get('data'),
                    'Franklin Aum.data',
                ).get('Overview'),
                'Franklin Aum.data.Overview',
            ).get('aum'),
            'Franklin Aum.data.Overview.aum',
        )

        holdings_portfolio = _expect_dict(
            _expect_dict(
                _expect_dict(
                    _operation_payload('Holdings').get('data'),
                    'Franklin Holdings.data',
                ).get('Portfolio'),
                'Franklin Holdings.data.Portfolio',
            ).get('portfolio'),
            'Franklin Holdings.data.Portfolio.portfolio',
        )
        daily_holdings = _expect_list(
            holdings_portfolio.get('dailyholdings'),
            'Franklin Holdings dailyholdings',
        )
        if len(daily_holdings) == 0:
            raise RuntimeError('Franklin EZBC dailyholdings is empty')

        bitcoin_row: dict[str, Any] | None = None
        total_market_value_usd = 0.0
        for idx, holding_raw in enumerate(daily_holdings, start=1):
            holding = _expect_dict(
                holding_raw,
                f'Franklin dailyholdings[{idx}]',
            )
            market_value = _parse_optional_float(
                holding.get('mktvalue'),
                label=f'Franklin dailyholdings[{idx}].mktvalue',
            )
            if market_value is not None:
                total_market_value_usd += market_value

            sec_name_raw = holding.get('secname')
            sec_name = sec_name_raw.lower() if isinstance(sec_name_raw, str) else ''
            if 'bitcoin' in sec_name:
                bitcoin_row = holding

        if bitcoin_row is None:
            raise RuntimeError('Franklin EZBC dailyholdings missing bitcoin row')

        as_of_raw = (
            bitcoin_row.get('asofdate')
            if isinstance(bitcoin_row.get('asofdate'), str)
            else aum.get('asofdate')
        )
        as_of_value = _expect_non_empty_str(as_of_raw, 'Franklin as-of date')

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'franklin',
            'ticker': _expect_non_empty_str(
                identifiers.get('ticker'),
                'Franklin identifiers.ticker',
            ),
            'as_of_date': as_of_value,
            'btc_units': _parse_required_float(
                bitcoin_row.get('quantityshrpar'),
                label='Franklin bitcoin quantityshrpar',
            ),
            'btc_market_value_usd': _parse_required_float(
                bitcoin_row.get('mktvalue'),
                label='Franklin bitcoin mktvalue',
            ),
            'btc_weight_pct': _parse_optional_float(
                bitcoin_row.get('pctofnetassets'),
                label='Franklin bitcoin pctofnetassets',
            ),
            'total_net_assets_usd': total_market_value_usd,
            'total_net_assets_reported_usd': _parse_scaled_currency(
                aum.get('totnetassets'),
                label='Franklin AUM totnetassets',
            ),
            'nav_per_share_usd': _parse_optional_float(
                pricing_nav.get('navvalue'),
                label='Franklin navvalue',
            ),
            'market_price_per_share_usd': _parse_optional_float(
                pricing_nav.get('mktpricevalue'),
                label='Franklin mktpricevalue',
            ),
            'holdings_row_count': len(daily_holdings),
            'source_fund_id': _expect_non_empty_str(
                product_common.get('fundid'),
                'Franklin product fundid',
            ),
            'source_cusip': _expect_non_empty_str(
                identifiers.get('cusip'),
                'Franklin identifiers.cusip',
            ),
            'source_isin': _expect_non_empty_str(
                identifiers.get('isin'),
                'Franklin identifiers.isin',
            ),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class GrayscaleGBTCAdapter:
    adapter_name = 'etf_grayscale_gbtc'
    _source_id = 'etf_grayscale_gbtc_daily'
    _source_name = 'Grayscale GBTC ETF page payload'
    _parser_name = 'grayscale_gbtc_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_GRAYSCALE_GBTC_PAGE_URI,
                issuer='grayscale',
                ticker='GBTC',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = fetch_browser_source(
            source=source,
            run_context=run_context,
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='grayscale',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context
        html = artifact.content.decode('utf-8', errors='replace')
        if 'Vercel Security Checkpoint' in html:
            raise RuntimeError('Grayscale GBTC page returned security checkpoint payload')

        anchor = html.find('\\"pricingDataDate\\":')
        if anchor < 0:
            raise RuntimeError('Grayscale GBTC payload missing pricingDataDate anchor')
        window_start = max(0, anchor - 20_000)
        window_end = min(len(html), anchor + 300_000)
        payload_window = html[window_start:window_end]

        pricing_date_raw = _extract_escaped_json_field(
            html=payload_window,
            field='pricingDataDate',
            label='Grayscale GBTC',
        )
        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'grayscale',
            'ticker': _extract_escaped_json_field(
                html=payload_window,
                field='ticker',
                label='Grayscale GBTC',
            ),
            'as_of_date': pricing_date_raw,
            'btc_units': _parse_required_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='totalAssetInTrust',
                    label='Grayscale GBTC',
                ),
                label='Grayscale totalAssetInTrust',
            ),
            'btc_market_value_usd': _parse_required_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='aum',
                    label='Grayscale GBTC',
                ),
                label='Grayscale aum',
            ),
            'btc_weight_pct': 100.0,
            'total_net_assets_usd': _parse_required_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='gaapAum',
                    label='Grayscale GBTC',
                ),
                label='Grayscale gaapAum',
            ),
            'shares_outstanding': _parse_required_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='sharesOutstanding',
                    label='Grayscale GBTC',
                ),
                label='Grayscale sharesOutstanding',
            ),
            'nav_per_share_usd': _parse_required_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='holdingsPerShare',
                    label='Grayscale GBTC',
                ),
                label='Grayscale holdingsPerShare',
            ),
            'market_price_per_share_usd': _parse_required_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='marketPricePerShare',
                    label='Grayscale GBTC',
                ),
                label='Grayscale marketPricePerShare',
            ),
            'premium_discount_pct': _parse_optional_float(
                _extract_escaped_json_field(
                    html=payload_window,
                    field='premiumDiscountPercent',
                    label='Grayscale GBTC',
                ),
                label='Grayscale premiumDiscountPercent',
            ),
            'source_cusip': _extract_escaped_json_field(
                html=payload_window,
                field='cusip',
                label='Grayscale GBTC',
            ),
            'holdings_row_count': 1,
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class FidelityFBTCAdapter:
    adapter_name = 'etf_fidelity_fbtc'
    _source_id = 'etf_fidelity_fbtc_daily'
    _source_name = 'Fidelity FBTC summary page'
    _parser_name = 'fidelity_fbtc_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_FIDELITY_FBTC_PAGE_URI,
                issuer='fidelity',
                ticker='FBTC',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = fetch_browser_source(
            source=source,
            run_context=run_context,
            browser_engine=_BROWSER_ENGINE_FIREFOX,
            timeout_ms=90_000,
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='fidelity',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context
        if artifact.artifact_format != 'html':
            raise RuntimeError(
                f'{self.adapter_name} expected html artifact, got {artifact.artifact_format}'
            )

        lines = _html_lines_from_content(artifact.content)
        net_assets_index = _find_line_index_containing(
            lines=lines,
            needle='Net assets',
            label='Fidelity net assets label',
        )
        total_bitcoin_index = _find_line_index_containing(
            lines=lines,
            needle='Total bitcoin in fund',
            label='Fidelity total bitcoin label',
            start_index=net_assets_index,
        )
        shares_index = _find_line_index_containing(
            lines=lines,
            needle='Shares outstanding',
            label='Fidelity shares outstanding label',
            start_index=total_bitcoin_index,
        )
        nav_index = _find_line_index_exact(
            lines=lines,
            candidates=('Nav',),
            label='Fidelity nav label',
            start_index=total_bitcoin_index,
        )

        net_assets_as_of_raw = _extract_as_of_after_index(
            lines=lines,
            anchor_index=net_assets_index,
            lookahead=4,
            label='Fidelity net assets as-of',
        )
        btc_as_of_raw = _extract_as_of_after_index(
            lines=lines,
            anchor_index=total_bitcoin_index,
            lookahead=6,
            label='Fidelity bitcoin as-of',
        )

        net_assets_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=net_assets_index,
            lookahead=4,
            label='Fidelity net assets',
        )
        btc_units_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=total_bitcoin_index,
            lookahead=6,
            label='Fidelity total bitcoin units',
        )
        shares_outstanding_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=shares_index,
            lookahead=3,
            label='Fidelity shares outstanding',
        )
        nav_per_share_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=nav_index,
            lookahead=3,
            label='Fidelity NAV per share',
        )

        total_net_assets_usd = _parse_scaled_compact_number(
            net_assets_raw,
            label='Fidelity net assets',
        )
        btc_units = _parse_required_float(
            btc_units_raw,
            label='Fidelity total bitcoin units',
        )
        shares_outstanding = _parse_scaled_compact_number(
            shares_outstanding_raw,
            label='Fidelity shares outstanding',
        )
        nav_per_share_usd = _parse_scaled_compact_number(
            nav_per_share_raw,
            label='Fidelity NAV per share',
        )

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'fidelity',
            'ticker': 'FBTC',
            'as_of_date': btc_as_of_raw,
            'net_assets_as_of_date': net_assets_as_of_raw,
            'btc_units': btc_units,
            'btc_market_value_usd': total_net_assets_usd,
            'total_net_assets_usd': total_net_assets_usd,
            'shares_outstanding': shares_outstanding,
            'nav_per_share_usd': nav_per_share_usd,
            'holdings_row_count': 1,
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class CoinSharesBRRRAdapter:
    adapter_name = 'etf_coinshares_brrr'
    _source_id = 'etf_coinshares_brrr_daily'
    _source_name = 'CoinShares BRRR ETF page'
    _parser_name = 'coinshares_brrr_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_COINSHARES_BRRR_PAGE_URI,
                issuer='coinshares',
                ticker='BRRR',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = fetch_browser_source(
            source=source,
            run_context=run_context,
            browser_engine=_BROWSER_ENGINE_FIREFOX,
            timeout_ms=90_000,
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='coinshares',
            adapter_name=self.adapter_name,
        )
        return artifact

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context
        if artifact.artifact_format != 'html':
            raise RuntimeError(
                f'{self.adapter_name} expected html artifact, got {artifact.artifact_format}'
            )

        lines = _html_lines_from_content(artifact.content)

        aum_index = _find_line_index_containing(
            lines=lines,
            needle='ASSETS UNDER MANAGEMENT (US$)',
            label='CoinShares AUM label',
        )
        nav_index = _find_line_index_containing(
            lines=lines,
            needle='NAV (US$)',
            label='CoinShares NAV label',
            start_index=aum_index,
        )
        holdings_count_index = _find_line_index_containing(
            lines=lines,
            needle='# of holdings',
            label='CoinShares holdings-count label',
            start_index=nav_index,
        )
        holdings_section_index = _find_line_index_containing(
            lines=lines,
            needle='BRRR Holdings',
            label='CoinShares holdings section',
            start_index=holdings_count_index,
        )
        bitcoin_row_index = _find_line_index_exact(
            lines=lines,
            candidates=('BITCOIN',),
            label='CoinShares bitcoin row',
            start_index=holdings_section_index,
        )
        if bitcoin_row_index + 3 >= len(lines):
            raise RuntimeError('CoinShares bitcoin holdings row is truncated')

        bitcoin_ticker = lines[bitcoin_row_index + 1].strip().upper()
        if bitcoin_ticker not in {'XBTUSD', 'BTC', 'XBT'}:
            raise RuntimeError(
                f'CoinShares bitcoin row ticker mismatch: {bitcoin_ticker}'
            )

        as_of_raw = _extract_as_of_after_index(
            lines=lines,
            anchor_index=holdings_section_index,
            lookahead=8,
            label='CoinShares holdings as-of',
        )
        aum_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=aum_index,
            lookahead=3,
            label='CoinShares AUM',
        )
        nav_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=nav_index,
            lookahead=3,
            label='CoinShares NAV',
        )
        holdings_count_raw = _extract_numeric_value_after_index(
            lines=lines,
            anchor_index=holdings_count_index,
            lookahead=3,
            label='CoinShares holdings count',
        )
        btc_units_raw = lines[bitcoin_row_index + 2]
        btc_market_value_raw = lines[bitcoin_row_index + 3]

        total_net_assets_usd = _parse_scaled_compact_number(
            aum_raw,
            label='CoinShares AUM',
        )
        btc_units = _parse_required_float(
            btc_units_raw,
            label='CoinShares bitcoin shares',
        )
        btc_market_value_usd = _parse_scaled_compact_number(
            btc_market_value_raw,
            label='CoinShares bitcoin market value',
        )
        holdings_row_count = int(
            _parse_required_float(
                holdings_count_raw,
                label='CoinShares holdings count',
            )
        )
        nav_per_share_usd = _parse_scaled_compact_number(
            nav_raw,
            label='CoinShares NAV',
        )

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'coinshares',
            'ticker': 'BRRR',
            'as_of_date': as_of_raw,
            'btc_units': btc_units,
            'btc_market_value_usd': btc_market_value_usd,
            'total_net_assets_usd': total_net_assets_usd,
            'nav_per_share_usd': nav_per_share_usd,
            'holdings_row_count': holdings_row_count,
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


class HashdexDEFIAdapter:
    adapter_name = 'etf_hashdex_defi'
    _source_id = 'etf_hashdex_defi_daily'
    _source_name = 'Hashdex DEFI payload JSON'
    _parser_name = 'hashdex_defi_parser'
    _parser_version = '1.0.0'

    def __init__(self) -> None:
        self._artifact_by_source_id: dict[str, RawArtifact] = {}

    def discover_sources(
        self,
        *,
        run_context: ScrapeRunContext,
    ) -> Sequence[SourceDescriptor]:
        return [
            _build_source_descriptor(
                run_context=run_context,
                source_id=self._source_id,
                source_name=self._source_name,
                source_uri=_HASHDEX_DEFI_PAYLOAD_URI,
                issuer='hashdex',
                ticker='DEFI',
            )
        ]

    def fetch(
        self,
        *,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> RawArtifact:
        artifact = fetch_http_source(
            source=source,
            run_context=run_context,
            request_headers={
                'User-Agent': _USER_AGENT,
                'Accept': 'application/json, text/plain, */*',
            },
        )
        _assert_artifact_origin_allowed_for_issuer(
            artifact=artifact,
            source=source,
            issuer='hashdex',
            adapter_name=self.adapter_name,
        )
        return _force_json_artifact(artifact)

    def parse(
        self,
        *,
        artifact: RawArtifact,
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[ParsedRecord]:
        del run_context
        if artifact.artifact_format != 'json':
            raise RuntimeError(
                f'{self.adapter_name} expected json artifact, got {artifact.artifact_format}'
            )

        payload_text = artifact.content.decode('utf-8')
        product_start_match = _RE_HASHDEX_DEFI_PRODUCT_START.search(payload_text)
        if product_start_match is None:
            raise RuntimeError('Hashdex payload missing DEFI product signature')

        download_link_match = _RE_HASHDEX_DEFI_DOWNLOAD_LINK.search(payload_text)
        if download_link_match is None:
            raise RuntimeError('Hashdex payload missing DEFI holdings download link')

        search_start = product_start_match.start()
        search_end = min(len(payload_text), search_start + 250_000)
        payload_window = payload_text[search_start:search_end]

        holding_refs_match = _RE_HASHDEX_DEFI_HOLDING_REFS.search(payload_window)
        if holding_refs_match is None:
            raise RuntimeError('Hashdex payload missing DEFI holding references')
        holding_refs = [
            item.strip()
            for item in holding_refs_match.group('holding_refs').split(',')
            if item.strip() != ''
        ]
        if len(holding_refs) == 0:
            raise RuntimeError('Hashdex payload DEFI holding references are empty')

        reference_row_match = _RE_HASHDEX_DEFI_REFERENCE_ROW_FULL.search(payload_window)
        if reference_row_match is None:
            raise RuntimeError('Hashdex payload missing DEFI reference metadata row')

        as_of_raw = _expect_non_empty_str(
            reference_row_match.group('reference_date'),
            'Hashdex DEFI reference date',
        )
        total_net_assets_raw = reference_row_match.group('total_net_assets')
        shares_outstanding_raw = reference_row_match.group('shares_outstanding')

        bitcoin_row_full_match = _RE_HASHDEX_DEFI_BITCOIN_ROW_FULL.search(payload_window)
        bitcoin_row_short_match = (
            None
            if bitcoin_row_full_match is not None
            else _RE_HASHDEX_DEFI_BITCOIN_ROW_SHORT.search(payload_window)
        )
        if bitcoin_row_full_match is None and bitcoin_row_short_match is None:
            raise RuntimeError('Hashdex payload missing bitcoin holding row')

        if bitcoin_row_full_match is not None:
            btc_price_raw = bitcoin_row_full_match.group('price')
            btc_units_raw = bitcoin_row_full_match.group('shares')
            btc_weight_raw = bitcoin_row_full_match.group('weight')
            btc_weight_pct_raw = bitcoin_row_full_match.group('weight_percentage')
            as_of_raw = _expect_non_empty_str(
                bitcoin_row_full_match.group('reference_date'),
                'Hashdex DEFI bitcoin reference date',
            )
            total_net_assets_raw = bitcoin_row_full_match.group('total_net_assets')
            shares_outstanding_raw = bitcoin_row_full_match.group('shares_outstanding')
        else:
            assert bitcoin_row_short_match is not None
            btc_price_raw = bitcoin_row_short_match.group('price')
            btc_units_raw = bitcoin_row_short_match.group('shares')
            btc_weight_raw = bitcoin_row_short_match.group('weight')
            btc_weight_pct_raw = bitcoin_row_short_match.group('weight_percentage')

        btc_price_usd = _parse_scaled_compact_number(
            btc_price_raw,
            label='Hashdex DEFI bitcoin price',
        )
        btc_units = _parse_required_float(
            btc_units_raw,
            label='Hashdex DEFI bitcoin shares',
        )
        total_net_assets_usd = _parse_scaled_compact_number(
            total_net_assets_raw,
            label='Hashdex DEFI total net assets',
        )
        shares_outstanding = _parse_scaled_compact_number(
            shares_outstanding_raw,
            label='Hashdex DEFI shares outstanding',
        )
        btc_weight_pct = _parse_optional_float(
            btc_weight_raw,
            label='Hashdex DEFI bitcoin weight',
        )
        if btc_weight_pct is None:
            btc_weight_pct = _parse_optional_float(
                btc_weight_pct_raw,
                label='Hashdex DEFI bitcoin weight percentage',
            )

        payload: dict[str, Any] = {
            'row_index': 1,
            'issuer': 'hashdex',
            'ticker': 'DEFI',
            'as_of_date': as_of_raw,
            'btc_units': btc_units,
            'btc_market_value_usd': btc_units * btc_price_usd,
            'btc_price_usd': btc_price_usd,
            'btc_weight_pct': btc_weight_pct,
            'total_net_assets_usd': total_net_assets_usd,
            'shares_outstanding': shares_outstanding,
            'holdings_row_count': len(holding_refs),
            'holdings_download_url': download_link_match.group('download_link'),
        }

        self._artifact_by_source_id[source.source_id] = artifact
        return [
            _build_single_record(
                artifact=artifact,
                payload=payload,
                parser_name=self._parser_name,
                parser_version=self._parser_version,
            )
        ]

    def normalize(
        self,
        *,
        parsed_records: Sequence[ParsedRecord],
        source: SourceDescriptor,
        run_context: ScrapeRunContext,
    ) -> Sequence[NormalizedMetricRecord]:
        del run_context
        if len(parsed_records) != 1:
            raise RuntimeError(
                f'{self.adapter_name} expected one parsed record, got {len(parsed_records)}'
            )
        artifact = self._artifact_by_source_id.get(source.source_id)
        if artifact is None:
            raise RuntimeError(
                f'{self.adapter_name} normalize requires parse result for source_id={source.source_id}'
            )
        return _normalize_daily_records(
            parsed_records=parsed_records,
            source=source,
            artifact=artifact,
        )


def build_s4_01_issuer_adapters() -> tuple[ScraperAdapter, ScraperAdapter, ScraperAdapter]:
    return (
        ISharesIBITAdapter(),
        InvescoBTCOAdapter(),
        BitwiseBITBAdapter(),
    )


def build_s4_02_issuer_adapters() -> tuple[
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
]:
    return (
        ISharesIBITAdapter(),
        InvescoBTCOAdapter(),
        BitwiseBITBAdapter(),
        Ark21SharesARKBAdapter(),
        VanEckHODLAdapter(),
        FranklinEZBCAdapter(),
        GrayscaleGBTCAdapter(),
    )


def build_s4_03_issuer_adapters() -> tuple[
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
    ScraperAdapter,
]:
    return (
        ISharesIBITAdapter(),
        InvescoBTCOAdapter(),
        BitwiseBITBAdapter(),
        Ark21SharesARKBAdapter(),
        VanEckHODLAdapter(),
        FranklinEZBCAdapter(),
        GrayscaleGBTCAdapter(),
        FidelityFBTCAdapter(),
        CoinSharesBRRRAdapter(),
        HashdexDEFIAdapter(),
    )
