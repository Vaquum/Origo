from __future__ import annotations

from .audit import ScraperAuditLog, get_scraper_audit_log
from .browser_fetch import fetch_browser_source
from .clickhouse_staging import persist_normalized_records_to_clickhouse
from .contracts import (
    ArtifactFormat,
    FetchMethod,
    MetricValue,
    NormalizedMetricRecord,
    ParsedRecord,
    PersistedRawArtifact,
    ProvenanceMetadata,
    RawArtifact,
    ScraperAdapter,
    ScrapeRunContext,
    SourceDescriptor,
)
from .errors import ScraperError, as_scraper_error
from .etf_adapters import (
    Ark21SharesARKBAdapter,
    BitwiseBITBAdapter,
    CoinSharesBRRRAdapter,
    FidelityFBTCAdapter,
    FranklinEZBCAdapter,
    GrayscaleGBTCAdapter,
    HashdexDEFIAdapter,
    InvescoBTCOAdapter,
    ISharesIBITAdapter,
    VanEckHODLAdapter,
    build_s4_01_issuer_adapters,
    build_s4_02_issuer_adapters,
    build_s4_03_issuer_adapters,
)
from .http_fetch import fetch_http_source
from .normalize import normalize_parsed_records, normalized_records_to_json_rows
from .parse_csv import parse_csv_artifact
from .parse_dispatch import parse_artifact
from .parse_html import parse_html_artifact
from .parse_json import parse_json_artifact
from .parse_pdf import parse_pdf_artifact
from .pipeline import PipelineRunResult, PipelineSourceResult, run_scraper_pipeline
from .retry import (
    RetryHookEvent,
    RetryPolicy,
    load_fetch_retry_policy_from_env,
    retry_with_backoff,
)
from .rights import ScraperRightsDecision, resolve_scraper_rights

__all__ = [
    'Ark21SharesARKBAdapter',
    'ArtifactFormat',
    'BitwiseBITBAdapter',
    'CoinSharesBRRRAdapter',
    'FetchMethod',
    'FidelityFBTCAdapter',
    'FranklinEZBCAdapter',
    'GrayscaleGBTCAdapter',
    'HashdexDEFIAdapter',
    'ISharesIBITAdapter',
    'InvescoBTCOAdapter',
    'MetricValue',
    'NormalizedMetricRecord',
    'ParsedRecord',
    'PersistedRawArtifact',
    'PipelineRunResult',
    'PipelineSourceResult',
    'ProvenanceMetadata',
    'RawArtifact',
    'RetryHookEvent',
    'RetryPolicy',
    'ScrapeRunContext',
    'ScraperAdapter',
    'ScraperAuditLog',
    'ScraperError',
    'ScraperRightsDecision',
    'SourceDescriptor',
    'VanEckHODLAdapter',
    'as_scraper_error',
    'build_s4_01_issuer_adapters',
    'build_s4_02_issuer_adapters',
    'build_s4_03_issuer_adapters',
    'fetch_browser_source',
    'fetch_http_source',
    'get_scraper_audit_log',
    'load_fetch_retry_policy_from_env',
    'normalize_parsed_records',
    'normalized_records_to_json_rows',
    'parse_artifact',
    'parse_csv_artifact',
    'parse_html_artifact',
    'parse_json_artifact',
    'parse_pdf_artifact',
    'persist_normalized_records_to_clickhouse',
    'resolve_scraper_rights',
    'retry_with_backoff',
    'run_scraper_pipeline',
]
