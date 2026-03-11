from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

BinanceDataset = Literal['spot_trades', 'spot_agg_trades', 'futures_trades']
RawQuerySource = Literal[
    'spot_trades',
    'spot_agg_trades',
    'futures_trades',
    'okx_spot_trades',
    'bybit_spot_trades',
    'etf_daily_metrics',
    'fred_series_metrics',
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]
RawQueryDataset = RawQuerySource
RawQueryMode = Literal['native', 'aligned_1s']
RawExportMode = Literal['native', 'aligned_1s']
ExportFormat = Literal['parquet', 'csv']
ExportStatus = Literal['queued', 'running', 'succeeded', 'failed']
RightsState = Literal['Hosted Allowed', 'BYOK Required', 'Ingest Only']
RawQueryFilterOp = Literal['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in']
type RawQueryFilterScalar = str | int | float | bool
type RawQueryFilterValue = RawQueryFilterScalar | list[RawQueryFilterScalar]


def _validate_query_window_selection(
    n_rows: int | None,
    n_random: int | None,
    time_range: tuple[str, str] | None,
) -> None:
    selected = sum(
        [
            n_rows is not None,
            n_random is not None,
            time_range is not None,
        ]
    )
    if selected != 1:
        raise ValueError(
            'Exactly one window mode must be provided: '
            'time_range | n_rows | n_random'
        )


def _validate_export_window_selection(
    month_year: tuple[int, int] | None,
    n_rows: int | None,
    n_random: int | None,
    time_range: tuple[str, str] | None,
) -> None:
    selected = sum(
        [
            month_year is not None,
            n_rows is not None,
            n_random is not None,
            time_range is not None,
        ]
    )
    if selected != 1:
        raise ValueError(
            'Exactly one window mode must be provided: '
            'month_year | time_range | n_rows | n_random'
        )


class RawQueryFilter(BaseModel):
    field: str
    op: RawQueryFilterOp
    value: RawQueryFilterValue

    @model_validator(mode='after')
    def validate_filter(self) -> RawQueryFilter:
        if self.field.strip() == '':
            raise ValueError('filters[].field must be non-empty')
        if self.op in {'in', 'not_in'}:
            if not isinstance(self.value, list):
                raise ValueError(
                    'filters[].value must be a non-empty list when op is in/not_in'
                )
            if len(self.value) == 0:
                raise ValueError(
                    'filters[].value must be a non-empty list when op is in/not_in'
                )
        elif isinstance(self.value, list):
            raise ValueError(
                'filters[].value must be scalar when op is not in/not_in'
            )
        return self


class RawQueryRequest(BaseModel):
    mode: RawQueryMode = 'native'
    sources: list[RawQuerySource] = Field(min_length=1)
    view_id: str | None = None
    view_version: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    filters: list[RawQueryFilter] | None = None
    n_rows: int | None = Field(default=None, gt=0)
    n_random: int | None = Field(default=None, gt=0)
    time_range: tuple[str, str] | None = None
    strict: bool = False

    @model_validator(mode='after')
    def validate_window_mode(self) -> RawQueryRequest:
        _validate_query_window_selection(
            n_rows=self.n_rows,
            n_random=self.n_random,
            time_range=self.time_range,
        )
        if (self.view_id is None) != (self.view_version is None):
            raise ValueError(
                'view_id and view_version must both be set or both be omitted'
            )
        if self.view_id is not None and self.view_id.strip() == '':
            raise ValueError('view_id must be non-empty when set')
        return self


class RawQuerySchemaField(BaseModel):
    name: str
    dtype: str


class RawQueryWarning(BaseModel):
    code: str
    message: str


class RawQueryFreshness(BaseModel):
    as_of_utc: str | None
    lag_seconds: int | None


def _empty_warnings() -> list[RawQueryWarning]:
    return []


class RawQueryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mode: RawQueryMode
    source: str | None
    sources: list[RawQuerySource] | None = None
    view_id: str | None = None
    view_version: int | None = None
    row_count: int
    schema_items: list[RawQuerySchemaField] = Field(
        validation_alias='schema',
        serialization_alias='schema',
    )
    freshness: RawQueryFreshness | None = None
    rights_state: RightsState
    rights_provisional: bool
    warnings: list[RawQueryWarning] = Field(default_factory=_empty_warnings)
    rows: list[dict[str, Any]]


class RawExportRequest(BaseModel):
    mode: RawExportMode = 'native'
    format: ExportFormat
    dataset: RawQueryDataset
    view_id: str | None = None
    view_version: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    month_year: tuple[int, int] | None = None
    n_rows: int | None = Field(default=None, gt=0)
    n_random: int | None = Field(default=None, gt=0)
    time_range: tuple[str, str] | None = None
    include_datetime: bool = True
    strict: bool = False
    auth_token: str | None = None

    @model_validator(mode='after')
    def validate_window_mode(self) -> RawExportRequest:
        _validate_export_window_selection(
            month_year=self.month_year,
            n_rows=self.n_rows,
            n_random=self.n_random,
            time_range=self.time_range,
        )
        if (self.view_id is None) != (self.view_version is None):
            raise ValueError(
                'view_id and view_version must both be set or both be omitted'
            )
        if self.view_id is not None and self.view_id.strip() == '':
            raise ValueError('view_id must be non-empty when set')
        return self


class RawExportSubmitResponse(BaseModel):
    export_id: str
    status: ExportStatus
    submitted_at: datetime
    status_path: str
    rights_state: RightsState
    rights_provisional: bool
    view_id: str | None = None
    view_version: int | None = None


class RawExportArtifact(BaseModel):
    format: ExportFormat
    uri: str
    row_count: int | None = None
    checksum_sha256: str | None = None


class RawExportStatusResponse(BaseModel):
    export_id: str
    status: ExportStatus
    mode: RawExportMode
    format: ExportFormat
    dataset: RawQueryDataset
    source: str
    rights_state: RightsState
    rights_provisional: bool
    view_id: str | None = None
    view_version: int | None = None
    submitted_at: datetime
    updated_at: datetime
    artifact: RawExportArtifact | None = None
    error_code: str | None = None
    error_message: str | None = None
