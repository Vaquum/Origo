from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

BinanceDataset = Literal['binance_spot_trades']
RawQuerySource = Literal[
    'binance_spot_trades',
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
    if selected > 1:
        raise ValueError(
            'At most one window mode can be provided: '
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


def _parse_strict_utc_date(*, label: str, raw_value: str) -> date:
    if len(raw_value) != 10:
        raise ValueError(f'{label} must be strict YYYY-MM-DD (UTC day)')
    try:
        parsed = datetime.strptime(raw_value, '%Y-%m-%d').date()
    except ValueError as exc:
        raise ValueError(
            f'{label} must be a valid strict YYYY-MM-DD date (UTC day)'
        ) from exc
    if parsed.strftime('%Y-%m-%d') != raw_value:
        raise ValueError(f'{label} must be strict YYYY-MM-DD (UTC day)')
    return parsed


def _validate_historical_window_selection(
    *,
    start_date: str | None,
    end_date: str | None,
    n_latest_rows: int | None,
    n_random_rows: int | None,
) -> None:
    date_mode_selected = start_date is not None or end_date is not None
    selected = sum(
        [
            date_mode_selected,
            n_latest_rows is not None,
            n_random_rows is not None,
        ]
    )
    if selected > 1:
        raise ValueError(
            'At most one window mode can be provided: '
            'date-window(start_date/end_date) | n_latest_rows | n_random_rows'
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


class HistoricalSpotTradesRequest(BaseModel):
    mode: RawQueryMode = 'native'
    start_date: str | None = None
    end_date: str | None = None
    n_latest_rows: int | None = Field(default=None, gt=0)
    n_random_rows: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    filters: list[RawQueryFilter] | None = None
    include_datetime_col: bool = True
    strict: bool = False

    @model_validator(mode='after')
    def validate_contract(self) -> HistoricalSpotTradesRequest:
        _validate_historical_window_selection(
            start_date=self.start_date,
            end_date=self.end_date,
            n_latest_rows=self.n_latest_rows,
            n_random_rows=self.n_random_rows,
        )
        parsed_start = (
            _parse_strict_utc_date(label='start_date', raw_value=self.start_date)
            if self.start_date is not None
            else None
        )
        parsed_end = (
            _parse_strict_utc_date(label='end_date', raw_value=self.end_date)
            if self.end_date is not None
            else None
        )
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise ValueError('date-window must satisfy start_date <= end_date')
        return self


class HistoricalSpotKlinesRequest(BaseModel):
    mode: RawQueryMode = 'native'
    start_date: str | None = None
    end_date: str | None = None
    n_latest_rows: int | None = Field(default=None, gt=0)
    n_random_rows: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    filters: list[RawQueryFilter] | None = None
    kline_size: int = Field(default=1, gt=0)
    strict: bool = False

    @model_validator(mode='after')
    def validate_contract(self) -> HistoricalSpotKlinesRequest:
        _validate_historical_window_selection(
            start_date=self.start_date,
            end_date=self.end_date,
            n_latest_rows=self.n_latest_rows,
            n_random_rows=self.n_random_rows,
        )
        parsed_start = (
            _parse_strict_utc_date(label='start_date', raw_value=self.start_date)
            if self.start_date is not None
            else None
        )
        parsed_end = (
            _parse_strict_utc_date(label='end_date', raw_value=self.end_date)
            if self.end_date is not None
            else None
        )
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise ValueError('date-window must satisfy start_date <= end_date')
        return self


class HistoricalETFDailyMetricsRequest(BaseModel):
    mode: RawQueryMode = 'native'
    start_date: str | None = None
    end_date: str | None = None
    n_latest_rows: int | None = Field(default=None, gt=0)
    n_random_rows: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    filters: list[RawQueryFilter] | None = None
    strict: bool = False

    @model_validator(mode='after')
    def validate_contract(self) -> HistoricalETFDailyMetricsRequest:
        _validate_historical_window_selection(
            start_date=self.start_date,
            end_date=self.end_date,
            n_latest_rows=self.n_latest_rows,
            n_random_rows=self.n_random_rows,
        )
        parsed_start = (
            _parse_strict_utc_date(label='start_date', raw_value=self.start_date)
            if self.start_date is not None
            else None
        )
        parsed_end = (
            _parse_strict_utc_date(label='end_date', raw_value=self.end_date)
            if self.end_date is not None
            else None
        )
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise ValueError('date-window must satisfy start_date <= end_date')
        return self


class HistoricalFREDSeriesMetricsRequest(BaseModel):
    mode: RawQueryMode = 'native'
    start_date: str | None = None
    end_date: str | None = None
    n_latest_rows: int | None = Field(default=None, gt=0)
    n_random_rows: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    filters: list[RawQueryFilter] | None = None
    strict: bool = False

    @model_validator(mode='after')
    def validate_contract(self) -> HistoricalFREDSeriesMetricsRequest:
        _validate_historical_window_selection(
            start_date=self.start_date,
            end_date=self.end_date,
            n_latest_rows=self.n_latest_rows,
            n_random_rows=self.n_random_rows,
        )
        parsed_start = (
            _parse_strict_utc_date(label='start_date', raw_value=self.start_date)
            if self.start_date is not None
            else None
        )
        parsed_end = (
            _parse_strict_utc_date(label='end_date', raw_value=self.end_date)
            if self.end_date is not None
            else None
        )
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise ValueError('date-window must satisfy start_date <= end_date')
        return self


class HistoricalBitcoinDatasetRequest(BaseModel):
    mode: RawQueryMode = 'native'
    start_date: str | None = None
    end_date: str | None = None
    n_latest_rows: int | None = Field(default=None, gt=0)
    n_random_rows: int | None = Field(default=None, gt=0)
    fields: list[str] | None = None
    filters: list[RawQueryFilter] | None = None
    strict: bool = False

    @model_validator(mode='after')
    def validate_contract(self) -> HistoricalBitcoinDatasetRequest:
        _validate_historical_window_selection(
            start_date=self.start_date,
            end_date=self.end_date,
            n_latest_rows=self.n_latest_rows,
            n_random_rows=self.n_random_rows,
        )
        parsed_start = (
            _parse_strict_utc_date(label='start_date', raw_value=self.start_date)
            if self.start_date is not None
            else None
        )
        parsed_end = (
            _parse_strict_utc_date(label='end_date', raw_value=self.end_date)
            if self.end_date is not None
            else None
        )
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise ValueError('date-window must satisfy start_date <= end_date')
        return self


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
