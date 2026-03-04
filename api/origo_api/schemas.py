from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

BinanceDataset = Literal['spot_trades', 'spot_agg_trades', 'futures_trades']


class RawQueryRequest(BaseModel):
    dataset: BinanceDataset
    fields: list[str] | None = None
    month_year: tuple[int, int] | None = None
    n_rows: int | None = Field(default=None, gt=0)
    n_random: int | None = Field(default=None, gt=0)
    time_range: tuple[str, str] | None = None
    include_datetime: bool = True
    strict: bool = False
    auth_token: str | None = None

    @model_validator(mode='after')
    def validate_window_mode(self) -> RawQueryRequest:
        selected = sum(
            [
                self.month_year is not None,
                self.n_rows is not None,
                self.n_random is not None,
                self.time_range is not None,
            ]
        )
        if selected != 1:
            raise ValueError(
                'Exactly one window mode must be provided: '
                'month_year | n_rows | n_random | time_range'
            )
        return self


class RawQuerySchemaField(BaseModel):
    name: str
    dtype: str


class RawQueryWarning(BaseModel):
    code: str
    message: str


def _empty_warnings() -> list[RawQueryWarning]:
    return []


class RawQueryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mode: str
    source: str | None
    row_count: int
    schema_items: list[RawQuerySchemaField] = Field(
        validation_alias='schema',
        serialization_alias='schema',
    )
    warnings: list[RawQueryWarning] = Field(default_factory=_empty_warnings)
    rows: list[dict[str, Any]]
