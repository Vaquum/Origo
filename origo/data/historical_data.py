from pathlib import Path
from typing import Any

import polars as pl

from origo.data._internal.binance_file_to_polars import binance_file_to_polars
from origo.data._internal.generic_endpoints import (
    query_etf_daily_metrics_data,
    query_fred_series_metrics_data,
    query_spot_klines_data,
    query_spot_trades_data,
)


class HistoricalData:
    def __init__(self, auth_token: str | None = None) -> None:
        """Stateful interface for exchange historical spot trades and klines."""

        self.auth_token = auth_token
        self.data: pl.DataFrame = pl.DataFrame()
        self.data_columns: list[str] = []

    @staticmethod
    def _strict_window_warning_present(
        *,
        n_latest_rows: int | None,
        n_random_rows: int | None,
    ) -> bool:
        return n_latest_rows is not None or n_random_rows is not None

    @classmethod
    def _enforce_strict_window_rules(
        cls,
        *,
        strict: bool,
        n_latest_rows: int | None,
        n_random_rows: int | None,
    ) -> None:
        if strict and cls._strict_window_warning_present(
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        ):
            raise ValueError('Warnings present while strict=true')

    def get_binance_file(
        self, file_url: str, has_header: bool = False, columns: list[str] | None = None
    ) -> None:
        """Get historical data from a Binance file based on the file URL.

        Data can be found here: https://data.binance.vision/

        Args:
            file_url (str): The URL of the Binance file
            has_header (bool): Whether the file has a header
            columns (List[str]): The columns to be included in the data

        Returns:
            self.data (pl.DataFrame)

        """

        self.data = binance_file_to_polars(file_url, has_header=has_header)
        if columns is not None:
            self.data.columns = columns

        self.data = self.data.with_columns(
            [
                pl.when(pl.col('timestamp') < 10**13)
                .then(pl.col('timestamp'))
                .otherwise(pl.col('timestamp') // 1000)
                .cast(pl.UInt64)
                .alias('timestamp')
            ]
        )

        self.data = self.data.with_columns(
            [pl.col('timestamp').cast(pl.Datetime('ms')).alias('datetime')]
        )

        self.data_columns = self.data.columns

    def get_binance_spot_trades(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
        include_datetime_col: bool = True,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_spot_trades_data(
            source='binance',
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            include_datetime_col=include_datetime_col,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_okx_spot_trades(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
        include_datetime_col: bool = True,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_spot_trades_data(
            source='okx',
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            include_datetime_col=include_datetime_col,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_bybit_spot_trades(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
        include_datetime_col: bool = True,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_spot_trades_data(
            source='bybit',
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            include_datetime_col=include_datetime_col,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_binance_spot_klines(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
        kline_size: int = 1,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_spot_klines_data(
            source='binance',
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            kline_size=kline_size,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_okx_spot_klines(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
        kline_size: int = 1,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_spot_klines_data(
            source='okx',
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            kline_size=kline_size,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_bybit_spot_klines(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
        kline_size: int = 1,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_spot_klines_data(
            source='bybit',
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            kline_size=kline_size,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_etf_daily_metrics(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_etf_daily_metrics_data(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def get_fred_series_metrics(
        self,
        mode: str = 'native',
        start_date: str | None = None,
        end_date: str | None = None,
        n_latest_rows: int | None = None,
        n_random_rows: int | None = None,
        fields: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        strict: bool = False,
    ) -> None:
        self._enforce_strict_window_rules(
            strict=strict,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
        )
        self.data = query_fred_series_metrics_data(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            n_latest_rows=n_latest_rows,
            n_random_rows=n_random_rows,
            fields=fields,
            filters=filters,
            auth_token=self.auth_token,
            show_summary=False,
        )
        self.data_columns = self.data.columns

    def _get_data_for_test(self, n_rows: int | None = 5000) -> None:
        """
        Get test klines data from local CSV file for testing purposes.

        NOTE: This is a test-only method used by SFDs to load sample data
        during test runs. Uses datasets/klines_2h_2020_2025.csv.

        Args:
            n_rows (int | None): Number of rows to read from CSV (default: 5000).
                                 If None, reads entire file.

        Returns:
            None (sets self.data with the loaded klines data)
        """

        dataset_path = (
            Path(__file__).resolve().parents[2] / 'datasets' / 'klines_2h_2020_2025.csv'
        )
        if not dataset_path.exists():
            raise FileNotFoundError(
                f'Test dataset file not found: {dataset_path}. '
                'Expected file relative to repository root.'
            )

        self.data = pl.read_csv(dataset_path, n_rows=n_rows)
        if 'datetime' not in self.data.columns:
            raise RuntimeError(
                f'Test dataset is missing required datetime column: {dataset_path}'
            )
        self.data = self.data.with_columns(
            [pl.col('datetime').str.to_datetime().alias('datetime')]
        )
        self.data_columns = self.data.columns
