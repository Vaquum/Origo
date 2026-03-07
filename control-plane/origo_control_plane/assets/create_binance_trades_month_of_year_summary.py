from dagster import AssetExecutionContext, asset

from .schema_migration_only import raise_schema_migration_only


@asset(group_name='schema_migration_only')
def create_binance_trades_month_of_year_summary(context: AssetExecutionContext) -> None:
    raise_schema_migration_only(context=context, asset_name='create_binance_trades_month_of_year_summary')
