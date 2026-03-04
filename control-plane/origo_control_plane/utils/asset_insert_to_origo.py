from typing import Any

import polars as pl
from dagster import AssetExecutionContext


def asset_insert_to_origo(
    data: pl.DataFrame,
    client: Any,
    context: AssetExecutionContext,
    file_url: str,
    clickhouse_database: str,
    clickhouse_table: str,
) -> None:
    """Inserts data into a ClickHouse table.

    Args:
        data (pl.DataFrame): The data to insert.
        client (clickhouse_connect.client.Client): The ClickHouse client.
        context (dagster.AssetExecutionContext): The Dagster context.
        file_url (str): The URL of the file to insert.
        clickhouse_database (str): The ClickHouse database.
        clickhouse_table (str): The ClickHouse table.
    """

    data = data.with_columns(
        [
            (
                pl.when(pl.col('timestamp') < 10**13)
                .then(pl.col('timestamp'))
                .otherwise(pl.col('timestamp') // 1_000)
                .cast(pl.Datetime('ms'))
                .alias('datetime')
            )
        ]
    )

    len_data = len(data)
    context.log.info(f'Loaded {len_data} rows from CSV to Arrow.')

    year_month = file_url.split('-')[2:4]
    year, month = year_month[0], year_month[1].split('.')[0]
    month = month.zfill(2)
    month_start = f'{year}-{month}-01'

    context.log.info(f'Data start date: {month_start}')

    check_result = client.query(f"""SELECT count(*)
                                FROM {clickhouse_database}.{clickhouse_table}
                                WHERE datetime >= toDate('{month_start}')
                                AND datetime < addMonths(toDate('{month_start}'), 1)""")

    existing_count = check_result.result_set[0][0]

    if existing_count > 0:
        client.command(f"""ALTER TABLE {clickhouse_database}.{clickhouse_table}
                       DELETE WHERE datetime >= toDate('{month_start}')
                       AND datetime < addMonths(toDate('{month_start}'), 1)""")

        context.log.info(f'Deleted existing data for {month_start}')

    client.insert_arrow(f'{clickhouse_database}.{clickhouse_table}', data.to_arrow())

    inserted_count = client.query(f"""SELECT count(*)
                                      FROM {clickhouse_database}.{clickhouse_table}
                                      WHERE datetime >= toDate('{month_start}')
                                      AND datetime < addMonths(toDate('{month_start}'), 1)""")
    inserted_count = inserted_count.result_set[0][0]
    context.log.info(f'Inserted {inserted_count} rows.')

    if inserted_count != len_data:
        context.log.error(
            f'Row count mismatch! Expected: {len_data}, Actual: {inserted_count}'
        )
        raise ValueError(
            f'Row count mismatch! Expected: {len_data}, Actual: {inserted_count}'
        )
