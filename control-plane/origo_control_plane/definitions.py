# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

from typing import Any

import dagster as dg
from dagster import Definitions

from .assets.create_binance_agg_trades_table import create_binance_agg_trades_table
from .assets.create_binance_futures_trades_table import (
    create_binance_futures_trades_table,
)
from .assets.create_binance_trades_daily_summary import (
    create_binance_trades_daily_summary,
)
from .assets.create_binance_trades_day_of_month_summary import (
    create_binance_trades_day_of_month_summary,
)
from .assets.create_binance_trades_hour_of_day_summary import (
    create_binance_trades_hour_of_day_summary,
)
from .assets.create_binance_trades_hourly_summary import (
    create_binance_trades_hourly_summary,
)
from .assets.create_binance_trades_month_of_year_summary import (
    create_binance_trades_month_of_year_summary,
)
from .assets.create_binance_trades_monthly_summary import (
    create_binance_trades_monthly_summary,
)
from .assets.create_binance_trades_table import create_binance_trades_table
from .assets.create_binance_trades_table_origo import create_binance_trades_table_origo
from .assets.create_binance_trades_week_of_year_summary import (
    create_binance_trades_week_of_year_summary,
)
from .assets.create_origo_database import create_origo_database
from .assets.daily_trades_to_origo import insert_daily_binance_trades_to_origo
from .assets.monthly_agg_trades_to_origo import (
    insert_monthly_binance_agg_trades_to_origo,
)
from .assets.monthly_futures_agg_trades_to_origo import (
    create_binance_futures_agg_trades_table,
    insert_monthly_binance_futures_agg_trades_to_origo,
)
from .assets.monthly_futures_trades_to_origo import (
    insert_monthly_binance_futures_trades_to_origo,
)
from .assets.monthly_trades_to_origo import insert_monthly_binance_trades_to_origo

define_asset_job: Any = getattr(dg, 'define_asset_job')
schedule: Any = getattr(dg, 'schedule')

# Database Maintenance Jobs

create_origo_database_job = define_asset_job(
    name='create_origo_database_job', selection=['create_origo_database']
)

create_binance_trades_table_job = define_asset_job(
    name='create_binance_trades_table_job', selection=['create_binance_trades_table']
)

create_binance_trades_table_origo_job = define_asset_job(
    name='create_binance_trades_table_origo_job',
    selection=['create_binance_trades_table_origo'],
)

create_binance_agg_trades_table_job = define_asset_job(
    name='create_binance_agg_trades_table_job',
    selection=['create_binance_agg_trades_table'],
)

create_binance_futures_trades_table_job = define_asset_job(
    name='create_binance_futures_trades_table_job',
    selection=['create_binance_futures_trades_table'],
)

create_binance_futures_agg_trades_table_job = define_asset_job(
    name='create_binance_futures_agg_trades_table_job',
    selection=['create_binance_futures_agg_trades_table'],
)

# Data Insertion Jobs

insert_monthly_binance_trades_job = define_asset_job(
    name='insert_monthly_trades_to_origo_job',
    selection=['insert_monthly_binance_trades_to_origo'],
)

insert_daily_binance_trades_job = define_asset_job(
    name='insert_daily_trades_to_origo_job',
    selection=['insert_daily_binance_trades_to_origo'],
)

insert_monthly_binance_agg_trades_job = define_asset_job(
    name='insert_monthly_agg_trades_to_origo_job',
    selection=['insert_monthly_binance_agg_trades_to_origo'],
)

insert_monthly_binance_futures_trades_job = define_asset_job(
    name='insert_monthly_futures_trades_to_origo_job',
    selection=['insert_monthly_binance_futures_trades_to_origo'],
)

insert_monthly_binance_futures_agg_trades_job = define_asset_job(
    name='insert_monthly_futures_agg_trades_to_origo_job',
    selection=['insert_monthly_binance_futures_agg_trades_to_origo'],
)

# summary Table Creation Jobs

create_binance_trades_monthly_summary_job = define_asset_job(
    name='create_binance_trades_monthly_summary_job',
    selection=['create_binance_trades_monthly_summary'],
)

create_binance_trades_daily_summary_job = define_asset_job(
    name='create_binance_trades_daily_summary_job',
    selection=['create_binance_trades_daily_summary'],
)

create_binance_trades_hourly_summary_job = define_asset_job(
    name='create_binance_trades_hourly_summary_job',
    selection=['create_binance_trades_hourly_summary'],
)

create_binance_trades_hour_of_day_summary_job = define_asset_job(
    name='create_binance_trades_hour_of_day_summary_job',
    selection=['create_binance_trades_hour_of_day_summary'],
)

create_binance_trades_day_of_month_summary_job = define_asset_job(
    name='create_binance_trades_day_of_month_summary_job',
    selection=['create_binance_trades_day_of_month_summary'],
)

create_binance_trades_week_of_year_summary_job = define_asset_job(
    name='create_binance_trades_week_of_year_summary_job',
    selection=['create_binance_trades_week_of_year_summary'],
)

create_binance_trades_month_of_year_summary_job = define_asset_job(
    name='create_binance_trades_month_of_year_summary_job',
    selection=['create_binance_trades_month_of_year_summary'],
)


@schedule(
    job=insert_daily_binance_trades_job,
    cron_schedule='0 1 * * *',
    execution_timezone='UTC',
)
def daily_pipeline_schedule() -> dict[str, str]:
    return {}


defs = Definitions(
    assets=[
        create_origo_database,
        create_binance_trades_table,
        create_binance_trades_table_origo,
        insert_monthly_binance_trades_to_origo,
        insert_daily_binance_trades_to_origo,
        create_binance_trades_monthly_summary,
        create_binance_trades_daily_summary,
        create_binance_trades_hourly_summary,
        create_binance_trades_hour_of_day_summary,
        create_binance_trades_day_of_month_summary,
        create_binance_trades_week_of_year_summary,
        create_binance_trades_month_of_year_summary,
        create_binance_agg_trades_table,
        insert_monthly_binance_agg_trades_to_origo,
        create_binance_futures_trades_table,
        insert_monthly_binance_futures_trades_to_origo,
        create_binance_futures_agg_trades_table,
        insert_monthly_binance_futures_agg_trades_to_origo,
    ],
    schedules=[daily_pipeline_schedule],
    jobs=[
        create_origo_database_job,
        create_binance_trades_table_job,
        create_binance_trades_table_origo_job,
        insert_monthly_binance_trades_job,
        insert_daily_binance_trades_job,
        create_binance_trades_monthly_summary_job,
        create_binance_trades_daily_summary_job,
        create_binance_trades_hourly_summary_job,
        create_binance_trades_hour_of_day_summary_job,
        create_binance_trades_day_of_month_summary_job,
        create_binance_trades_week_of_year_summary_job,
        create_binance_trades_month_of_year_summary_job,
        create_binance_agg_trades_table_job,
        insert_monthly_binance_agg_trades_job,
        create_binance_futures_trades_table_job,
        insert_monthly_binance_futures_trades_job,
        create_binance_futures_agg_trades_table_job,
        insert_monthly_binance_futures_agg_trades_job,
    ],
)

# TODO: Put everything in to same order in all segments of the code
