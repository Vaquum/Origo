"""Daily UTC partitions of the BTC briefing assets, one per spot trading day."""

from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(start_date='2017-08-17')
