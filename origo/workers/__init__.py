"""Observed workers: long-running minute loops outside the Dagster run queue.

A worker owns one feed, writes receipts to ClickHouse, reports to Dagster through the
webserver and keeps a heartbeat that its own watchdog and the container healthcheck read.
"""
