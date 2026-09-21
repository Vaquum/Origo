"""Report certified data progress without turning a worker tick into freshness."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from origo.workers.report import Reporter


def report_data_progress(
    reporter: Reporter, asset: str, *, source_end: datetime, observed_at: datetime,
    previous_end: datetime | None, metadata: Mapping[str, object], max_age_seconds: int,
) -> datetime | None:
    data = {**metadata, 'source_timestamp': source_end.isoformat(),
            'observed_at': observed_at.isoformat(),
            'lag_seconds': (observed_at - source_end).total_seconds()}
    reporter.observed(asset, last_updated=source_end, metadata=data)
    # Installed Dagster's time-window policy uses the materialization event's time.
    # Emit that event only on verified, timely advancement, never on a timer tick.
    age = (observed_at - source_end).total_seconds()
    advanced = previous_end is None or source_end > previous_end
    if advanced and 0 <= age <= max_age_seconds:
        if reporter.materialized(asset, partition=None, metadata=data):
            return source_end
    return previous_end
