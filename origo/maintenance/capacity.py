"""Capacity acceptance counts physical storage and verifies recorded migration evidence."""

from pydantic import BaseModel, ConfigDict, Field


class CapacityEvidence(BaseModel):
    model_config = ConfigDict(extra='forbid')

    dagster_allocated_bytes: int = Field(ge=0)
    clickhouse_business_bytes: int = Field(gt=0)
    diagnostic_allocated_bytes: int = Field(ge=0)
    source_runs_before: int = Field(gt=0)
    source_runs_after: int = Field(gt=0)
    source_identity_sha256_before: str = Field(pattern=r'^[0-9a-f]{64}$')
    source_identity_sha256_after: str = Field(pattern=r'^[0-9a-f]{64}$')
    source_histories_verified: int = Field(ge=0)
    source_history_mismatches: int = Field(ge=0)
    current_state_sha256_before: str = Field(pattern=r'^[0-9a-f]{64}$')
    current_state_sha256_after: str = Field(pattern=r'^[0-9a-f]{64}$')
    captured_logs_before: int = Field(ge=0)
    captured_logs_verified: int = Field(ge=0)
    captured_log_mismatches: int = Field(ge=0)
    observed_ingress_runs: int = Field(gt=0)
    observed_window_seconds: float = Field(gt=0)
    replay_written_runs: int = Field(gt=0)
    replay_retired_runs: int = Field(ge=0)
    replay_seconds: float = Field(gt=0)
    replay_backlog_before: int = Field(ge=0)
    replay_backlog_after: int = Field(ge=0)
    dagit_query_p95_seconds: float = Field(ge=0)

    def violations(self) -> list[str]:
        failures: list[str] = []
        if self.dagster_allocated_bytes * 10 >= self.clickhouse_business_bytes:
            failures.append('Dagster occupies at least 10% of ClickHouse business data.')
        if (
            self.source_runs_before != self.source_runs_after
            or self.source_identity_sha256_before != self.source_identity_sha256_after
            or self.source_histories_verified != self.source_runs_before
            or self.source_history_mismatches
        ):
            failures.append('Complete source provenance was not verified unchanged.')
        if self.current_state_sha256_before != self.current_state_sha256_after:
            failures.append('Current Dagit state changed during migration.')
        if self.captured_logs_before != self.captured_logs_verified or self.captured_log_mismatches:
            failures.append('Captured source logs were not verified unchanged.')
        required_rate = 10 * self.observed_ingress_runs / self.observed_window_seconds
        if self.replay_written_runs / self.replay_seconds < required_rate:
            failures.append('Replay did not sustain ten times observed ingress.')
        if (
            self.replay_retired_runs < self.replay_written_runs
            or self.replay_backlog_after > self.replay_backlog_before
        ):
            failures.append('Projection cleanup did not keep pace with replay.')
        if self.dagit_query_p95_seconds > 0.25:
            failures.append('Dagit job-query p95 exceeds 250 ms.')
        return failures
