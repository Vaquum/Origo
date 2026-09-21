"""Typed identities for the existing worker receipt path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

Verdict = Literal['PASS', 'FAIL', 'UNKNOWN']


@dataclass(frozen=True)
class AttemptIdentity:
    work_id: str
    attempt_id: UUID
    owner_epoch: str
    state_token: str
    prerequisite_key: str

    def __post_init__(self) -> None:
        if not self.work_id or len(self.work_id) > 1024 or self.attempt_id.int == 0:
            raise ValueError('An attempt requires bounded work identity and a nonzero UUID.')
        if not self.owner_epoch or UUID(self.owner_epoch).int == 0:
            raise ValueError('An attempt requires a nonzero owner epoch.')
        if len(self.state_token) > 256 or len(self.prerequisite_key) > 1024:
            raise ValueError('Attempt token/prerequisite identity is too long.')


@dataclass(frozen=True)
class MetricVerdict:
    metric_id: str
    entity: str
    statistic: str
    observed: float | None
    threshold: float | None
    unit: str
    verdict: Verdict
    evidence_refs: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class AcceptanceReport:
    schema_version: int
    environment: Literal['isolated', 'production']
    code_sha: str
    runtime_identity_sha256: str
    policy_sha256: str
    inventory_sha256: str
    evidence_manifest_sha256: str
    window_start: datetime
    window_end: datetime
    observed_buckets: int
    expected_buckets: int
    results: tuple[MetricVerdict, ...]
    verdict: Verdict
