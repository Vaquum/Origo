from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import origo_control_plane.s34_etf_ishares_archive_bootstrap_runner as runner
import pytest

from origo.scraper.contracts import RawArtifact


def test_run_s34_etf_ishares_archive_bootstrap_rejects_prehistory_start() -> None:
    with pytest.raises(RuntimeError, match='before supported history start'):
        runner.run_s34_etf_ishares_archive_bootstrap_or_raise(
            run_id='bootstrap-1',
            start_date=date(2024, 1, 10),
            end_date=date(2024, 1, 11),
        )


def test_run_s34_etf_ishares_archive_bootstrap_skips_existing_and_no_data(
    monkeypatch: Any,
) -> None:
    persisted_artifact_ids: list[str] = []

    class _FakeAdapter:
        adapter_name = 'etf_ishares_ibit'

        def fetch(self, *, source: Any, run_context: Any) -> RawArtifact:
            del run_context
            return RawArtifact(
                artifact_id=f"artifact-{source.metadata['request_as_of_date']}",
                source_id=source.source_id,
                source_uri=source.source_uri,
                fetched_at_utc=datetime(2026, 3, 28, 0, 0, tzinfo=UTC),
                fetch_method='http',
                artifact_format='csv',
                content_sha256='sha',
                content=b'csv',
            )

    monkeypatch.setattr(runner, 'ISharesIBITAdapter', _FakeAdapter)
    monkeypatch.setattr(
        runner,
        '_load_existing_valid_ishares_archive_days_or_raise',
        lambda **_: ('2024-01-11',),
    )
    monkeypatch.setattr(runner, 'load_fetch_retry_policy_from_env', lambda: object())
    monkeypatch.setattr(
        runner,
        'retry_with_backoff',
        lambda *, operation_name, operation, policy: operation(),
    )

    def _validate(**kwargs: Any) -> tuple[str, bool]:
        request_day = kwargs['request_day'].isoformat()
        if request_day == '2024-01-15':
            return request_day, True
        return request_day, False

    monkeypatch.setattr(runner, '_validate_ishares_artifact_or_raise', _validate)
    monkeypatch.setattr(
        runner,
        'persist_raw_artifact',
        lambda *, artifact, run_context: persisted_artifact_ids.append(artifact.artifact_id),
    )

    summary = runner.run_s34_etf_ishares_archive_bootstrap_or_raise(
        run_id='bootstrap-1',
        start_date=date(2024, 1, 11),
        end_date=date(2024, 1, 15),
    )

    assert summary['requested_weekday_count'] == 3
    assert summary['skipped_existing_days'] == ['2024-01-11']
    assert summary['skipped_no_data_days'] == ['2024-01-15']
    assert summary['persisted_days'] == ['2024-01-12']
    assert summary['persisted_no_data_days'] == ['2024-01-15']
    assert summary['persisted_artifact_count'] == 2
    assert persisted_artifact_ids == ['artifact-2024-01-12', 'artifact-2024-01-15']
