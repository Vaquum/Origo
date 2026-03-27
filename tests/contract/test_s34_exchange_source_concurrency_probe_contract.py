from __future__ import annotations

from typing import Any

import origo_control_plane.s34_exchange_source_concurrency_probe as probe
import pytest


def _level_result(*, concurrency: int, passed: bool) -> probe.ProbeLevelResult:
    failure_count = 0 if passed else 1
    return probe.ProbeLevelResult(
        concurrency=concurrency,
        passed=passed,
        rounds=2,
        attempts=2 * concurrency,
        success_count=(2 * concurrency) - failure_count,
        failure_count=failure_count,
        failure_kinds={} if passed else {'http_429': 1},
        status_code_counts={} if passed else {'429': 1},
        median_duration_seconds=1.0,
        p95_duration_seconds=1.5,
        max_duration_seconds=2.0,
        bytes_downloaded=1024 * concurrency,
    )


def test_probe_search_finds_first_failing_ceiling(monkeypatch: Any) -> None:
    observed_levels: list[int] = []

    def fake_run_probe_level_or_raise(
        *,
        dataset: probe.Dataset,
        concurrency: int,
        rounds: int,
        sample_dates: list[str],
    ) -> probe.ProbeLevelResult:
        assert dataset == 'okx_spot_trades'
        assert rounds == 2
        assert sample_dates[0] == '2024-01-01'
        observed_levels.append(concurrency)
        return _level_result(concurrency=concurrency, passed=concurrency <= 3)

    monkeypatch.setattr(
        probe,
        '_run_probe_level_or_raise',
        fake_run_probe_level_or_raise,
    )

    result = probe._search_concurrency_ceiling_or_raise(
        dataset='okx_spot_trades',
        sample_start_date='2024-01-01',
        sample_day_count=8,
        rounds_per_level=2,
        initial_concurrency=1,
        max_concurrency_cap=8,
    )

    assert result.ceiling_found is True
    assert result.max_passing_concurrency == 3
    assert result.first_failing_concurrency == 4
    assert observed_levels == [1, 2, 4, 3]


def test_probe_search_reports_open_ceiling_when_cap_passes(monkeypatch: Any) -> None:
    def fake_run_probe_level_or_raise(
        *,
        dataset: probe.Dataset,
        concurrency: int,
        rounds: int,
        sample_dates: list[str],
    ) -> probe.ProbeLevelResult:
        assert dataset == 'bybit_spot_trades'
        return _level_result(concurrency=concurrency, passed=True)

    monkeypatch.setattr(
        probe,
        '_run_probe_level_or_raise',
        fake_run_probe_level_or_raise,
    )

    result = probe._search_concurrency_ceiling_or_raise(
        dataset='bybit_spot_trades',
        sample_start_date='2024-01-01',
        sample_day_count=8,
        rounds_per_level=2,
        initial_concurrency=20,
        max_concurrency_cap=160,
    )

    assert result.ceiling_found is False
    assert result.max_passing_concurrency == 160
    assert result.first_failing_concurrency is None


def test_probe_search_fails_loud_when_initial_level_fails(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        probe,
        '_run_probe_level_or_raise',
        lambda **_: _level_result(concurrency=1, passed=False),
    )

    with pytest.raises(
        RuntimeError,
        match='No passing concurrency level found',
    ):
        probe._search_concurrency_ceiling_or_raise(
            dataset='okx_spot_trades',
            sample_start_date='2024-01-01',
            sample_day_count=8,
            rounds_per_level=2,
            initial_concurrency=1,
            max_concurrency_cap=8,
        )


def test_okx_rate_probe_returns_first_passing_interval(monkeypatch: Any) -> None:
    observed_intervals: list[float] = []

    def fake_run_okx_rate_level_or_raise(
        *,
        interval_seconds: float,
        attempts_per_level: int,
        sample_dates: list[str],
        cooldown_seconds: float,
    ) -> probe.ProbeRateLevelResult:
        assert attempts_per_level == 5
        assert sample_dates[0] == '2024-01-01'
        assert cooldown_seconds == 6.0
        observed_intervals.append(interval_seconds)
        return probe.ProbeRateLevelResult(
            interval_seconds=interval_seconds,
            passed=interval_seconds >= 0.75,
            attempts=5,
            success_count=5 if interval_seconds >= 0.75 else 4,
            failure_count=0 if interval_seconds >= 0.75 else 1,
            failure_kinds={} if interval_seconds >= 0.75 else {'http_429': 1},
            status_code_counts={} if interval_seconds >= 0.75 else {'429': 1},
            median_duration_seconds=0.5,
            p95_duration_seconds=0.7,
            max_duration_seconds=1.0,
            bytes_downloaded=1024,
        )

    monkeypatch.setattr(
        probe,
        '_run_okx_rate_level_or_raise',
        fake_run_okx_rate_level_or_raise,
    )

    result = probe._search_okx_min_safe_interval_or_raise(
        sample_start_date='2024-01-01',
        sample_day_count=8,
        attempts_per_level=5,
        cooldown_seconds=6.0,
        initial_interval_seconds=0.0,
        max_interval_seconds=1.0,
        interval_step_seconds=0.25,
    )

    assert result.minimal_passing_interval_seconds == 0.75
    assert result.maximal_safe_requests_per_second == 1.333333
    assert observed_intervals == [0.0, 0.25, 0.5, 0.75]


def test_okx_rate_probe_fails_loud_when_no_interval_passes(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        probe,
        '_run_okx_rate_level_or_raise',
        lambda **_: probe.ProbeRateLevelResult(
            interval_seconds=0.0,
            passed=False,
            attempts=5,
            success_count=4,
            failure_count=1,
            failure_kinds={'http_429': 1},
            status_code_counts={'429': 1},
            median_duration_seconds=0.5,
            p95_duration_seconds=0.7,
            max_duration_seconds=1.0,
            bytes_downloaded=1024,
        ),
    )

    with pytest.raises(
        RuntimeError,
        match='No passing OKX rate interval found',
    ):
        probe._search_okx_min_safe_interval_or_raise(
            sample_start_date='2024-01-01',
            sample_day_count=8,
            attempts_per_level=5,
            cooldown_seconds=6.0,
            initial_interval_seconds=0.0,
            max_interval_seconds=1.0,
            interval_step_seconds=0.25,
        )
