"""Executable rejection cases for absent, changed and falsely summarized evidence."""

from datetime import timedelta
from pathlib import Path

import pytest

from origo.steady_state.policy import load_inventory, load_policy
from origo.steady_state.trial_resources import validate_isolated_environment
from origo.steady_state.verification import EvidenceError, verify_bundle

from .steady_state_evidence_cases import START, evaluator, evidence_writer, sample


def test_verifier_requires_complete_frozen_production_window(tmp_path: Path) -> None:
    writer, identity = evidence_writer(tmp_path / 'short')
    writer.append_sample(sample(START, identity))
    checked = evaluator(writer)
    checked.evaluate_ss12()
    verdicts = {item.statistic: item.verdict for item in checked.results}
    assert verdicts['window_hours_min'] == 'FAIL'
    assert verdicts['missing_or_unknown_buckets_max'] == 'FAIL'
    assert verdicts['unresolved_blocking_work_max'] == 'UNKNOWN'

    # A 72-hour timer with only its two endpoints does not establish intervening data.
    writer.append_sample(sample(START + timedelta(hours=72), identity))
    checked = evaluator(writer)
    checked.evaluate_ss12()
    assert any(
        item.statistic == 'missing_or_unknown_buckets_max' and item.verdict == 'FAIL'
        for item in checked.results
    )
    second = writer.write_identity(
        {
            'kind': 'steady_state_identity',
            'schema_version': 1,
            'environment': 'production',
            'code_sha': 'b' * 40,
            'policy_sha256': load_policy().sha256,
            'inventory_sha256': load_inventory().sha256,
            'fixture_only': True,
        }
    )
    writer.append_sample(sample(START + timedelta(hours=72, minutes=1), second))
    writer.write_manifest()
    with pytest.raises(EvidenceError, match='more than one runtime identity'):
        verify_bundle(
            writer.root, profile='production', policy=load_policy(), inventory=load_inventory()
        )
    isolated, isolated_identity = evidence_writer(tmp_path / 'isolated', environment='isolated')
    isolated.append_sample(sample(START, isolated_identity))
    isolated.write_manifest()
    with pytest.raises(EvidenceError, match='isolated or replayed'):
        verify_bundle(
            isolated.root, profile='production', policy=load_policy(), inventory=load_inventory()
        )


@pytest.mark.parametrize(
    'blocking',
    [
        {'status': 'observed'},
        {'status': 'observed', 'open_failures': [{'count': None}], 'outstanding_attempts': {}},
        {'status': 'observed', 'open_failures': [], 'outstanding_attempts': {'worker': -1}},
        {'status': 'observed', 'open_failures': [{'count': '0'}], 'outstanding_attempts': {}},
    ],
)
def test_missing_blocking_counts_are_unknown_not_zero(
    tmp_path: Path,
    blocking: dict[str, object],
) -> None:
    writer, identity = evidence_writer(tmp_path)
    writer.append_sample({**sample(START, identity), 'blocking': blocking})
    checked = evaluator(writer)
    checked.evaluate_ss12()
    result = next(
        item for item in checked.results if item.statistic == 'unresolved_blocking_work_max'
    )
    assert result.verdict == 'UNKNOWN' and result.observed is None


def test_a_claimed_capacity_summary_without_raw_progress_is_not_acceptance(tmp_path: Path) -> None:
    writer, identity = evidence_writer(tmp_path, environment='isolated')
    policy, inventory = load_policy(), load_inventory()
    writer.write_artifact(
        'trials/capacity_trial.json',
        {
            'kind': 'steady_state_capacity_trial',
            'schema_version': 1,
            'environment': 'isolated',
            'code_sha': 'a' * 40,
            'policy_sha256': policy.sha256,
            'inventory_sha256': inventory.sha256,
            'trial_start': START.isoformat(),
            'trial_end': (START + timedelta(hours=6)).isoformat(),
            'normal_freshness_hours_after_recovery': 3,
            'sources': {
                key: {
                    'arrival_minutes': 120,
                    'useful_minutes': 180,
                    'withheld_minutes': 60,
                    'drain_minutes': 120,
                    'lost_rows': 0,
                    'duplicate_selected_rows': 0,
                    'hidden_backlog_minutes': 0,
                }
                for key in inventory.sources
            },
        },
    )
    checked = evaluator(writer)
    checked.evaluate_ss05()
    assert checked.results
    assert all(item.verdict == 'UNKNOWN' for item in checked.results)
    assert all('raw progress' in item.reason for item in checked.results)


def test_observer_is_read_only_and_benchmark_refuses_production(tmp_path: Path) -> None:
    from origo.steady_state.capture import ReadOnlyClient

    class NoConnection:
        def execute(self, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
            raise AssertionError('The rejected command must not reach a connection.')

        def disconnect(self) -> None:
            raise AssertionError('There is no connection to close.')

    observer = ReadOnlyClient(NoConnection())
    for query in (
        'INSERT INTO x VALUES (1)',
        'SELECT 1; DROP TABLE x',
        "SELECT * FROM url('https://example.invalid', 'CSV')",
    ):
        with pytest.raises(PermissionError):
            observer.execute(query)
    for environment in (
        {'CLICKHOUSE_HOST': '37.27.112.167'},
        {'CLICKHOUSE_PASSWORD': 'forbidden-inherited-secret'},
        {'DOCKER_HOST': 'ssh://remote.invalid'},
        {'DAGSTER_WEBSERVER_URL': 'https://production.invalid'},
        {'ORIGO_SOURCE_PUBLICATION_ROOT': '/opt/origo/shadow'},
    ):
        with pytest.raises(PermissionError):
            validate_isolated_environment(environment, tmp_path / 'not-created')
    for destination in (
        Path('/opt/origo/trial'),
        Path('/opt/parquet/trial'),
        Path('/var/lib/trial'),
    ):
        with pytest.raises(PermissionError):
            validate_isolated_environment({}, destination)
    validate_isolated_environment({}, tmp_path / 'not-created')
    assert not (tmp_path / 'not-created').exists()
