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
    writer, _identity = evidence_writer(tmp_path, environment='isolated')
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


def test_controlled_faults_preserve_data_and_native_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    binance_fixture_server_root_url: str,
) -> None:
    """Real local provider failure, database restart, render death and native retry.

    This is a bounded regression, not a claim of a five-minute or six-hour trial.
    The sole database that may be stopped is the label-checked owned test container.
    """
    import time
    from dataclasses import replace

    from dagster import Definitions
    from dagster._core.test_utils import instance_for_test

    from origo.sources.bundle import build_source_bundle
    from origo.sources.storage import StorageError
    from origo.steady_state.trial_resources import OwnedClickHouse

    from .test_steady_state_publication import (
        CANONICAL_DAY,
        MINUTE_KEY,
        PROVENANCE,
        SPEC,
        _kill_full_render_after,
        _Prepared,
        rest,
    )

    from .steady_state_evidence_cases import clear_inherited_test_connections
    clear_inherited_test_connections(monkeypatch)

    with OwnedClickHouse(tmp_path / 'owned-fault-case', memory_gib=3) as owned:
        for key, value in owned.environment.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
        prepared = _Prepared(tmp_path / 'files', monkeypatch, binance_fixture_server_root_url)
        try:
            prepared.build_minute()
            before = prepared.publish_mount(allow_full=False)
            token = prepared.store.snapshot().token
            previous = prepared.mirror_identities()
            arrow = prepared.arrow_targets()
            captured_provider = rest.get_response

            def unavailable(*args: object, **kwargs: object) -> object:
                raise RuntimeError(
                    'Controlled provider interruption at the recorded transport boundary.'
                )

            monkeypatch.setattr(rest, 'get_response', unavailable)
            with pytest.raises(RuntimeError, match='Controlled provider interruption'):
                prepared.runtime.build(MINUTE_KEY, provisional=True)
            assert prepared.store.snapshot().token == token
            assert prepared.manifest()['pinned_token'] == before['pinned_token']
            assert prepared.mirror_identities() == previous and prepared.arrow_targets() == arrow

            prepared.client.disconnect()
            owned.stop()
            with pytest.raises(StorageError):
                prepared.store.snapshot()
            # Readers retain the last committed files while the source store is unavailable.
            assert prepared.mirror_identities() == previous and prepared.arrow_targets() == arrow
            owned.start()
            deadline = time.monotonic() + 45
            while True:
                try:
                    assert prepared.store.snapshot().token == token
                    break
                except StorageError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.25)
            monkeypatch.setattr(rest, 'get_response', captured_provider)
            prepared.requests[:] = list(PROVENANCE['requests'])
            prepared.runtime.build(MINUTE_KEY, provisional=True)
            assert prepared.store.snapshot().token == token
            assert prepared.publish_mount(allow_full=False)['pinned_token'] == token

            # A real rendering process dies with incomplete historical files private.
            prepared.runtime.build(CANONICAL_DAY)
            _kill_full_render_after(prepared.environment, prepared.mount, die_after=6)
            assert prepared.mirror_identities() == previous and prepared.arrow_targets() == arrow
            fixture_spec = replace(
                SPEC,
                partitions=replace(
                    SPEC.partitions,
                    first_day=prepared.store.anchor().date(),
                ),
            )
            bundle = build_source_bundle(fixture_spec)
            definitions = Definitions(assets=bundle.assets, jobs=bundle.jobs)
            native_root = tmp_path / 'native-recovery'
            native_root.mkdir()
            with instance_for_test(temp_dir=str(native_root)) as instance:
                result = definitions.resolve_job_def(
                    f'publish_{SPEC.key}_mount_job'
                ).execute_in_process(instance=instance)
                assert result.success
                run = instance.get_run_by_id(result.run_id)
                assert run is not None and run.status.value == 'SUCCESS'
            assert prepared.manifest()['pinned_token'] == prepared.store.snapshot().token
            assert prepared.manifest()['render']['checkpoints_reused'] >= 6
            prepared.assert_reader_contents(((2020, 1), (2025, 1)))
        finally:
            prepared.close()
