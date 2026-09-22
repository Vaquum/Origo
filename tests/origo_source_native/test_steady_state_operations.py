"""Native queued execution and semantic deployment outcome regression (M11)."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

from origo.orchestration.policy import reconcile_stale_concurrency_claims
from origo.steady_state.deployment import MAINTENANCE_JOB, verify_observation, wait_for_deployment
from origo.steady_state.trial_native import NativeMaintenance


def test_maintenance_resource_and_deploy_outcomes_are_observed(tmp_path: Path, monkeypatch) -> None:
    from origo.steady_state.trial_resources import OwnedClickHouse
    from .steady_state_evidence_cases import clear_inherited_test_connections
    clear_inherited_test_connections(monkeypatch)
    with OwnedClickHouse(tmp_path / 'owned-database') as owned:
        for key, value in owned.environment.items():
            monkeypatch.setenv(key, value)
        _native_outcomes(tmp_path, monkeypatch)


def _native_outcomes(tmp_path: Path, monkeypatch) -> None:
    import os
    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.workers.receipts import ensure_monitoring_tables
    from origo.sources.binance_perp_trades import BINANCE_PERP_TRADES_SPEC
    from origo.sources.lifecycle import SourceRuntime
    from origo.sources.storage import SourceStore
    from .test_binance_perp_daily_source_adapter import daily, archive_response
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        database = get_clickhouse_settings().database
        ensure_monitoring_tables(client, database)
        # Native metadata health retains its existing fraction bound. Load one
        # authentic complete archive rather than falsifying business volume.
        monkeypatch.setattr(daily, 'get_response', archive_response)
        runtime = SourceRuntime(BINANCE_PERP_TRADES_SPEC,
            SourceStore(client, database, BINANCE_PERP_TRADES_SPEC),
            Path(os.environ['ORIGO_SOURCE_LOCK_DIR']), 'native-maintenance-fixture')
        runtime.setup(anchor=datetime(2024, 4, 20, tzinfo=UTC))
        runtime.build('2024-04-20')
    finally:
        client.disconnect()
    ready_at = datetime.now(UTC) - timedelta(seconds=1)
    with NativeMaintenance(tmp_path / 'native-maintenance') as native:
        from origo.steady_state.trial_http import NativeHTTP
        from origo.workers.dagster_reader import DagsterReader
        from origo.workers.report import Reporter
        with NativeHTTP(native) as http:
            url = f'http://127.0.0.1:{http.port}'
            assert DagsterReader(url).backfill_owns_publication('binance_spot_trades') is False
            assert Reporter(url).materialized('binance_spot_trades_provisional_feed',
                partition=None, metadata={'test_observation': True})
            ports, sockets = http.allowed_endpoints()
            assert http.port in ports and (len(ports) == 2 or len(sockets) == 1)
        run_id = native.submit()
        # This is a real queued run launched through the repository's gRPC launcher.
        before = next(item for item in native.records() if item['run_id'] == run_id)
        assert before['status'] == 'QUEUED'
        assert before['started_at'] is None
        result = native.wait(run_id)
        assert result['status'] == 'SUCCESS', result
        assert result['created_at'] <= result['started_at'] <= result['ended_at']
        assert 0 <= result['queue_seconds'] <= 60
        assert result['job_name'] == MAINTENANCE_JOB
        instance = native.instance
        assert instance is not None
        # A completed run's leaked claim is releasable; another pending run is not.
        pending = native.submit()
        storage = instance.event_log_storage
        storage.set_concurrency_slots('steady_state_done', 1)
        storage.set_concurrency_slots('steady_state_pending', 1)
        storage.claim_concurrency_slot('steady_state_done', run_id, 'completed-step')
        storage.claim_concurrency_slot('steady_state_pending', pending, 'pending-step')
        completed_claims = sum(
            1 for key in storage.get_concurrency_keys()
            for item in storage.get_concurrency_info(key).pending_steps
            if item.run_id == run_id
        )
        assert completed_claims >= 1
        assert reconcile_stale_concurrency_claims(instance) == 1  # one terminal owner, not step count
        assert storage.get_concurrency_info('steady_state_done').pending_steps == []
        assert [item.run_id for item in storage.get_concurrency_info('steady_state_pending').pending_steps] == [pending]
        assert native.wait(pending)['status'] == 'SUCCESS'

    # These are controlled observation-shape fixtures, not a claim that seven
    # production daemons or publication are healthy. The maintenance result is real.
    sha = 'a' * 40
    after = datetime.now(UTC)
    document = {
        'code_sha': sha, 'daemons_healthy': True,
        'maintenance': {'runId': run_id, 'jobName': MAINTENANCE_JOB, 'status': result['status'],
                        'creationTime': result['created_at']},
        'sources': {'source': {'components_complete': True, 'files_verified': True,
                              'build_completed_at': after.isoformat(),
                              'publication_completed_at': after.isoformat(),
                              'new_interval_end': after.isoformat(),
                              'published_end': after.isoformat()}},
    }
    arguments = dict(expected_sha=sha, maintenance_run_id=run_id, ready_at=ready_at, sources=('source',))
    assert verify_observation(document, **arguments) == ()
    launched_only = deepcopy(document)
    launched_only['maintenance']['status'] = 'STARTED'
    assert any('not SUCCESS' in reason for reason in verify_observation(launched_only, **arguments))
    wrong_run = deepcopy(document)
    wrong_run['maintenance']['runId'] = pending
    assert any('another run' in reason for reason in verify_observation(wrong_run, **arguments))
    old = deepcopy(document)
    old['sources']['source']['publication_completed_at'] = (ready_at - timedelta(minutes=1)).isoformat()
    assert any('predates' in reason for reason in verify_observation(old, **arguments))
    elapsed = [0.0]
    def clock() -> float:
        return elapsed[0]
    def advance(seconds: float) -> None:
        elapsed[0] += seconds
    failed = wait_for_deployment(
        lambda deadline: launched_only, **arguments, timeout_seconds=30, clock=clock, sleep=advance,
    )
    assert failed['verdict'] == 'FAIL' and failed['elapsed_seconds'] == 30
