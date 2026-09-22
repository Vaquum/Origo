"""Native queued execution and semantic deployment outcome regression (M11)."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

from origo.orchestration.policy import reconcile_stale_concurrency_claims
from origo.steady_state.deployment import MAINTENANCE_JOB, verify_observation, wait_for_deployment
from origo.steady_state.trial_native import NativeMaintenance


def test_maintenance_resource_and_deploy_outcomes_are_observed(tmp_path: Path) -> None:
    ready_at = datetime.now(UTC) - timedelta(seconds=1)
    with NativeMaintenance(tmp_path / 'native-maintenance') as native:
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
        assert reconcile_stale_concurrency_claims(instance) == 1
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
