from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import time

import pytest
import yaml
from dagster import DagsterRunStatus, DagsterEvent, RunsFilter
from dagster._core.launcher.base import LaunchRunContext, WorkerStatus
from dagster._core.launcher.default_run_launcher import DefaultRunLauncher
from dagster._core.run_coordinator.base import SubmitRunContext
from dagster._core.storage.tags import GRPC_INFO_TAG
from dagster._core.test_utils import create_run_for_test as _create_run, instance_for_test
from dagster._core.remote_origin import (
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
    ManagedGrpcPythonEnvCodeLocationOrigin,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon
from dagster._grpc.types import GetCurrentRunsResult
from dagster._serdes import serialize_value

from origo.orchestration.policy import (
    CLAIM_TAG,
    IDENTITY_TAG,
    REDUNDANT_TAG,
    WORKER_TAG,
    advance_frontier,
    execution_tags,
)
from origo.orchestration.recovery import recover_queue, recover_retired_workers

ROOT = Path(__file__).resolve().parents[2]


def create_run_for_test(instance, **kwargs):
    name = kwargs.setdefault('job_name', 'refresh_binance_spot_depth20_data_source_job')
    kwargs['remote_job_origin'] = RemoteJobOrigin(
        RemoteRepositoryOrigin(
            ManagedGrpcPythonEnvCodeLocationOrigin(
                LoadableTargetOrigin(module_name='origo.definitions'), location_name='origo'
            ),
            '__repository__',
        ),
        name,
    )
    return _create_run(instance, **kwargs)


@pytest.fixture
def instance(tmp_path):
    config = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
    width = 10
    config['run_coordinator']['config']['max_concurrent_runs'] = width + 9
    for limit in config['run_coordinator']['config']['tag_concurrency_limits']:
        if limit.get('value') == 'backfill':
            limit['limit'] = width
    with instance_for_test(
        temp_dir=str(tmp_path),
        overrides={
            key: config[key] for key in ('run_coordinator', 'run_launcher', 'run_monitoring')
        },
    ) as value:
        yield value


def submit(
    instance,
    *,
    job='refresh_binance_spot_depth20_data_source_job',
    day='2017-08-17',
    config=None,
    tags=None,
):
    run = create_run_for_test(
        instance,
        job_name=job,
        run_config=config or {},
        tags={
            'dagster/partition': day,
            **(tags or {}),
        },
    )
    return instance.run_coordinator.submit_run(SubmitRunContext(run, None))


def test_duplicate_admission_preserves_distinct_source_work(instance):
    with ThreadPoolExecutor(max_workers=8) as pool:
        runs = list(
            pool.map(lambda n: submit(instance, tags={'dagster/run_key': str(n)}), range(16))
        )
    assert sum(r.status == DagsterRunStatus.QUEUED for r in runs) == 1
    assert sum(r.status == DagsterRunStatus.CANCELED for r in runs) == 15
    assert submit(instance, day='2017-08-18').status == DagsterRunStatus.QUEUED
    assert (
        submit(
            instance,
            config={'ops': {'refresh': {'config': {'source_partition_key': '2017-08-18'}}}},
        ).status
        == DagsterRunStatus.QUEUED
    )
    assert (
        submit(instance, tags={'origo_source_partition': '2026-09-17T00:40:00Z'}).status
        == DagsterRunStatus.QUEUED
    )
    assert (
        submit(instance, tags={'origo_source_state_token': 'updated'}).status
        == DagsterRunStatus.QUEUED
    )


def test_each_native_backfill_keeps_its_receipts(instance):
    first = submit(
        instance,
        job='backfill_binance_spot_trades_source_job',
        tags={'dagster/backfill': 'lspzlemm'},
    )
    second = submit(
        instance,
        job='backfill_binance_spot_trades_source_job',
        tags={'dagster/backfill': 'gxioueux'},
    )
    assert first.status == second.status == DagsterRunStatus.QUEUED
    assert first.tags[IDENTITY_TAG] != second.tags[IDENTITY_TAG]


def test_one_pending_refresh_survives_while_a_worker_is_active(instance):
    first = submit(instance, job='build_depth_snapshot_store_arrow_job', day='depth20_snapshots')
    instance.report_dagster_event(
        DagsterEvent('PIPELINE_START', first.job_name), run_id=first.run_id
    )
    second = submit(instance, job=first.job_name, day='depth20_snapshots')
    third = submit(instance, job=first.job_name, day='depth20_snapshots')
    assert second.status == DagsterRunStatus.QUEUED
    assert third.status == DagsterRunStatus.CANCELED
    assert instance.get_run_by_id(first.run_id).status == DagsterRunStatus.STARTED


def test_native_queue_reserves_both_workloads_and_contains_one_noisy_job(tmp_path, monkeypatch):
    for width in (10, 31):
        config = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
        config['run_coordinator']['config']['max_concurrent_runs'] = width + 9
        for limit in config['run_coordinator']['config']['tag_concurrency_limits']:
            if limit.get('value') == 'backfill':
                limit['limit'] = width
        home = tmp_path / str(width)
        home.mkdir()
        with instance_for_test(temp_dir=str(home), overrides={
            key: config[key] for key in ('run_coordinator', 'run_launcher', 'run_monitoring')
        }) as instance:
            _assert_native_queue_capacity(instance, width)
    _assert_book_native_queue_progress(tmp_path / 'native-book', monkeypatch)


def _assert_book_native_queue_progress(home, monkeypatch):
    import logging
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from dagster import AssetKey, BackfillPolicy
    from dagster._core.execution import job_backfill
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
    from dagster._core.remote_origin import RemotePartitionSetOrigin

    from origo.orchestration.policy import ACTIVE, BOOK_BACKFILL_TAG
    from .test_revisioned_source_framework_backfill import _repository_origin

    config = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
    overrides = {key: config[key] for key in ('run_coordinator', 'run_launcher', 'run_monitoring')}
    assert overrides['run_coordinator']['config']['max_concurrent_runs'] == 40
    selected = [(datetime(2026, 9, 1, tzinfo=UTC) + timedelta(hours=n)).strftime('%Y-%m-%dT%HZ')
                for n in range(48)]
    parent_id = 'native-book-restart'
    home.mkdir()
    instance = None

    # RPC execution is controlled; admission, launch claims, events, native checkpoint
    # and durable instance state below are the actual Dagster implementations.
    def launch(self, context):
        instance.report_dagster_event(DagsterEvent('PIPELINE_START', context.dagster_run.job_name),
                                      run_id=context.dagster_run.run_id)

    monkeypatch.setattr(DefaultRunLauncher, 'launch_run', launch)
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)

    def dequeue():
        for run in daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time()):
            assert daemon._dequeue_run(instance, None, run, instance.get_concurrency_config(), time.time())

    metadata = SimpleNamespace(backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
                               has_partitions_definition=lambda: False)
    monkeypatch.setattr(job_backfill, '_get_partition_set', lambda workspace, parent: metadata)

    def submit_chunk(current, workspace, parent, names, executor=None):
        for name in names:
            yield submit(current, job='backfill_binance_spot_book_source_job', day=name,
                          tags={'dagster/backfill': parent.backfill_id}).run_id
        dequeue()

    monkeypatch.setattr(job_backfill, 'submit_backfill_runs', submit_chunk)
    chunk = job_backfill._get_partitions_chunk
    visits = [0]

    class ControllerRestart(RuntimeError):
        pass

    def stop_after_checkpoint(*args):
        visits[0] += 1
        if visits[0] == 2:
            raise ControllerRestart('Controller stops after its persisted 25-partition checkpoint.')
        return chunk(*args)

    monkeypatch.setattr(job_backfill, '_get_partitions_chunk', stop_after_checkpoint)
    with instance_for_test(temp_dir=str(home), overrides=overrides) as instance:
        for market in ('spot', 'perp'):
            canonical = f'refresh_binance_{market}_book_canonical_source_job'
            asset = AssetKey(f'build_binance_{market}_book_canonical_revision_origo')
            for job, tags, assets in (
                (f'backfill_binance_{market}_book_source_job', {}, None),
                (canonical, {'dagster/backfill': 'native-shape'}, None),
                ('__ASSET_JOB', {'dagster/backfill': 'native-shape'}, frozenset({asset})),
                (canonical, {'dagster/asset_partition_range_start': selected[0],
                             'dagster/asset_partition_range_end': selected[0]}, None),
            ):
                run = create_run_for_test(instance, job_name=job, tags=tags,
                                           asset_selection=assets, status=DagsterRunStatus.CANCELED)
                assert execution_tags(run)[BOOK_BACKFILL_TAG] == 'true'
                assert execution_tags(run)['origo/workload'] == 'backfill'
            for job in (canonical, f'audit_binance_{market}_book_source_job',
                        f'repair_binance_{market}_book_source_job'):
                run = create_run_for_test(instance, job_name=job, tags={'dagster/partition': selected[0]},
                                           status=DagsterRunStatus.CANCELED)
                assert BOOK_BACKFILL_TAG not in execution_tags(run)
        legacy = create_run_for_test(instance, job_name='backfill_binance_spot_book_source_job',
                                      tags={'dagster/partition': '2026-08-31T23Z'},
                                      status=DagsterRunStatus.STARTED)
        for n in range(8):
            create_run_for_test(instance, job_name='backfill_binance_perp_book_source_job',
                                tags={'dagster/partition': selected[n], 'dagster/backfill': 'perp-control'},
                                status=DagsterRunStatus.QUEUED)
        recover_queue(instance)
        assert instance.get_run_by_id(legacy.run_id).tags[BOOK_BACKFILL_TAG] == 'true'
        for n in range(15):
            submit(instance, job='backfill_binance_spot_trades_source_job', day=f'2026-08-{n + 1:02d}')
        for job in ('refresh_binance_spot_book_canonical_source_job',
                    'refresh_binance_perp_book_canonical_source_job',
                    'audit_binance_spot_book_source_job', 'audit_binance_perp_book_source_job',
                    'refresh_binance_spot_trades_canonical_source_job',
                    'refresh_binance_perp_trades_canonical_source_job',
                    'refresh_binance_spot_aggtrades_canonical_source_job',
                    'refresh_binance_perp_aggtrades_canonical_source_job'):
            submit(instance, job=job, day='2026-10-18T07Z')
        origin = RemotePartitionSetOrigin(_repository_origin(), 'backfill_binance_spot_book_source_job_partition_set')
        instance.add_backfill(PartitionBackfill(
            backfill_id=parent_id, status=BulkActionStatus.REQUESTED, from_failure=False,
            tags={}, backfill_timestamp=time.time(), partition_names=selected, partition_set_origin=origin,
        ))
        with pytest.raises(ControllerRestart):
            job_backfill.execute_job_backfill_iteration(instance.get_backfill(parent_id),
                logging.getLogger(__name__), SimpleNamespace(create_request_context=lambda: None), None, instance)
        parent = instance.get_backfill(parent_id)
        assert list(parent.partition_names) == selected and parent.last_submitted_partition_name == selected[24]
        assert len(instance.get_runs(RunsFilter(tags={'dagster/backfill': parent_id}))) == 25
        active = instance.get_runs(RunsFilter(statuses=ACTIVE))
        books = [run for run in active if run.tags.get(BOOK_BACKFILL_TAG) == 'true']
        assert len(books) == 16 and {'spot', 'perp'} == {
            'perp' if 'perp' in run.job_name else 'spot' for run in books}
        assert sum(run.tags['origo/workload'] == 'backfill' for run in active) == 31
        assert sum(run.tags['origo/workload'] == 'routine' for run in active) == 8

    monkeypatch.setattr(job_backfill, '_get_partitions_chunk', chunk)
    with instance_for_test(temp_dir=str(home), overrides=overrides) as instance:
        recover_queue(instance)
        parent = instance.get_backfill(parent_id)
        assert list(parent.partition_names) == selected and parent.last_submitted_partition_name == selected[24]
        job_backfill.execute_job_backfill_iteration(parent, logging.getLogger(__name__),
            SimpleNamespace(create_request_context=lambda: None), None, instance)
        runs = instance.get_runs(RunsFilter(tags={'dagster/backfill': parent_id}))
        assert len(runs) == 48 and {run.tags['dagster/partition'] for run in runs} == set(selected)
        assert instance.get_backfill(parent_id).last_submitted_partition_name == selected[-1]
        waiting = {run.run_id for run in runs if run.status == DagsterRunStatus.QUEUED}
        instance.report_run_success(instance.get_run_by_id(legacy.run_id))
        dequeue()
        assert any(instance.get_run_by_id(identity).status == DagsterRunStatus.STARTED for identity in waiting)
        assert len(instance.get_runs(RunsFilter(statuses=ACTIVE, tags={BOOK_BACKFILL_TAG: 'true'}))) == 16
        assert list(instance.get_backfill(parent_id).partition_names) == selected


def _assert_native_queue_capacity(instance, width):
    for n in range(width + 5):
        submit(instance, job='backfill_binance_spot_trades_source_job', day=f'2020-{n // 28 + 1:02d}-{n % 28 + 1:02d}')
    for job in (
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'publish_binance_spot_trades_mount_job',
        'refresh_binance_spot_trades_provisional_source_job',
    ):
        for day in ('2017-08-17', '2017-08-18', '2017-08-19'):
            submit(instance, job=job, day=day)
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    assert len(runs) == width + 8
    assert sum(r.tags['origo/workload'] == 'backfill' for r in runs) == width
    assert sum(r.tags['origo/workload'] == 'routine' for r in runs) == 8
    assert runs[0].tags['origo/workload'] == 'routine'
    for job in {r.job_name for r in runs if r.tags['origo/workload'] == 'routine'}:
        assert sum(r.job_name == job for r in runs) == 2


def serve(instance, runs):
    """Launch accounting as the run launcher records it, then take the runs off the queue."""
    for run in runs:
        advance_frontier(instance, instance.get_run_by_id(run.run_id))
        instance.report_run_canceled(run)


def test_a_long_backfill_never_holds_another_sources_backfill_back(instance):
    for n in range(20):
        submit(
            instance,
            job='backfill_binance_spot_trades_source_job',
            day=f'2020-03-{n + 1:02d}',
            tags={'dagster/backfill': 'spotbulk'},
        )
    for n in range(6):
        submit(
            instance,
            job='backfill_binance_perp_trades_source_job',
            day=f'2020-03-{n + 1:02d}',
            tags={'dagster/backfill': 'perpbulk'},
        )
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    for _ in range(2):
        runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
        # The later backfill takes turns in the shared lane instead of queueing behind 20 runs.
        assert len(runs) == 10
        assert sum(run.tags['dagster/backfill'] == 'perpbulk' for run in runs) == 5
        # Deployment recovery keeps the places admission gave.
        recover_queue(instance)


def test_a_late_backfill_joins_the_lane_at_its_current_place(instance):
    spot = [
        submit(
            instance,
            job='backfill_binance_spot_trades_source_job',
            day=f'2020-{n // 28 + 3:02d}-{n % 28 + 1:02d}',
            tags={'dagster/backfill': 'spotbulk'},
        )
        for n in range(40)
    ]
    # The head of the long backfill has already been served.
    serve(instance, spot[:25])
    for n in range(20):
        submit(
            instance,
            job='backfill_binance_perp_trades_source_job',
            day=f'2020-03-{n + 1:02d}',
            tags={'dagster/backfill': 'perpbulk'},
        )
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    # The newcomer alternates with the older backfill's remaining queue instead of taking the lane.
    assert len(runs) == 10
    assert sum(run.tags['dagster/backfill'] == 'spotbulk' for run in runs) == 5


def test_a_new_backfill_joins_at_the_served_frontier(instance):
    def backfill(share, job, count):
        return [
            submit(instance, job=job, day=f'2021-{n // 28 + 1:02d}-{n % 28 + 1:02d}', tags={'dagster/backfill': share})
            for n in range(count)
        ]

    older = backfill('olderbulk', 'backfill_binance_spot_trades_source_job', 30)
    behind = backfill('behindbulk', 'backfill_binance_perp_trades_source_job', 20)
    # The older backfill was served through place 19 while the other one waited at places 0-19.
    serve(instance, older[:20])
    newer = backfill('newbulk', 'backfill_binance_perp_aggtrades_source_job', 10)
    places = [-int(instance.get_run_by_id(run.run_id).tags['dagster/priority']) for run in newer]
    assert places == list(range(20, 30))
    serve(instance, behind)
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    # The newcomer starts at served service, so the older backfill keeps half the lane.
    assert sum(run.tags['dagster/backfill'] == 'olderbulk' for run in runs) == 5


def test_a_stream_of_new_backfills_cannot_starve_an_older_one(instance):
    older = [
        submit(
            instance,
            job='backfill_binance_spot_trades_source_job',
            day=f'2020-03-{n + 1:02d}',
            tags={'dagster/backfill': 'olderbulk'},
        )
        for n in range(20)
    ]
    serve(instance, older[:10])
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    for wave in range(3):
        for n in range(4):
            submit(
                instance,
                job='backfill_binance_perp_trades_source_job',
                day=f'2022-{wave + 1:02d}-{n + 1:02d}',
                tags={'dagster/backfill': f'single{wave}{n}'},
            )
        runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
        # Each wave of one-run backfills queues behind the older backfill's next place.
        assert runs[0].tags['dagster/backfill'] == 'olderbulk'
        serve(instance, runs[:5])


def test_restarted_recovery_continues_after_places_it_kept(instance):
    legacy = [
        create_run_for_test(
            instance,
            job_name='backfill_binance_spot_trades_source_job',
            status=DagsterRunStatus.QUEUED,
            tags={'dagster/backfill': 'legacybulk', 'dagster/partition': f'2020-03-{n + 1:02d}'},
        )
        for n in range(6)
    ]
    # An interrupted recovery placed the first three before it stopped.
    for place, run in enumerate(legacy[:3]):
        instance.add_run_tags(run.run_id, execution_tags(run, place))
    recover_queue(instance)
    places = [-int(instance.get_run_by_id(run.run_id).tags['dagster/priority']) for run in legacy]
    assert places == [0, 1, 2, 3, 4, 5]


def test_maintenance_lane_dequeues_while_routine_and_backfill_lanes_are_full(instance):
    probe = create_run_for_test(instance, job_name='maintain_operational_metadata_job')
    tags = execution_tags(probe)
    assert (tags['origo/workload'], tags['dagster/priority']) == ('maintenance', '300')
    assert 'origo/routine_job' not in tags
    for index in range(10):
        create_run_for_test(
            instance,
            job_name='backfill_binance_spot_trades_source_job',
            status=DagsterRunStatus.STARTED,
            tags={'origo/workload': 'backfill', IDENTITY_TAG: f'backfill-{index}'},
        )
    for index in range(8):
        create_run_for_test(
            instance,
            job_name=f'routine_job_{index}',
            status=DagsterRunStatus.STARTED,
            tags={'origo/workload': 'routine', IDENTITY_TAG: f'routine-{index}'},
        )
    routine = submit(instance, job='refresh_binance_spot_depth20_data_source_job', day='2026-09-17')
    first = submit(instance, job='maintain_operational_metadata_job', day='2026-09-17T14:00:00Z')
    second = submit(instance, job='maintain_operational_metadata_job', day='2026-09-17T14:10:00Z')
    assert routine.status == first.status == second.status == DagsterRunStatus.QUEUED
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    assert [run.run_id for run in runs] == [first.run_id]
    instance.report_dagster_event(
        DagsterEvent('PIPELINE_START', first.job_name), run_id=first.run_id
    )
    assert instance.get_run_by_id(first.run_id).status == DagsterRunStatus.STARTED
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    assert runs == []


def test_recovery_preserves_claimed_and_distinct_work(instance):
    retained = create_run_for_test(
        instance,
        job_name='refresh_binance_spot_depth20_data_source_job',
        status=DagsterRunStatus.QUEUED,
        tags={'dagster/partition': '2017-08-17'},
    )
    duplicate = create_run_for_test(
        instance, job_name=retained.job_name, status=DagsterRunStatus.QUEUED, tags=retained.tags
    )
    unique = create_run_for_test(
        instance,
        job_name=retained.job_name,
        status=DagsterRunStatus.QUEUED,
        tags={'dagster/partition': '2017-08-18'},
    )
    claimed = create_run_for_test(
        instance, job_name=retained.job_name, status=DagsterRunStatus.QUEUED, tags=retained.tags
    )
    instance.add_run_tags(claimed.run_id, {CLAIM_TAG: claimed.run_id})
    result = recover_queue(instance)
    assert result['redundant_canceled'] == 1
    assert instance.get_run_by_id(duplicate.run_id).status == DagsterRunStatus.CANCELED
    for run in (retained, unique, claimed):
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.QUEUED
    assert recover_queue(instance)['redundant_canceled'] == 0


def test_recovery_cancellation_wins_a_dequeuer_race(instance, monkeypatch):
    run = create_run_for_test(instance, status=DagsterRunStatus.STARTING, tags={})
    instance.add_run_tags(run.run_id, {REDUNDANT_TAG: run.run_id})
    launches = []
    monkeypatch.setattr(
        DefaultRunLauncher,
        'launch_run',
        lambda self, context: launches.append(context.dagster_run.run_id),
    )
    instance.run_launcher.launch_run(LaunchRunContext(run, None))
    assert not launches
    assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.CANCELED


def test_worker_health_requires_positive_evidence(instance, monkeypatch):
    from origo.orchestration import launcher
    from dagster._core.errors import DagsterUserCodeUnreachableError

    run = create_run_for_test(
        instance,
        status=DagsterRunStatus.STARTED,
        tags={GRPC_INFO_TAG: json.dumps({'host': 'localhost', 'socket': '/tmp/worker'})},
    )
    monkeypatch.setattr(
        launcher.DagsterGrpcClient,
        'get_current_runs',
        lambda self: serialize_value(
            GetCurrentRunsResult(current_runs=[run.run_id], serializable_error_info=None)
        ),
    )
    assert instance.run_launcher.check_run_worker_health(run).status == WorkerStatus.RUNNING
    monkeypatch.setattr(
        launcher.DagsterGrpcClient,
        'get_current_runs',
        lambda self: serialize_value(
            GetCurrentRunsResult(current_runs=[], serializable_error_info=None)
        ),
    )
    assert instance.run_launcher.check_run_worker_health(run).status == WorkerStatus.NOT_FOUND

    def unreachable(self):
        raise DagsterUserCodeUnreachableError('transport unavailable')

    monkeypatch.setattr(launcher.DagsterGrpcClient, 'get_current_runs', unreachable)
    assert instance.run_launcher.check_run_worker_health(run).status == WorkerStatus.UNKNOWN


def test_retirement_preserves_current_and_uncertain_workers(instance):
    dead = create_run_for_test(
        instance, status=DagsterRunStatus.STARTED, tags={WORKER_TAG: 'retired-container'}
    )
    living = create_run_for_test(
        instance, status=DagsterRunStatus.STARTED, tags={WORKER_TAG: 'current-container'}
    )
    unknown = create_run_for_test(instance, status=DagsterRunStatus.STARTED)
    assert recover_retired_workers(instance, {'retired-container'}) == 1
    assert instance.get_run_by_id(dead.run_id).status == DagsterRunStatus.FAILURE
    for run in (living, unknown):
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.STARTED


def test_legacy_retirement_only_covers_old_local_grpc_workers(instance):
    local = create_run_for_test(
        instance,
        status=DagsterRunStatus.STARTED,
        tags={GRPC_INFO_TAG: json.dumps({'host': 'localhost', 'socket': '/tmp/retired-worker'})},
    )
    remote = create_run_for_test(
        instance,
        status=DagsterRunStatus.STARTED,
        tags={GRPC_INFO_TAG: json.dumps({'host': 'another-server', 'port': 4000})},
    )
    assert recover_retired_workers(instance, set(), legacy_before=time.time() + 1) == 1
    assert instance.get_run_by_id(local.run_id).status == DagsterRunStatus.FAILURE
    assert instance.get_run_by_id(remote.run_id).status == DagsterRunStatus.STARTED


def test_runtime_bounds_preserve_daily_retry_envelope(instance):
    short = create_run_for_test(instance, job_name='refresh_binance_spot_depth20_data_source_job')
    daily = create_run_for_test(instance, job_name='refresh_binance_spot_trades_canonical_source_job')
    bulk = create_run_for_test(
        instance,
        job_name='backfill_binance_spot_trades_source_job',
        tags={'dagster/max_runtime': '0'},
    )
    assert execution_tags(short)['dagster/max_runtime'] == '1800'
    assert execution_tags(daily)['dagster/max_runtime'] == '93600'
    assert execution_tags(bulk)['dagster/max_runtime'] == '93600'


def test_benchmark_help_is_available():
    import os
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, 'tools/benchmark_orchestration.py', '--help'],
        cwd=ROOT,
        env={**os.environ, 'PYTHONPATH': str(ROOT)},
        capture_output=True,
        text=True,
        check=True,
    )
    assert '--archives' in result.stdout


def test_depth_chunk_identity_keeps_both_feeds_and_minutes(instance):
    from origo.orchestration.policy import outstanding_configs

    job = 'build_depth_snapshot_store_arrow_job'

    def config(minute):
        return {
            'ops': {
                'build_depth_snapshot_store_arrow': {'config': {'source_partition_key': minute}}
            }
        }

    submit(instance, job=job, day='depth20_snapshots', config=config('2026-09-17T00:40:00+0000'))
    submit(instance, job=job, day='depth200_snapshots', config=config('2026-09-17T00:41:00+0000'))
    assert outstanding_configs(
        instance,
        job,
        'build_depth_snapshot_store_arrow',
        'source_partition_key',
        partition='depth20_snapshots',
    ) == {'2026-09-17T00:40:00+0000'}
    assert outstanding_configs(
        instance,
        job,
        'build_depth_snapshot_store_arrow',
        'source_partition_key',
        partition='depth200_snapshots',
    ) == {'2026-09-17T00:41:00+0000'}


def test_daily_publication_precedes_minute_catchup(instance):
    for job in (
        'refresh_binance_spot_depth20_data_source_job',
        'refresh_binance_spot_depth200_data_source_job',
        'refresh_binance_spot_trades_provisional_source_job',
        'build_depth_snapshot_store_arrow_job',
    ):
        for minute in range(15):
            submit(instance, job=job, day=f'2026-09-17T00:{minute:02d}:00+0000')
    daily = submit(instance, job='refresh_binance_spot_trades_canonical_source_job', day='2026-09-16')
    feed = submit(instance, job='publish_btc_briefing_feed_job', day='2026-09-16')
    daemon = QueuedRunCoordinatorDaemon(interval_seconds=1)
    runs = daemon._get_runs_to_dequeue(instance, instance.get_concurrency_config(), time.time())
    assert [r.run_id for r in runs[:2]] == [daily.run_id, feed.run_id]


def test_startup_recovery_completes_outside_captured_logging(origo_test_env, tmp_path, monkeypatch):
    """Both deployment entry points cancel redundant queued runs before any captured op
    exists. Cancelling a run whose event shard is uninitialised logs through Alembic
    under the storage lock; with root python-log capture that log re-enters the same
    storage, and on the pre-fix shape both commands hang forever."""
    import os
    import subprocess
    import sys

    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    home = tmp_path / 'dagster-home'
    home.mkdir()
    storage = {'config': {'base_dir': str(home / 'storage')}}
    overrides = {
        'run_storage': {
            'module': 'origo.maintenance.run_storage',
            'class': 'OrigoSqliteRunStorage',
            **storage,
        },
        'event_log_storage': {
            'module': 'origo.maintenance.event_storage',
            'class': 'OrigoSqliteEventLogStorage',
            **storage,
        },
        'python_logs': {'managed_python_loggers': [''], 'python_log_level': 'INFO'},
    }
    commands = (
        ('origo.sources.bootstrap', 'prepare_revisioned_sources_job', '2017-08-17', []),
        (
            'origo.orchestration.recovery',
            'recover_orchestration_job',
            '2017-08-18',
            ['--deadline-seconds', '30'],
        ),
    )
    with instance_for_test(temp_dir=str(home), overrides=overrides) as instance:
        env = {**os.environ, 'DAGSTER_HOME': str(home), 'PYTHONPATH': str(ROOT)}
        for module, job_name, day, arguments in commands:
            kept, redundant = (
                create_run_for_test(
                    instance, status=DagsterRunStatus.QUEUED, tags={'dagster/partition': day}
                )
                for _ in range(2)
            )
            completed = subprocess.run(
                [sys.executable, '-m', module, *arguments],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert completed.returncode == 0, completed.stdout + completed.stderr
            assert instance.get_run_by_id(kept.run_id).status == DagsterRunStatus.QUEUED
            assert instance.get_run_by_id(redundant.run_id).status == DagsterRunStatus.CANCELED
            assert any(
                entry.dagster_event is not None
                and entry.dagster_event.event_type_value == 'PIPELINE_CANCELED'
                for entry in instance.all_logs(redundant.run_id)
            )
            recorded = instance.get_runs(RunsFilter(job_name=job_name))
            assert recorded and recorded[0].status == DagsterRunStatus.SUCCESS
            messages = [entry.user_message for entry in instance.all_logs(recorded[0].run_id)]
            assert any("'redundant_canceled': 1" in message for message in messages)


def _claim(instance, pool, run_id, step_key='maintain_operational_metadata'):
    storage = instance.event_log_storage
    assert storage.supports_global_concurrency_limits
    storage.set_concurrency_slots(pool, 1)
    storage.claim_concurrency_slot(pool, run_id, step_key)
    return storage


def test_reconcile_frees_only_dead_run_pool_claims(instance):
    from origo.orchestration.policy import reconcile_stale_concurrency_claims

    dead = create_run_for_test(
        instance,
        job_name='maintain_operational_metadata_job',
        status=DagsterRunStatus.STARTED,
    )
    live = create_run_for_test(
        instance,
        job_name='maintain_operational_metadata_job',
        status=DagsterRunStatus.STARTED,
    )
    storage = _claim(instance, 'dead_pool', dead.run_id)
    _claim(instance, 'live_pool', live.run_id)
    instance.report_run_failed(dead)
    assert reconcile_stale_concurrency_claims(instance) == 1
    assert storage.get_concurrency_info('dead_pool').pending_steps == []
    assert [step.run_id for step in storage.get_concurrency_info('live_pool').pending_steps] == [
        live.run_id
    ]


def test_maintenance_schedule_heals_stale_claim_then_gates(instance):
    from types import SimpleNamespace

    from origo.maintenance import dagster_metadata

    dead = create_run_for_test(
        instance,
        job_name='maintain_operational_metadata_job',
        status=DagsterRunStatus.STARTED,
    )
    storage = _claim(instance, 'operational_metadata', dead.run_id)
    instance.report_run_failed(dead)
    context = SimpleNamespace(instance=instance)
    assert dagster_metadata._should_execute(context) is True
    assert storage.get_concurrency_info('operational_metadata').pending_steps == []
    create_run_for_test(
        instance,
        job_name='maintain_operational_metadata_job',
        status=DagsterRunStatus.QUEUED,
    )
    assert dagster_metadata._should_execute(context) is False


def test_recover_queue_reports_freed_stale_slots(instance):
    dead = create_run_for_test(
        instance,
        job_name='maintain_operational_metadata_job',
        status=DagsterRunStatus.STARTED,
    )
    storage = _claim(instance, 'operational_metadata', dead.run_id)
    instance.report_run_failed(dead)
    assert recover_queue(instance)['stale_slots_freed'] == 1
    assert storage.get_concurrency_info('operational_metadata').pending_steps == []
