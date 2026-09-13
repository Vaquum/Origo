import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterInstance,
    DagsterRunStatus,
    DataVersion,
    MaterializeResult,
    RunsFilter,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from dagster._core.storage.runs.sqlite.sqlite_run_storage import SqliteRunStorage

from origo.maintenance import retention
from origo.maintenance.backup import BackupReceipt, require_backup
from origo.maintenance.event_storage import OrigoSqliteEventLogStorage
from origo.maintenance.protocol import (
    Candidate,
    Journal,
    OperationalMetadataMaintenanceConfig,
    manifest_sha256,
    policy_sha256,
    save_journal,
)
from origo.maintenance.retention import artifact_bytes, protection, reclaim, scan_batch
from origo.maintenance.run_storage import OrigoSqliteRunStorage
from origo.maintenance.sqlite import Layout, connection, maintenance_lock, refresh_statistics

ROOT = Path(__file__).resolve().parents[2]
ARCHIVES = ROOT / 'tests/fixtures/binance/spot/daily/trades/revisioned'
PARTITIONS = StaticPartitionsDefinition(['2017-08-17', '2020-01-01'])
POLICY = OperationalMetadataMaintenanceConfig(metadata_budget_bytes=600 * 1024**3)


@asset(name='metadata_proof_archive', partitions_def=PARTITIONS)
def archive_fact(context: AssetExecutionContext) -> MaterializeResult:
    if context.run.tags.get('proof_fail') == 'true':
        raise RuntimeError('Controlled execution failure before reading the real archive.')
    path = ARCHIVES / f'BTCUSDT-trades-{context.partition_key}.zip'
    payload = path.read_bytes()
    return MaterializeResult(
        data_version=DataVersion(hashlib.sha256(payload).hexdigest()),
        metadata={'archive': path.name, 'bytes': len(payload)},
    )


@pytest.fixture
def metadata_instance(tmp_path: Path) -> Iterator[DagsterInstance]:
    directory = tmp_path / 'instance'
    directory.mkdir()
    with DagsterInstance.local_temp(
        str(directory),
        overrides={
            'run_storage': {
                'module': 'origo.maintenance.run_storage',
                'class': 'OrigoSqliteRunStorage',
                'config': {'base_dir': str(directory / 'runs')},
            },
            'event_log_storage': {
                'module': 'origo.maintenance.event_storage',
                'class': 'OrigoSqliteEventLogStorage',
                'config': {'base_dir': str(directory / 'events')},
            },
        },
    ) as instance:
        yield instance


def execute_archive(
    instance: DagsterInstance, *, tags: dict[str, str] | None = None, partition: str = '2017-08-17'
) -> str:
    result = materialize(
        [archive_fact],
        instance=instance,
        partition_key=partition,
        tags={'.dagster/repository': '__repository__@origo', **(tags or {})},
        raise_on_error=False,
    )
    return result.run_id


def planned(instance: DagsterInstance, run_id: str) -> tuple[Layout, Journal, Candidate]:
    layout = Layout.from_instance(instance)
    record = instance.get_run_records(RunsFilter(run_ids=[run_id]), limit=1)[0]
    assert isinstance(instance.run_storage, OrigoSqliteRunStorage)
    row = Candidate(
        run_id=run_id,
        storage_id=record.storage_id,
        status=record.dagster_run.status.value,
        ended_at=record.end_time or record.update_timestamp.timestamp(),
        allocated_bytes=artifact_bytes(layout, run_id, time.monotonic() + 20),
    )
    journal = Journal(
        instance_id=instance.run_storage.get_run_storage_id(),
        policy_sha256=policy_sha256(POLICY),
        manifest=[row],
        manifest_created_at=time.time(),
    )
    journal.manifest_sha256 = manifest_sha256(journal)
    return layout, journal, row


def test_statistics_restore_actual_job_query_plan(metadata_instance: DagsterInstance) -> None:
    instance = metadata_instance
    for tags in ({}, {}, {'filter': 'yes'}, {'.dagster/repository': 'another_repository'}):
        execute_archive(instance, tags=tags)
    layout = Layout.from_instance(instance)
    refresh_statistics((layout.runs, layout.events, layout.schedules), time.monotonic() + 15, 1)
    upstream = SqliteRunStorage.from_local(str(layout.runs.parent))
    assert isinstance(instance.run_storage, OrigoSqliteRunStorage)
    for tags in (
        {'.dagster/repository': '__repository__@origo'},
        {'.dagster/repository': ['__repository__@origo', 'another_repository'], 'filter': 'yes'},
    ):
        filters = RunsFilter(job_name='__ephemeral_asset_job__', tags=tags)
        for ascending in (False, True):
            original = upstream.get_runs(filters, limit=2, ascending=ascending)
            actual = instance.run_storage.get_runs(filters, limit=2, ascending=ascending)
            assert [row.run_id for row in actual] == [row.run_id for row in original]
            assert instance.run_storage.get_runs_count(filters) == upstream.get_runs_count(filters)
            if actual:
                cursor = actual[-1].run_id
                assert [
                    row.run_id
                    for row in instance.run_storage.get_runs(
                        filters, cursor=cursor, limit=2, ascending=ascending
                    )
                ] == [
                    row.run_id
                    for row in upstream.get_runs(
                        filters, cursor=cursor, limit=2, ascending=ascending
                    )
                ]
    filters = RunsFilter(
        job_name='__ephemeral_asset_job__', tags={'.dagster/repository': '__repository__@origo'}
    )
    query = str(
        instance.run_storage._runs_query(filters, limit=5).compile(
            compile_kwargs={'literal_binds': True}
        )
    )
    with sqlite3.connect(layout.runs) as database:
        plan = '\n'.join(str(row) for row in database.execute('EXPLAIN QUERY PLAN ' + query))
        assert 'idx_runs_by_job' in plan
        assert 'idx_run_tags_run_idx' in plan
        assert 'run_id=?' in plan
        assert 'EXISTS' in query
        assert 'SCAN run_tags' not in plan
        assert database.execute('SELECT count(*) FROM sqlite_stat1').fetchone()[0] > 0


def test_cleanup_preserves_dagit_state_and_recovery(metadata_instance: DagsterInstance) -> None:
    instance = metadata_instance
    run_id = execute_archive(instance)
    key = AssetKey('metadata_proof_archive')
    before = instance.fetch_materializations(key, limit=1).records[0]
    before_status = instance.get_status_by_partition(key, ['2017-08-17'], PARTITIONS)
    instance.delete_run(run_id)
    after = instance.fetch_materializations(key, limit=1).records[0]
    assert after == before
    assert instance.get_status_by_partition(key, ['2017-08-17'], PARTITIONS) == before_status
    assert instance.get_materialized_partitions(key) == {'2017-08-17'}
    assert instance.get_run_by_id(run_id) is None
    assert instance.get_latest_materialization_event(key) == before.event_log_entry


def test_superseded_metadata_compaction(metadata_instance: DagsterInstance) -> None:
    instance = metadata_instance
    first = execute_archive(instance)
    key = AssetKey('metadata_proof_archive')
    instance.delete_run(first)
    second = execute_archive(instance)
    current = instance.fetch_materializations(key, limit=1).records[0]
    assert isinstance(instance.event_log_storage, OrigoSqliteEventLogStorage)
    _, scanned, compacted = instance.event_log_storage.compact_retired_state(
        Layout.from_instance(instance).runs, 0, 500, time.monotonic() + 15, 1
    )
    assert scanned > 0 and compacted > 0
    assert instance.fetch_materializations(key, limit=10).records == [current]
    assert instance.get_run_by_id(second) is not None


def test_retention_cutoffs_and_protected_runs(metadata_instance: DagsterInstance) -> None:
    instance = metadata_instance
    succeeded = execute_archive(instance)
    failed = execute_archive(instance, tags={'proof_fail': 'true'}, partition='2020-01-01')
    layout = Layout.from_instance(instance)
    success_record = instance.get_run_records(RunsFilter(run_ids=[succeeded]), limit=1)[0]
    failure_record = instance.get_run_records(RunsFilter(run_ids=[failed]), limit=1)[0]
    assert failure_record.dagster_run.status == DagsterRunStatus.FAILURE
    now = time.time()
    deadline = time.monotonic() + 15
    assert (
        protection(instance, layout, success_record, POLICY, now + 29 * 86400, deadline)
        == 'retention_window'
    )
    assert protection(instance, layout, success_record, POLICY, now + 31 * 86400, deadline) == ''
    assert (
        protection(instance, layout, failure_record, POLICY, now + 89 * 86400, deadline)
        == 'retention_window'
    )
    assert (
        protection(instance, layout, failure_record, POLICY, now + 91 * 86400, deadline)
        == 'latest_failure_or_verdict'
    )
    execute_archive(
        instance, tags={'dagster/parent_run_id': succeeded, 'dagster/root_run_id': succeeded}
    )
    assert (
        protection(instance, layout, success_record, POLICY, now + 91 * 86400, deadline)
        == 'retry_lineage_reference'
    )


def test_dry_run_reports_reclaimable_bytes(metadata_instance: DagsterInstance) -> None:
    instance = metadata_instance
    run_id = execute_archive(instance)
    layout, journal, _ = planned(instance, run_id)
    rows = scan_batch(
        instance, layout, journal, POLICY, time.time() + 31 * 86400, time.monotonic() + 15
    )
    assert [row.run_id for row in rows if not row.reason] == [run_id]
    assert rows[0].allocated_bytes > 0
    assert instance.get_run_by_id(run_id) is not None and layout.shard(run_id).exists()


def test_catch_up_reclaims_bytes_and_resumes(
    metadata_instance: DagsterInstance, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    instance = metadata_instance
    run_id = execute_archive(instance)
    layout, journal, candidate = planned(instance, run_id)
    future = time.time() + 31 * 86400
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    path = tmp_path / 'journal.json'
    candidate.phase = 'deleting'
    save_journal(path, journal)
    # Exercise the real interruption boundary: Dagster's first deletion committed,
    # but the event-store half and artifact reclamation have not run.
    instance.run_storage.delete_run(run_id)
    reclaimed = reclaim(instance, layout, candidate, journal, path, POLICY, time.monotonic() + 15)
    assert reclaimed > 0 and not layout.shard(run_id).exists()
    retained = instance.get_latest_materialization_event(AssetKey('metadata_proof_archive'))
    assert retained is not None
    with pytest.raises(RuntimeError, match='absent run'):
        instance.handle_new_event(retained)
    assert not layout.shard(run_id).exists()

    assert instance.get_materialized_partitions(AssetKey('metadata_proof_archive')) == {
        '2017-08-17'
    }
    loaded = Journal.model_validate_json(path.read_bytes())
    assert loaded.manifest[0].phase == 'reclaimed'
    assert (
        reclaim(instance, layout, loaded.manifest[0], loaded, path, POLICY, time.monotonic() + 15)
        == 0
    )


def test_live_revalidation_and_backup_gate(
    metadata_instance: DagsterInstance, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    instance = metadata_instance
    run_id = execute_archive(instance)
    layout, journal, candidate = planned(instance, run_id)
    with pytest.raises(RuntimeError, match='verified restore receipt'):
        require_backup(journal, POLICY, time.time())
    now = time.time()
    receipt = BackupReceipt(
        instance_id=journal.instance_id,
        manifest_sha256=journal.manifest_sha256,
        snapshot_id='test:restored-instance',
        restored_home=str(tmp_path),
        verified_at=now,
        expires_at=now + 86400,
        verified_runs=1,
    )
    path = tmp_path / 'receipt.json'
    path.write_text(receipt.model_dump_json())
    apply = OperationalMetadataMaintenanceConfig(
        metadata_budget_bytes=POLICY.metadata_budget_bytes,
        dry_run=False,
        backup_receipt=str(path),
        approved_manifest_sha256=journal.manifest_sha256,
    )
    scan_batch(instance, layout, journal, POLICY, now + 31 * 86400, time.monotonic() + 15)
    from origo.maintenance.worker import directory_bytes

    journal.retained_floor_bytes = (
        directory_bytes(layout, time.monotonic() + 15) - journal.inventory_eligible_bytes
    )
    require_backup(journal, apply, now)
    with pytest.raises(RuntimeError, match='expired'):
        require_backup(journal, apply, now + 2 * 86400)
    instance.add_run_tags(run_id, {'origo_metadata_preserve': 'true'})
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: now + 31 * 86400, monotonic=time.monotonic)
    )
    assert (
        reclaim(
            instance,
            layout,
            candidate,
            journal,
            tmp_path / 'journal.json',
            apply,
            time.monotonic() + 15,
        )
        == 0
    )
    assert candidate.revalidation_reason == 'live_revalidation:operator_preserved'
    assert instance.get_run_by_id(run_id) is not None


def test_lock_and_runtime_bounds(metadata_instance: DagsterInstance, tmp_path: Path) -> None:
    path = tmp_path / 'maintenance.lock'
    with maintenance_lock(path, 0):
        started = time.monotonic()
        with pytest.raises(TimeoutError, match='already running'), maintenance_lock(path, 0.05):
            pytest.fail('Concurrent maintenance entered the lock.')
        assert time.monotonic() - started < 0.5
    with maintenance_lock(path, 0):
        assert path.exists()
    with connection(
        Layout.from_instance(metadata_instance).runs, time.monotonic() + 0.05, 0
    ) as database:
        with pytest.raises(sqlite3.OperationalError, match='interrupted'):
            database.execute(
                'WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n'
            ).fetchone()


def test_maintenance_job_schedule_and_deploy() -> None:
    import yaml

    from origo.definitions import defs
    from origo.maintenance.dagster_metadata import operational_metadata_maintenance_schedule

    job = defs.resolve_job_def('maintain_operational_metadata_job')
    assert job.name == operational_metadata_maintenance_schedule.job_name
    assert operational_metadata_maintenance_schedule.cron_schedule == '17 2 * * *'
    assert operational_metadata_maintenance_schedule.default_status.value == 'RUNNING'
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert (
        'dagster job launch -m origo.definitions -j maintain_operational_metadata_job' in workflow
    )
    for file in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        compose = yaml.safe_load((ROOT / file).read_text())
        for service in ('dagit', 'dagster'):
            environment = compose['services'][service]['environment']
            assert 'ORIGO_METADATA_DRY_RUN=${ORIGO_METADATA_DRY_RUN:-true}' in environment
            assert any(
                value.startswith('ORIGO_OPERATIONAL_METADATA_BUDGET_BYTES=')
                for value in environment
            )
    config = yaml.safe_load((ROOT / 'dagster.yaml').read_text())
    assert config['run_storage']['class'] == 'OrigoSqliteRunStorage'
    assert config['event_log_storage']['class'] == 'OrigoSqliteEventLogStorage'


def _real_minute_bars(path: Path, *, limit: int | None = None) -> None:
    import polars as pl

    frame = pl.read_csv(
        ARCHIVES / 'BTCUSDT-trades-2017-08-17.csv',
        has_header=False,
        new_columns=[
            'trade_id',
            'price',
            'quantity',
            'quote_quantity',
            'timestamp',
            'maker',
            'best_match',
        ],
    )
    if limit is not None:
        frame = frame.head(limit)
    frame = frame.with_columns(
        pl.col('timestamp')
        .cast(pl.Datetime('ms', time_zone='UTC'))
        .dt.truncate('1m')
        .alias('datetime'),
        pl.col('maker').cast(pl.String).str.to_lowercase().eq('true').alias('maker'),
    )
    bars = (
        frame.group_by('datetime')
        .agg(
            pl.col('price').first().alias('open'),
            pl.col('price').max().alias('high'),
            pl.col('price').min().alias('low'),
            pl.col('price').last().alias('close'),
            pl.col('price').mean().alias('mean'),
            pl.col('price').std(ddof=0).alias('std'),
            pl.col('quantity').sum().alias('volume'),
            pl.col('maker').mean().alias('maker_ratio'),
            pl.len().cast(pl.Int64).alias('no_of_trades'),
            pl.col('quote_quantity').first().alias('open_liquidity'),
            pl.col('quote_quantity').max().alias('high_liquidity'),
            pl.col('quote_quantity').min().alias('low_liquidity'),
            pl.col('quote_quantity').last().alias('close_liquidity'),
            pl.col('quote_quantity').sum().alias('liquidity_sum'),
            pl.col('quantity').filter(pl.col('maker')).sum().alias('maker_volume'),
            pl.col('quote_quantity').filter(pl.col('maker')).sum().alias('maker_liquidity'),
        )
        .sort('datetime')
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    bars.write_parquet(path)


def test_redundant_triggers_do_not_create_runs(
    metadata_instance: DagsterInstance, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dagster import DagsterEventType, build_run_status_sensor_context

    from origo.definitions import defs
    from origo.maintenance.arrow_inputs import changed_arrow_requests

    instance = metadata_instance
    parquet = tmp_path / 'parquet'
    path = parquet / 'time/1m/2017/08.parquet'
    _real_minute_bars(path, limit=25)
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(parquet))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    source_id = execute_archive(instance)
    source = instance.get_run_by_id(source_id)
    event = (
        instance.get_records_for_run(source_id, of_type=DagsterEventType.PIPELINE_SUCCESS)
        .records[0]
        .event_log_entry.dagster_event
    )
    assert source is not None and event is not None
    with build_run_status_sensor_context(
        sensor_name='bar_store_source_sensor',
        dagster_run=source,
        dagster_event=event,
        dagster_instance=instance,
    ) as context:
        requests = changed_arrow_requests(context)
        assert len(requests) == 1 and requests[0].partition_key == 'time_1m'
        result = defs.resolve_job_def('build_bar_store_arrow_job').execute_in_process(
            instance=instance, partition_key='time_1m', tags=requests[0].tags
        )
        assert result.success
        assert changed_arrow_requests(context) == []
        instance.delete_run(result.run_id)
        assert changed_arrow_requests(context) == []
        # Atomic publication of additional genuine archive rows closes more intervals.
        temporary = path.with_suffix('.partial')
        _real_minute_bars(temporary)
        temporary.replace(path)
        next_requests = changed_arrow_requests(context)
        assert len(next_requests) == 1 and next_requests[0].run_key != requests[0].run_key


@pytest.fixture(scope='module')
def diagnostic_server(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[str, object, Path]]:
    import socket
    import subprocess
    from uuid import uuid4

    from origo.assets.create_origo_database import ClickHouseSettings, make_clickhouse_client

    data = tmp_path_factory.mktemp('diagnostic-server') / 'data'
    data.mkdir()
    import os

    logs = data.parent / 'logs'
    users = data.parent / 'users'
    logs.mkdir()
    users.mkdir()
    name = 'origo-metadata-tests-' + uuid4().hex[:12]
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    image = subprocess.run(
        ['docker', 'build', '--quiet', '-f', str(ROOT / 'Dockerfile.clickhouse'), str(ROOT)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    subprocess.run(
        [
            'docker',
            'run',
            '--detach',
            '--name',
            name,
            '--user',
            f'{os.getuid()}:{os.getgid()}',
            '--volume',
            f'{logs}:/var/log/clickhouse-server',
            '--volume',
            f'{users}:/etc/clickhouse-server/users.d',
            '--publish',
            f'127.0.0.1:{port}:9000',
            '--volume',
            f'{data}:/var/lib/clickhouse',
            '--env',
            'CLICKHOUSE_PASSWORD=test-password',
            image,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    settings = ClickHouseSettings(
        host='127.0.0.1', port=port, user='default', password='test-password', database='origo'
    )
    client = make_clickhouse_client(settings)
    deadline = time.monotonic() + 60
    try:
        while True:
            try:
                client.execute('SELECT 1')
                break
            except Exception as error:
                if time.monotonic() >= deadline:
                    logs = subprocess.run(
                        ['docker', 'logs', name], capture_output=True, text=True, check=True
                    )
                    error_path = data.parent / 'startup-error.log'
                    subprocess.run(
                        [
                            'docker',
                            'cp',
                            f'{name}:/var/log/clickhouse-server/clickhouse-server.err.log',
                            str(error_path),
                        ],
                        capture_output=True,
                        check=True,
                    )
                    raise RuntimeError(
                        logs.stderr[-2000:] + error_path.read_text()[-6000:]
                    ) from error
                time.sleep(0.2)
        yield name, settings, data
    finally:
        client.disconnect()
        subprocess.run(
            ['docker', 'rm', '--force', name], capture_output=True, text=True, check=True
        )


def _diagnostic_client(server: tuple[str, object, Path]) -> object:
    from origo.assets.create_origo_database import make_clickhouse_client

    return make_clickhouse_client(server[1])


def test_clickhouse_diagnostic_ttl_survives_restart(
    diagnostic_server: tuple[str, object, Path],
) -> None:
    import subprocess
    import xml.etree.ElementTree as ET

    from origo.maintenance.clickhouse import DIAGNOSTIC_LOGS, maintain_diagnostics

    client = _diagnostic_client(diagnostic_server)
    root = ET.parse(ROOT / 'clickhouse-config.xml').getroot()
    for name in DIAGNOSTIC_LOGS:
        assert 'INTERVAL 14 DAY' in root.findtext(f'{name}/ttl', '')
    initial_tables = {
        row[0] for row in client.execute("SELECT name FROM system.tables WHERE database='system'")
    }
    assert 'crash_log' not in initial_tables
    before = maintain_diagnostics(client, POLICY, time.monotonic() + 30)
    assert 'crash_log' in before.tables and not before.errors
    assert client.execute('SELECT count() FROM system.crash_log') == [(0,)]

    assert not before.drift, [
        client.execute(f'SHOW CREATE TABLE system.{name}') for name in before.drift
    ]
    client.disconnect()
    subprocess.run(['docker', 'restart', diagnostic_server[0]], check=True, capture_output=True)
    client = _diagnostic_client(diagnostic_server)
    deadline = time.monotonic() + 30
    while True:
        try:
            client.execute('SELECT 1')
            break
        except Exception as error:
            if time.monotonic() >= deadline:
                raise RuntimeError('Diagnostic server did not restart.') from error
            time.sleep(0.2)
    client.execute('SYSTEM FLUSH LOGS')
    after = maintain_diagnostics(client, POLICY, time.monotonic() + 30)
    assert not after.drift
    assert set(before.tables) <= set(after.tables)
    client.disconnect()


def test_clickhouse_catch_up_preserves_recent_and_source_data(
    diagnostic_server: tuple[str, object, Path],
) -> None:
    from datetime import date, datetime

    from origo.maintenance.clickhouse import maintain_diagnostics

    client = _diagnostic_client(diagnostic_server)
    historical = json.loads(
        (ROOT / 'tests/fixtures/dagster_metadata/metric_log_sample.json').read_text()
    )
    client.execute(
        'CREATE TABLE system.metric_log_286 (hostname String DEFAULT hostName(),event_date Date,event_time DateTime,CurrentMetric_Query Int64) ENGINE=MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date,event_time)'
    )
    client.execute(
        'INSERT INTO system.metric_log_286 (event_date,event_time,CurrentMetric_Query) VALUES',
        [
            (
                date.fromisoformat(historical['event_date']),
                datetime.fromisoformat(historical['event_time']),
                historical['CurrentMetric_Query'],
            )
        ],
    )
    client.execute('SYSTEM FLUSH LOGS')
    client.execute(
        'INSERT INTO system.metric_log_286 SELECT hostname,event_date,event_time,CurrentMetric_Query FROM system.metric_log ORDER BY event_date DESC,event_time DESC LIMIT 1'
    )
    client.execute('CREATE DATABASE IF NOT EXISTS origo')
    client.execute(
        'CREATE TABLE origo.retention_business (trade_id UInt64,price Float64) ENGINE=MergeTree ORDER BY trade_id'
    )
    first = (ARCHIVES / 'BTCUSDT-trades-2017-08-17.csv').read_text().splitlines()[0].split(',')
    client.execute(
        'INSERT INTO origo.retention_business VALUES', [(int(first[0]), float(first[1]))]
    )
    client.execute(
        'ALTER TABLE system.metric_log_286 MODIFY SETTING old_parts_lifetime=0, cleanup_delay_period=1, cleanup_delay_period_random_add=0'
    )
    part_path = client.execute(
        "SELECT path FROM system.parts WHERE database='system' AND table='metric_log_286' AND active AND max_date<today()-14"
    )[0][0]
    expired_directory = diagnostic_server[2] / Path(part_path).relative_to('/var/lib/clickhouse')
    assert expired_directory.exists()
    assert (
        sum(path.stat().st_blocks * 512 for path in expired_directory.rglob('*') if path.is_file())
        > 0
    )
    recent = client.execute(
        'SELECT event_time,CurrentMetric_Query FROM system.metric_log_286 WHERE event_time>=now()-INTERVAL 14 DAY'
    )
    assert recent
    apply = OperationalMetadataMaintenanceConfig(
        dry_run=False, metadata_budget_bytes=POLICY.metadata_budget_bytes
    )
    inventory = maintain_diagnostics(client, apply, time.monotonic() + 30)
    assert inventory.scheduled_action.startswith('drop_expired_part:metric_log_286:')
    assert (
        client.execute('SELECT event_time,CurrentMetric_Query FROM system.metric_log_286') == recent
    )
    assert client.execute('SELECT * FROM origo.retention_business') == [
        (int(first[0]), float(first[1]))
    ]
    assert 'toIntervalDay(14)' in client.execute('SHOW CREATE TABLE system.metric_log_286')[0][0]
    deadline = time.monotonic() + 20
    while expired_directory.exists():
        if time.monotonic() >= deadline:
            pytest.fail('Expired ClickHouse part blocks were not physically removed.')
        time.sleep(0.1)
    client.execute('DROP TABLE system.metric_log_286 SYNC')
    client.disconnect()


def test_clickhouse_expiry_lag_and_failure_visibility(
    diagnostic_server: tuple[str, object, Path],
) -> None:
    from datetime import date, datetime

    from origo.maintenance.clickhouse import maintain_diagnostics

    client = _diagnostic_client(diagnostic_server)
    row = json.loads((ROOT / 'tests/fixtures/dagster_metadata/metric_log_sample.json').read_text())
    client.execute(
        'CREATE TABLE system.metric_log_287 (hostname String DEFAULT hostName(),event_date Date,event_time DateTime,CurrentMetric_Query Int64) ENGINE=MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date,event_time)'
    )
    client.execute(
        'INSERT INTO system.metric_log_287 (event_date,event_time,CurrentMetric_Query) VALUES',
        [
            (
                date.fromisoformat(row['event_date']),
                datetime.fromisoformat(row['event_time']),
                row['CurrentMetric_Query'],
            )
        ],
    )
    report = maintain_diagnostics(client, POLICY, time.monotonic() + 30)
    assert 'metric_log_287' in report.drift
    assert any(error.startswith('expiry_lag:metric_log_287:') for error in report.errors)
    assert report.entirely_expired_bytes > 0
    assert client.execute('SELECT count(*) FROM system.metric_log_287') == [(1,)]
    client.execute(
        'ALTER TABLE system.metric_log_287 UPDATE CurrentMetric_Query=intDiv(1,CurrentMetric_Query) WHERE 1'
    )
    deadline = time.monotonic() + 20
    while True:
        failures = client.execute(
            "SELECT latest_fail_reason FROM system.mutations WHERE database='system' AND table='metric_log_287' AND latest_fail_reason!=''"
        )
        if failures:
            break
        if time.monotonic() >= deadline:
            pytest.fail('The real zero-counter mutation did not expose its error.')
        time.sleep(0.1)
    failed = maintain_diagnostics(client, POLICY, time.monotonic() + 30)
    assert any(error.startswith('mutation_failed:metric_log_287:') for error in failed.errors)
    client.execute("KILL MUTATION WHERE database='system' AND table='metric_log_287' SYNC")
    client.execute('DROP TABLE system.metric_log_287 SYNC')
    client.disconnect()


def _diagnostic_environment(
    server: tuple[str, object, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = server[1]
    monkeypatch.setenv('CLICKHOUSE_HOST', settings.host)
    monkeypatch.setenv('CLICKHOUSE_PORT', str(settings.port))
    monkeypatch.setenv('CLICKHOUSE_USER', settings.user)
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', settings.password)
    monkeypatch.setenv('CLICKHOUSE_DATABASE', settings.database)
    monkeypatch.setenv('ORIGO_CLICKHOUSE_DATA_ROOT', str(server[2]))


def test_metadata_budget_and_catch_up_progress(
    metadata_instance: DagsterInstance,
    diagnostic_server: tuple[str, object, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.maintenance.worker import maintain

    _diagnostic_environment(diagnostic_server, monkeypatch)
    run_id = execute_archive(metadata_instance)
    outcome = maintain(metadata_instance, POLICY.model_copy(update={'metadata_budget_bytes': 1}))
    assert any(value.startswith('metadata_budget:') for value in outcome.violations)
    assert outcome.report.allocated_bytes > 1 and outcome.report.deleted == 0
    assert outcome.inventory_complete and outcome.retained_floor_bytes > 1
    assert outcome.last_success_at == 0
    assert metadata_instance.get_run_by_id(run_id) is not None
    healthy = maintain(metadata_instance, POLICY)
    assert not healthy.violations
    assert healthy.last_success_at > 0
    assert healthy.report.ingress_runs_per_second == 0
    assert healthy.report.cleanup_runs_per_second == 0
    assert healthy.filesystem_free_bytes > 0
    assert healthy.diagnostic_allocated_bytes > 0
    journal = Journal.model_validate_json(Path(healthy.journal_path).read_bytes())
    assert len(journal.reports) == 2


def test_maintenance_logs_and_latency_checks(
    metadata_instance: DagsterInstance,
    diagnostic_server: tuple[str, object, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster import DagsterEventType

    from origo.maintenance.dagster_metadata import maintain_operational_metadata_job

    _diagnostic_environment(diagnostic_server, monkeypatch)
    result = maintain_operational_metadata_job.execute_in_process(
        instance=metadata_instance,
        run_config={'ops': {'maintain_operational_metadata': {'config': POLICY.model_dump()}}},
        raise_on_error=False,
    )
    assert result.success
    checks = [
        event.asset_check_evaluation_data
        for event in result.all_events
        if event.event_type == DagsterEventType.ASSET_CHECK_EVALUATION
    ]
    assert len(checks) == 1 and checks[0].passed
    metadata = checks[0].metadata['maintenance'].value
    assert max(metadata['query_p95_seconds'].values()) < 0.25
    assert {
        'scanned',
        'deleted',
        'protected',
        'reclaimed_bytes',
        'backlog_runs',
        'duration_seconds',
    } <= metadata['report'].keys()
    logs = metadata_instance.get_records_for_run(result.run_id).records
    assert any('Statistics:' in row.event_log_entry.message for row in logs)
    assert any('ClickHouse diagnostics:' in row.event_log_entry.message for row in logs)
    record = metadata_instance.get_run_records(RunsFilter(run_ids=[result.run_id]), limit=1)[0]
    assert (
        protection(
            metadata_instance,
            Layout.from_instance(metadata_instance),
            record,
            POLICY,
            time.time() + 31 * 86400,
            time.monotonic() + 15,
        )
        == 'current_asset_check'
    )

    failed = maintain_operational_metadata_job.execute_in_process(
        instance=metadata_instance,
        run_config={
            'ops': {
                'maintain_operational_metadata': {
                    'config': POLICY.model_copy(update={'metadata_budget_bytes': 1}).model_dump()
                }
            }
        },
        raise_on_error=False,
    )
    assert not failed.success
    checks = [
        event.asset_check_evaluation_data
        for event in failed.all_events
        if event.event_type == DagsterEventType.ASSET_CHECK_EVALUATION
    ]
    assert len(checks) == 1 and not checks[0].passed
    logs = metadata_instance.get_records_for_run(failed.run_id).records
    assert any('metadata_budget:' in row.event_log_entry.message for row in logs)
    from dagster import AssetCheckKey

    key = AssetCheckKey(AssetKey('maintain_operational_metadata'), 'operational_metadata_health')
    before_checks = metadata_instance.event_log_storage.get_asset_check_summary_records([key])
    layout, journal, candidate = planned(metadata_instance, result.run_id)
    future = time.time() + 31 * 86400
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    assert (
        reclaim(
            metadata_instance,
            layout,
            candidate,
            journal,
            layout.runs.parent / 'check-proof.json',
            POLICY,
            time.monotonic() + 15,
        )
        > 0
    ), candidate.reason
    assert (
        metadata_instance.event_log_storage.get_asset_check_summary_records([key]) == before_checks
    )
    with sqlite3.connect(layout.events) as database:
        assert database.execute(
            'SELECT count(*) FROM asset_check_executions WHERE run_id=?', (result.run_id,)
        ).fetchone() == (0,)


def test_source_receipt_survives_history_retirement(
    metadata_instance: DagsterInstance,
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from dagster import Definitions, build_schedule_context
    from dagster._core.definitions.schedule_definition import ScheduleExecutionData

    from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
    from origo.sources import capacity
    from origo.sources.adapters import binance_daily
    from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
    from origo.sources.bundle import build_source_bundle
    from origo.sources.lifecycle import SourceRuntime
    from origo.sources.storage import SourceStore

    from .test_binance_daily_source_adapter import archive_response
    from .test_revisioned_source_framework_backfill import _run, _start_monitors

    monkeypatch.setattr(binance_daily, 'get_response', archive_response)
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setattr(
        capacity, '_volumes', lambda runtime: (capacity._Volume('test-volume', tmp_path),)
    )
    monkeypatch.setattr(
        capacity._Volume, 'sample', lambda self: (10**12, 9 * 10**11, 10**8, 9 * 10**7)
    )
    spec = BINANCE_SPOT_TRADES_SPEC
    client = make_clickhouse_client(get_clickhouse_settings())
    store = SourceStore(client, origo_test_env['CLICKHOUSE_DATABASE'], spec)
    runtime = SourceRuntime(spec, store, tmp_path / 'locks', str(uuid4()))
    source = build_source_bundle(spec)
    runtime.setup()
    _start_monitors(metadata_instance)
    environment = (runtime, metadata_instance, source)
    try:
        assert _run(environment, probe=True).success
        schedule = next(
            item for item in source.schedules if item.name.endswith('_canonical_schedule')
        )
        definitions = Definitions(
            assets=source.assets,
            jobs=source.jobs,
            sensors=source.sensors,
            schedules=source.schedules,
        )

        def tick() -> ScheduleExecutionData:
            with build_schedule_context(
                instance=metadata_instance,
                scheduled_execution_time=datetime(2017, 8, 18, tzinfo=UTC),
                repository_def=definitions.get_repository_def(),
            ) as context:
                return schedule.evaluate_tick(context)

        request = tick().run_requests[0]
        job = next(item for item in source.jobs if item.name == schedule.job_name)
        result = job.execute_in_process(
            instance=metadata_instance,
            partition_key=request.partition_key,
            run_config=request.run_config,
            tags=request.tags,
        )
        assert result.success
        # Supersede its current check dependency with another real verification.
        assert _run(environment, reconcile=True).success
        before = store.records(canonical_only=True)
        key = AssetKey('build_binance_spot_trades_canonical_revision_origo')
        materialization = metadata_instance.get_latest_materialization_event(key)
        layout, journal, candidate = planned(metadata_instance, result.run_id)
        future = time.time() + 31 * 86400
        monkeypatch.setattr(
            retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
        )
        reclaimed = reclaim(
            metadata_instance,
            layout,
            candidate,
            journal,
            tmp_path / 'receipt-journal.json',
            POLICY,
            time.monotonic() + 30,
        )
        assert reclaimed > 0, candidate.reason
        assert metadata_instance.get_run_by_id(result.run_id) is None
        receipt = store.run_receipt(request.tags['origo_source_event'])
        assert receipt == (0, 'SUCCESS', result.run_id)
        assert tick().run_requests == []
        assert store.records(canonical_only=True) == before
        assert metadata_instance.get_latest_materialization_event(key) == materialization
    finally:
        client.disconnect()


def test_restore_verifies_real_instance_and_rejects_missing_evidence(
    metadata_instance: DagsterInstance, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import shutil

    import yaml

    from origo.maintenance import backup

    run_id = execute_archive(metadata_instance)
    layout, journal, _ = planned(metadata_instance, run_id)
    original = tmp_path / 'instance'
    restored = tmp_path / 'restored'
    # All work is complete and this test has no concurrent instance writers.
    shutil.copytree(original, restored)

    def relocated(path: Path) -> str:
        return str(restored / path.relative_to(original.resolve()))

    configuration = {
        'local_artifact_storage': {
            'module': 'dagster._core.storage.root',
            'class': 'LocalArtifactStorage',
            'config': {'base_dir': str(restored)},
        },
        'run_storage': {
            'module': 'origo.maintenance.run_storage',
            'class': 'OrigoSqliteRunStorage',
            'config': {'base_dir': relocated(layout.runs.parent)},
        },
        'event_log_storage': {
            'module': 'origo.maintenance.event_storage',
            'class': 'OrigoSqliteEventLogStorage',
            'config': {'base_dir': relocated(layout.events.parent)},
        },
        'schedule_storage': {
            'module': 'dagster._core.storage.schedules',
            'class': 'SqliteScheduleStorage',
            'config': {'base_dir': relocated(layout.schedules.parent)},
        },
        'compute_logs': {
            'module': 'dagster._core.storage.local_compute_log_manager',
            'class': 'LocalComputeLogManager',
            'config': {'base_dir': relocated(layout.compute)},
        },
    }
    (restored / 'dagster.yaml').write_text(yaml.safe_dump(configuration))
    with pytest.raises(ValueError, match='production filesystem reserve'):
        backup.verify_restored_backup(
            restored, layout, journal, 'quiesced-test-copy', time.monotonic() + 15
        )
    # Only the volume boundary is simulated; every restored database and API is real.
    monkeypatch.setattr(backup, '_separate_filesystems', lambda restored_home, production: True)
    receipt = backup.verify_restored_backup(
        restored, layout, journal, 'quiesced-test-copy', time.monotonic() + 15
    )
    assert receipt.verified_runs == 1 and receipt.manifest_sha256 == journal.manifest_sha256
    assert receipt.instance_id == metadata_instance.run_storage.get_run_storage_id()
    shard = Path(relocated(layout.shard(run_id)))
    with sqlite3.connect(shard) as database:
        database.execute('DELETE FROM event_logs')
    with pytest.raises(RuntimeError, match='no execution evidence'):
        backup.verify_restored_backup(
            restored, layout, journal, 'damaged-test-copy', time.monotonic() + 15
        )
    assert metadata_instance.get_records_for_run(run_id).records


def test_slow_retirement_does_not_block_an_unrelated_writer(
    metadata_instance: DagsterInstance, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import subprocess
    import sys
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from dagster import DagsterRun

    from origo.maintenance.dagster_metadata import maintain_operational_metadata_job

    # Real Dagster allocations necessarily repeat one of the old 256 stripes.
    seen: dict[str, DagsterRun] = {}
    for _ in range(257):
        live = metadata_instance.create_run_for_job(maintain_operational_metadata_job)
        stripe = hashlib.sha256(live.run_id.encode()).hexdigest()[:2]
        if stripe in seen:
            retired = seen[stripe]
            break
        seen[stripe] = live
    else:
        pytest.fail('257 allocated runs did not collide in 256 stripes.')
    metadata_instance.report_run_canceled(retired)
    layout, journal, candidate = planned(metadata_instance, retired.run_id)
    future = time.time() + 91 * 86400
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    entered, release = threading.Event(), threading.Event()

    def slow_receipt(run: DagsterRun) -> None:
        assert run.run_id == retired.run_id
        entered.set()
        if not release.wait(15):
            raise TimeoutError('Test did not release the slow receipt operation.')

    monkeypatch.setattr(retention, 'preserve_source_receipt', slow_receipt)
    with ThreadPoolExecutor(max_workers=2) as executor:
        cleanup = executor.submit(
            reclaim,
            metadata_instance,
            layout,
            candidate,
            journal,
            tmp_path / 'slow-journal.json',
            POLICY,
            time.monotonic() + 30,
        )
        try:
            assert entered.wait(5), cleanup.exception(timeout=1)
            writer = executor.submit(
                metadata_instance.report_engine_event,
                'Unrelated run remains writable during retirement.',
                live,
            )
            writer.result(timeout=2)
            # The unrelated writer's release must not unlock the retired run for
            # another process (POSIX locks are process-owned, not descriptor-owned).
            script = """import sys
from pathlib import Path
from origo.maintenance.run_locks import run_lock
try:
    with run_lock(Path(sys.argv[1]), int(sys.argv[2]), 0):
        raise AssertionError('Retiring run was unlocked')
except TimeoutError:
    print('retirement still locked')
"""
            result = subprocess.run(
                [
                    sys.executable,
                    '-c',
                    script,
                    str(metadata_instance.event_log_storage.writer_lock_path()),
                    str(candidate.storage_id),
                ],
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            )
            assert 'retirement still locked' in result.stdout
        finally:
            release.set()
        assert cleanup.result(timeout=10) > 0
    assert metadata_instance.get_run_by_id(retired.run_id) is None
    assert metadata_instance.get_run_by_id(live.run_id) is not None
    assert any(
        'Unrelated run remains writable' in row.event_log_entry.message
        for row in metadata_instance.get_records_for_run(live.run_id).records
    )
    assert metadata_instance.event_log_storage.writer_lock_path().stat().st_size == 0


def test_first_apply_resumes_after_live_revalidation(
    metadata_instance: DagsterInstance,
    diagnostic_server: tuple[str, object, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.maintenance import worker

    _diagnostic_environment(diagnostic_server, monkeypatch)
    preserved = execute_archive(metadata_instance)
    eligible = execute_archive(metadata_instance)
    layout, journal, candidate = planned(metadata_instance, preserved)
    journal.manifest.append(planned(metadata_instance, eligible)[2])
    future = time.time() + 31 * 86400
    scan_batch(metadata_instance, layout, journal, POLICY, future, time.monotonic() + 15)
    journal.retained_floor_bytes = (
        worker.directory_bytes(layout, time.monotonic() + 15) - journal.inventory_eligible_bytes
    )
    journal.manifest_sha256 = manifest_sha256(journal)
    approved = journal.manifest_sha256
    receipt = BackupReceipt(
        instance_id=journal.instance_id,
        manifest_sha256=approved,
        snapshot_id='verified-first-apply-test',
        restored_home=str(tmp_path),
        verified_at=future,
        expires_at=future + 86400,
        verified_runs=2,
    )
    receipt_path = tmp_path / 'first-apply-receipt.json'
    receipt_path.write_text(receipt.model_dump_json())
    config = POLICY.model_copy(
        update={
            'dry_run': False,
            'backup_receipt': str(receipt_path),
            'approved_manifest_sha256': approved,
        }
    )
    require_backup(journal, config, future)
    metadata_instance.add_run_tags(preserved, {'origo_metadata_preserve': 'true'})
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    monkeypatch.setattr(
        worker, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    path = layout.runs.parent / 'operational-maintenance' / 'journal.json'
    # Stop at the reported crash boundary: only the revalidation result was saved.
    assert (
        reclaim(metadata_instance, layout, candidate, journal, path, config, time.monotonic() + 15)
        == 0
    )
    resumed = Journal.model_validate_json(path.read_bytes())
    assert all(row.phase == 'planned' for row in resumed.manifest)
    assert resumed.manifest[0].reason == ''
    assert resumed.manifest[0].revalidation_reason == 'live_revalidation:operator_preserved'
    assert resumed.manifest_sha256 == manifest_sha256(resumed) == approved
    outcome = worker.maintain(metadata_instance, config)
    assert outcome.report.deleted == 1 and outcome.report.protected == 1
    assert not outcome.violations
    assert metadata_instance.get_run_by_id(preserved) is not None
    assert metadata_instance.get_run_by_id(eligible) is None
    assert Journal.model_validate_json(path.read_bytes()).first_apply_completed


def test_retired_storage_id_can_be_reused_during_event_cleanup(
    metadata_instance: DagsterInstance, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from origo.maintenance.dagster_metadata import maintain_operational_metadata_job

    retired = execute_archive(metadata_instance)
    layout, journal, candidate = planned(metadata_instance, retired)
    future = time.time() + 31 * 86400
    monkeypatch.setattr(
        retention, 'time', SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    )
    entered, release = threading.Event(), threading.Event()
    storage = metadata_instance.event_log_storage
    original = storage.delete_events

    def slow_events(run_id: str) -> None:
        entered.set()
        if not release.wait(10):
            raise TimeoutError('Test did not release event cleanup.')
        original(run_id)

    monkeypatch.setattr(storage, 'delete_events', slow_events)
    with ThreadPoolExecutor(max_workers=2) as executor:
        cleanup = executor.submit(
            reclaim,
            metadata_instance,
            layout,
            candidate,
            journal,
            tmp_path / 'reused-id.json',
            POLICY,
            time.monotonic() + 20,
        )
        try:
            assert entered.wait(5)
            live = metadata_instance.create_run_for_job(maintain_operational_metadata_job)
            record = metadata_instance.get_run_records(RunsFilter(run_ids=[live.run_id]), limit=1)[
                0
            ]
            assert record.storage_id == candidate.storage_id
            executor.submit(
                metadata_instance.report_engine_event, 'Reused row ID remains writable.', live
            ).result(timeout=2)
        finally:
            release.set()
        assert cleanup.result(timeout=5) > 0
    assert metadata_instance.get_run_by_id(live.run_id) is not None
    assert metadata_instance.get_run_by_id(retired) is None


def test_completed_inventory_survives_scheduled_dry_runs(
    metadata_instance: DagsterInstance,
    diagnostic_server: tuple[str, object, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.maintenance import worker

    _diagnostic_environment(diagnostic_server, monkeypatch)
    run_ids = [execute_archive(metadata_instance) for _ in range(3)]
    future = time.time() + 31 * 86400
    clock = SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    monkeypatch.setattr(worker, 'time', clock)
    monkeypatch.setattr(retention, 'time', clock)
    # The runtime reserve limits each invocation to one real scan batch.
    config = POLICY.model_copy(update={'max_runs_per_batch': 1, 'max_runtime_seconds': 10})
    for index in range(3):
        outcome = worker.maintain(metadata_instance, config)
        assert outcome.inventory_complete is (index == 2)
        assert outcome.report.scanned == 1 and outcome.report.deleted == 0
    path = Path(outcome.journal_path)
    completed = Journal.model_validate_json(path.read_bytes())
    assert completed.inventory_scanned == 3 and completed.inventory_eligible_bytes > 0
    assert completed.retained_floor_bytes > 0
    assert completed.manifest[0].run_id == run_ids[0]
    approved = completed.manifest_sha256
    receipt = BackupReceipt(
        instance_id=completed.instance_id,
        manifest_sha256=approved,
        snapshot_id='completed-inventory-test',
        restored_home=str(tmp_path),
        verified_at=future,
        expires_at=future + 86400,
        verified_runs=1,
    )
    receipt_path = tmp_path / 'completed-inventory-receipt.json'
    receipt_path.write_text(receipt.model_dump_json())
    apply = config.model_copy(
        update={
            'dry_run': False,
            'backup_receipt': str(receipt_path),
            'approved_manifest_sha256': approved,
        }
    )
    require_backup(completed, apply, future)
    for _ in range(2):
        outcome = worker.maintain(metadata_instance, config)
        repeated = Journal.model_validate_json(path.read_bytes())
        assert outcome.inventory_complete
        assert outcome.report.scanned == outcome.report.deleted == 0
        assert repeated.inventory_started_at == completed.inventory_started_at
        assert repeated.inventory_scanned == completed.inventory_scanned
        assert repeated.inventory_eligible_bytes == completed.inventory_eligible_bytes
        assert repeated.retained_floor_bytes == completed.retained_floor_bytes
        assert repeated.manifest == completed.manifest
        assert repeated.manifest_sha256 == manifest_sha256(repeated) == approved
        assert not outcome.violations
        assert outcome.query_p95_seconds and outcome.diagnostic_allocated_bytes > 0
        require_backup(repeated, apply, future)
    applied = worker.maintain(metadata_instance, apply)
    assert applied.report.deleted == 1 and not applied.violations
    assert metadata_instance.get_run_by_id(run_ids[0]) is None
    assert Journal.model_validate_json(path.read_bytes()).first_apply_completed
    # Once first apply commits, subsequent dry runs may inventory the next cycle.
    next_cycle = worker.maintain(metadata_instance, config)
    assert next_cycle.report.scanned == 1 and not next_cycle.inventory_complete
    assert metadata_instance.get_run_by_id(run_ids[1]) is not None


def test_empty_inventory_keeps_discovering_eligible_runs(
    metadata_instance: DagsterInstance,
    diagnostic_server: tuple[str, object, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from origo.maintenance import worker

    _diagnostic_environment(diagnostic_server, monkeypatch)
    run_id = execute_archive(metadata_instance)
    recent = worker.maintain(metadata_instance, POLICY)
    assert recent.inventory_complete and recent.report.candidates == 0
    future = time.time() + 31 * 86400
    clock = SimpleNamespace(time=lambda: future, monotonic=time.monotonic)
    monkeypatch.setattr(worker, 'time', clock)
    monkeypatch.setattr(retention, 'time', clock)
    aged = worker.maintain(metadata_instance, POLICY)
    journal = Journal.model_validate_json(Path(aged.journal_path).read_bytes())
    assert aged.inventory_complete and aged.report.candidates == 1
    assert journal.manifest[0].run_id == run_id
    assert journal.inventory_started_at == future
    # A changed retention policy invalidates the old inventory and its approval.
    changed_policy = POLICY.model_copy(update={'success_retention_days': 60})
    changed = worker.maintain(metadata_instance, changed_policy)
    journal = Journal.model_validate_json(Path(changed.journal_path).read_bytes())
    assert changed.inventory_complete and changed.report.candidates == 0
    assert journal.policy_sha256 == policy_sha256(changed_policy)
    assert not journal.manifest and changed.manifest_sha256 != aged.manifest_sha256
    assert metadata_instance.get_run_by_id(run_id) is not None
