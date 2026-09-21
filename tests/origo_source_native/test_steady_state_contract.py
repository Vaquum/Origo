"""M12: the pinned inventory, the registry and the physical products keep one contract.

The four sources are driven through the real provisional worker over their authentic
captured REST fixtures (no market rows are invented); the read-only capture then observes
the resulting ClickHouse metadata, manifests and Parquet/Arrow files, and the verifier
evaluates that one-bucket bundle. CANARY stays non-public throughout.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters.binance_daily import Response
from origo.sources.contracts import Partition, RevisionedSourceSpec
from origo.sources.lifecycle import SourceRuntime
from origo.sources.profiles import (
    perp_agg_consumers,
    perp_consumers,
    spot_agg_consumers,
    spot_consumers,
)
from origo.sources.profiles.formulas import (
    perp_agg_series,
    perp_series,
    spot_agg_series,
    spot_series,
)
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.steady_state.capture import CaptureConfig, EvidenceWriter, capture_sample
from origo.steady_state.policy import (
    load_inventory,
    load_policy,
    registry_discrepancies,
    required_entities,
    series_discrepancies,
)
from origo.steady_state.verification import load_bundle, verify_bundle
from origo.workers.dagster_reader import DagsterReader
from origo.workers.provisional import ProvisionalFeed
from origo.workers.receipts import ensure_monitoring_tables
from origo.workers.report import Reporter

from .helpers import ORIGO_DATABASE
from .test_provisional_worker import _Dagster, _Reporter
from .test_steady_state_capacity import _RecordedMinute

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/binance'
DECLARATIONS = (
    ('spot', 'trades', 'binance_spot_rest'),
    ('futures', 'trades', 'binance_perp_rest'),
    ('spot', 'aggtrades', 'binance_spot_agg_rest'),
    ('futures', 'aggtrades', 'binance_perp_agg_rest'),
)
SERIES_MODULES = {
    'binance_spot_trades': (spot_series, spot_consumers),
    'binance_perp_trades': (perp_series, perp_consumers),
    'binance_spot_aggtrades': (spot_agg_series, spot_agg_consumers),
    'binance_perp_aggtrades': (perp_agg_series, perp_agg_consumers),
}


def replay_all_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[RevisionedSourceSpec], dict[str, Partition]]:
    """Real adapters over the captured fixture responses, one authentic minute per source."""
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path / 'locks'))
    monkeypatch.setenv('ORIGO_SOURCE_PUBLICATION_ROOT', str(tmp_path / 'source-files'))
    monkeypatch.setenv('LOCAL_PARQUET_DIR', str(tmp_path / 'parquet'))
    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path / 'arrow'))
    monkeypatch.setenv('BINANCE_API_KEY', '0' * 64)
    specs: list[RevisionedSourceSpec] = []
    minutes: dict[str, Partition] = {}
    for spec, (market, family, module_name) in zip(SOURCE_REGISTRY, DECLARATIONS, strict=True):
        fixture = FIXTURES / market / 'rest' / family
        provenance = json.loads((fixture / 'provenance.json').read_text())
        calls = cast(list[dict[str, object]], list(provenance['requests']))
        module = importlib.import_module('origo.sources.adapters.' + module_name)
        assert spec.provisional is not None
        minute = spec.provisional.partition(
            str(provenance['minute_start']).replace('+00:00', 'Z').replace(' ', 'T')
        )
        minutes[spec.key] = minute

        def replay(
            url: str,
            *,
            params: dict[str, str | int],
            headers: dict[str, str],
            weight: int,
            calls: list[dict[str, object]] = calls,
            fixture: Path = fixture,
        ) -> Response:
            assert calls, 'Unexpected request beyond captured fixture'
            request = calls.pop(0)
            assert url == request['url'] and params == request['params']
            body = (fixture / str(request['file'])).read_bytes()
            assert hashlib.sha256(body).hexdigest() == request['sha256']
            return Response(body, {}, int(str(request.get('status', 200))))

        monkeypatch.setattr(module, 'get_response', replay)
        specs.append(replace(spec, provisional=_RecordedMinute(spec.provisional, minute)))
    return specs, minutes


def test_real_data_inventory_and_consumers_keep_their_contract(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inventory = load_inventory()
    policy = load_policy()
    # The literal inventory and the live registry agree, product by product.
    assert registry_discrepancies(inventory, SOURCE_REGISTRY) == ()
    for key, (series_module, consumers_module) in SERIES_MODULES.items():
        assert (
            series_discrepancies(
                inventory.sources[key],
                series_module.SPECS,
                consumers_module.HUGGINGFACE_DATASETS,
                consumers_module.EXPORT_START_DATE,
            )
            == ()
        )
        assert len(inventory.sources[key].series) == 12
    canary = inventory.sources['binance_perp_aggtrades']
    assert canary.rollout_stage == 'CANARY'
    assert all(not consumer.public for consumer in canary.consumers)
    assert canary.consumer('huggingface_shadow').destination == 'local'
    for key in ('binance_spot_trades', 'binance_perp_trades', 'binance_spot_aggtrades'):
        source = inventory.sources[key]
        assert source.consumer('mount').public and source.consumer('huggingface').public
        assert source.consumer('huggingface').destination == 'remote'
    entities = required_entities(inventory)
    assert entities['mount'] == tuple(f'{key}:mount' for key in inventory.sources)
    assert entities['depth'] == ('depth20', 'depth200')

    # Real builds and publications over the authentic fixtures, through the real worker.
    specs, minutes = replay_all_sources(tmp_path, monkeypatch)
    client = make_clickhouse_client(get_clickhouse_settings())
    try:
        for spec in specs:
            store = SourceStore(client, ORIGO_DATABASE, spec)
            SourceRuntime(spec, store, tmp_path / 'locks', 'contract').setup(
                anchor=minutes[spec.key].start
            )
        ensure_monitoring_tables(client, ORIGO_DATABASE)
        feed = ProvisionalFeed(
            specs,
            publication_root=tmp_path / 'source-files',
            reporter=cast(Reporter, _Reporter()),
            dagster=cast(DagsterReader, _Dagster()),
            host='isolated-s439',
        )
        tick = max(partition.end for partition in minutes.values()) + timedelta(seconds=5)
        outcome = feed.tick(tick)
        assert outcome.failed == ()
        for spec in specs:
            assert f'{spec.key}:mount' in outcome.processed

        # The read-only capture observes the products the worker just committed.
        config = CaptureConfig.from_environ(
            {
                'CLICKHOUSE_DATABASE': ORIGO_DATABASE,
                'ORIGO_SOURCE_PUBLICATION_ROOT': str(tmp_path / 'source-files'),
                'LOCAL_PARQUET_DIR': str(tmp_path / 'parquet'),
                'LOCAL_ARROW_DIR': str(tmp_path / 'arrow'),
                'ORIGO_HEARTBEAT_DIR': str(tmp_path / 'heartbeats'),
                'ORIGO_SOURCE_LOCK_DIR': str(tmp_path / 'locks'),
                'ORIGO_CODE_SHA': 'contract-test',
            },
            environment='isolated',
            remote_probes=False,
        )
        writer = EvidenceWriter(tmp_path / 'evidence')
        now = tick + timedelta(minutes=1)
        sample = capture_sample(
            config, client=client, writer=writer, now=now, policy=policy, inventory=inventory
        )
    finally:
        client.disconnect()
    sources = cast(dict[str, dict[str, object]], sample['sources'])
    consumers = cast(dict[str, dict[str, object]], sample['consumers'])
    for spec in specs:
        probe = sources[spec.key]
        assert probe['status'] == 'observed', probe
        assert probe['contiguous_end'] == minutes[spec.key].end.isoformat()
        assert probe['incomplete_partition_count'] == 0
        mount = consumers[f'{spec.key}:mount']
        assert mount['status'] == 'observed', mount
        manifest = cast(dict[str, object], mount['manifest'])
        assert manifest['exists'] and manifest['active_through'] == minutes[spec.key].end.isoformat()
        series = cast(dict[str, dict[str, dict[str, object]]], mount['series'])
        assert set(series) == {item.name for item in inventory.sources[spec.key].series}
        for name, checks in series.items():
            for part in ('month', 'arrow'):
                artifact = checks[part]
                assert artifact['listed'] and artifact['exists'], (name, part, artifact)
                assert artifact['sha256_verified'] and artifact['readable'], (name, part, artifact)
                assert artifact['rows'] == artifact['manifest_rows'], (name, part, artifact)
        # Canonical-only consumers were not rendered: recorded as absent, never invented.
        daily = next(consumer.key for consumer in inventory.sources[spec.key].consumers if consumer.canonical_only)
        assert cast(dict[str, object], consumers[f'{spec.key}:{daily}']['manifest'])['exists'] is False
    depth = cast(dict[str, dict[str, object]], sample['depth'])
    assert {probe['status'] for probe in depth.values()} == {'unknown'}
    assert all('reason' in probe for probe in depth.values())
    assert cast(dict[str, object], sample['dagster'])['status'] == 'unknown'

    # The bundle is sealed and the verifier reads it back without trusting any flag.
    bundle = load_bundle(tmp_path / 'evidence', timedelta(seconds=policy.sample_period_seconds))
    assert bundle.rejected == [] and len(bundle.samples) == 1
    report, denominators = verify_bundle(
        tmp_path / 'evidence', profile='isolated', policy=policy, inventory=inventory
    )
    # The replayed minutes are the fixtures' real minutes: three of them are days to
    # months older than the tick, so their ingestion lag is truthfully a FAIL, and no
    # controlled trial artifacts accompany the bucket, so nothing is promoted to PASS.
    assert report.verdict == 'FAIL'
    by_key = {(r.metric_id, r.entity, r.statistic): r for r in report.results}
    bucket = next(iter(bundle.samples))
    for spec in specs:
        lag = by_key[('SS-01', spec.key, 'lag_seconds_max')]
        expected_lag = (bucket - minutes[spec.key].end).total_seconds()
        assert lag.observed == expected_lag
        assert lag.verdict == ('PASS' if expected_lag <= 600 else 'FAIL')
        mount_lag = by_key[('SS-02', f'{spec.key}:mount', 'lag_seconds_max')]
        assert mount_lag.observed == expected_lag
        assert by_key[('SS-02', f'{spec.key}:mount', 'series_missing_or_unreadable_max')].observed == 0.0
        assert by_key[('SS-02', f'{spec.key}:mount', 'required_series_count')].observed == 12.0
        assert by_key[('SS-02', f'{spec.key}:mount', 'unsupported_forward_claims_max')].verdict == 'PASS'
    assert by_key[('SS-05', 'trial', 'trial_hours_min')].verdict == 'UNKNOWN'
    assert denominators[f'SS-02:{specs[0].key}:mount'] == {'buckets': 1, 'known': 1}
