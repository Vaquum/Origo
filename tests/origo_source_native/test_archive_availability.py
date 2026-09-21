"""Canonical availability is an observed provider fact, not an invented publish time."""

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.sources.adapters import binance_daily as daily
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.contracts import SourceError
from origo.sources.lifecycle import SourceRuntime
from origo.sources.registry import SOURCE_REGISTRY
from origo.sources.storage import SourceStore
from origo.steady_state.policy import load_inventory, registry_discrepancies

from .helpers import ORIGO_DATABASE
from .test_binance_daily_source_adapter import archive_response


def test_availability_polling_is_five_minutes_and_matches_inventory() -> None:
    assert all(spec.orchestration.canonical_cron == '*/5 * * * *' for spec in SOURCE_REGISTRY)
    assert registry_discrepancies(load_inventory(), SOURCE_REGISTRY) == ()


def test_availability_records_unavailable_unknown_and_success_separately(
    origo_test_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = make_clickhouse_client(get_clickhouse_settings())
    spec = BINANCE_SPOT_TRADES_SPEC
    runtime = SourceRuntime(
        spec, SourceStore(client, ORIGO_DATABASE, spec), tmp_path / 'locks', str(uuid4())
    )
    outcome = ['PROVIDER_HTTP_404']

    def provider(url: str) -> daily.Response:
        if outcome[0]:
            raise SourceError(outcome[0], 'Controlled provider availability failure.')
        return archive_response(url)

    monkeypatch.setattr(daily, 'get_response', provider)
    before = datetime.now(UTC)
    try:
        runtime.setup()
        partition = spec.canonical.partition('2020-01-01')
        with pytest.raises(SourceError, match='Controlled'):
            runtime.discover(partition)
        outcome[0] = 'PROVIDER_TRANSPORT_FAILED'
        with pytest.raises(SourceError, match='Controlled'):
            runtime.discover(partition)
        outcome[0] = ''
        revision = runtime.discover(partition)
        rows = client.execute(
            f'SELECT evidence_json, complete, observed_at FROM {ORIGO_DATABASE}.source_observation_log '
            'WHERE source_key=%(source)s AND partition_key=%(partition)s ORDER BY observed_at',
            {'source': spec.key, 'partition': partition.key},
        )
        documents = [json.loads(str(row[0])) for row in rows]
        assert [row[1] for row in rows] == [0, 0, 1]
        assert [document['available'] for document in documents] == [False, None, True]
        assert [document['error_code'] for document in documents] == [
            'PROVIDER_HTTP_404',
            'PROVIDER_TRANSPORT_FAILED',
            '',
        ]
        assert documents[-1]['revision'] == revision
        assert all(document['probe'] == 'checksum_discovery' for document in documents)
        assert all(document['operation'] == 'archive_availability' for document in documents)
        after = datetime.now(UTC)
        assert all(before <= row[2].replace(tzinfo=UTC) <= after for row in rows)
    finally:
        client.disconnect()
