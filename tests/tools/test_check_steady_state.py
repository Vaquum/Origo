"""Observer safety and evidence identity tests; no production connections."""

from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path

import pytest

from origo.steady_state.capture import CaptureConfig, EvidenceWriter, ReadOnlyClient
from origo.steady_state.policy import canonical_json
from origo.steady_state.verification import EvidenceError, load_bundle
from tools.check_steady_state import main


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, object]]] = []

    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.calls.append((query, dict(settings or {})))
        return [(1,)]

    def disconnect(self) -> None:
        self.calls.clear()


@pytest.mark.parametrize(
    'query',
    [
        'INSERT INTO t VALUES (1)',
        'WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x',
        'SELECT 1; DROP TABLE t',
        "SELECT 1 INTO OUTFILE '/tmp/forbidden'",
        'SELECT 1 SETTINGS readonly=0',
        "SELECT * FROM file('/tmp/private')",
        "SELECT * FROM url('https://example.invalid', 'CSV')",
        'EXPLAIN ALTER TABLE t',
    ],
)
def test_observer_refuses_non_read_sql_before_calling_the_client(query: str) -> None:
    underlying = RecordingClient()
    with pytest.raises(PermissionError):
        ReadOnlyClient(underlying).execute(query)
    assert underlying.calls == []


@pytest.mark.parametrize('value', [0, -1, True, float('nan'), float('inf'), '0'])
def test_observer_limits_cannot_be_disabled(value: object) -> None:
    underlying = RecordingClient()
    with pytest.raises(ValueError):
        ReadOnlyClient(underlying).execute('SELECT 1', settings={'max_threads': value})
    assert underlying.calls == []


@pytest.mark.parametrize('setting', ['readonly', 'result_overflow_mode', 'unknown_option'])
def test_observer_fixed_settings_cannot_be_weakened(setting: str) -> None:
    with pytest.raises(PermissionError):
        ReadOnlyClient(RecordingClient()).execute('SELECT 1', settings={setting: 'break'})


def test_observer_keeps_stricter_bounds_and_allows_system_metadata() -> None:
    underlying = RecordingClient()
    observer = ReadOnlyClient(underlying)
    assert observer.execute(
        'SELECT name FROM system.tables', settings={'max_execution_time': 0.5, 'max_threads': 999}
    ) == [(1,)]
    settings = underlying.calls[0][1]
    assert settings['readonly'] == 1
    assert settings['max_execution_time'] == 0.5
    assert settings['max_threads'] == 2
    assert settings['max_memory_usage'] == 512 * 1024 * 1024
    observer.execute("WITH x AS (SELECT 'DROP; TABLE' AS message) SELECT * FROM x;")
    assert len(underlying.calls) == 2


def test_capture_database_is_a_valid_identifier() -> None:
    with pytest.raises(ValueError):
        CaptureConfig.from_environ(
            {'CLICKHOUSE_DATABASE': 'origo; DROP DATABASE x'}, environment='isolated'
        )


def identity_bundle(root: Path) -> tuple[EvidenceWriter, str]:
    writer = EvidenceWriter(root)
    digest = writer.write_identity(
        {
            'kind': 'steady_state_identity',
            'schema_version': 1,
            'environment': 'isolated',
            'code_sha': 'a' * 40,
        }
    )
    writer.write_manifest()
    return writer, digest


def test_identity_contents_are_bound_even_when_the_manifest_is_rehashed(tmp_path: Path) -> None:
    writer, digest = identity_bundle(tmp_path)
    assert digest in load_bundle(tmp_path, timedelta(minutes=1)).identities
    path = tmp_path / 'identity' / f'{digest}.json'
    document = {
        'kind': 'steady_state_identity',
        'schema_version': 1,
        'environment': 'production',
        'code_sha': 'b' * 40,
        'runtime_identity_sha256': digest,
    }
    path.write_bytes(canonical_json(document))
    writer.write_manifest()
    with pytest.raises(EvidenceError, match='fingerprint'):
        load_bundle(tmp_path, timedelta(minutes=1))


def test_unlisted_nested_manifest_is_not_ignored(tmp_path: Path) -> None:
    identity_bundle(tmp_path)
    nested = tmp_path / 'unlisted' / 'manifest.json'
    nested.parent.mkdir()
    nested.write_text('{}')
    with pytest.raises(EvidenceError, match='manifest/file mismatch'):
        load_bundle(tmp_path, timedelta(minutes=1))


def test_verify_missing_evidence_is_unknown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(['verify', '--evidence', str(tmp_path), '--profile', 'production']) == 2
    assert '"verdict": "UNKNOWN"' in capsys.readouterr().out


def test_capture_requires_read_only_before_any_connection(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match='requires --read-only'):
        main(['capture', '--output', str(tmp_path), '--environment', 'production'])
    assert list(tmp_path.iterdir()) == []


def test_policy_prints_every_required_metric_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from origo.assets import create_origo_database

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError('The policy command must not connect to runtime.')

    monkeypatch.setattr(create_origo_database, 'make_clickhouse_client', unexpected)
    assert main(['policy', '--format', 'json']) == 0
    text = capsys.readouterr().out
    for number in range(1, 13):
        assert f'SS-{number:02d}' in text
    assert 'policy_sha256' in text and 'inventory_sha256' in text
