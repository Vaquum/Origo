"""Operational verifier fixtures, never actual production/market-data evidence."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from origo.steady_state.capture import EvidenceWriter
from origo.steady_state.policy import load_inventory, load_policy, sha256_bytes
from origo.steady_state.verification import Evaluator, load_bundle

START = datetime(2026, 9, 21, 12, tzinfo=UTC)


def evidence_writer(root: Path, *, environment: str = 'production') -> tuple[EvidenceWriter, str]:
    writer = EvidenceWriter(root)
    identity = writer.write_identity(
        {
            'kind': 'steady_state_identity',
            'schema_version': 1,
            'environment': environment,
            'code_sha': 'a' * 40,
            'policy_sha256': load_policy().sha256,
            'inventory_sha256': load_inventory().sha256,
            'fixture_only': True,
        }
    )
    return writer, identity


def sample(bucket: datetime, identity: str) -> dict[str, object]:
    began = (bucket + timedelta(seconds=2)).isoformat()
    return {
        'kind': 'steady_state_sample',
        'schema_version': 1,
        'bucket': bucket.isoformat(),
        'observed_start': began,
        'observed_end': (bucket + timedelta(seconds=3)).isoformat(),
        'sample_id': sha256_bytes(f'{bucket.isoformat()}|{began}|offline-test'.encode()),
        'identity_sha256': identity,
        'host': 'offline-test',
        'sources': {},
        'consumers': {},
        'depth': {},
    }


def evaluator(writer: EvidenceWriter) -> Evaluator:
    writer.write_manifest()
    bundle = load_bundle(writer.root, timedelta(minutes=1))
    return Evaluator(bundle, load_policy(), load_inventory(), 'production')
