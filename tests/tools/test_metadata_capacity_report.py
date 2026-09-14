"""An incomplete evidence file must not certify production capacity."""

import subprocess
import sys
from pathlib import Path

TOOL = Path(__file__).resolve().parents[2] / 'tools/metadata_capacity_report.py'


def test_capacity_requires_complete_measured_evidence(tmp_path: Path) -> None:
    evidence = tmp_path / 'missing-proof.json'
    evidence.write_text('{}')
    result = subprocess.run(
        [sys.executable, str(TOOL), '--evidence', str(evidence)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert 'validation errors for CapacityEvidence' in result.stderr
    assert 'source_histories_verified' in result.stderr and 'replay_written_runs' in result.stderr


def test_capacity_refuses_absent_evidence(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(TOOL), '--evidence', str(tmp_path / 'absent.json')],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0 and 'FileNotFoundError' in result.stderr
