from __future__ import annotations

import pytest

from origo.events.backfill_state import CanonicalBackfillStateStore
from origo.events.errors import ReconciliationError


class _FakeCoverageClient:
    def __init__(
        self,
        *,
        proof_rows: list[tuple[object, ...]],
        watermark_rows: list[tuple[object, ...]],
    ) -> None:
        self._proof_rows = proof_rows
        self._watermark_rows = watermark_rows

    def execute(
        self,
        query: str,
        _params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        if 'canonical_backfill_partition_proofs' in query:
            return self._proof_rows
        if 'canonical_projector_watermarks' in query:
            return self._watermark_rows
        raise AssertionError(f'unexpected query: {query}')


def test_projector_partition_coverage_matches_terminal_proof_exactly() -> None:
    store = CanonicalBackfillStateStore(
        client=_FakeCoverageClient(
            proof_rows=[
                ('2024-01-01', 'proved_complete'),
                ('2024-01-02', 'empty_proved'),
                ('2024-01-03', 'quarantined'),
            ],
            watermark_rows=[
                ('2024-01-01',),
                ('2024-01-02',),
            ],
        ),
        database='origo',
    )

    assert store.assert_projector_partition_coverage_matches_terminal_proof_or_raise(
        projector_id='etf_daily_metrics_native_v1',
        source_id='etf',
        stream_id='etf_daily_metrics',
    ) == ('2024-01-01', '2024-01-02')


def test_projector_partition_coverage_fails_loud_on_partition_set_mismatch() -> None:
    store = CanonicalBackfillStateStore(
        client=_FakeCoverageClient(
            proof_rows=[
                ('2024-01-01', 'proved_complete'),
                ('2024-01-02', 'proved_complete'),
            ],
            watermark_rows=[
                ('2024-01-01',),
            ],
        ),
        database='origo',
    )

    with pytest.raises(
        ReconciliationError,
        match='projector watermark partition coverage to exactly match terminal proof partition coverage',
    ) as exc_info:
        store.assert_projector_partition_coverage_matches_terminal_proof_or_raise(
            projector_id='etf_daily_metrics_native_v1',
            source_id='etf',
            stream_id='etf_daily_metrics',
        )
    assert exc_info.value.code == 'BACKFILL_PROJECTOR_WATERMARK_PARTITION_SET_MISMATCH'


def test_projector_partition_coverage_requires_terminal_partition_proof() -> None:
    store = CanonicalBackfillStateStore(
        client=_FakeCoverageClient(
            proof_rows=[
                ('2024-01-01', 'quarantined'),
            ],
            watermark_rows=[],
        ),
        database='origo',
    )

    with pytest.raises(
        ReconciliationError,
        match='requires at least one terminal partition proof',
    ) as exc_info:
        store.assert_projector_partition_coverage_matches_terminal_proof_or_raise(
            projector_id='fred_series_metrics_aligned_1s_v1',
            source_id='fred',
            stream_id='fred_series_metrics',
        )
    assert exc_info.value.code == 'BACKFILL_TERMINAL_PARTITION_PROOFS_MISSING'
