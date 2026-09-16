from pathlib import Path

import pytest

from tools.benchmark_source_backfill import archive_evidence, throughput

ARCHIVES = Path('tests/fixtures/binance/spot/daily/trades/revisioned')


def test_throughput_denominator_is_source_rows() -> None:
    assert throughput(194010, 3.824381)['seconds_per_million'] == pytest.approx(19.71229)
    with pytest.raises(ValueError, match='positive'):
        throughput(0, 1)


def test_benchmark_requires_archive_checksum(tmp_path: Path) -> None:
    day = '2017-08-17'
    name = f'BTCUSDT-trades-{day}.zip'
    (tmp_path / name).write_bytes((ARCHIVES / name).read_bytes())
    (tmp_path / (name + '.CHECKSUM')).write_text((ARCHIVES / (name + '.CHECKSUM')).read_text())
    assert archive_evidence(tmp_path, [day])[0]['bytes'] == (ARCHIVES / name).stat().st_size
    (tmp_path / name).write_bytes((ARCHIVES / 'BTCUSDT-trades-2020-01-01.zip').read_bytes())
    with pytest.raises(ValueError, match='checksum mismatch'):
        archive_evidence(tmp_path, [day])
