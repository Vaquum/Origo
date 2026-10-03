from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from origo.sources.adapters import binance_daily
from origo.sources.adapters.book_spool import Market
from origo.workers.book_capture import AttemptBudget

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('filename', ['docker-compose.yml', 'docker-compose.deploy.yml'])
def test_capture_and_provisional_services_deploy_as_disjoint_units(filename: str) -> None:
    compose = yaml.safe_load((ROOT / filename).read_text())
    services = compose['services']
    for market in ('spot', 'perp'):
        capture = services[f'book-capture-{market}']
        worker = services[f'provisional-binance-{market}-book']
        assert capture['command'] == f'python -m origo.workers.book_capture --market {market}'
        assert worker['command'] == 'python -m origo.workers.provisional'
        assert worker['environment']['ORIGO_PROVISIONAL_SOURCE'] == f'binance_{market}_book'
        assert 'ORIGO_PROVISIONAL_SOURCE' not in capture['environment']
        assert capture['read_only'] and capture['cap_drop'] == ['ALL']
        assert capture['cpus'] == 1 and capture['mem_limit'] == '512m'
        assert 'book-spool:/var/lib/origo-book-spool' in worker['volumes']
        assert 'book-spool:/var/lib/origo-book-spool' in capture['volumes']
    assert services['book-capture-spot']['ports'] == ['127.0.0.1:8088:8088']
    assert 'ports' not in services['book-capture-perp']
    for job_service in ('dagster', 'dagit'):
        assert 'book-spool:/var/lib/origo-book-spool' in services[job_service]['volumes']
    assert 'book-spool' in compose['volumes']
    workflow = (ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert workflow.count('ORIGO_BOOK_TOP20_TOKEN: ${{ secrets.ORIGO_BOOK_TOP20_TOKEN }}') == 2
    assert '"ORIGO_BOOK_TOP20_TOKEN": os.environ["ORIGO_BOOK_TOP20_TOKEN"]' in workflow
    for market in ('spot', 'perp'):
        assert (
            f'book-capture-{market} provisional-binance' in workflow
            or f'book-capture-{market}' in workflow
        )
        assert f'provisional-binance-{market}-book' in workflow


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_capture_services_share_writable_limiter_volume_and_preserve_cooldown_on_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
) -> None:
    for filename in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        services = yaml.safe_load((ROOT / filename).read_text())['services']
        for name in (f'book-capture-{market}', f'provisional-binance-{market}-book', 'dagster'):
            assert 'source-locks:/opt/origo/locks' in services[name]['volumes']
        assert (
            services[f'book-capture-{market}']['environment']['ORIGO_SOURCE_LOCK_DIR']
            == '/opt/origo/locks'
        )
    # Persist the existing host-family circuit exactly where both Compose callers mount it.
    monkeypatch.setenv('ORIGO_SOURCE_LOCK_DIR', str(tmp_path))
    family = 'api_binance_com' if market == 'spot' else 'fapi_binance_com'
    ledger = tmp_path / f'binance_rest_budget.{family}.state'
    ledger.write_text('9999999999 9999999999')
    calls: list[str] = []

    def forbidden(*args: object, **kwargs: object) -> object:
        calls.append('outbound')
        pytest.fail('Persisted provider circuit must prevent transport.')

    monkeypatch.setattr(binance_daily, '_request', forbidden)
    first = AttemptBudget(tmp_path, market)
    with pytest.raises(binance_daily.SourceError) as blocked:
        first.seed()
    assert blocked.value.code == 'PROVIDER_RATE_CIRCUIT'
    replacement = AttemptBudget(tmp_path, market)
    with pytest.raises(binance_daily.SourceError):
        replacement.seed()
    assert replacement.evidence()['seed_total'] == 2
    assert not calls and ledger.read_text() == '9999999999 9999999999'


@pytest.mark.parametrize('market', ['spot', 'perp'])
def test_provider_pause_does_not_block_capture_process_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: Market,
) -> None:
    from datetime import UTC, datetime, timedelta
    from origo.workers import book_capture
    from origo.workers.runtime import heartbeat_path, touch_heartbeat

    def unavailable(*args: object, **kwargs: object) -> binance_daily.Response:
        raise binance_daily.SourceError('PROVIDER_RATE_CIRCUIT', 'Controlled provider cooldown.')

    monkeypatch.setattr(book_capture, 'get_response', unavailable)
    budget = AttemptBudget(tmp_path / 'locks', market)
    sampler = book_capture.BookSampler(tmp_path / 'spool', market)
    for _ in range(3):
        with pytest.raises(binance_daily.SourceError, match='Controlled provider cooldown'):
            budget.seed()
    with pytest.raises(binance_daily.SourceError) as limited:
        budget.seed()
    assert limited.value.code == 'BOOK_ATTEMPT_LIMIT'
    directory = tmp_path / 'heartbeats'
    touch_heartbeat(heartbeat_path(directory, f'book_capture_{market}'))
    book_capture.publish_status(sampler, budget, book_capture.status_path(directory, market), limited.value.code)
    now = datetime.now(UTC)
    assert book_capture.check_capture(directory, market, now) == 0
    assert sampler.book is None
    with pytest.raises(binance_daily.SourceError) as unavailable_book:
        sampler.top20(now)
    assert unavailable_book.value.code == 'BOOK_UNAVAILABLE'
    assert book_capture.check_capture(directory, market, now + timedelta(seconds=181)) == 1
