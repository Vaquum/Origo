<div align="center">
  <br />
  <a href="https://github.com/Vaquum"><img src="https://github.com/Vaquum/Home/raw/main/assets/Logo.png" alt="Vaquum" width="150" /></a>
  <br />
</div>
<br />
<div align="center"><b>Vaquum Origo turns raw Binance market archives into checksum-verified trade tables, rebuildable bar projections, and replayable research datasets.</b></div>

<div align="center">
  <a href="#origo">Origo</a> •
  <a href="#what-origo-is-not">What Origo Is Not</a> •
  <a href="#capabilities">Capabilities</a> •
  <a href="#first-backfill">First Backfill</a> •
  <a href="#learn-more">Learn More</a>
</div>
<br />
<div align="center">
  <a href="https://docs.vaquum.fi/origo/"><img src="https://img.shields.io/badge/docs-origo-blue" alt="Origo docs" /></a>
  <a href="https://github.com/Vaquum/Origo/actions/workflows/pr_checks_tests.yml"><img src="https://github.com/Vaquum/Origo/actions/workflows/pr_checks_tests.yml/badge.svg" alt="PR tests" /></a>
  <a href="https://github.com/Vaquum/Origo/actions/workflows/deploy_on_merge.yml"><img src="https://github.com/Vaquum/Origo/actions/workflows/deploy_on_merge.yml/badge.svg" alt="Deploy on merge" /></a>
  <a href="https://github.com/Vaquum/Origo/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license" /></a>
</div>

<hr />

<a id="origo"></a>

# Origo — Data layer

*Event-sourced Bitcoin market data platform that turns raw Binance archives into checksum-verified trade tables, rebuildable bar projections, and replayable research datasets.*

Origo ingests Binance BTCUSDT market data into ClickHouse under Dagster orchestration, records every ingested source file in a checksum ledger, and rebuilds every daily-partitioned projection deterministically from that raw record; the rolling `_latest` tables and live depth snapshots capture the span after the last daily load and sit outside that rebuild guarantee. The Dagster deployment in this repository is Origo's control plane — the term names that orchestration component, not the project. The project evolves from tdw-control-plane, this repository's original identity.

## What Origo Is Not

Origo is not:

- a trade execution system
- a signal research or backtesting engine
- a multi-exchange market data vendor

In the wider Vaquum architecture, Origo sits upstream as the data layer. Limen consumes its outputs downstream as the research engine, and Nexus, Praxis, and Veritas sit further downstream for decisioning, execution, and oversight.

## Capabilities

- Revisioned spot source: checksum-verified Binance BTCUSDT daily trade archives (from 2017-08-17) built per day into raw trades, 1-minute klines, dollar, volume, tick and dollar-imbalance bars and the aligned 1-minute rows, with provisional minute intervals covering the open day, activation history, reconciliation and rollback
- Legacy spot table names (`binance_daily_spot_trades`, `binance_spot_klines`, the bar tables and the `_latest` names) served as views over the source's current state; the spot aligned rows are `binance_spot_trades_aligned_current`
- Revisioned perp source: checksum-verified Binance USDT-M perpetual BTCUSDT daily trade archives (from 2019-09-08) built per day into raw trades, 1-minute klines, dollar, volume, tick and dollar-imbalance bars and the aligned 1-minute rows, with provisional minute intervals covering the open day, activation history, reconciliation and rollback
- Revisioned spot aggregate source (CANARY): checksum-verified Binance BTCUSDT daily aggregate-trade archives (from 2017-08-17) built per day into raw aggregates, 1-minute klines, dollar, volume, tick and dollar-imbalance bars and the aligned 1-minute rows, with provisional minute intervals covering the open day; tick bars count aggregate events and dollar bars accumulate aggregate notional
- Revisioned perp aggregate source (CANARY): checksum-verified Binance USDT-M perpetual BTCUSDT daily aggregate-trade archives (from 2019-12-31) built per day into raw aggregates, 1-minute klines, dollar, volume, tick and dollar-imbalance bars and the aligned 1-minute rows, with provisional minute intervals covering the open day; tick bars count aggregate events and dollar bars accumulate aggregate notional
- Binance spot order-book depth snapshots (20- and 200-level) with 1-minute projections and per-minute reconciliation
- Hugging Face publishing of twelve kline datasets (six time intervals and six dollar-bar sizes), the local monthly Parquet mirror of the same series and the versioned mmap-ready Arrow bar store, all published by the revisioned spot source from one pinned state
- Ratcheted CI gates on every PR: strict pyright typing, fail-loud (no silent fallbacks), Conventional Commits, and version plus CHANGELOG trails
- Automatic production deploy of merged `main` through GitHub Actions

## First Backfill

The first runnable path is the local Docker Compose stack: prepare the spot source, backfill one day, and read it back through the query module.

1. Clone the repository and set the one required secret:

```bash
git clone https://github.com/Vaquum/Origo.git
cd Origo
export CLICKHOUSE_PASSWORD=<choose-a-password>
```

Supported runtime: the containers run Python 3.11 (`python:3.11.12`) and the package requires Python `>=3.11`; the host needs Docker with Compose. `CLICKHOUSE_PASSWORD` is the only variable without a default — `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, and `CLICKHOUSE_DATABASE` default to the in-network values (`clickhouse`, `9000` native / `8123` HTTP, `default`, `origo`). `HF_TOKEN` and `HUGGINGFACE_DATASET_REPO_ID` are needed only for Hugging Face publishing, and the `BINANCE_SPOT_DEPTH20_*` / `BINANCE_SPOT_DEPTH200_*` pairs only for the live depth collectors; `docker-compose.deploy.yml` requires them all, the local `docker-compose.yml` does not. The local compose file passes `CLICKHOUSE_PASSWORD` only to the ClickHouse server, so the commands below inject it into the Dagster container with `-e`.

1. Start the stack:

```bash
docker compose up -d --build
```

1. Prepare the spot source (the stack's bootstrap does this on start; the job repeats it idempotently):

```bash
docker compose exec -e CLICKHOUSE_PASSWORD dagster \
  dagster job launch -m origo.definitions -j prepare_revisioned_sources_job
```

1. Backfill one day of the spot source — raw trades, every bar projection and the aligned rows, then the public files:

```bash
docker compose exec -e CLICKHOUSE_PASSWORD dagster \
  dagster job backfill -m origo.definitions \
  -j backfill_binance_spot_trades_source_job \
  --partitions 2024-01-02 --noprompt
```

1. Read the day back through the query module:

```bash
docker compose exec -e CLICKHOUSE_PASSWORD dagster python -c "
from origo.query.binance_spot_kline_rollups import dollar_month, time_month
print(time_month(interval_minutes=60, year=2024, month=1))
print(dollar_month(ratio=15, year=2024, month=1))
"
```

`time_month` rolls the 1-minute projection up to any minute interval, and `dollar_month` rolls the 1M-dollar bar base up by an integer ratio. Both read the legacy table names, which are views over the spot source: `binance_spot_klines` and `binance_spot_dollar_klines` hold the canonical days, and `binance_spot_klines_latest` and `binance_spot_trades_latest` the provisional minutes of the open day. Beyond the quickstart, every job, schedule, and sensor is defined in `origo/definitions.py` and can be launched from the Dagster UI at `http://localhost:4000`.

## Risk Boundary

Origo is research software. Data outputs are not investment advice, trading advice, execution simulation, regulatory approval, or a promise of future performance. Past performance is not predictive, and trading digital assets can result in total loss of capital.

## Learn more

- Start with the full [documentation hub](https://docs.vaquum.fi/origo/overview/docs-hub)
- See [What is Origo](https://docs.vaquum.fi/origo/overview/what-is-origo), [Product Boundaries](https://docs.vaquum.fi/origo/overview/product-boundaries), and [System Architecture](https://docs.vaquum.fi/origo/overview/system-architecture) for scope and shape
- Use [Get Started Locally](https://docs.vaquum.fi/origo/guides/get-started-locally) for the local stack
- Run [your first native query](https://docs.vaquum.fi/origo/guides/run-your-first-native-query) and [your first aligned query](https://docs.vaquum.fi/origo/guides/run-your-first-aligned-query) against the warehouse
- Rebuild derived tables with [Rebuild Projections](https://docs.vaquum.fi/origo/guides/rebuild-projections) and check coverage with [Understand Historical Coverage](https://docs.vaquum.fi/origo/guides/understand-historical-coverage)
- Export datasets with [Export Data](https://docs.vaquum.fi/origo/guides/export-data)
- See the mechanical PR gates in [AGENTS.md](https://github.com/Vaquum/Origo/blob/main/AGENTS.md) and their implementations under [tools](https://github.com/Vaquum/Origo/tree/main/tools)
- Follow the change record in [CHANGELOG.md](https://github.com/Vaquum/Origo/blob/main/CHANGELOG.md)
- Contribute through [Contributing](https://docs.vaquum.fi/origo/developer/contributing) and the [Developer docs](https://docs.vaquum.fi/origo/developer)

## Contributing

Contribution starts through [AGENTS.md](https://github.com/Vaquum/Origo/blob/main/AGENTS.md), the [Contributing guide](https://docs.vaquum.fi/origo/developer/contributing), or [open issues](https://github.com/Vaquum/Origo/issues). Before contributing, start with the [Developer docs](https://docs.vaquum.fi/origo/developer).

## Support

Use [open issues](https://github.com/Vaquum/Origo/issues) for support requests and scope questions.

## Vulnerabilities

Report vulnerabilities privately to the repository owner, [@mikkokotila](https://github.com/mikkokotila). Do not report vulnerabilities through public issues.

## Citations

Published work should cite:

Vaquum Origo [Computer software]. (2026). Retrieved from [GitHub](https://github.com/Vaquum/Origo).

## License

[MIT License](https://github.com/Vaquum/Origo/blob/main/LICENSE).
