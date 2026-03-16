<h1 align="center">
  <br>
  <a href="https://github.com/Vaquum"><img src="https://github.com/Vaquum/Home/raw/main/assets/Logo.png" alt="Vaquum" width="150"></a>
  <br>
</h1>

<h3 align="center">Vaquum Origo is an event-sourced market data platform for deterministic, replayable financial data access.</h3>

<p align="center">
  <a href="#value-proposition">Value Proposition</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#contributing">Contributing</a> •
  <a href="#license">License</a>
</p>
<hr>

# Value Proposition

Origo is an event-sourced market data platform that delivers multi-exchange trade data, ETF data, blockchain data, and global liquidity signals through deterministic source-native endpoints or a zero-wrangling 1-second normalized master table. Replayable, auditable, and schema-strict by design, it turns fragmented financial data into a reliable query surface.

# Quick Start

1) Clone the repository and prepare env vars:

```bash
git clone https://github.com/Vaquum/Origo.git
cd Origo
cp .env.example .env
```

2) Bootstrap the local stack (ClickHouse + Dagster + API):

```bash
scripts/s7_docker_stack.sh bootstrap
```

3) Query source-native data (`mode=native`):

```bash
export ORIGO_INTERNAL_API_KEY='replace-with-internal-key'
curl -sS -X POST 'http://localhost:18000/v1/raw/query' \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: ${ORIGO_INTERNAL_API_KEY}" \
  --data '{
    "mode":"native",
    "sources":["binance_spot_trades"],
    "time_range":["2017-08-17T12:00:00Z","2017-08-17T13:00:00Z"],
    "strict":false
  }'
```

4) Query the 1-second normalized table (`mode=aligned_1s`):

```bash
curl -sS -X POST 'http://localhost:18000/v1/raw/query' \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: ${ORIGO_INTERNAL_API_KEY}" \
  --data '{
    "mode":"aligned_1s",
    "sources":["binance_spot_trades","okx_spot_trades","bybit_spot_trades","etf_daily_metrics","fred_series_metrics"],
    "time_range":["2024-01-01T00:00:00Z","2024-01-01T00:05:00Z"],
    "strict":false
  }'
```

# Contributing

The simplest way to start contributing is by [joining an open discussion](https://github.com/Vaquum/Origo/issues?q=is%3Aissue%20state%3Aopen%20label%3Aquestion%2Fdiscussion), contributing to [the docs](https://github.com/Vaquum/Origo/tree/main/docs), or by [picking up an open issue](https://github.com/Vaquum/Origo/issues?q=is%3Aissue%20state%3Aopen%20label%3Abug%20OR%20label%3Aenhancement%20OR%20label%3A%22good%20first%20issue%22%20OR%20label%3A%22help%20wanted%22%20OR%20label%3APriority%20OR%20label%3Aprocess).

**Before contributing, make sure to start by reading through the** [Developer Documentation](https://github.com/Vaquum/Origo/tree/main/docs/Developer).

# Vulnerabilities

Report vulnerabilities privately through [GitHub Security Advisories](https://github.com/Vaquum/Origo/security/advisories/new).

# Citations

If you use Origo for published work, please cite:

Vaquum Origo [Computer software]. (2026). Retrieved from http://github.com/vaquum/origo.

# License

[MIT License](https://github.com/Vaquum/Origo/blob/main/LICENSE).
