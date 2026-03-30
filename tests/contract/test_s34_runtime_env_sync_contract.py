from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_EXAMPLE = _REPO_ROOT / '.env.example'
_DEPLOY_WORKFLOW = _REPO_ROOT / '.github' / 'workflows' / 'deploy-on-merge.yml'


def _load_env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in _ENV_EXAMPLE.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key] = value
    return values


def test_env_example_defines_runtime_worker_and_timeout_contract() -> None:
    env_values = _load_env_example()

    assert env_values['ORIGO_S34_ETF_PARTITION_WORKERS'] == '10'
    assert env_values['ORIGO_S34_FRED_PARTITION_WORKERS'] == '10'
    assert env_values['ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS'] == '120'
    assert env_values['ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS'] == '120'
    assert env_values['ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS'] == '120'


def test_deploy_workflow_syncs_required_runtime_worker_and_timeout_envs() -> None:
    workflow_text = _DEPLOY_WORKFLOW.read_text(encoding='utf-8')

    required_fragments = (
        'read_root_env_example_value ORIGO_S34_ETF_PARTITION_WORKERS',
        'ORIGO_S34_ETF_PARTITION_WORKERS must be present and non-empty in .env.example.',
        'ORIGO_S34_ETF_PARTITION_WORKERS in .env.example must be an integer >= 10.',
        'origo_s34_etf_partition_workers_b64',
        "ORIGO_S34_ETF_PARTITION_WORKERS_B64='${origo_s34_etf_partition_workers_b64}'",
        'decode_env_b64_required ORIGO_S34_ETF_PARTITION_WORKERS_B64 ORIGO_S34_ETF_PARTITION_WORKERS',
        'require_env ORIGO_S34_ETF_PARTITION_WORKERS',
        'ORIGO_S34_ETF_PARTITION_WORKERS must be an integer >= 10.',
        '$0 !~ /^ORIGO_S34_ETF_PARTITION_WORKERS=/ &&',
        'echo "ORIGO_S34_ETF_PARTITION_WORKERS=${ORIGO_S34_ETF_PARTITION_WORKERS}"',
        'echo "  ORIGO_S34_ETF_PARTITION_WORKERS=${ORIGO_S34_ETF_PARTITION_WORKERS}"',
        'read_root_env_example_value ORIGO_S34_FRED_PARTITION_WORKERS',
        'ORIGO_S34_FRED_PARTITION_WORKERS must be present and non-empty in .env.example.',
        'ORIGO_S34_FRED_PARTITION_WORKERS in .env.example must be an integer >= 10.',
        'origo_s34_fred_partition_workers_b64',
        "ORIGO_S34_FRED_PARTITION_WORKERS_B64='${origo_s34_fred_partition_workers_b64}'",
        'decode_env_b64_required ORIGO_S34_FRED_PARTITION_WORKERS_B64 ORIGO_S34_FRED_PARTITION_WORKERS',
        'require_env ORIGO_S34_FRED_PARTITION_WORKERS',
        'ORIGO_S34_FRED_PARTITION_WORKERS must be an integer >= 10.',
        '$0 !~ /^ORIGO_S34_FRED_PARTITION_WORKERS=/ &&',
        'echo "ORIGO_S34_FRED_PARTITION_WORKERS=${ORIGO_S34_FRED_PARTITION_WORKERS}"',
        'echo "  ORIGO_S34_FRED_PARTITION_WORKERS=${ORIGO_S34_FRED_PARTITION_WORKERS}"',
        'read_root_env_example_value ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS',
        'ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS must be present and non-empty in .env.example.',
        'ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS in .env.example must be an integer > 0.',
        'origo_binance_source_http_timeout_seconds_b64',
        "ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS_B64='${origo_binance_source_http_timeout_seconds_b64}'",
        'decode_env_b64_required ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS_B64 ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS',
        'require_env ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS',
        'ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS must be an integer > 0.',
        '$0 !~ /^ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS=/ &&',
        'echo "ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS=${ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS}"',
        'echo "  ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS=${ORIGO_BINANCE_SOURCE_HTTP_TIMEOUT_SECONDS}"',
        'read_root_env_example_value ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS',
        'ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS must be present and non-empty in .env.example.',
        'ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS in .env.example must be an integer > 0.',
        'origo_okx_source_http_timeout_seconds_b64',
        "ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS_B64='${origo_okx_source_http_timeout_seconds_b64}'",
        'decode_env_b64_required ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS_B64 ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS',
        'require_env ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS',
        'ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS must be an integer > 0.',
        '$0 !~ /^ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS=/ &&',
        'echo "ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS=${ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS}"',
        'echo "  ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS=${ORIGO_OKX_SOURCE_HTTP_TIMEOUT_SECONDS}"',
        'read_root_env_example_value ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS',
        'ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS must be present and non-empty in .env.example.',
        'ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS in .env.example must be an integer > 0.',
        'origo_bybit_source_http_timeout_seconds_b64',
        "ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS_B64='${origo_bybit_source_http_timeout_seconds_b64}'",
        'decode_env_b64_required ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS_B64 ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS',
        'require_env ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS',
        'ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS must be an integer > 0.',
        '$0 !~ /^ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS=/ &&',
        'echo "ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS=${ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS}"',
        'echo "  ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS=${ORIGO_BYBIT_SOURCE_HTTP_TIMEOUT_SECONDS}"',
    )

    for fragment in required_fragments:
        assert fragment in workflow_text
