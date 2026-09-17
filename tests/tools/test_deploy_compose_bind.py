from __future__ import annotations

from pathlib import Path
from typing import Final, NamedTuple

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DEPLOY_COMPOSE: Final[Path] = REPO_ROOT / 'docker-compose.deploy.yml'

LOOPBACK: Final[str] = '127.0.0.1'
LONG_FORM_KEYS: Final[frozenset[str]] = frozenset(
    {'target', 'published', 'host_ip', 'protocol', 'mode', 'name', 'app_protocol'}
)


class PublishedPort(NamedTuple):
    service: str
    host_ip: str  # '' means every interface (no explicit bind)
    published: str
    target: str
    raw: str


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(' '))


def _clean(value: str) -> str:
    return value.strip().strip('"').strip("'")


def _from_short_form(service: str, raw: str) -> PublishedPort:
    """'[ip:]published[:target]' (protocol suffix stripped) -> PublishedPort."""
    value = _clean(raw).split('/', 1)[0]
    parts = value.split(':')
    if len(parts) == 3:
        host_ip, published, target = parts
    elif len(parts) == 2:
        host_ip, published, target = '', parts[0], parts[1]
    else:
        host_ip, published, target = '', '', parts[0]
    return PublishedPort(service, host_ip, published, target, raw.strip())


def _from_long_form(service: str, fields: dict[str, str]) -> PublishedPort:
    return PublishedPort(
        service,
        fields.get('host_ip', ''),
        fields.get('published', ''),
        fields.get('target', ''),
        str(fields),
    )


def _parse_inline_mapping(item: str) -> dict[str, str]:
    body = item.strip().lstrip('{').rstrip('}')
    fields: dict[str, str] = {}
    for pair in body.split(','):
        if ':' in pair:
            key, _, val = pair.partition(':')
            fields[key.strip()] = _clean(val)
    return fields


def _published_ports() -> list[PublishedPort]:
    """Every published port in the deploy compose (short-form or long-form).

    Hand-parsed with the stdlib only, matching the other pure-stdlib contract
    gates: walk the indentation services: -> <service>: -> ports: and collect
    each list item, normalising the "ip:host:container" short form, the inline
    ``{host_ip, published, target}`` flow map, and the block long form.
    """
    lines = DEPLOY_COMPOSE.read_text(encoding='utf-8').splitlines()
    ports: list[PublishedPort] = []

    in_services = False
    service = ''
    service_indent = -1
    in_ports = False
    ports_indent = -1
    block: dict[str, str] | None = None

    def flush() -> None:
        nonlocal block
        if block is not None:
            ports.append(_from_long_form(service, block))
            block = None

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        indent = _indent(line)

        if indent == 0:
            flush()
            in_services = stripped == 'services:'
            service, in_ports = '', False
            continue
        if not in_services:
            continue

        # Inside a ports: list?
        if in_ports and indent > ports_indent:
            if stripped.startswith('- '):
                flush()
                item = stripped[2:].strip()
                if item.startswith('{'):
                    ports.append(_from_long_form(service, _parse_inline_mapping(item)))
                elif item.split(':', 1)[0].strip() in LONG_FORM_KEYS and ':' in item:
                    block = {}
                    key, _, val = item.partition(':')
                    if _clean(val):
                        block[key.strip()] = _clean(val)
                else:
                    ports.append(_from_short_form(service, item))
            elif block is not None:  # continuation of a block long-form entry
                key, _, val = stripped.partition(':')
                block[key.strip()] = _clean(val)
            continue

        # Not (or no longer) inside a ports list.
        flush()
        in_ports = False

        if service == '' or indent <= service_indent:
            if stripped.endswith(':'):
                service = stripped[:-1]
                service_indent = indent
            continue
        if stripped == 'ports:':
            in_ports = True
            ports_indent = indent

    flush()
    return ports


def test_all_published_ports_bind_loopback() -> None:
    exposed = [f'{p.service}: {p.raw}' for p in _published_ports() if p.host_ip != LOOPBACK]
    assert exposed == []


def test_dagit_bound_to_loopback() -> None:
    dagit = [p for p in _published_ports() if p.service == 'dagit']
    assert len(dagit) == 1
    assert (dagit[0].host_ip, dagit[0].published, dagit[0].target) == (LOOPBACK, '4000', '3000')


def test_recovery_requires_positive_container_retirement(tmp_path: Path) -> None:
    import os
    import subprocess
    import textwrap

    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    start = workflow.index('          prior_workers=()')
    end_marker = '            "${recovery_args[@]}"'
    end = workflow.index(end_marker, start) + len(end_marker)
    recovery = textwrap.dedent(workflow[start:end])
    # Execute the deployed shell path against Docker responses, including failures.
    docker = r'''
set -euo pipefail
PROJECT_NAME=test
function docker() {
    case "$*" in
      'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit')
        printf '%s\n' old-daemon old-ui ;;
      'inspect --format {{.Id}} {{.Config.Hostname}} old-daemon')
        printf '%s\n' 'old-daemon daemon-host' ;;
      'inspect --format {{.Id}} {{.Config.Hostname}} old-ui')
        printf '%s\n' 'old-ui ui-host' ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 clickhouse dagster dagit monitor vector depth-worker provisional-worker')
        return 0 ;;
      'ps -aq --no-trunc')
        if [ "$RETIREMENT_CASE" = inventory-error ]; then return 1; fi
        if [ "$RETIREMENT_CASE" != removed ]; then printf '%s\n' old-daemon old-ui; fi
        printf '%s\n' new-daemon new-ui ;;
      'inspect --format {{.State.Running}} '*)
        if [ "$RETIREMENT_CASE" = inspect-error ]; then return 1; fi
        if [ "$RETIREMENT_CASE" = running ]; then printf true; else printf false; fi ;;
      'compose -p test -f docker-compose.deploy.yml exec -T dagster sh -c '*)
        return 0 ;;
      'compose -p test -f docker-compose.deploy.yml exec -T dagster python -m origo.orchestration.recovery'*)
        printf '%s\n' "$@" > "$RECOVERY_CALL" ;;
      *) printf 'Unexpected Docker call: %s\n' "$*" >&2; return 2 ;;
    esac
}
'''
    for case in ('removed', 'stopped', 'running', 'inventory-error', 'inspect-error'):
        invocation = tmp_path / case
        result = subprocess.run(
            ['bash', '-c', docker + recovery],
            env={**os.environ, 'RETIREMENT_CASE': case, 'RECOVERY_CALL': str(invocation)},
            capture_output=True,
            text=True,
        )
        if case.endswith('-error'):
            assert result.returncode != 0
            assert not invocation.exists()
        else:
            assert result.returncode == 0, result.stderr
            arguments = invocation.read_text().splitlines()
            assert arguments[arguments.index('--deadline-seconds') + 1] == '900'
            if case == 'running':
                assert '--retired-worker' not in arguments
                assert '--legacy-before' not in arguments
            else:
                assert arguments.count('--retired-worker') == 2
                assert 'daemon-host' in arguments and 'ui-host' in arguments
                assert int(arguments[arguments.index('--legacy-before') + 1]) > 0
