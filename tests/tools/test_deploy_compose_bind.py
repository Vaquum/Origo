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
    exposed = [
        f'{p.service}: {p.raw}'
        for p in _published_ports()
        if p.host_ip != LOOPBACK
        and (p.service, p.host_ip, p.published, p.target) != ('law', '0.0.0.0', '8484', '8484')
    ]
    assert exposed == []


def test_dagit_bound_to_loopback() -> None:
    dagit = [p for p in _published_ports() if p.service == 'dagit']
    assert len(dagit) == 1
    assert (dagit[0].host_ip, dagit[0].published, dagit[0].target) == (LOOPBACK, '4000', '3000')


def test_only_provisional_workers_select_a_source_and_compute_their_heartbeat() -> None:
    for name in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        compose = (REPO_ROOT / name).read_text()
        assert 'ORIGO_WORKER_HEARTBEAT' not in compose
        assert compose.count('ORIGO_PROVISIONAL_SOURCE:') == 4


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
    docker = r"""
set -euo pipefail
PROJECT_NAME=test
function docker() {
    case "$*" in
      'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit')
        printf '%s\n' old-daemon old-ui ;;
      'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit provisional-worker')
        printf '%s\n' old-daemon old-ui old-spot ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 --force-recreate dagster dagit provisional-worker')
        return 0 ;;
      'inspect --format {{.Id}} {{.Config.Hostname}} old-daemon')
        printf '%s\n' 'old-daemon daemon-host' ;;
      'inspect --format {{.Id}} {{.Config.Hostname}} old-ui')
        printf '%s\n' 'old-ui ui-host' ;;
      'inspect --format {{.Id}} '*)
        printf '%s\n' "${@: -1}" ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 clickhouse dagster dagit monitor vector depth-worker provisional-worker provisional-binance-perp-trades provisional-binance-spot-aggtrades provisional-binance-perp-aggtrades law market-state')
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
"""
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


def test_cube_activation_requires_all_previous_writers_retired(tmp_path: Path) -> None:
    import os
    import subprocess
    import textwrap

    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    start = workflow.index('          prior_workers=()')
    end = workflow.index("          echo 'post-up: ClickHouse smoke test starting'", start)
    rollout = textwrap.dedent(workflow[start:end])
    docker = r"""
set -euo pipefail
PROJECT_NAME=test
function docker() {
    case "$*" in
      'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit')
        printf '%s\n' old-daemon old-ui ;;
      'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit provisional-worker')
        printf 'inventory\n' >> "$CALLS"
        if [ "$RETIREMENT_CASE" = prior-ps-error ]; then return 1; fi
        printf '%s\n' old-daemon old-ui old-spot ;;
      'inspect --format {{.Id}} {{.Config.Hostname}} old-daemon')
        printf '%s\n' 'old-daemon daemon-host' ;;
      'inspect --format {{.Id}} {{.Config.Hostname}} old-ui')
        printf '%s\n' 'old-ui ui-host' ;;
      'inspect --format {{.Id}} '*)
        if [ "$RETIREMENT_CASE" = prior-inspect-error ]; then return 1; fi
        printf '%s\n' "${@: -1}" ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 --force-recreate dagster dagit provisional-worker')
        printf 'recreate\n' >> "$CALLS" ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 clickhouse dagster dagit monitor vector depth-worker provisional-worker provisional-binance-perp-trades provisional-binance-spot-aggtrades provisional-binance-perp-aggtrades law market-state')
        printf 'up\n' >> "$CALLS" ;;
      'ps -aq --no-trunc')
        if [ "$RETIREMENT_CASE" = inventory-error ]; then return 1; fi
        if [ "$RETIREMENT_CASE" != removed ]; then
            printf '%s\n' old-daemon old-ui old-spot
        fi
        printf '%s\n' new-daemon new-ui new-spot ;;
      'inspect --format {{.State.Running}} '*)
        printf 'inspect %s\n' "${@: -1}" >> "$CALLS"
        if [ "$RETIREMENT_CASE" = inspect-error ]; then return 1; fi
        if [ "${@: -1}" = old-spot ]; then
            if [ "$RETIREMENT_CASE" = spot-inspect-error ]; then return 1; fi
            if [ "$RETIREMENT_CASE" = spot-running ]; then printf true; return 0; fi
            if [ "$RETIREMENT_CASE" = spot-unknown ]; then printf unknown; return 0; fi
        fi
        if [ "$RETIREMENT_CASE" = running ]; then printf true; else printf false; fi ;;
      'compose -p test -f docker-compose.deploy.yml exec -T dagster sh -c '*)
        return 0 ;;
      'compose -p test -f docker-compose.deploy.yml exec -T dagster python -m origo.orchestration.recovery'*)
        printf 'recovery\n' >> "$CALLS" ;;
      'compose -p test -f docker-compose.deploy.yml exec -T dagster python -m origo.sources.rollout')
        if read -r swallowed; then
            printf 'Activation consumed deployment stdin: %s\n' "$swallowed" >&2
            return 2
        fi
        printf 'activate\n' >> "$CALLS" ;;
      *) printf 'Unexpected Docker call: %s\n' "$*" >&2; return 2 ;;
    esac
}
"""
    for case in (
        'removed', 'stopped', 'repeat', 'running', 'spot-running', 'spot-unknown',
        'prior-ps-error', 'prior-inspect-error', 'inventory-error', 'inspect-error',
        'spot-inspect-error',
    ):
        calls = tmp_path / f'cube-{case}'
        attempts = 2 if case == 'repeat' else 1
        result = subprocess.run(
            ['bash', '-s'],
            input=docker + (rollout + '\n') * attempts + '\nprintf "after-rollout\\n" >> "$CALLS"\n',
            env={**os.environ, 'RETIREMENT_CASE': case, 'CALLS': str(calls)},
            capture_output=True,
            text=True,
        )
        observed = calls.read_text().splitlines()
        if case in ('removed', 'stopped', 'repeat'):
            assert result.returncode == 0, result.stderr
            phases = [call for call in observed if not call.startswith('inspect ')]
            assert phases == (
                ['inventory', 'recreate', 'up', 'recovery', 'activate'] * attempts
                + ['after-rollout']
            )
            if case != 'removed':
                assert observed.count('inspect old-spot') == attempts
        else:
            assert result.returncode != 0, case
            assert 'activate' not in observed and 'after-rollout' not in observed
            if case.startswith('prior-'):
                assert 'recreate' not in observed
            if case in ('inventory-error', 'inspect-error'):
                assert 'recovery' not in observed
            if case.startswith('spot-'):
                assert 'inspect old-spot' in observed


def test_egress_preflight_detaches_stdin_and_precedes_replacement(tmp_path: Path) -> None:
    import os
    import subprocess
    import textwrap

    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    start = workflow.index('          bash deploy/prepare_binance_egress.sh')
    end = workflow.index('          # Bootstrap runs', start)
    invocation = textwrap.dedent(workflow[start:end])
    deploy = tmp_path / 'deploy'
    deploy.mkdir()
    (deploy / 'prepare_binance_egress.sh').write_text(
        'printf "setup\\n" >> "$CALLS"\nexit "$SETUP_EXIT"\n'
    )
    stub = r"""
set -euo pipefail
PROJECT_NAME=test
function docker() {
    case "$*" in
      'compose -p test -f docker-compose.deploy.yml run --rm --no-deps -T --entrypoint python provisional-binance-perp-trades -c '*)
        printf 'preflight\n' >> "$CALLS"
        if read -r swallowed; then
            printf 'Preflight consumed deployment stdin: %s\n' "$swallowed" >&2
            return 2
        fi
        case "$*" in
          *'socket.AF_INET'*'connection.bind((ip, 0))'*) ;;
          *) return 3 ;;
        esac
        return "$PREFLIGHT_EXIT" ;;
      'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit'|'compose -p test -f docker-compose.deploy.yml ps -q dagster dagit provisional-worker')
        return 0 ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 --force-recreate dagster dagit provisional-worker')
        printf 'recreate\n' >> "$CALLS" ;;
      'compose -p test -f docker-compose.deploy.yml up -d --wait --wait-timeout 600 clickhouse dagster dagit monitor vector depth-worker provisional-worker provisional-binance-perp-trades provisional-binance-spot-aggtrades provisional-binance-perp-aggtrades law market-state')
        printf 'up\n' >> "$CALLS" ;;
      *) printf 'Unexpected Docker call: %s\n' "$*" >&2; return 2 ;;
    esac
}
"""
    for setup_exit, preflight_exit, expected in (
        ('0', '0', ['setup', 'preflight', 'recreate', 'up', 'after-up']),
        ('1', '0', ['setup']),
        ('0', '1', ['setup', 'preflight']),
    ):
        calls = tmp_path / f'calls-{setup_exit}-{preflight_exit}'
        result = subprocess.run(
            ['bash', '-s'],
            input=stub + invocation + '\nprintf "after-up\\n" >> "$CALLS"\n',
            cwd=tmp_path,
            env={
                **os.environ,
                'CALLS': str(calls),
                'SETUP_EXIT': setup_exit,
                'PREFLIGHT_EXIT': preflight_exit,
            },
            capture_output=True,
            text=True,
        )
        assert (result.returncode == 0) == (setup_exit == preflight_exit == '0'), result.stderr
        assert calls.read_text().splitlines() == expected


def test_egress_setup_validates_before_install_and_preserves_primary(tmp_path: Path) -> None:
    import os
    import subprocess

    # Generated networkd shape from the production eno1 contract; no market data.
    network = """[Match]
Name=eno1

[Network]
Address=37.27.112.167/32
Address=2a01:4f9:3070:2304::2/64
DNS=185.12.64.1
DNS=2a01:4ff:ff00::add:2
DNS=185.12.64.2
DNS=2a01:4ff:ff00::add:1

[Route]
Destination=0.0.0.0/0
Gateway=37.27.112.129
GatewayOnLink=true

[Route]
Destination=::/0
Gateway=fe80::1
GatewayOnLink=true
"""
    fixture = tmp_path / 'eno1.network'
    fixture.write_text(network)
    # Redirect only host-side commands: execute the actual setup/validation script.
    stub = r"""
set -euo pipefail
function cp() {
    if [ "$1" = -a ] && [ "$2" = /etc/netplan ]; then
        mkdir -p "$3/netplan"
        if [ -f "$INSTALLED" ]; then
            command cp "$INSTALLED" "$3/netplan/60-origo-egress.yaml"
        fi
    else command cp "$@"; fi
}
function netplan() {
    test "$1" = generate && test "$2" = --root-dir
    printf 'generate\n' >> "$CALLS"
    mkdir -p "$3/run/systemd/network"
    local generated="$3/run/systemd/network/10-netplan-eno1.network"
    command cp "$BASE_NETWORK" "$generated"
    if [ -f "$3/etc/netplan/60-origo-egress.yaml" ]; then
        if [ "$SETUP_CASE" = generate-fails ] || [ "$SETUP_CASE" = initial-failure ]; then return 1; fi
        printf 'Address=37.27.112.140/32\nAddress=37.27.112.144/32\n' >> "$generated"
        if [ "$SETUP_CASE" = changed-dns ] && [ "$(grep -c generate "$CALLS")" = 2 ]; then printf 'DNS=1.1.1.1\n' >> "$generated"; fi
        if [ "$SETUP_CASE" = changed-primary ]; then
            python3 - "$generated" <<'UPDATE'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
path.write_text(path.read_text().replace('Address=37.27.112.167/32', 'Address=37.27.112.140/32'))
UPDATE
        fi
    fi
}
function install() {
    if [ "$4" = /etc/netplan/60-origo-egress.yaml ]; then
        printf 'install\n' >> "$CALLS"
        command install -m 600 "$3" "$INSTALLED"
    else command install "$@"; fi
}
function ip() {
    case "$*" in
      '-o -4 addr show dev eno1')
        awk '{print "2: eno1 inet " $1 " scope global eno1"}' "$ADDRESSES" ;;
      'addr add '*'/32 dev eno1')
        printf 'add %s\n' "$3" >> "$CALLS"
        printf '%s\n' "$3" >> "$ADDRESSES" ;;
      '-4 route get 1.1.1.1')
        printf 'route\n' >> "$CALLS"
        if [ "$SETUP_CASE" = changed-route ]; then
            printf '1.1.1.1 via 37.27.112.129 dev eno1 src 37.27.112.140\n'
        else printf '1.1.1.1 via 37.27.112.129 dev eno1 src 37.27.112.167\n'; fi ;;
      *) return 2 ;;
    esac
}
source "$SETUP_SCRIPT"
"""
    installed = tmp_path / '60-origo-egress.yaml'
    addresses = tmp_path / 'addresses'
    addresses.write_text('37.27.112.167/32\n')
    for case in (
        'initial-failure',
        'first',
        'repeat',
        'generate-fails',
        'changed-dns',
        'changed-primary',
        'changed-route',
    ):
        calls = tmp_path / f'calls-{case}'
        before = installed.read_bytes() if installed.exists() else None
        result = subprocess.run(
            ['bash', '-c', stub],
            env={
                **os.environ,
                'SETUP_SCRIPT': str(REPO_ROOT / 'deploy/prepare_binance_egress.sh'),
                'BASE_NETWORK': str(fixture),
                'INSTALLED': str(installed),
                'ADDRESSES': str(addresses),
                'CALLS': str(calls),
                'SETUP_CASE': case,
            },
            capture_output=True,
            text=True,
        )
        observed = calls.read_text().splitlines()
        if case in ('first', 'repeat'):
            assert result.returncode == 0, result.stderr
            assert observed[:3] == ['generate', 'generate', 'install']
            assert observed[-1] == 'route'
            assert installed.stat().st_mode & 0o777 == 0o600
            assert (
                installed.read_bytes() == (REPO_ROOT / 'deploy/60-origo-egress.yaml').read_bytes()
            )
            additions = [call for call in observed if call.startswith('add ')]
            assert additions == (
                ['add 37.27.112.140/32', 'add 37.27.112.144/32'] if case == 'first' else []
            )
        else:
            assert result.returncode != 0, case
            assert (installed.read_bytes() if installed.exists() else None) == before
            if case != 'changed-route':
                assert 'install' not in observed and not any(
                    call.startswith('add ') for call in observed
                )


def test_only_raw_perp_uses_host_network_with_deployment_identity() -> None:
    compose = DEPLOY_COMPOSE.read_text()
    raw_perp = compose.split('  provisional-binance-perp-trades:\n', 1)[1].split(
        '  provisional-binance-spot-aggtrades:', 1
    )[0]
    assert compose.count('network_mode: host') == 1
    for declaration in (
        'network_mode: host',
        'hostname: ${ORIGO_PERP_WORKER_HOSTNAME:?deployment identity required}',
        'CLICKHOUSE_HOST: 127.0.0.1',
        'DAGSTER_WEBSERVER_URL: http://127.0.0.1:4000',
        'ORIGO_PROVISIONAL_SOURCE: binance_perp_trades',
        'ORIGO_BINANCE_PERP_EGRESS_IPS: 37.27.112.140,37.27.112.144',
    ):
        assert declaration in raw_perp
    assert 'ports:' not in raw_perp
    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert (
        'ORIGO_PERP_WORKER_HOSTNAME: perp-${{ github.run_id }}-${{ github.run_attempt }}'
        in workflow
    )
    assert '"ORIGO_PERP_WORKER_HOSTNAME": os.environ["ORIGO_PERP_WORKER_HOSTNAME"]' in workflow


def test_law_volume_port_and_credentials_are_isolated() -> None:
    for filename in ('docker-compose.yml', 'docker-compose.deploy.yml'):
        text = (REPO_ROOT / filename).read_text()
        law = text.split('  law:\n', 1)[1].split('  vector:\n', 1)[0]
        assert '"0.0.0.0:8484:8484"' in law
        assert 'law-samples:/var/lib/origo-law:ro' in law
        assert 'read_only: true' in law and 'cpus: 0.5' in law and 'mem_limit: 256m' in law
        assert '"--check"' in law
        assert 'user: "65534:65534"' in law
        assert 'cap_drop: [ALL]' in law
        assert 'security_opt: ["no-new-privileges:true"]' in law
        for forbidden in (
            'environment:',
            'env_file:',
            'docker.sock',
            'network_mode:',
            'clickhouse-data:',
        ):
            assert forbidden not in law
        monitor = text.split('  monitor:\n', 1)[1].split('  law:\n', 1)[0]
        assert 'law-samples:/var/lib/origo-law\n' in monitor
        assert 'ORIGO_LAW_PAGE_URL=http://law:8485/healthz' in monitor
        assert text.count('law-samples:/var/lib/origo-law') == 2
    test_all_published_ports_bind_loopback()


def test_law_deploy_preserves_workers_egress_preflight_and_recovery() -> None:
    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    command = 'up -d --wait --wait-timeout 600 clickhouse dagster dagit monitor vector depth-worker provisional-worker provisional-binance-perp-trades provisional-binance-spot-aggtrades provisional-binance-perp-aggtrades law market-state'
    assert command in workflow
    assert workflow.index('bash deploy/prepare_binance_egress.sh') < workflow.index(command)
    assert workflow.index(command) < workflow.index('python -m origo.orchestration.recovery')
    assert '</dev/null' in workflow
    test_only_raw_perp_uses_host_network_with_deployment_identity()


def test_deployed_clickhouse_image_includes_law_reader_profile() -> None:
    # Production ships configuration in its SHA-tagged image, not a host bind mount.
    dockerfile = (REPO_ROOT / 'Dockerfile.clickhouse').read_text()
    assert 'COPY clickhouse-users.xml /etc/clickhouse-server/users.d/clickhouse-users.xml' in dockerfile
    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    build = workflow.split('      - name: Build and push ClickHouse image', 1)[1].split('      - name:', 1)[0]
    assert 'context: .' in build and 'file: ./Dockerfile.clickhouse' in build
    assert '${{ env.CLICKHOUSE_IMAGE }}' in build
    assert 'origo-clickhouse:${GITHUB_SHA}' in workflow
    clickhouse = DEPLOY_COMPOSE.read_text().split('  clickhouse:', 1)[1].split('  dagit:', 1)[0]
    assert 'image: ${CLICKHOUSE_IMAGE:?CLICKHOUSE_IMAGE is required}' in clickhouse
    assert 'CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}' in clickhouse


def test_market_state_api_is_deployed_on_loopback_with_bounded_resources() -> None:
    for filename in ('docker-compose.deploy.yml', 'docker-compose.yml'):
        text = (REPO_ROOT / filename).read_text()
        services, volumes = text.split('\nvolumes:\n', 1)
        assert services.count('\n  market-state:\n') == 1 and volumes.count('\n  market-state:') == 1
        service = services.split('\n  market-state:\n', 1)[1]
        for declaration in (
            '"127.0.0.1:8486:8486"',
            'market-state:/opt/origo/market-state',
            'source-locks:/opt/origo/locks',
            'worker-heartbeats:/opt/origo/heartbeats',
            'ORIGO_SOURCE_LOCK_DIR=/opt/origo/locks',
            'command: python -m origo.workers.market_state_api',
            'test: ["CMD", "python", "-m", "origo.workers.market_state_api", "--check"]',
            'restart: unless-stopped',
        ):
            assert declaration in service, (filename, declaration)
        dependencies = service.split('depends_on:', 1)[1].split('command:', 1)[0]
        assert 'clickhouse:\n        condition: service_healthy' in dependencies
        assert 'dagster:\n        condition: service_healthy' in dependencies
        for forbidden in ('network_mode:', 'law-samples', 'ORIGO_PROVISIONAL_SOURCE', 'docker.sock'):
            assert forbidden not in service
    deploy = DEPLOY_COMPOSE.read_text().split('\nvolumes:\n', 1)
    service = deploy[0].split('\n  market-state:\n', 1)[1]
    for declaration in (
        'image: ${APP_IMAGE:?APP_IMAGE is required}',
        'cpus: 2',
        'mem_limit: 2g',
        'read_only: true',
        'cap_drop: [ALL]',
        'security_opt: ["no-new-privileges:true"]',
        'tmpfs:\n      - /tmp',
        'interval: 60s',
        'timeout: 15s',
        'retries: 3',
        'start_period: 60s',
    ):
        assert declaration in service, declaration
    assert '  market-state:\n    # Compose-managed with a pinned name' in deploy[1]
    assert 'name: tdw-control-plane_market-state' in deploy[1]
    workflow = (REPO_ROOT / '.github/workflows/deploy_on_merge.yml').read_text()
    assert 'provisional-binance-perp-aggtrades law market-state \\' in workflow
    test_all_published_ports_bind_loopback()
