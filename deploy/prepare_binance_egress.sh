#!/usr/bin/env bash
set -euo pipefail

egress_source="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/60-origo-egress.yaml"
egress_root="$(mktemp -d)"
trap 'rm -rf "$egress_root"' EXIT
mkdir -p "$egress_root/etc"
cp -a /etc/netplan "$egress_root/etc/"
netplan generate --root-dir "$egress_root"
egress_network="$egress_root/run/systemd/network/10-netplan-eno1.network"
cp "$egress_network" "$egress_root/eno1.before"
install -m 600 "$egress_source" "$egress_root/etc/netplan/60-origo-egress.yaml"
netplan generate --root-dir "$egress_root"
python3 - "$egress_root/eno1.before" "$egress_network" <<'PYTHON'
import pathlib
import sys

before, after = (pathlib.Path(path).read_text().splitlines() for path in sys.argv[1:])
added = {"Address=37.27.112.140/32", "Address=37.27.112.144/32"}
if [line for line in before if line not in added] != [line for line in after if line not in added]:
    raise SystemExit("Egress configuration changes existing eno1 settings.")
addresses = [line for line in after if line.startswith("Address=")]
if not addresses or addresses[0] != "Address=37.27.112.167/32" or not added <= set(after):
    raise SystemExit("Egress configuration must retain the primary address first.")
if not any(":" in address for address in addresses) or not {
    "Destination=0.0.0.0/0", "Gateway=37.27.112.129", "Destination=::/0", "Gateway=fe80::1"
} <= set(after):
    raise SystemExit("Egress configuration must preserve both default routes and IPv6.")
PYTHON
install -m 600 "$egress_source" /etc/netplan/60-origo-egress.yaml
for egress_ip in 37.27.112.140 37.27.112.144; do
    if ! ip -o -4 addr show dev eno1 | awk '{print $4}' | grep -Fxq "$egress_ip/32"; then
        ip addr add "$egress_ip/32" dev eno1
    fi
done
if ! ip -4 route get 1.1.1.1 | grep -Eq '(^| )src 37[.]27[.]112[.]167( |$)'; then
    echo 'Default outbound source must remain 37.27.112.167.' >&2
    exit 1
fi
