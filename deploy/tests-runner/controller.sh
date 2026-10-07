#!/usr/bin/env bash
set -euo pipefail
exec 9>/run/origo-tests-runner.lock
flock -n 9
APP_ID=$(sed -n 's/^APP_ID=//p' /etc/origo-tests-runner/app.env)
INSTALLATION_ID=$(sed -n 's/^INSTALLATION_ID=//p' /etc/origo-tests-runner/app.env)
[[ $APP_ID =~ ^[0-9]+$ && $INSTALLATION_ID =~ ^[0-9]+$ ]]
export APP_ID INSTALLATION_ID
state=/var/lib/libvirt/images/origo-tests
ssh_options=(-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=yes
    -o HostKeyAlias=origo-tests -o UserKnownHostsFile=/etc/origo-tests-runner/known_hosts
    -i /etc/origo-tests-runner/guest-key)
stop_guest() {
    if [[ $(virsh domstate origo-tests) != 'shut off' ]]; then
        virsh destroy origo-tests
    fi
}
trap stop_guest EXIT
stop_guest
# Only this disposable overlay is replaced; the controller owns the clean image.
rm -f "$state/runner.qcow2"
qemu-img create -f qcow2 -F qcow2 -b "$state/clean.qcow2" "$state/runner.qcow2" 100G
virsh start origo-tests
address=
ready=false
for ((attempt=0; attempt<120; attempt++)); do
    address=$(virsh domifaddr origo-tests | awk '/ipv4/ {sub("/.*", "", $4); print $4}')
    if [[ -n $address ]] && ssh "${ssh_options[@]}" "runner@$address" true; then
        ready=true
        break
    fi
    sleep 1
done
test "$ready" = true
registration=$(python3 /opt/origo-tests-runner/registration.py)
printf '%s\n' "$registration" | ssh "${ssh_options[@]}" "runner@$address" \
    'read -r token; cd /opt/actions-runner; ./config.sh --unattended --ephemeral --replace --url https://github.com/Vaquum/Origo --token "$token" --name origo-tests --labels origo-tests --work /opt/origo-ci/actions-work'
unset registration
ssh "${ssh_options[@]}" "runner@$address" 'cd /opt/actions-runner; ./run.sh' &
listener_pid=$!
started=0
offline=0
while kill -0 "$listener_pid" 2>/dev/null; do
    status=$(python3 /opt/origo-tests-runner/registration.py --status)
    if [[ $status == busy && $started == 0 ]]; then started=$SECONDS; fi
    if [[ $started != 0 && $((SECONDS-started)) -ge 4200 ]]; then
        echo 'Outside-guest active-job deadline reached' >&2
        exit 1
    fi
    if [[ $status == absent || ( $started != 0 && $status != busy ) ]]; then break; fi
    if [[ $status == offline ]]; then offline=$((offline+1)); else offline=0; fi
    if [[ $offline -ge 30 ]]; then echo 'Runner remained offline for five minutes' >&2; exit 1; fi
    sleep 10
done
